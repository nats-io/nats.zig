//! Background I/O Task for NATS Client
//!
//! Async task that handles:
//! - All socket reads (fillMore)
//! - Message routing (MSG/HMSG to subscription queues)
//! - PONG responses to server PING
//! - Reconnection (including handshake writes)
//!
//! Caller context handles:
//! - PUB, SUB, UNSUB writes
//! - Client-initiated PING
//! - Flush operations
//!
//! Both contexts share the socket writer via write_mutex.
//! Runs as async task started by Client.connect().

const std = @import("std");
const posix = std.posix;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const Client = @import("../Client.zig");
const State = @import("state.zig").State;
const protocol = @import("../protocol.zig");
const dbg = @import("../dbg.zig");
const memory = @import("../memory.zig");
const TieredSlab = memory.TieredSlab;
const defaults = @import("../defaults.zig");

const Message = Client.Message;

/// Poll timeout when buffer empty (milliseconds).
/// Derived from defaults.Poll.timeout_us for configurability.
/// 0 = busy poll (from timeout_us=0), >=1 otherwise.
const POLL_TIMEOUT_MS: i32 = if (defaults.Poll.timeout_us == 0)
    0
else
    @max(1, @divFloor(defaults.Poll.timeout_us + 999, 1000));

const Io = std.Io;

/// Gets current monotonic time in nanoseconds.
fn getNowNs(io: Io) u64 {
    const ts = Io.Timestamp.now(io, .awake);
    return @intCast(ts.nanoseconds);
}

/// Drain return queue - free returned buffers back to slab.
/// Called periodically from read loop to reclaim memory.
/// Uses batch pop to reduce atomic operations from N to ceil(N/64).
inline fn drainReturnQueue(client: *Client) void {
    const slab = &client.tiered_slab;
    var batch_buf: [64][]u8 = undefined;
    while (true) {
        const count = client.return_queue.popBatch(&batch_buf);
        if (count == 0) break;
        for (batch_buf[0..count]) |buf| {
            slab.free(buf);
        }
    }
}

/// Main I/O task entry point. Called via io.async() from connect().
/// Reader: reads socket, routes MSG, responds to PING with PONG.
/// Exits cleanly when stream is closed (close then cancel).
pub fn run(client: *Client, allocator: Allocator) void {
    dbg.print("io_task[fd={d}]: STARTED", .{client.stream.socket.handle});
    var loop_count: u64 = 0;

    // Health check throttling (100ms interval to avoid hot-loop impact)
    // Use iteration counter to avoid syscall every loop (~10ms at 1M loops/sec)
    const health_check_interval_ns: u64 = 100_000_000;
    var last_health_check_ns: u64 = 0;
    var health_check_counter: u32 = 0;

    outer: while (true) {
        if (dbg.enabled) loop_count += 1;
        // HOT PATH: Exit check - intentionally non-atomic for performance.
        // Safe because of "close-then-cancel" pattern (see module doc).
        // Stale .closed read causes socket op failure, task exits anyway.
        if (client.state == .closed) break :outer;

        // Periodic health check (detects stale connections when server killed)
        // Only check timestamp every N iterations to avoid syscall overhead
        health_check_counter +%= 1;
        if (health_check_counter >= defaults.Spin.health_check_iterations) {
            health_check_counter = 0;

            const now_ns = getNowNs(client.io);
            if (now_ns - last_health_check_ns >= health_check_interval_ns) {
                last_health_check_ns = now_ns;
                if (client.checkHealthAndDetectStale()) {
                    // Connection stale - trigger disconnect/reconnect
                    const state = State.atomicLoad(&client.state);
                    if (client.options.reconnect and state != .closed) {
                        if (!handleDisconnect(client, allocator)) break :outer;
                        continue :outer;
                    }
                    // Reconnect disabled - set closed state before exiting
                    @atomicStore(State, &client.state, .closed, .release);
                    client.pushEvent(.{ .closed = {} });
                    break :outer;
                }
            }
        }

        var made_progress = true;
        while (made_progress) {
            made_progress = false;

            drainReturnQueue(client);

            const route_result = tryRouteBufferedMessages(client, allocator);
            if (route_result == .progress) {
                dbg.print("io_task[fd={d}]: routed messages", .{client.stream.socket.handle});
                made_progress = true;
            }
            if (route_result == .disconnected) {
                const state = State.atomicLoad(&client.state);
                if (client.options.reconnect and state != .closed) {
                    if (!handleDisconnect(client, allocator)) break :outer;
                    continue :outer;
                }
                // Reconnect disabled - set closed state before exiting
                @atomicStore(State, &client.state, .closed, .release);
                client.pushEvent(.{ .closed = {} });
                break :outer;
            }

            if (!made_progress) {
                const read_result = tryFillBuffer(client);
                if (read_result == .progress) {
                    dbg.print("io_task[fd={d}]: read data from socket", .{client.stream.socket.handle});
                }
                if (read_result == .canceled) break :outer;
                if (read_result == .disconnected) {
                    const state = State.atomicLoad(&client.state);
                    if (client.options.reconnect and state != .closed) {
                        if (!handleDisconnect(client, allocator)) break :outer;
                        continue :outer;
                    }
                    // Reconnect disabled - set closed state before exiting
                    @atomicStore(State, &client.state, .closed, .release);
                    client.pushEvent(.{ .closed = {} });
                    break :outer;
                }
                if (read_result == .progress) made_progress = true;
            }
        }

        // Auto-flush: check if publish() requested a flush
        // Only flush if still connected (avoid BADF on closed socket)
        if (client.flush_requested.load(.monotonic)) {
            if (client.flush_requested.swap(false, .acquire)) {
                // Atomic check before acquiring mutex
                if (State.atomicLoad(&client.state) == .connected) {
                    client.write_mutex.lock(client.io) catch continue :outer;
                    defer client.write_mutex.unlock(client.io);
                    // Double-check after mutex
                    if (State.atomicLoad(&client.state) == .connected) {
                        client.active_writer.flush() catch {};
                        // TLS: flush underlying TCP writer too
                        if (client.use_tls) {
                            client.writer.interface.flush() catch {};
                        }
                    }
                }
            }
        }

        // No progress - yield to allow other threads to run
        std.Thread.yield() catch {};
    }
    if (dbg.enabled) {
        const stats = &client.io_task_stats;
        dbg.print(
            "io_task: EXITED loops={d} fill_calls={d} buffered_hits={d} " ++
                "poll_timeouts={d} read_ok={d}",
            .{
                loop_count,
                stats.fill_calls,
                stats.fill_buffered_hits,
                stats.fill_poll_timeouts,
                stats.fill_read_success,
            },
        );
    }
}

/// Result of read/route operations.
const ReadResult = enum {
    progress,
    no_progress,
    disconnected,
    canceled,
};

/// Result of poll() operation for disconnect detection.
const PollResult = enum {
    has_data,
    no_data,
    disconnected,
};

/// Poll socket for readable data with timeout (cross-platform).
/// Also detects disconnect via POLLHUP/POLLERR.
/// On Linux, POLLIN and POLLHUP can both be set when there's
/// buffered data AND the connection is closing. POLLHUP is prioritized
/// to detect dead connections even with buffered data.
inline fn pollForData(fd: posix.fd_t, timeout_ms: i32) PollResult {
    var fds = [_]posix.pollfd{.{
        .fd = fd,
        .events = posix.POLL.IN,
        .revents = 0,
    }};
    const ready = posix.poll(&fds, timeout_ms) catch return .no_data;
    if (ready == 0) return .no_data;

    // Single load, combined checks (avoid 3 separate loads)
    const revents = fds[0].revents;
    // POLLHUP/POLLERR means connection is dead - even if POLLIN is also set
    if ((revents & (posix.POLL.HUP | posix.POLL.ERR)) != 0)
        return .disconnected;

    if ((revents & posix.POLL.IN) != 0) return .has_data;
    return .no_data;
}

/// Try to fill buffer without blocking forever.
/// Uses poll() to check for data, then fillMore() to read.
/// For TLS: loops until we get decrypted data or no more TCP data available.
/// This handles TLS record fragmentation where a record spans multiple TCP segments.
inline fn tryFillBuffer(client: *Client) ReadResult {
    if (dbg.enabled) client.io_task_stats.fill_calls += 1;
    // HOT PATH: Non-atomic read - see module doc "State checks (hot path)"
    if (client.state == .closed) return .canceled;

    const reader = client.active_reader;
    const fd = client.stream.socket.handle;
    const before = reader.buffered().len;
    if (dbg.enabled) client.io_task_stats.fill_buffered_hits += before;

    // TLS: loop until we decrypt data or truly no more data available.
    // Key insight: encrypted data may be in TCP reader's buffer (not socket).
    // After TLS decrypts one record, more encrypted records may remain in the
    // TCP buffer. poll() only sees the socket, not the TCP reader's buffer!
    // So we must: 1) check TCP buffer first, 2) only poll if TCP buffer empty.
    if (client.use_tls) {
        while (true) {
            // Atomic read: race with deinit closing socket before fillMore()
            if (State.atomicLoad(&client.state) == .closed) return .canceled;

            // Check if TCP reader has buffered encrypted data (poll can't see this)
            const tcp_buffered = client.reader.interface.buffered().len;

            if (tcp_buffered == 0) {
                // TCP buffer empty - poll socket for more encrypted data
                const poll_result = pollForData(fd, POLL_TIMEOUT_MS);
                if (poll_result == .disconnected) return .disconnected;
                if (poll_result == .no_data) {
                    if (dbg.enabled)
                        client.io_task_stats.fill_poll_timeouts += 1;
                    // No more data anywhere - return what we have
                    return if (reader.buffered().len > before)
                        .progress
                    else
                        .no_progress;
                }
            }
            // Either TCP buffer has data, or poll said socket has data

            reader.fillMore() catch |err| {
                if (err == error.Canceled) return .canceled;
                if (err == error.EndOfStream or
                    err == error.ConnectionResetByPeer or
                    err == error.BrokenPipe or
                    err == error.NotOpenForReading)
                {
                    return .disconnected;
                }
                // Other errors: check if we made progress before failing
                return if (reader.buffered().len > before)
                    .progress
                else
                    .no_progress;
            };

            // Got decrypted data - success
            if (reader.buffered().len > before) {
                if (dbg.enabled) client.io_task_stats.fill_read_success += 1;
                return .progress;
            }
            // No decrypted data yet - TLS needs more data, loop
        }
    }

    // Non-TLS: simple poll + read
    const poll_result = pollForData(fd, POLL_TIMEOUT_MS);

    if (poll_result == .disconnected) {
        return .disconnected;
    }
    if (poll_result == .no_data) {
        if (dbg.enabled) client.io_task_stats.fill_poll_timeouts += 1;
        return .no_progress;
    }

    // Atomic read: race with deinit closing socket before fillMore()
    if (State.atomicLoad(&client.state) == .closed) return .canceled;

    // Socket has data -> fillMore() will return immediately
    reader.fillMore() catch |err| {
        if (err == error.Canceled) return .canceled;
        if (err == error.EndOfStream or
            err == error.ConnectionResetByPeer or
            err == error.BrokenPipe or
            err == error.NotOpenForReading)
        {
            return .disconnected;
        }
        return .no_progress;
    };

    const after = reader.buffered().len;
    if (after > before) {
        if (dbg.enabled) client.io_task_stats.fill_read_success += 1;
        return .progress;
    }
    return .no_progress;
}

/// Route buffered messages (no I/O, buffer processing only).
/// Handles: MSG -> route to queue, PING -> write PONG.
/// Uses lock-free SpscQueue - no yields needed.
inline fn tryRouteBufferedMessages(
    client: *Client,
    allocator: Allocator,
) ReadResult {
    const reader = client.active_reader;
    const slab = &client.tiered_slab;

    // HOT PATH: Non-atomic read - see module doc "State checks (hot path)"
    if (client.state == .closed) return .canceled;

    const data = reader.buffered();
    if (data.len == 0) return .no_progress;

    var offset: usize = 0;
    while (offset < data.len) {
        var consumed: usize = 0;
        const result = client.parser.parse(
            allocator,
            data[offset..],
            &consumed,
        ) catch {
            // Scan to next CRLF for recovery (skip corrupted data)
            // Uses SIMD on supported platforms
            if (std.mem.indexOf(u8, data[offset..], "\r\n")) |crlf_pos| {
                const bytes_skipped = crlf_pos + 2;
                offset += bytes_skipped;

                // Track and rate-limit protocol error notifications
                client.protocol_errors += 1;
                const msgs_since = client.stats.msgs_in -|
                    client.last_parse_error_notified_at;
                const interval = client.options.error_notify_interval_msgs;
                if (client.protocol_errors == 1 or msgs_since >= interval) {
                    client.last_parse_error_notified_at = client.stats.msgs_in;
                    client.pushEvent(.{
                        .protocol_error = .{
                            .bytes_skipped = bytes_skipped,
                            .count = client.protocol_errors,
                        },
                    });
                }
                dbg.print(
                    "parse error (#{d}, skipped {d} bytes, rate-limited)",
                    .{ client.protocol_errors, bytes_skipped },
                );
            } else {
                break;
            }
            continue;
        };

        if (result) |cmd| {
            switch (cmd) {
                .msg => |args| {
                    routeMessageToSub(client, slab, args);
                    client.stats.msgs_in += 1;
                    client.stats.bytes_in += args.payload.len;
                },
                .hmsg => |args| {
                    routeHMessageToSub(client, slab, args);
                    client.stats.msgs_in += 1;
                    client.stats.bytes_in += args.total_len;
                },
                .ping => {
                    client.write_mutex.lock(client.io) catch
                        return .disconnected;
                    defer client.write_mutex.unlock(client.io);
                    client.active_writer.writeAll("PONG\r\n") catch {
                        return .disconnected;
                    };
                    client.active_writer.flush() catch return .disconnected;
                },
                .pong => {
                    const now = getNowNs(client.io);
                    dbg.print("Got PONG, storing timestamp={d}", .{now});
                    client.pings_outstanding.store(0, .monotonic);
                    client.last_pong_received_ns.store(now, .release);
                },
                .info => |info| {
                    if (client.server_info) |*old| {
                        old.deinit(allocator);
                    }
                    client.server_info = info;
                    client.max_payload = info.max_payload;
                },
                .ok => {},
                .err => |err_msg| {
                    if (handleServerError(client, err_msg)) {
                        return .disconnected;
                    }
                },
            }
            offset += consumed;
        } else {
            break;
        }
    }

    if (offset > 0) {
        reader.toss(offset);
        return .progress;
    }

    return .no_progress;
}

/// Route MSG to subscription queue.
inline fn routeMessageToSub(
    client: *Client,
    slab: *TieredSlab,
    args: protocol.MsgArgs,
) void {
    dbg.print("routeMsg[fd={d}]: sid={d} subject={s}", .{ client.stream.socket.handle, args.sid, args.subject });
    const sub = client.getSubscriptionBySid(args.sid) orelse {
        dbg.print("routeMsg[fd={d}]: NO SUB FOUND for sid={d}", .{ client.stream.socket.handle, args.sid });
        return;
    };

    const subj_len = args.subject.len;
    const payload_len = args.payload.len;
    const reply_len = if (args.reply_to) |rt| rt.len else 0;
    const total_size = subj_len + payload_len + reply_len;

    // Bounds verification - assert our arithmetic is correct
    const subj_end = subj_len;
    const payload_end = subj_end + payload_len;
    const reply_end = payload_end + reply_len;
    assert(reply_end == total_size);

    const buf = slab.alloc(total_size) orelse {
        sub.alloc_failed_msgs += 1;
        // Rate-limit: push event on 1st failure OR after interval msgs
        const msgs_since = client.stats.msgs_in -| sub.last_alloc_notified_at;
        const interval = client.options.error_notify_interval_msgs;
        if (sub.alloc_failed_msgs == 1 or msgs_since >= interval) {
            sub.last_alloc_notified_at = client.stats.msgs_in;
            client.pushEvent(.{
                .alloc_failed = .{
                    .sid = args.sid,
                    .count = sub.alloc_failed_msgs,
                },
            });
        }
        dbg.print(
            "alloc failed sid={d} (#{d}, rate-limited every {d} msgs)",
            .{ args.sid, sub.alloc_failed_msgs, interval },
        );
        return;
    };

    @memcpy(buf[0..subj_end], args.subject);
    @memcpy(buf[subj_end..payload_end], args.payload);
    if (args.reply_to) |rt| {
        @memcpy(buf[payload_end..reply_end], rt);
    }

    const subject = buf[0..subj_end];
    const data_slice = buf[subj_end..payload_end];
    const reply_to: ?[]const u8 = if (reply_len > 0)
        buf[payload_end..reply_end]
    else
        null;

    const msg = Message{
        .subject = subject,
        .sid = args.sid,
        .reply_to = reply_to,
        .data = data_slice,
        .headers = null,
        .owned = true,
        .backing_buf = buf,
        .return_queue = &client.return_queue,
    };

    sub.pushMessage(msg) catch {
        dbg.print("routeMsg: PUSH FAILED (slow consumer) sid={d}", .{args.sid});
        sub.dropped_msgs += 1;
        slab.free(buf);
        if (sub.dropped_msgs == 1) {
            client.pushEvent(.{ .slow_consumer = .{ .sid = args.sid } });
        }
        return;
    };
    dbg.print("routeMsg: pushed to queue, sid={d}", .{args.sid});
    sub.received_msgs += 1;
}

/// Route HMSG to subscription queue.
inline fn routeHMessageToSub(
    client: *Client,
    slab: *TieredSlab,
    args: protocol.HMsgArgs,
) void {
    const sub = client.getSubscriptionBySid(args.sid) orelse return;

    const subj_len = args.subject.len;
    const data_len = args.payload.len;
    const hdr_len = args.headers.len;
    const reply_len = if (args.reply_to) |rt| rt.len else 0;
    const total_size = subj_len + data_len + hdr_len + reply_len;

    // Bounds verification - assert our arithmetic is correct
    const subj_end = subj_len;
    const data_end = subj_end + data_len;
    const hdr_end = data_end + hdr_len;
    const reply_end = hdr_end + reply_len;
    assert(reply_end == total_size);

    const buf = slab.alloc(total_size) orelse {
        sub.alloc_failed_msgs += 1;
        // Rate-limit: push event on 1st failure OR after interval msgs
        const msgs_since = client.stats.msgs_in -| sub.last_alloc_notified_at;
        const interval = client.options.error_notify_interval_msgs;
        if (sub.alloc_failed_msgs == 1 or msgs_since >= interval) {
            sub.last_alloc_notified_at = client.stats.msgs_in;
            client.pushEvent(.{
                .alloc_failed = .{
                    .sid = args.sid,
                    .count = sub.alloc_failed_msgs,
                },
            });
        }
        dbg.print(
            "alloc failed sid={d} (#{d}, rate-limited every {d} msgs)",
            .{ args.sid, sub.alloc_failed_msgs, interval },
        );
        return;
    };

    @memcpy(buf[0..subj_end], args.subject);
    @memcpy(buf[subj_end..data_end], args.payload);
    @memcpy(buf[data_end..hdr_end], args.headers);
    if (args.reply_to) |rt| {
        @memcpy(buf[hdr_end..reply_end], rt);
    }

    const subject = buf[0..subj_end];
    const data_slice = buf[subj_end..data_end];
    const headers = buf[data_end..hdr_end];
    const reply_to: ?[]const u8 = if (reply_len > 0)
        buf[hdr_end..reply_end]
    else
        null;

    const msg = Message{
        .subject = subject,
        .sid = args.sid,
        .reply_to = reply_to,
        .data = data_slice,
        .headers = headers,
        .owned = true,
        .backing_buf = buf,
        .return_queue = &client.return_queue,
    };

    sub.pushMessage(msg) catch {
        sub.dropped_msgs += 1;
        slab.free(buf);
        if (sub.dropped_msgs == 1) {
            client.pushEvent(.{ .slow_consumer = .{ .sid = args.sid } });
        }
    };
    sub.received_msgs += 1;
}

/// Handle disconnect - backup subs, attempt reconnection, restore subs.
/// Returns true if reconnected successfully, false if should exit task.
fn handleDisconnect(client: *Client, allocator: Allocator) bool {
    @atomicStore(State, &client.state, .disconnected, .release);

    client.pushEvent(.{ .disconnected = .{ .err = null } });

    client.backupSubscriptions() catch |err| {
        dbg.print("backupSubscriptions failed: {s}", .{@errorName(err)});
    };

    if (tryReconnectLoop(client, allocator)) {
        client.restoreSubscriptions() catch {
            dbg.print("Failed to restore subscriptions after reconnect", .{});
        };

        client.pushEvent(.{ .reconnected = {} });
        return true;
    } else {
        @atomicStore(State, &client.state, .closed, .release);
        client.pushEvent(.{ .closed = {} });
        return false;
    }
}

/// Attempt reconnection loop with backoff.
/// Returns true if reconnected, false if failed or canceled.
fn tryReconnectLoop(client: *Client, allocator: Allocator) bool {
    @atomicStore(State, &client.state, .reconnecting, .release);
    const max_attempts = if (client.options.max_reconnect_attempts == 0)
        std.math.maxInt(u32)
    else
        client.options.max_reconnect_attempts;

    var attempt: u32 = 0;
    while (attempt < max_attempts) {
        attempt += 1;
        client.reconnect_attempt = attempt;

        // Wait with backoff (except first attempt) - cancellation point
        if (attempt > 1) {
            const delay_ms = calculateReconnectDelay(client, attempt);
            client.io.sleep(
                .fromMilliseconds(delay_ms),
                .awake,
            ) catch |err| {
                if (err == error.Canceled) return false;
            };
        }

        for (client.server_pool.servers[0..client.server_pool.count]) |*server| {
            client.tryConnect(allocator, server) catch continue;
            @atomicStore(State, &client.state, .connected, .release);
            client.stats.reconnects += 1;
            client.reconnect_attempt = 0;
            return true;
        }
    }

    return false;
}

/// Calculate reconnect delay with exponential backoff and jitter.
/// If custom_reconnect_delay callback is set, uses that instead.
fn calculateReconnectDelay(client: *Client, attempt: u32) u32 {
    assert(attempt > 0);

    // Use custom callback if provided
    if (client.options.custom_reconnect_delay) |cb| {
        return cb(attempt);
    }

    // Exponential backoff: base * 2^(attempt-1), capped at max
    const base_ms = client.options.reconnect_wait_ms;
    const max_ms = client.options.reconnect_wait_max_ms;
    const jitter_pct = client.options.reconnect_jitter_percent;

    // Calculate exponential delay: base * 2^(attempt-2) for attempt > 1
    // attempt 2 -> base, attempt 3 -> base*2, attempt 4 -> base*4, etc.
    const shift: u5 = @intCast(@min(attempt -| 2, 30));
    const exp_delay: u64 = @as(u64, base_ms) << shift;
    const capped_delay: u32 = @intCast(@min(exp_delay, max_ms));

    // Apply jitter: delay +/- jitter_pct%
    if (jitter_pct == 0) return capped_delay;

    // Simple jitter using attempt number as pseudo-random seed
    // Real randomness would require io.random() but we keep it simple
    const jitter_range = (capped_delay * jitter_pct) / 100;
    const jitter_offset = (attempt * 7) % (jitter_range * 2 + 1);
    const jitter: i64 = @as(i64, jitter_offset) - @as(i64, jitter_range);

    const final_delay: i64 = @as(i64, capped_delay) + jitter;
    return @intCast(@max(final_delay, 1));
}

/// Handle server -ERR message. Categorizes error and pushes event.
/// Also stores as last_error for later retrieval via getLastError().
/// Returns true if error is fatal (should disconnect), false otherwise.
fn handleServerError(client: *Client, msg: []const u8) bool {
    const events = @import("../events.zig");

    // Categorize error (case-insensitive matching like Go/C clients)
    const err_type: anyerror = blk: {
        if (containsIgnoreCase(msg, "authorization")) {
            break :blk events.Error.AuthorizationViolation;
        }
        if (containsIgnoreCase(msg, "permissions violation")) {
            break :blk events.Error.PermissionViolation;
        }
        if (containsIgnoreCase(msg, "stale connection")) {
            break :blk events.Error.StaleConnection;
        }
        if (containsIgnoreCase(msg, "maximum connections")) {
            break :blk events.Error.MaxConnectionsExceeded;
        }
        break :blk events.Error.ServerError;
    };

    // Store as last_error for retrieval via getLastError()
    client.last_error = err_type;
    if (msg.len <= 256) {
        const len: u8 = @intCast(msg.len);
        @memcpy(client.last_error_msg[0..len], msg);
        client.last_error_msg_len = len;
    } else {
        // Truncate if message too long
        @memcpy(&client.last_error_msg, msg[0..256]);
        client.last_error_msg_len = 255;
    }

    client.pushEvent(.{ .err = .{ .err = err_type, .msg = msg } });

    // Fatal errors trigger disconnect/reconnect
    return err_type == events.Error.AuthorizationViolation or
        err_type == events.Error.StaleConnection or
        err_type == events.Error.MaxConnectionsExceeded;
}

/// Case-insensitive substring search (no allocations).
fn containsIgnoreCase(haystack: []const u8, needle: []const u8) bool {
    if (needle.len > haystack.len) return false;
    var i: usize = 0;
    while (i <= haystack.len - needle.len) : (i += 1) {
        var match = true;
        for (0..needle.len) |j| {
            const h = haystack[i + j];
            const n = needle[j];
            const hl = if (h >= 'A' and h <= 'Z') h + 32 else h;
            const nl = if (n >= 'A' and n <= 'Z') n + 32 else n;
            if (hl != nl) {
                match = false;
                break;
            }
        }
        if (match) return true;
    }
    return false;
}
