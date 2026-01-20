//! Background I/O Task for NATS Client
//!
//! Pure reader task: reads from socket, routes messages, responds to PING.
//! All writes (PUB, SUB, flush) happen in user thread.
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

const Message = Client.Message;

/// Poll timeout when buffer empty (milliseconds).
/// Use 1ms timeout - kernel wait acts as yield point.
const POLL_TIMEOUT_MS: i32 = 1;

/// Gets current time in nanoseconds.
fn getNowNs() error{TimerUnavailable}!u64 {
    const instant = std.time.Instant.now() catch return error.TimerUnavailable;
    const secs: u64 = @intCast(instant.timestamp.sec);
    const nsecs: u64 = @intCast(instant.timestamp.nsec);
    return secs * std.time.ns_per_s + nsecs;
}

/// Drain return queue - free returned buffers back to slab.
/// Called periodically from read loop to reclaim memory.
inline fn drainReturnQueue(client: *Client) void {
    const slab = &client.tiered_slab;
    while (client.return_queue.pop()) |buf| {
        slab.free(buf);
    }
}

/// Main I/O task entry point. Called via io.async() from connect().
/// Pure reader: reads socket, routes MSG, responds to PING with PONG.
/// Exits cleanly when stream is closed (close then cancel).
pub fn run(client: *Client, allocator: Allocator) void {
    dbg.print("io_task: STARTED", .{});
    var loop_count: u64 = 0;

    // Health check throttling (100ms interval to avoid hot-loop impact)
    // Use iteration counter to avoid syscall every loop (~10ms at 1M loops/sec)
    const health_check_interval_ns: u64 = 100_000_000;
    var last_health_check_ns: u64 = 0;
    var health_check_counter: u32 = 0;
    const health_check_iterations: u32 = 10000;

    outer: while (true) {
        if (dbg.enabled) loop_count += 1;
        // Exit immediately if client is closing
        // Non-atomic read OK - close-then-cancel pattern ensures exit via
        // stream error or cancellation even if we see stale value briefly
        if (client.state == .closed) break :outer;

        // Periodic health check (detects stale connections when server killed)
        // Only check timestamp every N iterations to avoid syscall overhead
        health_check_counter +%= 1;
        if (health_check_counter >= health_check_iterations) {
            health_check_counter = 0;

            const now_ns = getNowNs() catch 0;
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

        // Made-progress loop: process all available data without blocking
        var made_progress = true;
        while (made_progress) {
            made_progress = false;

            // Drain return queue each iteration (prevents queue overflow)
            drainReturnQueue(client);

            // Route buffered messages (no I/O)
            const route_result = tryRouteBufferedMessages(client, allocator);
            if (route_result == .progress) made_progress = true;
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

            // Only read if routing made no progress (empty/partial)
            if (!made_progress) {
                const read_result = tryFillBuffer(client);
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

        // No progress - ALWAYS yield to allow other tasks to run
        // This is CRITICAL for async() mode where cooperative yielding is needed
        client.io.sleep(.fromNanoseconds(0), .awake) catch |err| {
            if (err == error.Canceled) break :outer;
        };
    }
    if (dbg.enabled) {
        dbg.print(
            "io_task: EXITED loops={d} fill_calls={d} buffered_hits={d} " ++
                "poll_timeouts={d} read_ok={d}",
            .{
                loop_count,
                fill_calls,
                fill_buffered_hits,
                fill_poll_timeouts,
                fill_read_success,
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
/// NOTE: On Linux, POLLIN and POLLHUP can both be set when there's
/// buffered data AND the connection is closing. We prioritize POLLHUP
/// to detect dead connections even with buffered data.
fn pollForData(fd: posix.fd_t, timeout_ms: i32) PollResult {
    var fds = [_]posix.pollfd{.{
        .fd = fd,
        .events = posix.POLL.IN,
        .revents = 0,
    }};
    const ready = posix.poll(&fds, timeout_ms) catch return .no_data;
    if (ready == 0) return .no_data; // Timeout

    const has_hup = (fds[0].revents & posix.POLL.HUP) != 0;
    const has_err = (fds[0].revents & posix.POLL.ERR) != 0;
    const has_in = (fds[0].revents & posix.POLL.IN) != 0;

    // POLLHUP/POLLERR means connection is dead - even if POLLIN is also set
    // (buffered data from before server died), we should disconnect
    if (has_hup or has_err) {
        return .disconnected;
    }

    // Only return has_data if no hangup/error
    if (has_in) return .has_data;

    return .no_data;
}

/// Debug counters for tryFillBuffer
var fill_calls: u64 = 0;
var fill_buffered_hits: u64 = 0;
var fill_poll_timeouts: u64 = 0;
var fill_read_success: u64 = 0;

/// Try to fill buffer without blocking forever (cross-platform).
/// Uses poll() to check for data, then fillMore() to read.
fn tryFillBuffer(client: *Client) ReadResult {
    if (dbg.enabled) fill_calls += 1;
    // Non-atomic read OK - just for faster exit, stream close handles it
    if (client.state == .closed) return .canceled;

    const reader = &client.reader.interface;

    // Cross-platform poll to check if socket has data or disconnect
    const fd = client.stream.socket.handle;
    const poll_result = pollForData(fd, POLL_TIMEOUT_MS);

    if (poll_result == .disconnected) {
        return .disconnected; // POLLHUP/POLLERR - server killed
    }
    if (poll_result == .no_data) {
        if (dbg.enabled) fill_poll_timeouts += 1;
        return .no_progress; // Timeout or no data
    }

    // Track buffer size before read
    const before = reader.buffered().len;
    if (dbg.enabled) fill_buffered_hits += before;

    // Re-check state before read (race with deinit closing socket)
    // Non-atomic read OK - just for faster exit
    if (client.state == .closed) return .canceled;

    // Socket has data → fillMore() will return immediately
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

    // Only report progress if we actually read new data
    const after = reader.buffered().len;
    if (after > before) {
        if (dbg.enabled) fill_read_success += 1;
        return .progress;
    }
    return .no_progress;
}

/// Route buffered messages (NO I/O - just process buffer).
/// Handles: MSG → route to queue, PING → write PONG.
/// Uses lock-free SpscQueue - no yields needed.
fn tryRouteBufferedMessages(client: *Client, allocator: Allocator) ReadResult {
    const reader = &client.reader.interface;
    const slab = &client.tiered_slab;

    // Non-atomic read OK - just for faster exit, stream close handles it
    if (client.state == .closed) return .canceled;

    // Check what's already buffered (no I/O)
    const data = reader.buffered();
    if (data.len == 0) return .no_progress;

    // Parse and route messages
    var offset: usize = 0;
    while (offset < data.len) {
        var consumed: usize = 0;
        const result = client.parser.parse(
            allocator,
            data[offset..],
            &consumed,
        ) catch {
            offset += 1;
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
                    // Respond to server PING with PONG (with mutex)
                    client.write_mutex.lock(client.io) catch {};
                    defer client.write_mutex.unlock(client.io);
                    client.writer.interface.writeAll("PONG\r\n") catch {};
                    client.writer.interface.flush() catch {};
                },
                .pong => {
                    // Server responded to our PING (keepalive)
                    dbg.print("Got PONG, resetting pings_outstanding", .{});
                    client.pings_outstanding = 0;
                    client.last_pong_received_ns = getNowNs() catch 0;
                },
                .info => |info| {
                    if (client.server_info) |*old| {
                        old.deinit(allocator);
                    }
                    client.server_info = info;
                    client.max_payload = info.max_payload;
                    // TODO: Check for lame duck mode when ServerInfo parses ldm
                    // if (info.lame_duck_mode and !client.lame_duck_notified) {
                    //     client.lame_duck_notified = true;
                    //     client.pushEvent(.{ .lame_duck = {} });
                    // }
                },
                .ok => {},
                .err => {},
            }
            offset += consumed;
        } else {
            break;
        }
    }

    // Toss consumed data
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
    const sub = client.getSubscriptionBySid(args.sid) orelse return;

    // Allocate message with backing buffer (direct slab call, no vtable)
    const total_size = args.subject.len + args.payload.len +
        (if (args.reply_to) |rt| rt.len else 0);
    const buf = slab.alloc(total_size) orelse {
        sub.alloc_failed_msgs += 1;
        return;
    };

    // Copy data into backing buffer
    var offset: usize = 0;
    @memcpy(buf[offset..][0..args.subject.len], args.subject);
    const subject = buf[offset..][0..args.subject.len];
    offset += args.subject.len;

    @memcpy(buf[offset..][0..args.payload.len], args.payload);
    const data_slice = buf[offset..][0..args.payload.len];
    offset += args.payload.len;

    const reply_to: ?[]const u8 = if (args.reply_to) |rt| blk: {
        @memcpy(buf[offset..][0..rt.len], rt);
        break :blk buf[offset..][0..rt.len];
    } else null;

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

    // Push to subscription queue
    sub.pushMessage(msg) catch {
        sub.dropped_msgs += 1;
        slab.free(buf);
        // Push slow_consumer event (only on first drop to avoid flood)
        if (sub.dropped_msgs == 1) {
            client.pushEvent(.{ .slow_consumer = .{ .sid = args.sid } });
        }
    };
    sub.received_msgs += 1;
}

/// Route HMSG to subscription queue.
inline fn routeHMessageToSub(
    client: *Client,
    slab: *TieredSlab,
    args: protocol.HMsgArgs,
) void {
    const sub = client.getSubscriptionBySid(args.sid) orelse return;
    const payload_len = args.total_len - args.header_len;

    // Allocate message with backing buffer (direct slab call, no vtable)
    const total_size = args.subject.len + payload_len + args.header_len +
        (if (args.reply_to) |rt| rt.len else 0);
    const buf = slab.alloc(total_size) orelse {
        sub.alloc_failed_msgs += 1;
        return;
    };

    // Copy data into backing buffer
    var offset: usize = 0;
    @memcpy(buf[offset..][0..args.subject.len], args.subject);
    const subject = buf[offset..][0..args.subject.len];
    offset += args.subject.len;

    @memcpy(buf[offset..][0..args.payload.len], args.payload);
    const data_slice = buf[offset..][0..args.payload.len];
    offset += args.payload.len;

    @memcpy(buf[offset..][0..args.headers.len], args.headers);
    const headers = buf[offset..][0..args.headers.len];
    offset += args.headers.len;

    const reply_to: ?[]const u8 = if (args.reply_to) |rt| blk: {
        @memcpy(buf[offset..][0..rt.len], rt);
        break :blk buf[offset..][0..rt.len];
    } else null;

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

    // Push to subscription queue
    sub.pushMessage(msg) catch {
        sub.dropped_msgs += 1;
        slab.free(buf);
        // Push slow_consumer event (only on first drop to avoid flood)
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

    // Push disconnected event (no specific error captured at this level)
    client.pushEvent(.{ .disconnected = .{ .err = null } });

    // Backup subscriptions before reconnect
    client.backupSubscriptions();

    // Try reconnection
    if (tryReconnectLoop(client, allocator)) {
        // Restore subscriptions after successful reconnect
        client.restoreSubscriptions() catch {
            dbg.print("Failed to restore subscriptions after reconnect", .{});
        };

        // Push reconnected event
        client.pushEvent(.{ .reconnected = {} });
        return true;
    } else {
        // Reconnection failed or canceled
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
            client.io.sleep(
                .fromMilliseconds(client.options.reconnect_wait_ms),
                .awake,
            ) catch |err| {
                if (err == error.Canceled) return false;
            };
        }

        // Try each server in pool
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
