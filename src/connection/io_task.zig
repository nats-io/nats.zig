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
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const Client = @import("../Client.zig");
const State = @import("state.zig").State;
const protocol = @import("../protocol.zig");
const dbg = @import("../dbg.zig");
const memory = @import("../memory.zig");
const TieredSlab = memory.TieredSlab;

const Message = Client.Message;

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

/// Reader unit. Blocks on the socket via `fillMore` (evented on zio, a
/// blocking recv on Threaded -- no poll(2)), parses and routes MSG/HMSG into the
/// per-sub queues, flags server PINGs for the writer, records PONGs, and owns the
/// reconnect state machine. Takes no `write_mutex` in steady state, so a
/// backpressured writer never stalls inbound delivery.
pub fn readerRun(client: *Client) void {
    dbg.print("reader[fd={d}]: STARTED", .{client.stream.socket.handle});

    reader: while (true) {
        // Always-on iteration counter (idle-CPU regression guard for tests).
        _ = client.io_task_loops.fetchAdd(1, .monotonic);
        // Close-then-cancel: teardown sets .closed then cancels this future;
        // the top-of-loop check makes the reader exit promptly once unblocked.
        if (client.state == .closed) break :reader;

        // Route complete frames already sitting in the read buffer.
        switch (tryRouteBufferedMessages(client)) {
            .disconnected => {
                if (handleTransportLoss(client)) break :reader;
                continue :reader;
            },
            else => {},
        }

        // Reclaim freed POOL buffers. The reader is the sole owner of the tier
        // free lists (it allocs on route and frees here), so pool frees stay
        // race-free. Fallback (slab-overflow) buffers do NOT come through here --
        // producers free those directly (Message.deinit -> slab.freeFallback),
        // which is why the fixed-size return queue can never overflow.
        drainReturnQueue(client);

        // Block for more socket data.
        switch (tryFillBlocking(client)) {
            .canceled => break :reader,
            .disconnected => {
                if (handleTransportLoss(client)) break :reader;
                continue :reader;
            },
            .progress, .no_progress => continue :reader,
        }
    }
    dbg.print("reader: EXITED", .{});
}

/// Writer unit. Parks on the writer eventcount with a timeout of "time to
/// the next PING", so keepalive folds in (no third unit). On each wake, under
/// `write_mutex`: emit any pending PONGs, drain the publish ring frame by frame,
/// flush both layers, then do the time-gated health/PING step. A write error or
/// a stale connection is handed to the reader (which owns teardown/reconnect) via
/// `transport_failed` + `shutdownRecvIfOpen`; the writer never writes `state`.
pub fn writerRun(client: *Client) void {
    dbg.print("writer: STARTED", .{});
    const ping_ms = client.options.ping_interval_ms;

    writer: while (true) {
        if (client.state == .closed) break :writer;

        // Snapshot the wake seq BEFORE doing work / re-checking, so a producer
        // that bumps it during this iteration is not lost (we re-check work
        // below and, failing that, park on this observed value).
        const observed = client.wake_seq.load(.seq_cst);

        if (State.atomicLoad(&client.state) == .connected and
            !client.transport_failed.load(.acquire))
        {
            drainWriter(client);
            // Time-gated client PING + stale detection (folded into the writer).
            if (client.checkHealthAndDetectStale()) {
                // Stale: hand off to the reader. The writer never writes state.
                client.transport_failed.store(true, .release);
                client.shutdownRecvIfOpen();
            }
        }

        if (client.state == .closed) break :writer;

        // Re-check for outstanding work before parking (lost-wakeup safety).
        if (State.atomicLoad(&client.state) == .connected and
            !client.transport_failed.load(.acquire) and
            (!client.publish_ring.isEmpty() or
                client.pong_pending.load(.monotonic) != 0 or
                client.flush_requested.load(.monotonic)))
        {
            continue :writer;
        }

        // Park until woken or the next PING is due. deadline 0 = untimed
        // (keepalive disabled): never a busy 0-timeout loop.
        //
        // The keepalive deadline only applies while connected: only then does
        // the health step above run and advance `last_ping_sent_ns`. While
        // disconnected/reconnecting that step is skipped, so a ping-based
        // deadline would stay pinned in the past and `parkWriter` would return
        // at once every iteration -- a busy spin. On a single-threaded executor
        // that spin starves the reader fiber that owns the reconnect loop, so
        // its backoff sleep never fires and reconnect stalls. When not
        // connected there is nothing to ping: park untimed until the reader
        // signals the writer (it calls `signalWriter` after a reconnect, and
        // teardown cancels this future), so the executor is free for the reader.
        var deadline_ns: u64 = 0;
        if (ping_ms != 0 and
            State.atomicLoad(&client.state) == .connected and
            !client.transport_failed.load(.acquire))
        {
            const last_ping = client.last_ping_sent_ns.load(.monotonic);
            deadline_ns = last_ping +| @as(u64, ping_ms) * 1_000_000;
        }
        client.parkWriter(observed, deadline_ns);
    }
    dbg.print("writer: EXITED", .{});
}

/// Blocking read for the reader unit: fills the reader buffer via `fillMore`
/// (no poll). On teardown the reader future is cancelled and/or the recv side is
/// shut down, so `fillMore` returns Canceled / EndOfStream promptly.
fn tryFillBlocking(client: *Client) ReadResult {
    if (State.atomicLoad(&client.state) == .closed) return .canceled;
    const reader = client.active_reader;
    const before = reader.buffered().len;
    reader.fillMore() catch |err| {
        if (err == error.Canceled) return .canceled;
        if (err == error.EndOfStream or
            err == error.ConnectionResetByPeer or
            err == error.BrokenPipe or
            err == error.NotOpenForReading)
        {
            return .disconnected;
        }
        // Any other read failure is transport loss too (e.g. a cancelled recv
        // surfaces as ReadFailed); the top-of-loop .closed check handles a
        // teardown, otherwise the reader reconnects.
        return .disconnected;
    };
    return if (reader.buffered().len > before) .progress else .no_progress;
}

/// Writer half of the drain: emit pending PONGs, drain the publish ring frame by
/// frame (advance only after a whole frame's writeAll so an error leaves the
/// frame intact for re-send), flush both layers, and on a write error hand off
/// to the reader. Runs under `write_mutex`.
fn drainWriter(client: *Client) void {
    const gen = client.conn_generation.load(.acquire);
    client.write_mutex.lock(client.io) catch return;

    if (State.atomicLoad(&client.state) != .connected or
        client.transport_failed.load(.acquire))
    {
        client.write_mutex.unlock(client.io);
        return;
    }

    var write_err = false;

    // Pending PONGs first: one per server PING, so the server's ping accounting
    // stays correct. Do not coalesce.
    const npong = client.pong_pending.swap(0, .acq_rel);
    var i: u32 = 0;
    while (i < npong) : (i += 1) {
        client.active_writer.writeAll("PONG\r\n") catch {
            write_err = true;
            break;
        };
    }

    if (!write_err) {
        while (client.publish_ring.peek()) |data| {
            client.active_writer.writeAll(data) catch {
                write_err = true;
                break;
            };
            client.publish_ring.advance();
        }
    }

    if (!write_err) {
        client.active_writer.flush() catch {
            write_err = true;
        };
        if (!write_err and client.use_tls) {
            client.writer.interface.flush() catch {
                write_err = true;
            };
        }
    }

    _ = client.flush_requested.swap(false, .acquire);

    if (write_err) {
        // Generation-guarded: act only if this is still the same live
        // connection, so a stale error can't tear down a socket a concurrent
        // reconnect already replaced. Set transport_failed before releasing the
        // lock (the reader's cleanupForReconnect takes write_mutex, so the
        // handoff must not race the swap).
        if (client.conn_generation.load(.acquire) == gen and
            State.atomicLoad(&client.state) == .connected)
        {
            client.transport_failed.store(true, .release);
        }
    }

    client.write_mutex.unlock(client.io);

    if (write_err and client.transport_failed.load(.acquire)) {
        // Signal the reader (owns teardown/reconnect). Lock already released.
        client.shutdownRecvIfOpen();
    }
}

/// Result of read/route operations.
const ReadResult = enum {
    progress,
    no_progress,
    disconnected,
    canceled,
};

/// Handle transport loss (disconnect). Returns true if the io_task should
/// exit its outer loop, false if it reconnected and should continue.
fn handleTransportLoss(client: *Client) bool {
    const state = State.atomicLoad(&client.state);
    if (client.options.reconnect and state != .closed) {
        // handleDisconnect: true = reconnected (continue), false = give up.
        return !handleDisconnect(client);
    }
    @atomicStore(State, &client.state, .closed, .release);
    // Reconnect disabled (or already closing): same terminal-close wake as the
    // give-up path, so parked nextMsg/request/flush return promptly.
    client.wakeBlockedOnClose();
    client.pushEvent(.{ .closed = {} });
    return true;
}

/// Route buffered messages (no I/O, buffer processing only).
/// Handles: MSG -> route to queue, PING -> write PONG.
/// Uses lock-free SpscQueue - no yields needed.
inline fn tryRouteBufferedMessages(
    client: *Client,
) ReadResult {
    const allocator = client.allocator;
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
                const msgs_since = client.statistics.msgs_in -|
                    client.last_parse_error_notified_at;
                const interval = client.options.error_notify_interval_msgs;
                if (client.protocol_errors == 1 or msgs_since >= interval) {
                    client.last_parse_error_notified_at = client.statistics.msgs_in;
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
                    client.statistics.msgs_in += 1;
                    client.statistics.bytes_in += args.payload.len;
                },
                .hmsg => |args| {
                    routeHMessageToSub(client, slab, args);
                    client.statistics.msgs_in += 1;
                    client.statistics.bytes_in += args.total_len;
                },
                .ping => {
                    // The reader never takes write_mutex to PONG (a
                    // backpressured writer holding it would stall the reader).
                    // Flag the pending PONG and wake the writer, which emits it.
                    _ = client.pong_pending.fetchAdd(1, .monotonic);
                    client.signalWriter();
                },
                .pong => {
                    const now = getNowNs(client.io);
                    dbg.print("Got PONG, storing timestamp={d}", .{now});
                    client.pings_outstanding.store(0, .monotonic);
                    client.last_pong_received_ns.store(now, .release);
                    // Wake any flush() parked on the PONG eventcount. The store
                    // above is release; signalPong's seq bump is seq_cst, so a
                    // waking flush observes the fresh timestamp too.
                    client.signalPong();
                },
                .info => |info| {
                    // REVIEWED(2025-03): server_info replacement
                    // races with user reads. Risk: user holding a
                    // slice from serverInfo() getters gets dangling
                    // pointer when old strings are freed. Window is
                    // narrow (reconnect only). Locking would add
                    // overhead to every getter for a rare event.
                    // x86_64 only; aarch64 risk is strictly worse.
                    if (client.server_info) |*old| {
                        old.deinit(allocator);
                    }
                    client.server_info = info;
                    client.max_payload = info.max_payload;
                    client.parser.max_payload = info.max_payload;
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
    client.read_mutex.lockUncancelable(client.io);
    defer client.read_mutex.unlock(client.io);

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
        const msgs_since = client.statistics.msgs_in -| sub.last_alloc_notified_at;
        const interval = client.options.error_notify_interval_msgs;
        if (sub.alloc_failed_msgs == 1 or msgs_since >= interval) {
            sub.last_alloc_notified_at = client.statistics.msgs_in;
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
        .return_lock = &client.return_lock,
        .slab = &client.tiered_slab,
    };

    sub.pushMessage(msg) catch {
        dbg.print("routeMsg: PUSH FAILED (slow consumer) sid={d}", .{args.sid});
        sub.dropped_msgs += 1;
        slab.free(buf);
        // REVIEWED(2025-03): Single notification is intentional.
        // Avoids flooding event queue during slow consumer.
        // Users monitor sub.dropped_msgs for ongoing counts.
        if (sub.dropped_msgs == 1) {
            client.pushEvent(.{ .slow_consumer = .{ .sid = args.sid } });
        }
        return;
    };
    // REVIEWED(2025-03): Non-atomic stats are safe here.
    // io_task is the sole writer; user reads after drain.
    dbg.print("routeMsg: pushed to queue, sid={d}", .{args.sid});
    sub.received_msgs += 1;
}

/// Route HMSG to subscription queue.
inline fn routeHMessageToSub(
    client: *Client,
    slab: *TieredSlab,
    args: protocol.HMsgArgs,
) void {
    client.read_mutex.lockUncancelable(client.io);
    defer client.read_mutex.unlock(client.io);

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
        const msgs_since = client.statistics.msgs_in -| sub.last_alloc_notified_at;
        const interval = client.options.error_notify_interval_msgs;
        if (sub.alloc_failed_msgs == 1 or msgs_since >= interval) {
            sub.last_alloc_notified_at = client.statistics.msgs_in;
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
        .return_lock = &client.return_lock,
        .slab = &client.tiered_slab,
    };

    sub.pushMessage(msg) catch {
        sub.dropped_msgs += 1;
        slab.free(buf);
        if (sub.dropped_msgs == 1) {
            client.pushEvent(.{ .slow_consumer = .{ .sid = args.sid } });
        }
        return;
    };
    sub.received_msgs += 1;
}

/// Handle disconnect - backup subs, attempt reconnection, restore subs.
/// Returns true if reconnected successfully, false if should exit task.
fn handleDisconnect(client: *Client) bool {
    @atomicStore(State, &client.state, .disconnected, .release);

    client.pushEvent(.{ .disconnected = .{ .err = null } });

    client.backupSubscriptions() catch |err| {
        dbg.print("backupSubscriptions failed: {s}", .{@errorName(err)});
    };

    // Close old stream before reconnect to prevent FD leak
    // (matches reconnect() ordering: backup then cleanup)
    client.cleanupForReconnect();

    if (tryReconnectLoop(client)) {
        client.restoreSubscriptions() catch {
            dbg.print("Failed to restore subscriptions after reconnect", .{});
        };

        // New connection epoch: a write-error from the writer captured on the
        // old socket is now stale and must be ignored (generation guard).
        _ = client.conn_generation.fetchAdd(1, .monotonic);
        client.transport_failed.store(false, .release);
        // Re-arm the writer (drain ring-buffered publishes, resume PING) and
        // release any flush() parked across the reconnect (it returns an error;
        // its PING went out on the dead socket).
        client.signalWriter();
        client.signalPong();

        client.pushEvent(.{ .reconnected = {} });
        return true;
    } else {
        @atomicStore(State, &client.state, .closed, .release);
        // Reconnect gave up: wake anyone parked in nextMsg/request/flush so they
        // return a closed error now, not after deinit.
        client.wakeBlockedOnClose();
        client.pushEvent(.{ .closed = {} });
        return false;
    }
}

/// Attempt reconnection loop with backoff.
/// Returns true if reconnected, false if failed or canceled.
fn tryReconnectLoop(client: *Client) bool {
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
            client.tryConnect(server) catch continue;
            @atomicStore(State, &client.state, .connected, .release);
            _ = client.statistics.reconnects.fetchAdd(1, .monotonic);
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

    const jitter_range = (capped_delay * jitter_pct) / 100;
    if (jitter_range == 0) return capped_delay;
    var rand_buf: [4]u8 = undefined;
    client.io.random(&rand_buf);
    const rand_val = std.mem.readInt(
        u32,
        &rand_buf,
        .little,
    );
    const jitter_offset = rand_val % (jitter_range * 2 + 1);
    const jitter: i64 = @as(i64, jitter_offset) -
        @as(i64, jitter_range);

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

    // REVIEWED(2025-03): last_error written without sync.
    // Acceptable: errors rare, x86_64 TSO ensures coherent
    // reads, msg is copied into fixed buffer before use.
    client.last_error = err_type;
    if (msg.len < 256) {
        const len: u8 = @intCast(msg.len);
        @memcpy(client.last_error_msg[0..len], msg);
        client.last_error_msg_len = len;
    } else {
        // Truncate to fit u8 length field
        @memcpy(
            client.last_error_msg[0..255],
            msg[0..255],
        );
        client.last_error_msg_len = 255;
    }

    // Use already-copied last_error_msg to avoid
    // dangling pointer into recycled parser buffer
    const safe_msg = if (client.last_error_msg_len > 0)
        client.last_error_msg[0..client.last_error_msg_len]
    else
        null;
    client.pushEvent(.{ .err = .{ .err = err_type, .msg = safe_msg } });

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
