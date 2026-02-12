//! Async Patterns Integration Tests
//!
//! Tests std.Io async patterns with NATS client including:
//! - io.select() for racing operations
//! - io.concurrent() + Io.Queue for workers
//! - io.async() for parallel receives
//! - Cancellation and cleanup patterns
//! - Batch receiving

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;
const ServerManager = utils.ServerManager;

const Io = std.Io;
const Allocator = std.mem.Allocator;
const Sub = nats.Client.Sub;

/// Sleep function compatible with io.async()
fn sleepMs(io: Io, ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

/// Test 1: Race subscription receive against timeout - timeout wins.
fn testAsyncSelectTimeout(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_select_timeout", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.timeout.test") catch {
        reportResult("async_select_timeout", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Do NOT publish - we want timeout to win
    var recv_future = io.async(Sub.next, .{sub});
    var timeout_future = io.async(sleepMs, .{ io, 50 });

    var winner: enum { none, message, timeout } = .none;
    defer if (winner != .message) {
        if (recv_future.cancel(io)) |m| m.deinit() else |_| {}
    };
    defer if (winner != .timeout) timeout_future.cancel(io);

    const result = io.select(.{
        .message = &recv_future,
        .timeout = &timeout_future,
    }) catch {
        reportResult("async_select_timeout", false, "select failed");
        return;
    };

    switch (result) {
        .message => {
            winner = .message;
            reportResult("async_select_timeout", false, "expected timeout");
        },
        .timeout => {
            winner = .timeout;
            reportResult("async_select_timeout", true, "");
        },
    }
}

/// Test 2: Race subscription receive against timeout - message wins.
fn testAsyncSelectMessage(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_select_message", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.message.test") catch {
        reportResult("async_select_message", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Publish message immediately
    client.publish("async.message.test", "select-test-msg") catch {
        reportResult("async_select_message", false, "publish failed");
        return;
    };

    var recv_future = io.async(Sub.next, .{sub});
    var timeout_future = io.async(sleepMs, .{ io, 500 });

    var winner: enum { none, message, timeout } = .none;
    defer if (winner != .message) {
        if (recv_future.cancel(io)) |m| m.deinit() else |_| {}
    };
    defer if (winner != .timeout) timeout_future.cancel(io);

    const result = io.select(.{
        .message = &recv_future,
        .timeout = &timeout_future,
    }) catch {
        reportResult("async_select_message", false, "select failed");
        return;
    };

    switch (result) {
        .message => |msg_result| {
            winner = .message;
            const msg = msg_result catch {
                reportResult("async_select_message", false, "msg error");
                return;
            };
            defer msg.deinit();
            if (std.mem.eql(u8, msg.data, "select-test-msg")) {
                reportResult("async_select_message", true, "");
            } else {
                reportResult("async_select_message", false, "wrong data");
            }
        },
        .timeout => {
            winner = .timeout;
            reportResult("async_select_message", false, "unexpected timeout");
        },
    }
}

/// Worker result for concurrent workers test.
const WorkerResult = struct {
    worker_id: u8,
    msg: nats.Message,

    fn deinit(self: WorkerResult) void {
        self.msg.deinit();
    }
};

/// Worker task for concurrent test.
fn workerTask(
    io: Io,
    worker_id: u8,
    sub: *Sub,
    queue: *Io.Queue(WorkerResult),
    done: *std.atomic.Value(bool),
) void {
    while (!done.load(.acquire)) {
        const msg = sub.nextWithTimeout(100) catch return orelse continue;
        queue.putOne(io, .{ .worker_id = worker_id, .msg = msg }) catch {
            msg.deinit();
            return;
        };
    }
}

/// Test 3: Multiple workers with io.concurrent() + Io.Queue.
fn testAsyncConcurrentWorkers(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_concurrent_workers", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Create 3 workers in queue group
    const w1_sub = client.subscribeQueue(
        "async.workers",
        "workers",
    ) catch {
        reportResult("async_concurrent_workers", false, "sub1 failed");
        return;
    };
    defer w1_sub.deinit();

    const w2_sub = client.subscribeQueue(
        "async.workers",
        "workers",
    ) catch {
        reportResult("async_concurrent_workers", false, "sub2 failed");
        return;
    };
    defer w2_sub.deinit();

    const w3_sub = client.subscribeQueue(
        "async.workers",
        "workers",
    ) catch {
        reportResult("async_concurrent_workers", false, "sub3 failed");
        return;
    };
    defer w3_sub.deinit();

    // Shared queue for results
    var queue_buf: [64]WorkerResult = undefined;
    var queue: Io.Queue(WorkerResult) = .init(&queue_buf);
    var done: std.atomic.Value(bool) = .init(false);

    // Launch workers
    var w1 = io.concurrent(workerTask, .{
        io, 1, w1_sub, &queue, &done,
    }) catch {
        reportResult("async_concurrent_workers", false, "w1 launch failed");
        return;
    };
    defer w1.cancel(io);

    var w2 = io.concurrent(workerTask, .{
        io, 2, w2_sub, &queue, &done,
    }) catch {
        reportResult("async_concurrent_workers", false, "w2 launch failed");
        return;
    };
    defer w2.cancel(io);

    var w3 = io.concurrent(workerTask, .{
        io, 3, w3_sub, &queue, &done,
    }) catch {
        reportResult("async_concurrent_workers", false, "w3 launch failed");
        return;
    };
    defer w3.cancel(io);

    // Publish messages
    const message_count: u32 = 30;
    var i: u32 = 0;
    while (i < message_count) : (i += 1) {
        client.publish("async.workers", "work-item") catch {};
    }

    // Consume results
    var total_received: u32 = 0;
    var timeout_count: u32 = 0;
    const max_timeouts: u32 = 10;

    while (total_received < message_count and timeout_count < max_timeouts) {
        // Use select with timeout to avoid hanging forever
        var get_future = io.async(Io.Queue(WorkerResult).getOne, .{ &queue, io });
        var timeout_future = io.async(sleepMs, .{ io, 200 });

        var winner: enum { none, result, timeout } = .none;
        defer if (winner != .result) {
            if (get_future.cancel(io)) |r| r.deinit() else |_| {}
        };
        defer if (winner != .timeout) timeout_future.cancel(io);

        const sel = io.select(.{
            .result = &get_future,
            .timeout = &timeout_future,
        }) catch break;

        switch (sel) {
            .result => |res| {
                winner = .result;
                const r = res catch break;
                r.deinit();
                total_received += 1;
            },
            .timeout => {
                winner = .timeout;
                timeout_count += 1;
            },
        }
    }

    // Signal workers to stop
    done.store(true, .release);

    if (total_received >= message_count * 9 / 10) {
        reportResult("async_concurrent_workers", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received",
            .{ total_received, message_count },
        ) catch "partial";
        reportResult("async_concurrent_workers", false, detail);
    }
}

/// Test 4: Multiple parallel subscriptions with io.async().
fn testAsyncParallelSubscriptions(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_parallel_subs", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub_a = client.subscribe("async.parallel.a") catch {
        reportResult("async_parallel_subs", false, "sub_a failed");
        return;
    };
    defer sub_a.deinit();

    const sub_b = client.subscribe("async.parallel.b") catch {
        reportResult("async_parallel_subs", false, "sub_b failed");
        return;
    };
    defer sub_b.deinit();

    const sub_c = client.subscribe("async.parallel.c") catch {
        reportResult("async_parallel_subs", false, "sub_c failed");
        return;
    };
    defer sub_c.deinit();

    // Publish to all three
    client.publish("async.parallel.a", "msg-a") catch {};
    client.publish("async.parallel.b", "msg-b") catch {};
    client.publish("async.parallel.c", "msg-c") catch {};

    // Launch parallel receives
    var future_a = io.async(Sub.next, .{sub_a});
    defer if (future_a.cancel(io)) |m| m.deinit() else |_| {};

    var future_b = io.async(Sub.next, .{sub_b});
    defer if (future_b.cancel(io)) |m| m.deinit() else |_| {};

    var future_c = io.async(Sub.next, .{sub_c});
    defer if (future_c.cancel(io)) |m| m.deinit() else |_| {};

    var received: u8 = 0;

    // DON'T deinit after await - defer handles cleanup via cancel
    // cancel() returns same result as await (idempotent)
    if (future_a.await(io)) |_| {
        received += 1;
    } else |_| {}

    if (future_b.await(io)) |_| {
        received += 1;
    } else |_| {}

    if (future_c.await(io)) |_| {
        received += 1;
    } else |_| {}

    if (received == 3) {
        reportResult("async_parallel_subs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/3 received",
            .{received},
        ) catch "partial";
        reportResult("async_parallel_subs", false, detail);
    }
}

/// Test 5: Cancellation of pending receive.
fn testAsyncCancellation(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_cancellation", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.cancel.test") catch {
        reportResult("async_cancellation", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Start async receive but cancel immediately (no message published)
    var future = io.async(Sub.next, .{sub});

    // Small delay then cancel
    io.sleep(.fromMilliseconds(10), .awake) catch {};

    // Cancel should not hang and should clean up properly
    if (future.cancel(io)) |msg| {
        // Got a message somehow - clean it up
        msg.deinit();
        reportResult("async_cancellation", true, "");
    } else |_| {
        // Expected: cancel returns error (Canceled or similar)
        reportResult("async_cancellation", true, "");
    }
}

/// Test 6: Cancel after message already received.
fn testAsyncCancelWithPendingMessage(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_cancel_with_msg", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.cancel.msg") catch {
        reportResult("async_cancel_with_msg", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Publish message first
    client.publish("async.cancel.msg", "pending-msg") catch {
        reportResult("async_cancel_with_msg", false, "publish failed");
        return;
    };

    // Let message arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Async receive - should get message quickly
    var future = io.async(Sub.next, .{sub});
    defer if (future.cancel(io)) |m| m.deinit() else |_| {};

    // DON'T defer deinit after await - outer defer handles cleanup
    if (future.await(io)) |msg| {
        if (std.mem.eql(u8, msg.data, "pending-msg")) {
            reportResult("async_cancel_with_msg", true, "");
        } else {
            reportResult("async_cancel_with_msg", false, "wrong data");
        }
    } else |_| {
        reportResult("async_cancel_with_msg", false, "await failed");
    }
}

/// Test 7: nextBatch() receives multiple messages.
fn testBatchReceive(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .sub_queue_size = 64,
    }) catch {
        reportResult("batch_receive", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.batch") catch {
        reportResult("batch_receive", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Publish 20 messages rapidly
    var i: u8 = 0;
    while (i < 20) : (i += 1) {
        client.publish("async.batch", "batch-data") catch {};
    }

    // Let messages arrive
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    // Batch receive
    var batch_buf: [32]nats.Message = undefined;
    const count = sub.nextBatch(io, &batch_buf) catch {
        reportResult("batch_receive", false, "nextBatch failed");
        return;
    };

    // Clean up received messages
    for (batch_buf[0..count]) |*msg| {
        msg.deinit();
    }

    // Drain any remaining
    const remaining = sub.tryNextBatch(&batch_buf);
    for (batch_buf[0..remaining]) |*msg| {
        msg.deinit();
    }

    const total = count + remaining;

    if (total >= 15) {
        reportResult("batch_receive", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/20 received",
            .{total},
        ) catch "partial";
        reportResult("batch_receive", false, detail);
    }
}

/// Test 8: tryNextBatch() non-blocking.
fn testTryNextBatch(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("try_next_batch", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.trybatch") catch {
        reportResult("try_next_batch", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    var batch_buf: [32]nats.Message = undefined;

    // Empty queue should return 0
    const empty_count = sub.tryNextBatch(&batch_buf);
    if (empty_count != 0) {
        reportResult("try_next_batch", false, "expected 0 on empty");
        return;
    }

    // Publish messages
    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        client.publish("async.trybatch", "data") catch {};
    }

    // Let messages arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Should get some messages
    const first_count = sub.tryNextBatch(&batch_buf);
    for (batch_buf[0..first_count]) |*msg| {
        msg.deinit();
    }

    // Drain remaining
    const second_count = sub.tryNextBatch(&batch_buf);
    for (batch_buf[0..second_count]) |*msg| {
        msg.deinit();
    }

    // Call again on drained queue
    const third_count = sub.tryNextBatch(&batch_buf);

    if (first_count > 0 and third_count == 0) {
        reportResult("try_next_batch", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "first={d} third={d}",
            .{ first_count, third_count },
        ) catch "error";
        reportResult("try_next_batch", false, detail);
    }
}

/// Inner function that returns early to test defer cleanup.
fn innerAsyncWithEarlyReturn(
    io: Io,
    sub: *Sub,
) bool {
    var future = io.async(Sub.next, .{sub});
    defer if (future.cancel(io)) |m| m.deinit() else |_| {};

    // Simulate early return (e.g., error condition)
    return true; // defer should clean up the future
}

/// Test 9: Defer pattern cleans up on early return.
fn testAsyncDeferCleanup(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("async_defer_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribe("async.defer.test") catch {
        reportResult("async_defer_cleanup", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Call inner function that returns early
    const result = innerAsyncWithEarlyReturn(io, sub);

    // If we get here without hanging, defer cleanup worked
    if (result) {
        reportResult("async_defer_cleanup", true, "");
    } else {
        reportResult("async_defer_cleanup", false, "unexpected result");
    }
}

/// Test 10: Race multiple subscriptions with io.select().
fn testSelectMultipleSubs(allocator: Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var threaded: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
    }) catch {
        reportResult("select_multiple_subs", false, "connect failed");
        return;
    };
    defer client.deinit();

    const fast_sub = client.subscribe("async.fast") catch {
        reportResult("select_multiple_subs", false, "fast_sub failed");
        return;
    };
    defer fast_sub.deinit();

    const slow_sub = client.subscribe("async.slow") catch {
        reportResult("select_multiple_subs", false, "slow_sub failed");
        return;
    };
    defer slow_sub.deinit();

    // Only publish to "fast"
    client.publish("async.fast", "fast-msg") catch {
        reportResult("select_multiple_subs", false, "publish failed");
        return;
    };

    var fast_future = io.async(Sub.next, .{fast_sub});
    var slow_future = io.async(Sub.next, .{slow_sub});

    var winner: enum { none, fast, slow } = .none;
    defer if (winner != .fast) {
        if (fast_future.cancel(io)) |m| m.deinit() else |_| {}
    };
    defer if (winner != .slow) {
        if (slow_future.cancel(io)) |m| m.deinit() else |_| {}
    };

    const result = io.select(.{
        .fast = &fast_future,
        .slow = &slow_future,
    }) catch {
        reportResult("select_multiple_subs", false, "select failed");
        return;
    };

    switch (result) {
        .fast => |msg_result| {
            winner = .fast;
            const msg = msg_result catch {
                reportResult("select_multiple_subs", false, "fast msg error");
                return;
            };
            defer msg.deinit();
            if (std.mem.eql(u8, msg.data, "fast-msg")) {
                reportResult("select_multiple_subs", true, "");
            } else {
                reportResult("select_multiple_subs", false, "wrong data");
            }
        },
        .slow => {
            winner = .slow;
            reportResult("select_multiple_subs", false, "slow won unexpectedly");
        },
    }
}

pub fn runAll(allocator: Allocator, _: *ServerManager) void {
    testAsyncSelectTimeout(allocator);
    testAsyncSelectMessage(allocator);
    testAsyncConcurrentWorkers(allocator);
    testAsyncParallelSubscriptions(allocator);
    testAsyncCancellation(allocator);
    testAsyncCancelWithPendingMessage(allocator);
    testBatchReceive(allocator);
    testTryNextBatch(allocator);
    testAsyncDeferCleanup(allocator);
    testSelectMultipleSubs(allocator);
}
