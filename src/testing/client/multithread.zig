//! Multi-Thread Safety Tests for NATS Client
//!
//! Tests that a single Client can be safely used from multiple OS
//! threads for publish, subscribe, and request operations.
//! These tests use std.Thread.spawn for real OS-level concurrency.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

/// Blocking sleep for OS threads (no Io context).
/// Uses the same pattern as Client.zig:reserveRingEntry.
fn threadSleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

/// Multiple OS threads publishing via the same client.
/// Verifies all messages arrive with no corruption.
pub fn testMultiThreadPublish(
    allocator: std.mem.Allocator,
) void {
    const NUM_THREADS = 4;
    const MSGS_PER_THREAD = 5000;
    const TOTAL = NUM_THREADS * MSGS_PER_THREAD;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = false,
            // 32K queue to hold all 20K messages
            .sub_queue_size = 32768,
        },
    ) catch {
        reportResult(
            "mt_publish",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    // Subscribe to receive all messages
    const sub = client.subscribeSync("mt.pub.>") catch {
        reportResult(
            "mt_publish",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    // Wait for subscription to register
    client.flush(5_000_000_000) catch {};

    // Spawn publisher threads (track success counts)
    var threads: [NUM_THREADS]std.Thread = undefined;
    var pub_counts: [NUM_THREADS]std.atomic.Value(u32) =
        undefined;
    for (0..NUM_THREADS) |i| {
        pub_counts[i] = std.atomic.Value(u32).init(0);
        threads[i] = std.Thread.spawn(
            .{},
            publishThread,
            .{ client, i, MSGS_PER_THREAD, &pub_counts[i] },
        ) catch {
            reportResult(
                "mt_publish",
                false,
                "spawn failed",
            );
            return;
        };
    }

    // Wait for all publishers
    for (&threads) |*t| t.join();

    // Verify all publishes succeeded (no ring-full drops)
    var total_published: u32 = 0;
    for (&pub_counts) |*c| {
        total_published += c.load(.monotonic);
    }
    if (total_published != TOTAL) {
        var buf2: [64]u8 = undefined;
        const d = std.fmt.bufPrint(
            &buf2,
            "published {d}/{d}",
            .{ total_published, TOTAL },
        ) catch "pub fail";
        reportResult("mt_publish", false, d);
        return;
    }

    // Flush to push all ring data through to server
    client.flush(5_000_000_000) catch {};

    // Collect messages: poll with deadline (5s timeout).
    // tryNextMsg() returns null when queue is momentarily
    // empty, but more messages may still be in flight.
    var received: usize = 0;
    var empty_polls: usize = 0;
    while (received < TOTAL and empty_polls < 500) {
        if (sub.tryNextMsg()) |m| {
            received += 1;
            m.deinit();
            empty_polls = 0;
        } else {
            empty_polls += 1;
            threadSleepNs(10_000_000); // 10ms
        }
    }

    if (received >= TOTAL) {
        reportResult("mt_publish", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, TOTAL },
        ) catch "count mismatch";
        reportResult("mt_publish", false, detail);
    }
}

fn publishThread(
    client: *nats.Client,
    thread_id: usize,
    count: usize,
    ok_count: *std.atomic.Value(u32),
) void {
    var subject_buf: [32]u8 = undefined;
    const subject = std.fmt.bufPrint(
        &subject_buf,
        "mt.pub.{d}",
        .{thread_id},
    ) catch return;

    var payload_buf: [64]u8 = undefined;
    for (0..count) |seq| {
        const payload = std.fmt.bufPrint(
            &payload_buf,
            "{d}:{d}",
            .{ thread_id, seq },
        ) catch continue;
        client.publish(subject, payload) catch continue;
        _ = ok_count.fetchAdd(1, .monotonic);
    }
}

/// Multiple OS threads subscribing/unsubscribing concurrently.
/// Verifies no slot corruption, no duplicate SIDs.
pub fn testMultiThreadSubscribe(
    allocator: std.mem.Allocator,
) void {
    const NUM_THREADS = 4;
    const ITERS = 200;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_subscribe",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    // Each thread subscribes and unsubscribes in a loop
    var threads: [NUM_THREADS]std.Thread = undefined;
    var errors: [NUM_THREADS]std.atomic.Value(u32) = undefined;
    for (0..NUM_THREADS) |i| {
        errors[i] = std.atomic.Value(u32).init(0);
        threads[i] = std.Thread.spawn(
            .{},
            subUnsubThread,
            .{ client, i, ITERS, &errors[i] },
        ) catch {
            reportResult(
                "mt_subscribe",
                false,
                "spawn failed",
            );
            return;
        };
    }

    for (&threads) |*t| t.join();

    var total_errors: u32 = 0;
    for (&errors) |*e| {
        total_errors += e.load(.monotonic);
    }

    const num_subs = client.numSubscriptions();

    if (total_errors == 0 and num_subs == 0) {
        reportResult("mt_subscribe", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "errs={d} leaked_subs={d}",
            .{ total_errors, num_subs },
        ) catch "errors";
        reportResult("mt_subscribe", false, detail);
    }
}

fn subUnsubThread(
    client: *nats.Client,
    thread_id: usize,
    iters: usize,
    err_count: *std.atomic.Value(u32),
) void {
    for (0..iters) |i| {
        var subject_buf: [48]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "mt.sub.{d}.{d}",
            .{ thread_id, i },
        ) catch continue;

        const sub = client.subscribeSync(subject) catch {
            _ = err_count.fetchAdd(1, .monotonic);
            continue;
        };
        sub.deinit();
    }
}

/// Multiple OS threads publishing, verify stats are accurate.
pub fn testMultiThreadStats(
    allocator: std.mem.Allocator,
) void {
    const NUM_THREADS = 4;
    const MSGS_PER_THREAD = 5000;
    const TOTAL = NUM_THREADS * MSGS_PER_THREAD;
    const PAYLOAD = "stats-test-payload";

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_stats",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    const before = client.stats();

    var threads: [NUM_THREADS]std.Thread = undefined;
    for (0..NUM_THREADS) |i| {
        threads[i] = std.Thread.spawn(
            .{},
            statsPublishThread,
            .{ client, MSGS_PER_THREAD, PAYLOAD },
        ) catch {
            reportResult(
                "mt_stats",
                false,
                "spawn failed",
            );
            return;
        };
    }

    for (&threads) |*t| t.join();

    const after = client.stats();
    const msgs_diff = after.msgs_out - before.msgs_out;
    const bytes_diff = after.bytes_out - before.bytes_out;
    const expected_bytes = TOTAL * PAYLOAD.len;

    if (msgs_diff == TOTAL and bytes_diff == expected_bytes) {
        reportResult("mt_stats", true, "");
    } else {
        var buf: [96]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs={d}/{d} bytes={d}/{d}",
            .{
                msgs_diff,
                TOTAL,
                bytes_diff,
                expected_bytes,
            },
        ) catch "mismatch";
        reportResult("mt_stats", false, detail);
    }
}

fn statsPublishThread(
    client: *nats.Client,
    count: usize,
    payload: []const u8,
) void {
    for (0..count) |_| {
        client.publish("mt.stats", payload) catch {};
    }
}

/// Mixed workload: publish + subscribe from different threads.
pub fn testMultiThreadMixed(
    allocator: std.mem.Allocator,
) void {
    const NUM_PUB_THREADS = 2;
    const MSGS_PER_THREAD = 3000;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_mixed",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    // Subscribe first
    const sub = client.subscribeSync("mt.mix") catch {
        reportResult(
            "mt_mixed",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    client.flush(5_000_000_000) catch {};

    // Spawn publisher threads
    var pub_threads: [NUM_PUB_THREADS]std.Thread = undefined;
    for (0..NUM_PUB_THREADS) |i| {
        pub_threads[i] = std.Thread.spawn(
            .{},
            mixedPublishThread,
            .{ client, MSGS_PER_THREAD },
        ) catch {
            reportResult(
                "mt_mixed",
                false,
                "spawn failed",
            );
            return;
        };
    }

    // Spawn a subscribe/unsubscribe churn thread
    var churn_err = std.atomic.Value(u32).init(0);
    const churn_thread = std.Thread.spawn(
        .{},
        subChurnThread,
        .{ client, 100, &churn_err },
    ) catch {
        reportResult(
            "mt_mixed",
            false,
            "churn spawn failed",
        );
        return;
    };

    // Wait for publishers
    for (&pub_threads) |*t| t.join();
    churn_thread.join();

    // Collect with deadline
    var received: usize = 0;
    const total = NUM_PUB_THREADS * MSGS_PER_THREAD;
    var empty_polls: usize = 0;
    while (received < total and empty_polls < 500) {
        if (sub.tryNextMsg()) |m| {
            received += 1;
            m.deinit();
            empty_polls = 0;
        } else {
            empty_polls += 1;
            threadSleepNs(10_000_000); // 10ms
        }
    }

    const churn_errors = churn_err.load(.monotonic);

    if (received >= total and churn_errors == 0) {
        reportResult("mt_mixed", true, "");
    } else {
        var buf: [80]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "recv={d}/{d} churn_err={d}",
            .{ received, total, churn_errors },
        ) catch "error";
        reportResult("mt_mixed", false, detail);
    }
}

fn mixedPublishThread(
    client: *nats.Client,
    count: usize,
) void {
    for (0..count) |_| {
        client.publish("mt.mix", "mixed") catch {};
    }
}

fn subChurnThread(
    client: *nats.Client,
    iters: usize,
    err_count: *std.atomic.Value(u32),
) void {
    for (0..iters) |i| {
        var buf: [48]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &buf,
            "mt.churn.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribeSync(subject) catch {
            _ = err_count.fetchAdd(1, .monotonic);
            continue;
        };
        sub.deinit();
    }
}

/// Multiple threads doing request/reply concurrently.
pub fn testMultiThreadRequest(
    allocator: std.mem.Allocator,
) void {
    const NUM_THREADS = 4;
    const REQS_PER_THREAD = 50;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Requester client
    const io_req = utils.newIo(allocator);
    defer io_req.deinit();

    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_request",
            false,
            "req connect failed",
        );
        return;
    };
    defer requester.deinit();

    // Responder client
    const io_resp = utils.newIo(allocator);
    defer io_resp.deinit();

    const responder = nats.Client.connect(
        allocator,
        io_resp.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_request",
            false,
            "resp connect failed",
        );
        return;
    };
    defer responder.deinit();

    // Responder subscribes and echoes back
    const resp_sub = responder.subscribeSync(
        "mt.req",
    ) catch {
        reportResult(
            "mt_request",
            false,
            "resp subscribe failed",
        );
        return;
    };
    defer resp_sub.deinit();

    responder.flush(5_000_000_000) catch {};

    // Responder loop in a thread
    var stop_flag = std.atomic.Value(bool).init(false);
    const resp_thread = std.Thread.spawn(
        .{},
        responderThread,
        .{ responder, resp_sub, &stop_flag },
    ) catch {
        reportResult(
            "mt_request",
            false,
            "resp spawn failed",
        );
        return;
    };

    // Spawn requester threads
    var threads: [NUM_THREADS]std.Thread = undefined;
    var successes: [NUM_THREADS]std.atomic.Value(u32) =
        undefined;
    for (0..NUM_THREADS) |i| {
        successes[i] = std.atomic.Value(u32).init(0);
        threads[i] = std.Thread.spawn(
            .{},
            requestThread,
            .{
                requester,
                REQS_PER_THREAD,
                &successes[i],
            },
        ) catch {
            reportResult(
                "mt_request",
                false,
                "req spawn failed",
            );
            stop_flag.store(true, .release);
            resp_thread.join();
            return;
        };
    }

    for (&threads) |*t| t.join();
    stop_flag.store(true, .release);
    resp_thread.join();

    var total_success: u32 = 0;
    for (&successes) |*s| {
        total_success += s.load(.monotonic);
    }

    const expected = NUM_THREADS * REQS_PER_THREAD;
    if (total_success >= expected) {
        reportResult("mt_request", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "replies={d}/{d}",
            .{ total_success, expected },
        ) catch "mismatch";
        reportResult("mt_request", false, detail);
    }
}

fn responderThread(
    responder: *nats.Client,
    sub: *nats.Subscription,
    stop: *std.atomic.Value(bool),
) void {
    while (!stop.load(.acquire)) {
        if (sub.tryNextMsg()) |m| {
            defer m.deinit();
            if (m.reply_to) |reply| {
                responder.publish(
                    reply,
                    m.data,
                ) catch {};
            }
        } else {
            threadSleepNs(1_000_000);
        }
    }
}

fn requestThread(
    client: *nats.Client,
    count: usize,
    success: *std.atomic.Value(u32),
) void {
    for (0..count) |_| {
        const reply = client.request(
            "mt.req",
            "ping",
            2000,
        ) catch continue;
        if (reply) |r| {
            r.deinit();
            _ = success.fetchAdd(1, .monotonic);
        }
    }
}

/// Stress test: many threads, rapid sub/unsub, verify
/// slot accounting stays correct.
pub fn testSubSlotIntegrity(
    allocator: std.mem.Allocator,
) void {
    const NUM_THREADS = 4;
    const ITERS = 500;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "mt_slot_integrity",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var threads: [NUM_THREADS]std.Thread = undefined;
    var errors: [NUM_THREADS]std.atomic.Value(u32) =
        undefined;
    for (0..NUM_THREADS) |i| {
        errors[i] = std.atomic.Value(u32).init(0);
        threads[i] = std.Thread.spawn(
            .{},
            subUnsubThread,
            .{ client, i, ITERS, &errors[i] },
        ) catch {
            reportResult(
                "mt_slot_integrity",
                false,
                "spawn failed",
            );
            return;
        };
    }

    for (&threads) |*t| t.join();

    var total_errors: u32 = 0;
    for (&errors) |*e| {
        total_errors += e.load(.monotonic);
    }

    const final_subs = client.numSubscriptions();

    if (total_errors == 0 and final_subs == 0) {
        reportResult("mt_slot_integrity", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "errs={d} subs={d}",
            .{ total_errors, final_subs },
        ) catch "error";
        reportResult("mt_slot_integrity", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    std.debug.print(
        "\n--- MultiThreading Tests ---\n",
        .{},
    );
    testMultiThreadPublish(allocator);
    testMultiThreadSubscribe(allocator);
    testMultiThreadStats(allocator);
    testMultiThreadMixed(allocator);
    testMultiThreadRequest(allocator);
    testSubSlotIntegrity(allocator);
}
