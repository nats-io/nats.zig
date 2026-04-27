//! Concurrency Tests for NATS Client
//!
//! Tests for race conditions, concurrent operations, and thread safety.
//! These tests verify the client behaves correctly under concurrent access.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const ServerManager = utils.ServerManager;

fn sleepMs(io: std.Io, ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

pub fn testConcurrentSubscribe(allocator: std.mem.Allocator) void {
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
        reportResult("concurrent_subscribe", false, "connect failed");
        return;
    };
    defer client.deinit();

    const NUM_SUBS = 10;
    var subs: [NUM_SUBS]?*nats.Subscription =
        [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: u32 = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit();
    };

    for (0..NUM_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "concurrent.sub.{d}",
            .{i},
        ) catch continue;

        subs[i] = client.subscribeSync(subject) catch {
            continue;
        };
        created += 1;
    }

    if (created != NUM_SUBS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/10",
            .{created},
        ) catch "e";
        reportResult("concurrent_subscribe", false, detail);
        return;
    }

    var sids: [NUM_SUBS]u64 = undefined;
    for (0..NUM_SUBS) |i| {
        if (subs[i]) |sub| {
            sids[i] = sub.sid;
            for (0..i) |j| {
                if (sids[j] == sids[i]) {
                    reportResult(
                        "concurrent_subscribe",
                        false,
                        "duplicate SID",
                    );
                    return;
                }
            }
        }
    }

    if (client.isConnected()) {
        reportResult("concurrent_subscribe", true, "");
    } else {
        reportResult("concurrent_subscribe", false, "disconnected");
    }
}

pub fn testRapidPublish(allocator: std.mem.Allocator) void {
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
        reportResult("rapid_publish", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("rapid.publish") catch {
        reportResult("rapid_publish", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    const NUM_MSGS = 100;
    var published: u32 = 0;
    for (0..NUM_MSGS) |_| {
        client.publish("rapid.publish", "data") catch {
            continue;
        };
        published += 1;
    }

    client.flush(500_000_000) catch {};

    if (published != NUM_MSGS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "pub {d}/100",
            .{published},
        ) catch "e";
        reportResult("rapid_publish", false, detail);
        return;
    }

    var received: u32 = 0;
    for (0..NUM_MSGS) |_| {
        if (sub.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            received += 1;
        } else break;
    }

    if (received == NUM_MSGS) {
        reportResult("rapid_publish", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/100",
            .{received},
        ) catch "e";
        reportResult("rapid_publish", false, detail);
    }
}

pub fn testConcurrentSubUnsub(allocator: std.mem.Allocator) void {
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
        reportResult("concurrent_sub_unsub", false, "connect failed");
        return;
    };
    defer client.deinit();

    const CYCLES = 20;
    var current_sub: ?*nats.Subscription = null;

    for (0..CYCLES) |i| {
        if (current_sub) |sub| {
            sub.unsubscribe() catch {};
            sub.deinit();
            current_sub = null;
        }

        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "cycle.{d}",
            .{i},
        ) catch continue;

        current_sub = client.subscribeSync(subject) catch {
            reportResult("concurrent_sub_unsub", false, "subscribe failed");
            return;
        };
    }

    if (current_sub) |sub| {
        sub.deinit();
    }

    if (client.isConnected()) {
        reportResult("concurrent_sub_unsub", true, "");
    } else {
        reportResult("concurrent_sub_unsub", false, "disconnected");
    }
}

pub fn testRaceSubscribeVsDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const pub_io = utils.newIo(allocator);
    defer pub_io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        pub_io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("race_sub_delivery", false, "pub connect failed");
        return;
    };
    defer publisher.deinit();

    const sub_io = utils.newIo(allocator);
    defer sub_io.deinit();

    const subscriber = nats.Client.connect(
        allocator,
        sub_io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("race_sub_delivery", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit();

    const sub = subscriber.subscribeSync("race.delivery") catch {
        reportResult("race_sub_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    publisher.publish("race.delivery", "race-msg-1") catch {
        reportResult("race_sub_delivery", false, "publish1 failed");
        return;
    };
    sub_io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("race.delivery", "race-msg-2") catch {
        reportResult("race_sub_delivery", false, "publish2 failed");
        return;
    };

    var received: u32 = 0;
    for (0..2) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            received += 1;
        }
    }

    if (received >= 1) {
        reportResult("race_sub_delivery", true, "");
    } else {
        reportResult("race_sub_delivery", false, "no messages received");
    }
}

pub fn testRaceUnsubscribeVsDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 64,
        .reconnect = false,
    }) catch {
        reportResult("race_unsub_delivery", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("race.unsub") catch {
        reportResult("race_unsub_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    for (0..50) |_| {
        client.publish("race.unsub", "msg") catch {};
    }

    sub.unsubscribe() catch {};

    for (0..50) |_| {
        client.publish("race.unsub", "msg") catch {};
    }

    if (client.isConnected()) {
        reportResult("race_unsub_delivery", true, "");
    } else {
        reportResult("race_unsub_delivery", false, "disconnected");
    }
}

pub fn testSidAllocationRecycling(allocator: std.mem.Allocator) void {
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
        reportResult("sid_allocation_recycle", false, "connect failed");
        return;
    };
    defer client.deinit();

    var seen_sids: [100]u64 = undefined;
    var seen_count: usize = 0;

    for (0..50) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "recycle.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribeSync(subject) catch {
            reportResult("sid_allocation_recycle", false, "subscribe failed");
            return;
        };

        if (seen_count < seen_sids.len) {
            for (seen_sids[0..seen_count]) |prev_sid| {
                if (prev_sid == sub.sid) {
                    reportResult("sid_allocation_recycle", false, "SID reused");
                    sub.deinit();
                    return;
                }
            }
            seen_sids[seen_count] = sub.sid;
            seen_count += 1;
        }

        sub.deinit();
    }

    for (1..seen_count) |i| {
        if (seen_sids[i] <= seen_sids[i - 1]) {
            reportResult("sid_allocation_recycle", false, "non-monotonic SIDs");
            return;
        }
    }

    if (client.isConnected()) {
        reportResult("sid_allocation_recycle", true, "");
    } else {
        reportResult("sid_allocation_recycle", false, "disconnected");
    }
}

pub fn testMultipleClientsSeparateIo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io1 = utils.newIo(allocator);
    defer io1.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_separate_io", false, "client1 failed");
        return;
    };
    defer client1.deinit();

    const io2 = utils.newIo(allocator);
    defer io2.deinit();

    const client2 = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_separate_io", false, "client2 failed");
        return;
    };
    defer client2.deinit();

    const io3 = utils.newIo(allocator);
    defer io3.deinit();

    const client3 = nats.Client.connect(
        allocator,
        io3.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_separate_io", false, "client3 failed");
        return;
    };
    defer client3.deinit();

    const sub = client1.subscribeSync("separate.io.test") catch {
        reportResult("multi_client_separate_io", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    io1.io().sleep(.fromMilliseconds(50), .awake) catch {};

    client2.publish("separate.io.test", "from-client2") catch {
        reportResult("multi_client_separate_io", false, "pub2 failed");
        return;
    };
    client3.publish("separate.io.test", "from-client3") catch {
        reportResult("multi_client_separate_io", false, "pub3 failed");
        return;
    };

    var received: u32 = 0;
    for (0..2) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            received += 1;
        }
    }

    if (received == 2) {
        reportResult("multi_client_separate_io", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{received}) catch "e";
        reportResult("multi_client_separate_io", false, detail);
    }
}

pub fn testParallelReceive(allocator: std.mem.Allocator) void {
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
        reportResult("parallel_recv", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub1 = client.subscribeSync("parallel.1") catch {
        reportResult("parallel_recv", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    const sub2 = client.subscribeSync("parallel.2") catch {
        reportResult("parallel_recv", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    const sub3 = client.subscribeSync("parallel.3") catch {
        reportResult("parallel_recv", false, "sub3 failed");
        return;
    };
    defer sub3.deinit();

    client.publish("parallel.1", "msg1") catch {};
    client.publish("parallel.2", "msg2") catch {};
    client.publish("parallel.3", "msg3") catch {};

    client.flush(500_000_000) catch {};

    var received: u32 = 0;

    if (sub1.nextMsgTimeout(1000) catch null) |m| {
        m.deinit();
        received += 1;
    }

    if (sub2.nextMsgTimeout(1000) catch null) |m| {
        m.deinit();
        received += 1;
    }

    if (sub3.nextMsgTimeout(1000) catch null) |m| {
        m.deinit();
        received += 1;
    }

    if (received == 3) {
        reportResult("parallel_recv", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/3",
            .{received},
        ) catch "e";
        reportResult("parallel_recv", false, detail);
    }
}

pub fn testRapidFlushOperations(allocator: std.mem.Allocator) void {
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
        reportResult("rapid_flush", false, "connect failed");
        return;
    };
    defer client.deinit();

    var success: u32 = 0;
    for (0..50) |_| {
        client.flushBuffer() catch {
            continue;
        };
        success += 1;
    }

    if (success != 50) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "flush {d}/50",
            .{success},
        ) catch "e";
        reportResult("rapid_flush", false, detail);
        return;
    }

    for (0..50) |_| {
        client.publish("flush.test", "x") catch {};
        client.flushBuffer() catch {};
    }

    if (client.isConnected()) {
        reportResult("rapid_flush", true, "");
    } else {
        reportResult("rapid_flush", false, "disconnected");
    }
}

pub fn testStatsConcurrency(allocator: std.mem.Allocator) void {
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
        reportResult("stats_concurrency", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("stats.test") catch {
        reportResult("stats_concurrency", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    const before = client.stats();

    const NUM_MSGS: u64 = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("stats.test", "stat-msg") catch {};
    }

    client.flush(500_000_000) catch {};

    for (0..NUM_MSGS) |_| {
        if (sub.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
        } else break;
    }

    const after = client.stats();

    const msgs_out_diff = after.msgs_out - before.msgs_out;
    const msgs_in_diff = after.msgs_in - before.msgs_in;

    if (msgs_out_diff != NUM_MSGS) {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs_out: {d} (expect {d})",
            .{ msgs_out_diff, NUM_MSGS },
        ) catch "e";
        reportResult("stats_concurrency", false, detail);
        return;
    }

    if (msgs_in_diff < NUM_MSGS) {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs_in: {d} (expect >= {d})",
            .{ msgs_in_diff, NUM_MSGS },
        ) catch "e";
        reportResult("stats_concurrency", false, detail);
        return;
    }

    reportResult("stats_concurrency", true, "");
}

pub fn testMixedWriteOrderingPublishBeforeSubscribe(
    allocator: std.mem.Allocator,
) void {
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
        reportResult("mixed_write_ordering", false, "connect failed");
        return;
    };
    defer client.deinit();

    const iterations = 100;
    for (0..iterations) |i| {
        {
            var subject_buf: [48]u8 = undefined;
            const subject = std.fmt.bufPrint(
                &subject_buf,
                "ordering.publish.before.sub.{d}",
                .{i},
            ) catch {
                reportResult("mixed_write_ordering", false, "subject format failed");
                return;
            };

            client.publish(subject, "queued-before-sub") catch {
                reportResult("mixed_write_ordering", false, "publish failed");
                return;
            };

            const sub = client.subscribeSync(subject) catch {
                reportResult("mixed_write_ordering", false, "subscribe failed");
                return;
            };
            defer sub.deinit();

            client.flush(1_000_000_000) catch {
                reportResult("mixed_write_ordering", false, "flush failed");
                return;
            };

            if (sub.nextMsgTimeout(50) catch null) |msg| {
                msg.deinit();
                var detail_buf: [64]u8 = undefined;
                const detail = std.fmt.bufPrint(
                    &detail_buf,
                    "pre-sub publish delivered at iter {d}",
                    .{i},
                ) catch "unexpected pre-sub delivery";
                reportResult("mixed_write_ordering", false, detail);
                return;
            }
        }
    }

    reportResult("mixed_write_ordering", true, "");
}

fn delayedUnsubscribe(
    io: std.Io,
    sub: *nats.Subscription,
    delay_ms: i64,
) void {
    sleepMs(io, delay_ms);
    sub.unsubscribe() catch {};
}

pub fn testBlockingNextMsgUnsubscribeWakeup(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_wrap = utils.newIo(allocator);
    defer io_wrap.deinit();
    const io = io_wrap.io();

    const client = nats.Client.connect(
        allocator,
        io,
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("blocking_nextmsg_unsub", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("blocking.nextmsg.unsub") catch {
        reportResult("blocking_nextmsg_unsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    var wake_thread = io.async(delayedUnsubscribe, .{ io, sub, 20 });
    defer _ = wake_thread.cancel(io);

    const Sel = std.Io.Select(union(enum) {
        recv: anyerror!nats.Client.Message,
        timeout: void,
    });
    var buf: [2]Sel.Union = undefined;
    var sel = Sel.init(io, &buf);
    sel.async(.recv, nats.Client.Sub.nextMsg, .{sub});
    sel.async(.timeout, sleepMs, .{ io, 200 });

    const result = sel.await() catch {
        sel.cancelDiscard();
        reportResult("blocking_nextmsg_unsub", false, "select canceled");
        return;
    };
    sel.cancelDiscard();

    switch (result) {
        .recv => |recv_result| {
            if (recv_result) |msg| {
                msg.deinit();
                reportResult("blocking_nextmsg_unsub", false, "unexpected message");
            } else |err| switch (err) {
                error.Closed, error.Canceled => {
                    reportResult("blocking_nextmsg_unsub", true, "");
                },
                else => {
                    reportResult("blocking_nextmsg_unsub", false, @errorName(err));
                },
            }
        },
        .timeout => {
            reportResult("blocking_nextmsg_unsub", false, "nextMsg did not wake");
        },
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testConcurrentSubscribe(allocator);
    testRapidPublish(allocator);
    testConcurrentSubUnsub(allocator);
    testRaceSubscribeVsDelivery(allocator);
    testRaceUnsubscribeVsDelivery(allocator);
    testSidAllocationRecycling(allocator);
    testMultipleClientsSeparateIo(allocator);
    testParallelReceive(allocator);
    testRapidFlushOperations(allocator);
    testStatsConcurrency(allocator);
    testMixedWriteOrderingPublishBeforeSubscribe(allocator);
    testBlockingNextMsgUnsubscribeWakeup(allocator);
}
