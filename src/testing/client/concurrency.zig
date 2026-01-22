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

pub fn testConcurrentSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    const NUM_SUBS = 10;
    var subs: [NUM_SUBS]?*nats.Subscription =
        [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: u32 = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..NUM_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "concurrent.sub.{d}",
            .{i},
        ) catch continue;

        subs[i] = client.subscribe(allocator, subject) catch {
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "rapid.publish") catch {
        reportResult("rapid_publish", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("rapid_publish", false, "flush1 failed");
        return;
    };

    const NUM_MSGS = 100;
    var published: u32 = 0;
    for (0..NUM_MSGS) |_| {
        client.publish("rapid.publish", "data") catch {
            continue;
        };
        published += 1;
    }

    client.flush(allocator) catch {
        reportResult("rapid_publish", false, "flush2 failed");
        return;
    };

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
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    const CYCLES = 20;
    var current_sub: ?*nats.Subscription = null;

    for (0..CYCLES) |i| {
        if (current_sub) |sub| {
            sub.unsubscribe() catch {};
            sub.deinit(allocator);
            current_sub = null;
        }

        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "cycle.{d}",
            .{i},
        ) catch continue;

        current_sub = client.subscribe(allocator, subject) catch {
            reportResult("concurrent_sub_unsub", false, "subscribe failed");
            return;
        };
    }

    if (current_sub) |sub| {
        sub.deinit(allocator);
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("race_sub_delivery", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const subscriber = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("race_sub_delivery", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit(allocator);

    const sub = subscriber.subscribe(allocator, "race.delivery") catch {
        reportResult("race_sub_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    subscriber.flush(allocator) catch {};

    publisher.publish("race.delivery", "race-msg-1") catch {
        reportResult("race_sub_delivery", false, "publish1 failed");
        return;
    };
    publisher.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("race.delivery", "race-msg-2") catch {
        reportResult("race_sub_delivery", false, "publish2 failed");
        return;
    };
    publisher.flush(allocator) catch {};

    var received: u32 = 0;
    for (0..2) |_| {
        if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
            m.deinit(allocator);
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 64,
        .reconnect = false,
    }) catch {
        reportResult("race_unsub_delivery", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "race.unsub") catch {
        reportResult("race_unsub_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("race_unsub_delivery", false, "flush1 failed");
        return;
    };

    for (0..50) |_| {
        client.publish("race.unsub", "msg") catch {};
    }
    client.flush(allocator) catch {};

    sub.unsubscribe() catch {};

    for (0..50) |_| {
        client.publish("race.unsub", "msg") catch {};
    }
    client.flush(allocator) catch {};

    if (client.isConnected()) {
        reportResult("race_unsub_delivery", true, "");
    } else {
        reportResult("race_unsub_delivery", false, "disconnected");
    }
}

pub fn testSidAllocationRecycling(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    var seen_sids: [100]u64 = undefined;
    var seen_count: usize = 0;

    for (0..50) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "recycle.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("sid_allocation_recycle", false, "subscribe failed");
            return;
        };

        if (seen_count < seen_sids.len) {
            for (seen_sids[0..seen_count]) |prev_sid| {
                if (prev_sid == sub.sid) {
                    reportResult("sid_allocation_recycle", false, "SID reused");
                    sub.deinit(allocator);
                    return;
                }
            }
            seen_sids[seen_count] = sub.sid;
            seen_count += 1;
        }

        sub.deinit(allocator);
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

pub fn testMultipleClientsSharedIo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_shared_io", false, "client1 failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_shared_io", false, "client2 failed");
        return;
    };
    defer client2.deinit(allocator);

    const client3 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_client_shared_io", false, "client3 failed");
        return;
    };
    defer client3.deinit(allocator);

    const sub = client1.subscribe(allocator, "shared.io.test") catch {
        reportResult("multi_client_shared_io", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client1.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    client2.publish("shared.io.test", "from-client2") catch {
        reportResult("multi_client_shared_io", false, "pub2 failed");
        return;
    };
    client3.publish("shared.io.test", "from-client3") catch {
        reportResult("multi_client_shared_io", false, "pub3 failed");
        return;
    };

    client2.flush(allocator) catch {};
    client3.flush(allocator) catch {};

    var received: u32 = 0;
    for (0..2) |_| {
        if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 2) {
        reportResult("multi_client_shared_io", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{received}) catch "e";
        reportResult("multi_client_shared_io", false, detail);
    }
}

pub fn testParallelReceive(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "parallel.1") catch {
        reportResult("parallel_recv", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "parallel.2") catch {
        reportResult("parallel_recv", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "parallel.3") catch {
        reportResult("parallel_recv", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("parallel_recv", false, "flush1 failed");
        return;
    };

    client.publish("parallel.1", "msg1") catch {};
    client.publish("parallel.2", "msg2") catch {};
    client.publish("parallel.3", "msg3") catch {};
    client.flush(allocator) catch {};

    var received: u32 = 0;

    if (sub1.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        received += 1;
    }

    if (sub2.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        received += 1;
    }

    if (sub3.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    var success: u32 = 0;
    for (0..50) |_| {
        client.flush(allocator) catch {
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
        client.flush(allocator) catch {};
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stats.test") catch {
        reportResult("stats_concurrency", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    const before = client.getStats();

    const NUM_MSGS: u64 = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("stats.test", "stat-msg") catch {};
    }
    client.flush(allocator) catch {};

    for (0..NUM_MSGS) |_| {
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
        } else break;
    }

    const after = client.getStats();

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

pub fn runAll(allocator: std.mem.Allocator) void {
    testConcurrentSubscribe(allocator);
    testRapidPublish(allocator);
    testConcurrentSubUnsub(allocator);
    testRaceSubscribeVsDelivery(allocator);
    testRaceUnsubscribeVsDelivery(allocator);
    testSidAllocationRecycling(allocator);
    testMultipleClientsSharedIo(allocator);
    testParallelReceive(allocator);
    testRapidFlushOperations(allocator);
    testStatsConcurrency(allocator);
}
