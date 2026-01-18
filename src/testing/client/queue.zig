//! Queue Group Tests for NATS Client
//!
//! Tests for queue group subscriptions and message distribution.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testQueueGroups(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const queue = "workers";
    const sub = client.subscribeQueue(allocator, "queue.test", queue) catch {
        reportResult("queue_groups", false, "queue subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    if (sub.sid == 0) {
        reportResult("queue_groups", false, "invalid queue sid");
        return;
    }

    client.flush(allocator) catch {
        reportResult("queue_groups", false, "flush failed");
        return;
    };

    reportResult("queue_groups", true, "");
}

pub fn testQueueGroupDistribution(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_group_distribution", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 3 queue group subscribers
    const sub1 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush(allocator) catch {};

    // Publish 30 messages
    for (0..30) |_| {
        client.publish("qdist.test", "work") catch {
            reportResult("queue_group_distribution", false, "publish failed");
            return;
        };
    }
    client.flush(allocator) catch {};

    // Count how many each receives
    var count1: u32 = 0;
    var count2: u32 = 0;
    var count3: u32 = 0;

    // Give time for messages to be distributed
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    while (true) {
        const msg = sub1.nextWithTimeout(allocator, 50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count1 += 1;
        } else break;
    }

    while (true) {
        const msg = sub2.nextWithTimeout(allocator, 50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count2 += 1;
        } else break;
    }

    while (true) {
        const msg = sub3.nextWithTimeout(allocator, 50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count3 += 1;
        } else break;
    }

    const total = count1 + count2 + count3;

    // All 30 should be received exactly once across all queue members
    if (total == 30) {
        reportResult("queue_group_distribution", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "total={d} (expected 30)",
            .{total},
        ) catch "error";
        reportResult("queue_group_distribution", false, detail);
    }
}

pub fn testQueueGroupMultipleClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A
    var io_a: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B
    var io_b: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C (publisher)
    var io_c: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "C connect failed");
        return;
    };
    defer client_c.deinit(allocator);

    // A and B subscribe to queue
    const sub_a = client_a.subscribeQueue(
        allocator,
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "A sub failed");
        return;
    };
    defer sub_a.deinit(allocator);

    const sub_b = client_b.subscribeQueue(
        allocator,
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "B sub failed");
        return;
    };
    defer sub_b.deinit(allocator);

    client_a.flush(allocator) catch {};
    client_b.flush(allocator) catch {};
    io_a.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // C publishes 20 messages
    for (0..20) |_| {
        client_c.publish("qmc.test", "work") catch {
            reportResult("queue_multi_client", false, "publish failed");
            return;
        };
    }
    client_c.flush(allocator) catch {};

    // Count messages received by each
    var count_a: u32 = 0;
    var count_b: u32 = 0;

    for (0..20) |_| {
        if (sub_a.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            count_a += 1;
        }
    }
    for (0..20) |_| {
        if (sub_b.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            count_b += 1;
        }
    }

    // Total should be 20 (distributed between A and B)
    if (count_a + count_b == 20) {
        reportResult("queue_multi_client", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}={d}",
            .{ count_a, count_b, count_a + count_b },
        ) catch "err";
        reportResult("queue_multi_client", false, detail);
    }
}

pub fn testQueueGroupSingleReceiver(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_single_recv", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Single subscriber in queue group
    const sub = client.subscribeQueue(allocator, "qsingle.test", "solo") catch {
        reportResult("queue_single_recv", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("qsingle.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // Should receive all 10
    var count: u32 = 0;
    for (0..15) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }

    if (count == 10) {
        reportResult("queue_single_recv", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/10", .{count}) catch "e";
        reportResult("queue_single_recv", false, detail);
    }
}

pub fn testQueueWithWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to wildcard with queue group
    const sub = client.subscribeQueue(allocator, "qw.>", "workers") catch {
        reportResult("queue_wildcard", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish to various subjects
    client.publish("qw.foo", "one") catch {};
    client.publish("qw.bar", "two") catch {};
    client.publish("qw.baz.deep", "three") catch {};
    client.flush(allocator) catch {};

    var count: u32 = 0;
    for (0..5) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }

    if (count == 3) {
        reportResult("queue_wildcard", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/3", .{count}) catch "e";
        reportResult("queue_wildcard", false, detail);
    }
}

pub fn testMultipleQueueGroups(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("multi_queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two different queue groups on same subject
    const sub_a = client.subscribeQueue(allocator, "mqg.test", "group-A") catch {
        reportResult("multi_queue_groups", false, "sub A failed");
        return;
    };
    defer sub_a.deinit(allocator);

    const sub_b = client.subscribeQueue(allocator, "mqg.test", "group-B") catch {
        reportResult("multi_queue_groups", false, "sub B failed");
        return;
    };
    defer sub_b.deinit(allocator);

    client.flush(allocator) catch {};

    // Publish one message
    client.publish("mqg.test", "hello") catch {
        reportResult("multi_queue_groups", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    // Both groups should receive (each group gets a copy)
    var count: u32 = 0;
    if (sub_a.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub_b.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 2) {
        reportResult("multi_queue_groups", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("multi_queue_groups", false, detail);
    }
}

pub fn testFourClientQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Create 4 subscriber clients + 1 publisher
    var ios: [5]std.Io.Threaded = undefined;
    for (&ios) |*io_ptr| {
        io_ptr.* = .init(allocator, .{ .environ = .empty });
    }
    defer for (&ios) |*io_ptr| io_ptr.deinit();

    var clients: [5]?*nats.Client = .{ null, null, null, null, null };
    defer for (&clients) |*c| {
        if (c.*) |client| client.deinit(allocator);
    };

    for (&clients, 0..) |*c, i| {
        c.* = nats.Client.connect(allocator, ios[i].io(), url, .{ .reconnect = false }) catch {
            reportResult("four_client_queue", false, "connect failed");
            return;
        };
    }

    // First 4 clients subscribe to queue
    var subs: [4]?*nats.Subscription = .{ null, null, null, null };
    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..4) |i| {
        subs[i] = clients[i].?.subscribeQueue(
            allocator,
            "fourq.test",
            "workers",
        ) catch {
            reportResult("four_client_queue", false, "subscribe failed");
            return;
        };
        clients[i].?.flush(allocator) catch {};
    }

    ios[0].io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Publisher sends 40 messages
    for (0..40) |_| {
        clients[4].?.publish("fourq.test", "work") catch {};
    }
    clients[4].?.flush(allocator) catch {};

    // Count per subscriber
    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    for (0..4) |i| {
        for (0..40) |_| {
            const msg = subs[i].?.nextWithTimeout(
                allocator,
                100,
            ) catch break;
            if (msg) |m| {
                m.deinit(allocator);
                counts[i] += 1;
            } else break;
        }
    }

    const total = counts[0] + counts[1] + counts[2] + counts[3];
    if (total == 40) {
        reportResult("four_client_queue", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}+{d}+{d}={d}",
            .{ counts[0], counts[1], counts[2], counts[3], total },
        ) catch "e";
        reportResult("four_client_queue", false, detail);
    }
}

// Test: Queue member joins mid-stream
// Verifies new member can join and receive messages.
pub fn testQueueMemberJoinsMidStream(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_join_midstream", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // First subscriber
    const sub1 = client.subscribeQueue(allocator, "qjoin.test", "workers") catch {
        reportResult("queue_join_midstream", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish some messages (sub1 should get all)
    for (0..10) |_| {
        client.publish("qjoin.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // Second subscriber joins
    const sub2 = client.subscribeQueue(allocator, "qjoin.test", "workers") catch {
        reportResult("queue_join_midstream", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish more messages (should be distributed)
    for (0..10) |_| {
        client.publish("qjoin.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // Count messages
    var count1: u32 = 0;
    var count2: u32 = 0;

    for (0..20) |_| {
        if (sub1.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            count1 += 1;
        }
    }
    for (0..20) |_| {
        if (sub2.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            count2 += 1;
        }
    }

    // Total should be 20
    if (count1 + count2 == 20) {
        // sub2 should have received some of the second batch
        if (count2 > 0) {
            reportResult("queue_join_midstream", true, "");
        } else {
            reportResult("queue_join_midstream", false, "sub2 got 0");
        }
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "total={d} (expect 20)",
            .{count1 + count2},
        ) catch "e";
        reportResult("queue_join_midstream", false, detail);
    }
}

// Test: Queue member leaves mid-stream
// Verifies remaining members continue to receive.
pub fn testQueueMemberLeaves(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_member_leaves", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two subscribers
    const sub1 = client.subscribeQueue(allocator, "qleave.test", "workers") catch {
        reportResult("queue_member_leaves", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribeQueue(allocator, "qleave.test", "workers") catch {
        reportResult("queue_member_leaves", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("qleave.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // sub1 leaves
    sub1.unsubscribe() catch {};
    client.flush(allocator) catch {};

    // Publish 10 more (only sub2 should receive)
    for (0..10) |_| {
        client.publish("qleave.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // sub2 should receive at least the second batch
    var count2: u32 = 0;
    for (0..25) |_| {
        if (sub2.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            count2 += 1;
        }
    }

    // sub2 should have at least 10 (the second batch)
    if (count2 >= 10) {
        reportResult("queue_member_leaves", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{count2}) catch "e";
        reportResult("queue_member_leaves", false, detail);
    }
}

// Test: Large queue group
// Verifies queue works with many members.
pub fn testLargeQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("large_queue_group", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 20 queue group subscribers
    const NUM_SUBS = 20;
    var subs: [NUM_SUBS]?*nats.Subscription = [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: usize = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..NUM_SUBS) |i| {
        subs[i] = client.subscribeQueue(
            allocator,
            "lqg.test",
            "big-workers",
        ) catch {
            break;
        };
        created += 1;
    }

    if (created != NUM_SUBS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "created {d}/20", .{created}) catch "e";
        reportResult("large_queue_group", false, detail);
        return;
    }

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Publish 100 messages
    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("lqg.test", "work") catch {};
    }
    client.flush(allocator) catch {};

    // Count total received across all subscribers
    var total: u32 = 0;
    for (0..NUM_SUBS) |i| {
        if (subs[i]) |sub| {
            for (0..NUM_MSGS) |_| {
                if (sub.nextWithTimeout(allocator, 50) catch null) |m| {
                    m.deinit(allocator);
                    total += 1;
                } else break;
            }
        }
    }

    // All 100 messages should be received exactly once
    if (total == NUM_MSGS) {
        reportResult("large_queue_group", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{total}) catch "e";
        reportResult("large_queue_group", false, detail);
    }
}

// Test: Queue group name validation
// Verifies queue group names with various characters work.
pub fn testQueueGroupNameValidation(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_name_validation", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Valid queue names
    const sub1 = client.subscribeQueue(allocator, "qn.test1", "workers-1") catch {
        reportResult("queue_name_validation", false, "workers-1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribeQueue(allocator, "qn.test2", "workers_2") catch {
        reportResult("queue_name_validation", false, "workers_2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribeQueue(allocator, "qn.test3", "WorkersABC") catch {
        reportResult("queue_name_validation", false, "WorkersABC failed");
        return;
    };
    defer sub3.deinit(allocator);

    // All should be connected
    if (client.isConnected()) {
        reportResult("queue_name_validation", true, "");
    } else {
        reportResult("queue_name_validation", false, "disconnected");
    }
}

// Test: Queue group fairness (rough distribution)
// Verifies messages are roughly evenly distributed.
pub fn testQueueGroupFairness(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_fairness", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // 5 subscribers
    const NUM_SUBS = 5;
    var subs: [NUM_SUBS]?*nats.Subscription = [_]?*nats.Subscription{null} ** NUM_SUBS;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..NUM_SUBS) |i| {
        subs[i] = client.subscribeQueue(allocator, "qfair.test", "fairness") catch {
            reportResult("queue_fairness", false, "subscribe failed");
            return;
        };
    }

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Publish 100 messages
    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("qfair.test", "msg") catch {};
    }
    client.flush(allocator) catch {};

    // Count per subscriber
    var counts: [NUM_SUBS]u32 = [_]u32{0} ** NUM_SUBS;
    for (0..NUM_SUBS) |i| {
        if (subs[i]) |sub| {
            for (0..NUM_MSGS) |_| {
                if (sub.nextWithTimeout(allocator, 50) catch null) |m| {
                    m.deinit(allocator);
                    counts[i] += 1;
                } else break;
            }
        }
    }

    // Total should be 100
    var total: u32 = 0;
    for (counts) |c| total += c;

    if (total != NUM_MSGS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "total={d}/100", .{total}) catch "e";
        reportResult("queue_fairness", false, detail);
        return;
    }

    // Check roughly fair distribution (each should get at least 10%)
    var min_count: u32 = NUM_MSGS;
    for (counts) |c| {
        if (c < min_count) min_count = c;
    }

    // With 5 subscribers and 100 messages, expect at least 5 per subscriber
    // (allowing for some variation)
    if (min_count >= 5) {
        reportResult("queue_fairness", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "min={d} (expect >= 5)",
            .{min_count},
        ) catch "e";
        reportResult("queue_fairness", false, detail);
    }
}

/// Runs all queue group tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testQueueGroups(allocator);
    testQueueGroupDistribution(allocator);
    testQueueGroupMultipleClients(allocator);
    testQueueGroupSingleReceiver(allocator);
    testQueueWithWildcard(allocator);
    testMultipleQueueGroups(allocator);
    testFourClientQueueGroup(allocator);
    testQueueMemberJoinsMidStream(allocator);
    testQueueMemberLeaves(allocator);
    testLargeQueueGroup(allocator);
    testQueueGroupNameValidation(allocator);
    testQueueGroupFairness(allocator);
}
