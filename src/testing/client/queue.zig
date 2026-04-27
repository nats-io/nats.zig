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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit();

    const queue = "workers";
    const sub = client.queueSubscribeSync("queue.test", queue) catch {
        reportResult("queue_groups", false, "queue subscribe failed");
        return;
    };
    defer sub.deinit();

    if (sub.sid == 0) {
        reportResult("queue_groups", false, "invalid queue sid");
        return;
    }

    reportResult("queue_groups", true, "");
}

pub fn testQueueGroupDistribution(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_group_distribution", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub1 = client.queueSubscribeSync("qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    const sub2 = client.queueSubscribeSync("qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    const sub3 = client.queueSubscribeSync("qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub3 failed");
        return;
    };
    defer sub3.deinit();

    for (0..30) |_| {
        client.publish("qdist.test", "work") catch {
            reportResult("queue_group_distribution", false, "publish failed");
            return;
        };
    }

    var count1: u32 = 0;
    var count2: u32 = 0;
    var count3: u32 = 0;

    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    while (true) {
        const msg = sub1.nextMsgTimeout(50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit();
            count1 += 1;
        } else break;
    }

    while (true) {
        const msg = sub2.nextMsgTimeout(50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit();
            count2 += 1;
        } else break;
    }

    while (true) {
        const msg = sub3.nextMsgTimeout(50) catch {
            break;
        };
        if (msg) |m| {
            m.deinit();
            count3 += 1;
        } else break;
    }

    const total = count1 + count2 + count3;

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

    const io_a = utils.newIo(allocator);
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "A connect failed");
        return;
    };
    defer client_a.deinit();

    const io_b = utils.newIo(allocator);
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "B connect failed");
        return;
    };
    defer client_b.deinit();

    const io_c = utils.newIo(allocator);
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_multi_client", false, "C connect failed");
        return;
    };
    defer client_c.deinit();

    const sub_a = client_a.queueSubscribeSync(
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "A sub failed");
        return;
    };
    defer sub_a.deinit();

    const sub_b = client_b.queueSubscribeSync(
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "B sub failed");
        return;
    };
    defer sub_b.deinit();

    io_a.io().sleep(.fromMilliseconds(50), .awake) catch {};

    for (0..20) |_| {
        client_c.publish("qmc.test", "work") catch {
            reportResult("queue_multi_client", false, "publish failed");
            return;
        };
    }

    var count_a: u32 = 0;
    var count_b: u32 = 0;

    for (0..20) |_| {
        if (sub_a.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            count_a += 1;
        }
    }
    for (0..20) |_| {
        if (sub_b.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            count_b += 1;
        }
    }

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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_single_recv", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.queueSubscribeSync("qsingle.test", "solo") catch {
        reportResult("queue_single_recv", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    for (0..10) |_| {
        client.publish("qsingle.test", "msg") catch {};
    }

    var count: u32 = 0;
    for (0..15) |_| {
        const msg = sub.nextMsgTimeout(200) catch break;
        if (msg) |m| {
            m.deinit();
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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.queueSubscribeSync("qw.>", "workers") catch {
        reportResult("queue_wildcard", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.publish("qw.foo", "one") catch {};
    client.publish("qw.bar", "two") catch {};
    client.publish("qw.baz.deep", "three") catch {};

    var count: u32 = 0;
    for (0..5) |_| {
        const msg = sub.nextMsgTimeout(200) catch break;
        if (msg) |m| {
            m.deinit();
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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("multi_queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub_a = client.queueSubscribeSync("mqg.test", "group-A") catch {
        reportResult("multi_queue_groups", false, "sub A failed");
        return;
    };
    defer sub_a.deinit();

    const sub_b = client.queueSubscribeSync("mqg.test", "group-B") catch {
        reportResult("multi_queue_groups", false, "sub B failed");
        return;
    };
    defer sub_b.deinit();

    client.publish("mqg.test", "hello") catch {
        reportResult("multi_queue_groups", false, "publish failed");
        return;
    };

    var count: u32 = 0;
    if (sub_a.nextMsgTimeout(500) catch null) |m| {
        m.deinit();
        count += 1;
    }
    if (sub_b.nextMsgTimeout(500) catch null) |m| {
        m.deinit();
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

    var ios: [5]*utils.TestIo = undefined;
    for (&ios) |*io_ptr| {
        io_ptr.* = utils.newIo(allocator);
    }
    defer for (ios) |io| io.deinit();

    var clients: [5]?*nats.Client = .{ null, null, null, null, null };
    defer for (&clients) |*c| {
        if (c.*) |client| client.deinit();
    };

    for (&clients, 0..) |*c, i| {
        c.* = nats.Client.connect(allocator, ios[i].io(), url, .{ .reconnect = false }) catch {
            reportResult("four_client_queue", false, "connect failed");
            return;
        };
    }

    var subs: [4]?*nats.Subscription = .{ null, null, null, null };
    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit();
    };

    for (0..4) |i| {
        subs[i] = clients[i].?.queueSubscribeSync(
            "fourq.test",
            "workers",
        ) catch {
            reportResult("four_client_queue", false, "subscribe failed");
            return;
        };
    }

    ios[0].io().sleep(.fromMilliseconds(50), .awake) catch {};

    for (0..40) |_| {
        clients[4].?.publish("fourq.test", "work") catch {};
    }

    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    for (0..4) |i| {
        for (0..40) |_| {
            const msg = subs[i].?.nextMsgTimeout(
                100,
            ) catch break;
            if (msg) |m| {
                m.deinit();
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

pub fn testQueueMemberJoinsMidStream(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_join_midstream", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub1 = client.queueSubscribeSync("qjoin.test", "workers") catch {
        reportResult("queue_join_midstream", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    for (0..10) |_| {
        client.publish("qjoin.test", "msg") catch {};
    }

    const sub2 = client.queueSubscribeSync("qjoin.test", "workers") catch {
        reportResult("queue_join_midstream", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    for (0..10) |_| {
        client.publish("qjoin.test", "msg") catch {};
    }

    var count1: u32 = 0;
    var count2: u32 = 0;

    for (0..20) |_| {
        if (sub1.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            count1 += 1;
        }
    }
    for (0..20) |_| {
        if (sub2.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            count2 += 1;
        }
    }

    if (count1 + count2 == 20) {
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

pub fn testQueueMemberLeaves(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_member_leaves", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub1 = client.queueSubscribeSync("qleave.test", "workers") catch {
        reportResult("queue_member_leaves", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    const sub2 = client.queueSubscribeSync("qleave.test", "workers") catch {
        reportResult("queue_member_leaves", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    for (0..10) |_| {
        client.publish("qleave.test", "msg") catch {};
    }

    sub1.unsubscribe() catch {};

    for (0..10) |_| {
        client.publish("qleave.test", "msg") catch {};
    }

    var count2: u32 = 0;
    for (0..25) |_| {
        if (sub2.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            count2 += 1;
        }
    }

    if (count2 >= 10) {
        reportResult("queue_member_leaves", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{count2}) catch "e";
        reportResult("queue_member_leaves", false, detail);
    }
}

pub fn testLargeQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("large_queue_group", false, "connect failed");
        return;
    };
    defer client.deinit();

    const NUM_SUBS = 20;
    var subs: [NUM_SUBS]?*nats.Subscription = [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: usize = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit();
    };

    for (0..NUM_SUBS) |i| {
        subs[i] = client.queueSubscribeSync(
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

    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("lqg.test", "work") catch {};
    }

    var total: u32 = 0;
    for (0..NUM_SUBS) |i| {
        if (subs[i]) |sub| {
            for (0..NUM_MSGS) |_| {
                if (sub.nextMsgTimeout(50) catch null) |m| {
                    m.deinit();
                    total += 1;
                } else break;
            }
        }
    }

    if (total == NUM_MSGS) {
        reportResult("large_queue_group", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{total}) catch "e";
        reportResult("large_queue_group", false, detail);
    }
}

pub fn testQueueGroupNameValidation(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_name_validation", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub1 = client.queueSubscribeSync("qn.test1", "workers-1") catch {
        reportResult("queue_name_validation", false, "workers-1 failed");
        return;
    };
    defer sub1.deinit();

    const sub2 = client.queueSubscribeSync("qn.test2", "workers_2") catch {
        reportResult("queue_name_validation", false, "workers_2 failed");
        return;
    };
    defer sub2.deinit();

    const sub3 = client.queueSubscribeSync("qn.test3", "WorkersABC") catch {
        reportResult("queue_name_validation", false, "WorkersABC failed");
        return;
    };
    defer sub3.deinit();

    if (client.isConnected()) {
        reportResult("queue_name_validation", true, "");
    } else {
        reportResult("queue_name_validation", false, "disconnected");
    }
}

pub fn testQueueGroupFairness(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("queue_fairness", false, "connect failed");
        return;
    };
    defer client.deinit();

    const NUM_SUBS = 5;
    var subs: [NUM_SUBS]?*nats.Subscription = [_]?*nats.Subscription{null} ** NUM_SUBS;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit();
    };

    for (0..NUM_SUBS) |i| {
        subs[i] = client.queueSubscribeSync("qfair.test", "fairness") catch {
            reportResult("queue_fairness", false, "subscribe failed");
            return;
        };
    }

    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("qfair.test", "msg") catch {};
    }

    var counts: [NUM_SUBS]u32 = [_]u32{0} ** NUM_SUBS;
    for (0..NUM_SUBS) |i| {
        if (subs[i]) |sub| {
            for (0..NUM_MSGS) |_| {
                if (sub.nextMsgTimeout(50) catch null) |m| {
                    m.deinit();
                    counts[i] += 1;
                } else break;
            }
        }
    }

    var total: u32 = 0;
    for (counts) |c| total += c;

    if (total != NUM_MSGS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "total={d}/100", .{total}) catch "e";
        reportResult("queue_fairness", false, detail);
        return;
    }

    var min_count: u32 = NUM_MSGS;
    for (counts) |c| {
        if (c < min_count) min_count = c;
    }

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
