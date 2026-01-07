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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    client.flush() catch {
        reportResult("queue_groups", false, "flush failed");
        return;
    };

    reportResult("queue_groups", true, "");
}

pub fn testQueueGroupDistribution(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    client.flush() catch {};

    // Publish 30 messages
    for (0..30) |_| {
        client.publish("qdist.test", "work") catch {
            reportResult("queue_group_distribution", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Count how many each receives
    var count1: u32 = 0;
    var count2: u32 = 0;
    var count3: u32 = 0;

    // Give time for messages to be distributed
    std.posix.nanosleep(0, 100_000_000); // 100ms

    while (true) {
        const msg = sub1.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count1 += 1;
        } else break;
    }

    while (true) {
        const msg = sub2.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count2 += 1;
        } else break;
    }

    while (true) {
        const msg = sub3.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
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
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("queue_multi_client", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("queue_multi_client", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C (publisher)
    var io_c: std.Io.Threaded = .init(allocator, .{});
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{}) catch {
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

    client_a.flush() catch {};
    client_b.flush() catch {};
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // C publishes 20 messages
    for (0..20) |_| {
        client_c.publish("qmc.test", "work") catch {
            reportResult("queue_multi_client", false, "publish failed");
            return;
        };
    }
    client_c.flush() catch {};

    // Count messages received by each
    var count_a: u32 = 0;
    var count_b: u32 = 0;

    for (0..20) |_| {
        if (sub_a.nextMessage(allocator, .{ .timeout_ms = 100 }) catch null) |m| {
            m.deinit(allocator);
            count_a += 1;
        }
    }
    for (0..20) |_| {
        if (sub_b.nextMessage(allocator, .{ .timeout_ms = 100 }) catch null) |m| {
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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
    client.flush() catch {};

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("qsingle.test", "msg") catch {};
    }
    client.flush() catch {};

    // Should receive all 10
    var count: u32 = 0;
    for (0..15) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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
    client.flush() catch {};

    // Publish to various subjects
    client.publish("qw.foo", "one") catch {};
    client.publish("qw.bar", "two") catch {};
    client.publish("qw.baz.deep", "three") catch {};
    client.flush() catch {};

    var count: u32 = 0;
    for (0..5) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    client.flush() catch {};

    // Publish one message
    client.publish("mqg.test", "hello") catch {
        reportResult("multi_queue_groups", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Both groups should receive (each group gets a copy)
    var count: u32 = 0;
    if (sub_a.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub_b.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
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
        io_ptr.* = .init(allocator, .{});
    }
    defer for (&ios) |*io_ptr| io_ptr.deinit();

    var clients: [5]?*nats.Client = .{ null, null, null, null, null };
    defer for (&clients) |*c| {
        if (c.*) |client| client.deinit(allocator);
    };

    for (&clients, 0..) |*c, i| {
        c.* = nats.Client.connect(allocator, ios[i].io(), url, .{}) catch {
            reportResult("four_client_queue", false, "connect failed");
            return;
        };
    }

    // First 4 clients subscribe to queue
    var subs: [4]?*nats.Subscription(nats.Client) = .{ null, null, null, null };
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
        clients[i].?.flush() catch {};
    }

    std.posix.nanosleep(0, 50_000_000); // 50ms settle

    // Publisher sends 40 messages
    for (0..40) |_| {
        clients[4].?.publish("fourq.test", "work") catch {};
    }
    clients[4].?.flush() catch {};

    // Count per subscriber
    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    for (0..4) |i| {
        for (0..40) |_| {
            const msg = subs[i].?.nextMessage(
                allocator,
                .{ .timeout_ms = 100 },
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

/// Runs all queue group tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testQueueGroups(allocator);
    testQueueGroupDistribution(allocator);
    testQueueGroupMultipleClients(allocator);
    testQueueGroupSingleReceiver(allocator);
    testQueueWithWildcard(allocator);
    testMultipleQueueGroups(allocator);
    testFourClientQueueGroup(allocator);
}
