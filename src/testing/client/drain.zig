//! Drain Tests for NATS  Client

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

pub fn testDrainOperation(allocator: std.mem.Allocator) void {
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
        reportResult("drain_operation", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "drain.test.1") catch {
        reportResult("drain_operation", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "drain.test.2") catch {
        reportResult("drain_operation", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush(allocator) catch {};

    _ = client.drain(allocator) catch {
        reportResult("drain_operation", false, "drain failed");
        return;
    };

    if (!client.isConnected()) {
        reportResult("drain_operation", true, "");
    } else {
        reportResult("drain_operation", false, "still connected");
    }
}

pub fn testDrainCleansUp(allocator: std.mem.Allocator) void {
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
        reportResult("drain_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "drain.cleanup.1") catch {
        reportResult("drain_cleanup", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "drain.cleanup.2") catch {
        reportResult("drain_cleanup", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.publish("drain.cleanup.1", "msg1") catch {};
    client.publish("drain.cleanup.2", "msg2") catch {};
    client.flush(allocator) catch {};

    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    _ = client.drain(allocator) catch {
        reportResult("drain_cleanup", false, "drain failed");
        return;
    };

    if (!client.isConnected()) {
        reportResult("drain_cleanup", true, "");
    } else {
        reportResult("drain_cleanup", false, "still connected after drain");
    }
}

pub fn testDrainTwice(allocator: std.mem.Allocator) void {
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
        reportResult("drain_twice", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    _ = client.drain(allocator) catch {
        reportResult("drain_twice", false, "first drain failed");
        return;
    };

    _ = client.drain(allocator) catch {};

    reportResult("drain_twice", true, "");
}

pub fn testDrainWithManySubscriptions(allocator: std.mem.Allocator) void {
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
        reportResult("drain_many_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var subs: [20]?*nats.Subscription = undefined;
    @memset(&subs, null);

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    var created: usize = 0;
    for (0..20) |i| {
        var sub_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &sub_buf,
            "drain.many.{d}",
            .{i},
        ) catch {
            continue;
        };
        subs[i] = client.subscribe(allocator, subject) catch break;
        created += 1;
    }

    client.flush(allocator) catch {};

    _ = client.drain(allocator) catch {
        reportResult("drain_many_subs", false, "drain failed");
        return;
    };

    if (!client.isConnected() and created >= 15) {
        reportResult("drain_many_subs", true, "");
    } else {
        reportResult("drain_many_subs", false, "unexpected state");
    }
}

/// Test subscription waitDrained with messages consumed.
pub fn testSubWaitDrained(allocator: std.mem.Allocator) void {
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
        reportResult("sub_wait_drained", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "wait.drained") catch {
        reportResult("sub_wait_drained", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish some messages
    for (0..5) |_| {
        client.publish("wait.drained", "data") catch {};
    }
    client.flush(allocator) catch {};

    // Wait for messages to arrive
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Start draining
    sub.drain() catch {
        reportResult("sub_wait_drained", false, "drain failed");
        return;
    };

    // Consume all messages
    for (0..10) |_| {
        const msg = sub.tryNext();
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    // Now wait for drain to complete (should succeed immediately)
    sub.waitDrained(1000) catch |err| {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "waitDrained: {s}", .{
            @errorName(err),
        }) catch "e";
        reportResult("sub_wait_drained", false, detail);
        return;
    };

    // Queue should be empty now
    if (sub.pending() == 0) {
        reportResult("sub_wait_drained", true, "");
    } else {
        reportResult("sub_wait_drained", false, "queue not empty");
    }
}

/// Test waitDrained returns error.NotDraining if not draining.
pub fn testWaitDrainedNotDraining(allocator: std.mem.Allocator) void {
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
        reportResult("wait_not_draining", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "not.draining") catch {
        reportResult("wait_not_draining", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Try waitDrained without calling drain() first
    sub.waitDrained(100) catch |err| {
        if (err == error.NotDraining) {
            reportResult("wait_not_draining", true, "");
            return;
        }
    };

    reportResult("wait_not_draining", false, "expected NotDraining");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testDrainOperation(allocator);
    testDrainCleansUp(allocator);
    testDrainTwice(allocator);
    testDrainWithManySubscriptions(allocator);
    testSubWaitDrained(allocator);
    testWaitDrainedNotDraining(allocator);
}
