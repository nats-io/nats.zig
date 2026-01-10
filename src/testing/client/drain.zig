//! Drain Tests for NATS Async Client
//!
//! Tests for async drain operations.

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

pub fn testAsyncDrainOperation(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_drain_operation", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create some subscriptions
    const sub1 = client.subscribe(allocator, "drain.test.1") catch {
        reportResult("async_drain_operation", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.test.2") catch {
        reportResult("async_drain_operation", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.flush() catch {};

    // Drain should clean up everything
    client.drain(allocator) catch {
        reportResult("async_drain_operation", false, "drain failed");
        return;
    };

    // After drain, client should not be connected
    if (!client.isConnected()) {
        reportResult("async_drain_operation", true, "");
    } else {
        reportResult("async_drain_operation", false, "still connected");
    }
}

/// Test: Drain cleans up subscriptions
pub fn testAsyncDrainCleansUp(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_drain_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create subscriptions and publish some messages
    const sub1 = client.subscribe(allocator, "drain.cleanup.1") catch {
        reportResult("async_drain_cleanup", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.cleanup.2") catch {
        reportResult("async_drain_cleanup", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.publish("drain.cleanup.1", "msg1") catch {};
    client.publish("drain.cleanup.2", "msg2") catch {};
    client.flush() catch {};

    // Small delay for messages to arrive
    std.posix.nanosleep(0, 50_000_000);

    // Drain - should clean up all subscriptions and close connection
    client.drain(allocator) catch {
        reportResult("async_drain_cleanup", false, "drain failed");
        return;
    };

    // Verify state
    if (!client.isConnected()) {
        reportResult("async_drain_cleanup", true, "");
    } else {
        reportResult("async_drain_cleanup", false, "still connected after drain");
    }
}

pub fn testDrainTwice(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("drain_twice", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // First drain
    client.drain(allocator) catch {
        reportResult("drain_twice", false, "first drain failed");
        return;
    };

    // Second drain should be safe (no-op or error, but not crash)
    client.drain(allocator) catch {};

    reportResult("drain_twice", true, "");
}

pub fn testDrainWithManySubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("drain_many_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 20 subscriptions
    var subs: [20]?*nats.Subscription = undefined;
    @memset(&subs, null);

    var created: usize = 0;
    for (0..20) |i| {
        var sub_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(&sub_buf, "drain.many.{d}", .{i}) catch {
            continue;
        };
        subs[i] = client.subscribe(allocator, subject) catch break;
        created += 1;
    }

    client.flush() catch {};

    // Drain should clean up all subscriptions
    client.drain(allocator) catch {
        // Cleanup any created subscriptions
        for (&subs) |*s| {
            if (s.*) |sub| sub.deinit(allocator);
        }
        reportResult("drain_many_subs", false, "drain failed");
        return;
    };

    // After drain, no need to cleanup subs - drain handles it
    if (!client.isConnected() and created >= 15) {
        reportResult("drain_many_subs", true, "");
    } else {
        reportResult("drain_many_subs", false, "unexpected state");
    }
}

/// Runs all async drain tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncDrainOperation(allocator);
    testAsyncDrainCleansUp(allocator);
    testDrainTwice(allocator);
    testDrainWithManySubscriptions(allocator);
}
