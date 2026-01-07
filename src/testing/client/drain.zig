//! Drain Tests for NATS Client
//!
//! Tests for drain operations.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testDrainOperation(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("drain_operation", false, "connect failed");
        return;
    };
    // deinit still required after drain (user may have defer pattern)
    defer client.deinit(allocator);

    // Create some subscriptions
    const sub1 = client.subscribe(allocator, "drain.test1") catch {
        reportResult("drain_operation", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.test2") catch {
        reportResult("drain_operation", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.flush() catch {};

    // Drain should clean up subscriptions and close connection
    client.drain(allocator) catch {
        reportResult("drain_operation", false, "drain failed");
        return;
    };

    // After drain, client should not be connected
    if (!client.isConnected()) {
        reportResult("drain_operation", true, "");
    } else {
        reportResult("drain_operation", false, "still connected after drain");
    }
}

pub fn testDrainCleansUp(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("drain_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe (will be cleaned by drain)
    _ = client.subscribe(allocator, "drainclean.test") catch {
        reportResult("drain_cleanup", false, "subscribe failed");
        return;
    };
    client.flush() catch {};

    // Publish some messages
    for (0..5) |_| {
        client.publish("drainclean.test", "pending") catch {};
    }
    client.flush() catch {};

    // Drain - this cleans up subscriptions
    client.drain(allocator) catch {
        reportResult("drain_cleanup", false, "drain failed");
        return;
    };

    // After drain, client should not be connected
    if (!client.isConnected()) {
        reportResult("drain_cleanup", true, "");
    } else {
        reportResult("drain_cleanup", false, "still connected");
    }
}

// Test 83: Subject token validation

/// Runs all drain tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testDrainOperation(allocator);
    testDrainCleansUp(allocator);
}
