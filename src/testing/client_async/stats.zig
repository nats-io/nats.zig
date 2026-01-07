//! Stats Tests for NATS Async Client
//!
//! Tests for async statistics.

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

pub fn testClientAsyncStats(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_stats = client.getStats();
    if (initial_stats.msgs_out != 0) {
        reportResult("client_async_stats", false, "initial msgs_out != 0");
        return;
    }

    client.publish("async.stats", "test") catch {};
    client.flush() catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1) {
        reportResult("client_async_stats", true, "");
    } else {
        reportResult("client_async_stats", false, "msgs_out not incremented");
    }
}

// ClientAsync Test 8: Server info available

pub fn testAsyncStatsIncrement(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stats_increment", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("async.stats.inc", "msg") catch {};
    }
    client.flush() catch {};

    const after = client.getStats();

    if (after.msgs_out >= before.msgs_out + 10) {
        reportResult("async_stats_increment", true, "");
    } else {
        reportResult("async_stats_increment", false, "stats not incremented");
    }
}

// Test: Bytes accuracy

pub fn testAsyncStatsBytesAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stats_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 100 bytes
    const payload = "0123456789" ** 10; // 100 bytes
    client.publish("async.stats.bytes", payload) catch {};
    client.flush() catch {};

    const after = client.getStats();

    // bytes_out should increase by at least 100
    if (after.bytes_out >= before.bytes_out + 100) {
        reportResult("async_stats_bytes", true, "");
    } else {
        reportResult("async_stats_bytes", false, "bytes not tracked");
    }
}

// NEW TESTS: Stress Tests

// Test: 500 message stress test

/// Runs all async stats tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testClientAsyncStats(allocator);
    testAsyncStatsIncrement(allocator);
    testAsyncStatsBytesAccuracy(allocator);
}
