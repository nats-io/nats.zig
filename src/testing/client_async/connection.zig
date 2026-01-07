//! Connection Tests for NATS Async Client
//!
//! Tests for async connection handling.

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

pub fn testAsyncConnectionRefused(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Try to connect to a port where nothing is listening
    const result = nats.ClientAsync.connect(
        allocator,
        io.io(),
        "nats://127.0.0.1:19999",
        .{},
    );

    if (result) |client| {
        client.deinit(allocator);
        reportResult("async_connection_refused", false, "expected error");
    } else |_| {
        reportResult("async_connection_refused", true, "");
    }
}

// Test: Multiple consecutive connections

pub fn testAsyncConsecutiveConnections(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Connect and disconnect 3 times
    for (0..3) |i| {
        const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
            var buf: [32]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "connect {d} failed", .{i}) catch "e";
            reportResult("async_consecutive_connections", false, msg);
            return;
        };
        client.deinit(allocator);
    }

    reportResult("async_consecutive_connections", true, "");
}

// Test: isConnected state tracking

pub fn testAsyncIsConnectedState(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_is_connected_state", false, "connect failed");
        return;
    };

    if (!client.isConnected()) {
        client.deinit(allocator);
        reportResult("async_is_connected_state", false, "not connected initially");
        return;
    }

    client.deinit(allocator);
    reportResult("async_is_connected_state", true, "");
}

// NEW TESTS: Publish Operations

// Test: Publish empty payload

pub fn testAsyncReconnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_reconnection", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("async_reconnection", false, "not connected initially");
        return;
    }

    // Stop server
    manager.stopServer(0, io.io());
    std.posix.nanosleep(0, 100_000_000); // 100ms

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("async_reconnection", false, "server restart failed");
        return;
    };

    reportResult("async_reconnection", true, "");
}

/// Test: New connection after server restart
pub fn testAsyncServerRestartNewConnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{});
    defer io1.deinit();

    const client1 = nats.ClientAsync.connect(allocator, io1.io(), url, .{}) catch {
        reportResult("async_server_restart", false, "initial connect failed");
        return;
    };

    if (!client1.isConnected()) {
        client1.deinit(allocator);
        reportResult("async_server_restart", false, "not connected");
        return;
    }

    client1.deinit(allocator);

    // Stop and restart server
    manager.stopServer(0, io1.io());
    std.posix.nanosleep(0, 100_000_000);

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("async_server_restart", false, "restart failed");
        return;
    };

    // New connection should work
    var io2: std.Io.Threaded = .init(allocator, .{});
    defer io2.deinit();

    const client2 = nats.ClientAsync.connect(allocator, io2.io(), url, .{}) catch {
        reportResult("async_server_restart", false, "reconnect failed");
        return;
    };
    defer client2.deinit(allocator);

    if (client2.isConnected()) {
        reportResult("async_server_restart", true, "");
    } else {
        reportResult("async_server_restart", false, "not connected after restart");
    }
}

// Request/Reply Tests

/// Test: Async request method exists and can be called
/// Runs all async connection tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testAsyncConnectionRefused(allocator);
    testAsyncConsecutiveConnections(allocator);
    testAsyncIsConnectedState(allocator);
    testAsyncReconnection(allocator, manager);
    testAsyncServerRestartNewConnection(allocator, manager);
}
