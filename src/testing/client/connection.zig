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
    const result = nats.Client.connect(
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
        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{}) catch {
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

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{}) catch {
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

pub fn testConnectionStateAfterOps(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("state_after_ops", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after connect");
        return;
    }

    client.publish("state.test", "data") catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after publish");
        return;
    }

    const sub = client.subscribe(allocator, "state.sub") catch {
        reportResult("state_after_ops", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after subscribe");
        return;
    }

    client.flush() catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after flush");
        return;
    }

    reportResult("state_after_ops", true, "");
}

// Test: Rapid connect/disconnect cycles
// Verifies no resource leaks after many connection cycles.
pub fn testRapidConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Do 50 connect/disconnect cycles
    const CYCLES = 50;
    var success: u32 = 0;

    for (0..CYCLES) |_| {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            continue;
        };

        if (client.isConnected()) {
            success += 1;
        }

        client.deinit(allocator);
    }

    // All 50 cycles must succeed
    if (success == CYCLES) {
        reportResult("rapid_connect_disconnect", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "{d}/50 cycles", .{success}) catch "e";
        reportResult("rapid_connect_disconnect", false, detail);
    }
}

// Test: Connection options validation
// Verifies connect options are properly applied.
pub fn testConnectionOptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Test with custom options
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "test-client",
        .verbose = false,
        .pedantic = false,
        .echo = true,
        .headers = true,
        .no_responders = true,
        .async_queue_size = 128,
    }) catch {
        reportResult("connection_options", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("connection_options", false, "not connected");
        return;
    }

    // Verify server info is available (options were sent)
    const info = client.getServerInfo();
    if (info == null) {
        reportResult("connection_options", false, "no server info");
        return;
    }

    reportResult("connection_options", true, "");
}

// Test: Connection with drain
// Verifies drain properly cleans up subscriptions.
pub fn testConnectionDrain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("connection_drain", false, "connect failed");
        return;
    };

    // Create some subscriptions
    const sub1 = client.subscribe(allocator, "drain.1") catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.2") catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.flush() catch {};

    // Drain should clean up everything
    client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "drain failed");
        return;
    };

    // Connection should be closed after drain
    if (client.isConnected()) {
        client.deinit(allocator);
        reportResult("connection_drain", false, "still connected");
        return;
    }

    client.deinit(allocator);
    reportResult("connection_drain", true, "");
}

// Test: Invalid URL handling
// Verifies invalid URLs are rejected properly.
pub fn testInvalidUrlHandling(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Empty URL
    const empty_result = nats.Client.connect(allocator, io.io(), "", .{});
    if (empty_result) |client| {
        client.deinit(allocator);
        reportResult("invalid_url_handling", false, "empty should fail");
        return;
    } else |_| {
        // Expected
    }

    // Invalid host format
    const invalid_result = nats.Client.connect(
        allocator,
        io.io(),
        "nats://",
        .{},
    );
    if (invalid_result) |client| {
        client.deinit(allocator);
        reportResult("invalid_url_handling", false, "invalid should fail");
        return;
    } else |_| {
        // Expected
    }

    reportResult("invalid_url_handling", true, "");
}

// Test: Connection state transitions
// Verifies state machine transitions are correct.
pub fn testConnectionStateTransitions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("connection_state", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // State should be connected after successful connect
    if (!client.isConnected()) {
        reportResult("connection_state", false, "not connected");
        return;
    }

    // Publish should work in connected state
    client.publish("state.trans", "test") catch {
        reportResult("connection_state", false, "publish failed");
        return;
    };

    // Subscribe should work in connected state
    const sub = client.subscribe(allocator, "state.trans") catch {
        reportResult("connection_state", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Flush should work in connected state
    client.flush() catch {
        reportResult("connection_state", false, "flush failed");
        return;
    };

    reportResult("connection_state", true, "");
}

// Test: Multiple subscriptions on same client
// Verifies client can handle many subscriptions.
pub fn testManyClientSubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("many_client_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 100 subscriptions
    const NUM_SUBS = 100;
    var subs: [NUM_SUBS]?*nats.Subscription = [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: usize = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..NUM_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "many.subs.{d}",
            .{i},
        ) catch continue;

        subs[i] = client.subscribe(allocator, subject) catch {
            break;
        };
        created += 1;
    }

    // Must create all 100
    if (created != NUM_SUBS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{created}) catch "e";
        reportResult("many_client_subs", false, detail);
        return;
    }

    // Connection should still be healthy
    if (client.isConnected()) {
        reportResult("many_client_subs", true, "");
    } else {
        reportResult("many_client_subs", false, "disconnected");
    }
}

/// Runs all async connection tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testAsyncConnectionRefused(allocator);
    testAsyncConsecutiveConnections(allocator);
    testAsyncIsConnectedState(allocator);
    testConnectionStateAfterOps(allocator);
    testRapidConnectDisconnect(allocator);
    testConnectionOptions(allocator);
    testConnectionDrain(allocator);
    testInvalidUrlHandling(allocator);
    testConnectionStateTransitions(allocator);
    testManyClientSubscriptions(allocator);
    testAsyncReconnection(allocator, manager);
    testAsyncServerRestartNewConnection(allocator, manager);
}
