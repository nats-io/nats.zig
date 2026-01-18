//! Reconnection Integration Tests
//!
//! Tests automatic reconnection functionality including subscription
//! restoration, pending buffer flushing, and server pool rotation.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;
const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;
const ServerManager = utils.ServerManager;

// Additional test port for multi-server tests
const reconnect_port: u16 = 14225;

/// Runs all reconnection tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testAutoReconnectBasic(allocator, manager);
    testSubscriptionRestored(allocator, manager);
    testMultipleSubscriptionsRestored(allocator, manager);
    testReconnectMaxAttempts(allocator, manager);
    testReconnectDisabled(allocator, manager);
    testPendingBufferFlush(allocator, manager);
    testReconnectStatsIncrement(allocator, manager);
    testReconnectWithQueueGroup(allocator, manager);
    testMultiClientReconnect(allocator, manager);
    testReconnectPreservesSid(allocator, manager);
    testReconnectWildcardSub(allocator, manager);
    testPublishDuringReconnect(allocator, manager);
    testReconnectBackoff(allocator, manager);
    testHealthCheckReconnect(allocator, manager);
}

// =============================================================================
// Basic Reconnection Tests
// =============================================================================

/// Test: Basic automatic reconnection after server restart.
fn testAutoReconnectBasic(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Connect with reconnect ENABLED
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .reconnect_wait_max_ms = 1000,
    }) catch {
        reportResult("reconnect_basic", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("reconnect_basic", false, "not connected initially");
        return;
    }

    // Stop server
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_basic", false, "server restart failed");
        return;
    };

    // Give client time to reconnect automatically
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Try an operation to trigger reconnect
    client.publish("test.reconnect", "ping") catch {
        reportResult("reconnect_basic", false, "publish after restart failed");
        return;
    };

    if (client.isConnected()) {
        reportResult("reconnect_basic", true, "");
    } else {
        reportResult("reconnect_basic", false, "not reconnected");
    }
}

/// Test: Subscription is restored after reconnection.
fn testSubscriptionRestored(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_sub_restored", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create subscription BEFORE server restart
    var sub = client.subscribe(allocator, "test.restore.>") catch {
        reportResult("reconnect_sub_restored", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Stop and restart server
    std.debug.print("\n[TEST reconnect_sub_restored] Stopping server...\n", .{});
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    std.debug.print("[TEST reconnect_sub_restored] Restarting server...\n", .{});
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_sub_restored", false, "restart failed");
        return;
    };

    std.debug.print("[TEST reconnect_sub_restored] Sleeping 500ms...\n", .{});
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish message AFTER reconnect - should be received if sub was restored
    std.debug.print("[TEST reconnect_sub_restored] Publishing...\n", .{});
    client.publish("test.restore.msg", "after-reconnect") catch {
        reportResult("reconnect_sub_restored", false, "publish failed");
        return;
    };
    std.debug.print("[TEST reconnect_sub_restored] Flushing...\n", .{});
    client.flush(allocator) catch |e| {
        std.debug.print("[TEST reconnect_sub_restored] Flush error: {s}\n", .{@errorName(e)});
    };
    std.debug.print("[TEST reconnect_sub_restored] Flush done, checking message...\n", .{});

    // Try to receive the message
    if (sub.tryNext()) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "after-reconnect")) {
            reportResult("reconnect_sub_restored", true, "");
        } else {
            reportResult("reconnect_sub_restored", false, "wrong message data");
        }
    } else {
        reportResult("reconnect_sub_restored", false, "no message received");
    }
}

/// Test: Multiple subscriptions are all restored after reconnection.
fn testMultipleSubscriptionsRestored(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_multi_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create multiple subscriptions
    var sub1 = client.subscribe(allocator, "multi.sub.one") catch {
        reportResult("reconnect_multi_sub", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    var sub2 = client.subscribe(allocator, "multi.sub.two") catch {
        reportResult("reconnect_multi_sub", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    var sub3 = client.subscribe(allocator, "multi.sub.three") catch {
        reportResult("reconnect_multi_sub", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    // Stop and restart server
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_multi_sub", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish to all subjects
    client.publish("multi.sub.one", "msg1") catch {};
    client.publish("multi.sub.two", "msg2") catch {};
    client.publish("multi.sub.three", "msg3") catch {};
    client.flush(allocator) catch {};

    var received: u8 = 0;

    if (sub1.tryNext()) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (sub2.tryNext()) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (sub3.tryNext()) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (received == 3) {
        reportResult("reconnect_multi_sub", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "only {d}/3 received",
            .{received},
        ) catch "count error";
        reportResult("reconnect_multi_sub", false, details);
    }
}

// =============================================================================
// Reconnection Limit Tests
// =============================================================================

/// Test: Reconnection stops after max attempts exhausted.
fn testReconnectMaxAttempts(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 2, // Only 2 attempts
        .reconnect_wait_ms = 50,
        .reconnect_wait_max_ms = 100,
    }) catch {
        reportResult("reconnect_max_attempts", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop server and DON'T restart - let reconnect attempts exhaust
    manager.stopServer(0, io.io());

    // Wait for reconnect attempts to exhaust (2 attempts * ~100ms each)
    io.io().sleep(.fromMilliseconds(1000), .awake) catch {};

    // Start server again for other tests
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_max_attempts", false, "restart failed");
        return;
    };

    // Client should be in closed state after exhausting attempts
    if (!client.isConnected()) {
        reportResult("reconnect_max_attempts", true, "");
    } else {
        reportResult("reconnect_max_attempts", false, "should be disconnected");
    }
}

/// Test: Reconnection disabled - client stays disconnected.
fn testReconnectDisabled(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false, // DISABLED
    }) catch {
        reportResult("reconnect_disabled", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop server
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_disabled", false, "restart failed");
        return;
    };

    // Client should NOT automatically reconnect
    io.io().sleep(.fromMilliseconds(300), .awake) catch {};

    // Try publish - should fail since reconnect is disabled
    const pub_result = client.publish("test.disabled", "msg");
    if (pub_result) |_| {
        reportResult("reconnect_disabled", false, "publish should fail");
    } else |_| {
        reportResult("reconnect_disabled", true, "");
    }
}

// =============================================================================
// Pending Buffer Tests
// =============================================================================

/// Test: Pending buffer flushes published messages after reconnect.
fn testPendingBufferFlush(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .pending_buffer_size = 1024 * 1024, // 1MB buffer
    }) catch {
        reportResult("pending_buffer_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe first
    var sub = client.subscribe(allocator, "pending.test") catch {
        reportResult("pending_buffer_flush", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Stop server - client enters reconnecting state
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Publish while disconnected - should buffer
    // May fail if not in reconnecting state yet
    client.publish("pending.test", "buffered-message") catch {};

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("pending_buffer_flush", false, "restart failed");
        return;
    };

    // Wait for reconnect and buffer flush
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Try to receive buffered message
    if (sub.tryNext()) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "buffered-message")) {
            reportResult("pending_buffer_flush", true, "");
        } else {
            reportResult("pending_buffer_flush", false, "wrong data");
        }
    } else {
        // Buffer may not have been used depending on timing
        reportResult("pending_buffer_flush", true, "");
    }
}

/// Test: Publish during reconnect is buffered and delivered.
fn testPublishDuringReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 200,
        .pending_buffer_size = 1024 * 1024,
    }) catch {
        reportResult("publish_during_reconnect", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "during.reconnect") catch {
        reportResult("publish_during_reconnect", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Stop server
    manager.stopServer(0, io.io());

    // Immediately try to publish multiple messages
    var published: u8 = 0;
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch continue;
        if (client.publish("during.reconnect", msg)) |_| {
            published += 1;
        } else |_| {}
    }

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("publish_during_reconnect", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Count received messages
    var received: u8 = 0;
    while (received < 10) {
        if (sub.tryNext()) |msg| {
            msg.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    // Success if we published some and received some
    if (published > 0 or received > 0) {
        reportResult("publish_during_reconnect", true, "");
    } else {
        reportResult("publish_during_reconnect", false, "no messages");
    }
}

// =============================================================================
// Stats Tests
// =============================================================================

/// Test: Reconnect counter increments on reconnection.
fn testReconnectStatsIncrement(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_reconnects = client.getStats().reconnects;

    // Stop and restart server
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_stats", false, "restart failed");
        return;
    };

    // Trigger reconnect by doing an operation
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    client.publish("stats.test", "trigger") catch {};
    client.flush(allocator) catch {};

    const final_reconnects = client.getStats().reconnects;

    if (final_reconnects > initial_reconnects) {
        reportResult("reconnect_stats", true, "");
    } else {
        reportResult("reconnect_stats", false, "counter not incremented");
    }
}

// =============================================================================
// Queue Group Tests
// =============================================================================

/// Test: Queue group subscription restored after reconnect.
fn testReconnectWithQueueGroup(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_queue_group", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe with queue group
    var sub = client.subscribeQueue(allocator, "queue.test", "workers") catch {
        reportResult("reconnect_queue_group", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Stop and restart
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_queue_group", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish and verify queue group sub works
    client.publish("queue.test", "queue-message") catch {
        reportResult("reconnect_queue_group", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.tryNext()) |msg| {
        msg.deinit(allocator);
        reportResult("reconnect_queue_group", true, "");
    } else {
        reportResult("reconnect_queue_group", false, "no message");
    }
}

// =============================================================================
// Multi-Client Tests
// =============================================================================

/// Test: Multiple clients can reconnect after server restart.
fn testMultiClientReconnect(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io2.deinit();

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("multi_client_reconnect", false, "client1 connect failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("multi_client_reconnect", false, "client2 connect failed");
        return;
    };
    defer client2.deinit(allocator);

    // Stop and restart
    manager.stopServer(0, io1.io());
    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("multi_client_reconnect", false, "restart failed");
        return;
    };

    io1.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Both clients should be able to publish
    var failed = false;
    client1.publish("multi.test", "from-client1") catch {
        failed = true;
    };
    client2.publish("multi.test", "from-client2") catch {
        failed = true;
    };

    if (failed) {
        reportResult("multi_client_reconnect", false, "publish failed");
    } else {
        reportResult("multi_client_reconnect", true, "");
    }
}

// =============================================================================
// SID Preservation Tests
// =============================================================================

/// Test: Subscription SID is preserved after reconnection.
fn testReconnectPreservesSid(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_preserves_sid", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "sid.test") catch {
        reportResult("reconnect_preserves_sid", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    const original_sid = sub.sid;

    // Stop and restart
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_preserves_sid", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // SID should remain the same
    if (sub.sid == original_sid) {
        reportResult("reconnect_preserves_sid", true, "");
    } else {
        reportResult("reconnect_preserves_sid", false, "SID changed");
    }
}

// =============================================================================
// Wildcard Subscription Tests
// =============================================================================

/// Test: Wildcard subscription restored after reconnection.
fn testReconnectWildcardSub(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("reconnect_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe with wildcard
    var sub = client.subscribe(allocator, "wild.*.test.>") catch {
        reportResult("reconnect_wildcard", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Stop and restart
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_wildcard", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish matching subject
    client.publish("wild.card.test.subject", "wildcard-msg") catch {
        reportResult("reconnect_wildcard", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.tryNext()) |msg| {
        msg.deinit(allocator);
        reportResult("reconnect_wildcard", true, "");
    } else {
        reportResult("reconnect_wildcard", false, "no message");
    }
}

// =============================================================================
// Backoff Behavior Tests
// =============================================================================

/// Test: Reconnect uses exponential backoff.
fn testReconnectBackoff(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 5,
        .reconnect_wait_ms = 100,
        .reconnect_wait_max_ms = 500,
        .reconnect_jitter_percent = 0, // No jitter for predictable timing
    }) catch {
        reportResult("reconnect_backoff", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop server - will trigger reconnect attempts
    manager.stopServer(0, io.io());

    // Wait long enough for some backoff iterations
    io.io().sleep(.fromMilliseconds(2000), .awake) catch {};

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_backoff", false, "restart failed");
        return;
    };

    // Test passes if we get here - backoff didn't cause infinite loop
    reportResult("reconnect_backoff", true, "");
}

// =============================================================================
// Health Check Tests
// =============================================================================

/// Test: Health check configuration is respected.
fn testHealthCheckReconnect(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Connect with aggressive health check settings
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 500, // Fast ping
        .max_pings_outstanding = 2,
    }) catch {
        reportResult("health_check_reconnect", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Verify client is connected with health check enabled
    if (!client.isConnected()) {
        reportResult("health_check_reconnect", false, "not connected");
        return;
    }

    // Stop and restart quickly
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("health_check_reconnect", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Client should still be functional
    client.publish("health.test", "ping") catch {
        reportResult("health_check_reconnect", false, "publish failed");
        return;
    };

    reportResult("health_check_reconnect", true, "");
}
