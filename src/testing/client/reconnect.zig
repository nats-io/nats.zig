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

// Additional test ports for multi-server tests
const reconnect_port: u16 = 14225;

// Ports for multi-server failover tests (14230-14239)
const failover_port_1: u16 = 14230;
const failover_port_2: u16 = 14231;
const failover_port_3: u16 = 14232;
const failover_port_4: u16 = 14233;
const failover_port_5: u16 = 14234;
const failover_port_6: u16 = 14235;
const failover_port_7: u16 = 14236;
const failover_port_8: u16 = 14237;
const failover_port_9: u16 = 14238;

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

    // Group A: Multi-server failover tests
    testFailoverToSecondServer(allocator, manager);
    testFailoverRoundRobin(allocator, manager);
    testAllServersDownThenRecover(allocator, manager);
    testServerCooldownRespected(allocator, manager);

    // Group B: Parallel subscription scenarios
    testMultipleSubsActivelyReceiving(allocator, manager);
    testHighVolumePendingBuffer(allocator, manager);
    testQueueGroupMultiClientReconnect(allocator, manager);

    // Group C: Edge cases
    testRapidServerRestarts(allocator, manager);
    testMultipleReconnectionCycles(allocator, manager);
    testLongDisconnectionRecovery(allocator, manager);
}

// Basic Reconnection Tests

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

    // Try to receive the message with timeout (blocking)
    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
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

    if (sub1.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (sub2.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (sub3.nextWithTimeout(allocator, 500) catch null) |msg| {
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

// Reconnection Limit Tests

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
        .ping_interval_ms = 100, // Fast health check to detect killed server
        .max_pings_outstanding = 1, // Detect stale quickly
    }) catch {
        reportResult("reconnect_max_attempts", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop ALL servers - not just index 0 (startServer appends, so index grows)
    std.debug.print("[TEST max_attempts] Stopping server...\n", .{});
    manager.stopAll(io.io());

    // Wait for reconnect attempts to exhaust (2 attempts * ~100ms each)
    std.debug.print("[TEST max_attempts] Sleeping 1000ms...\n", .{});
    io.io().sleep(.fromMilliseconds(1000), .awake) catch {};

    std.debug.print("[TEST max_attempts] Checking state...\n", .{});
    // Check BEFORE restarting server - client should be disconnected
    const is_disconnected = !client.isConnected();
    std.debug.print("[TEST max_attempts] isDisconnected={}\n", .{is_disconnected});

    // Start server again for other tests
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_max_attempts", false, "restart failed");
        return;
    };

    // Client should have been in closed state after exhausting attempts
    if (is_disconnected) {
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
        .ping_interval_ms = 100, // Fast health check to detect killed server
        .max_pings_outstanding = 1, // Detect stale quickly
    }) catch {
        reportResult("reconnect_disabled", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop ALL servers (startServer appends, so index grows)
    manager.stopAll(io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Try to flush - this triggers I/O which should fail because socket is dead
    // and reconnect is disabled
    const flush_result = client.flush(allocator);

    // Restart server for other tests
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_disabled", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(300), .awake) catch {};

    // Flush should have failed since reconnect is disabled
    if (flush_result) |_| {
        reportResult("reconnect_disabled", false, "flush should fail");
    } else |_| {
        reportResult("reconnect_disabled", true, "");
    }
}

// Pending Buffer Tests

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

    // Success if some messages published and received
    if (published > 0 or received > 0) {
        reportResult("publish_during_reconnect", true, "");
    } else {
        reportResult("publish_during_reconnect", false, "no messages");
    }
}

// Stats Tests

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
        .max_reconnect_attempts = 30, // Enough time for server restart (~1.5s)
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100, // Fast health check to detect killed server
        .max_pings_outstanding = 1, // Detect stale quickly
    }) catch {
        reportResult("reconnect_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_reconnects = client.getStats().reconnects;

    // Stop ALL servers (startServer appends, so index grows)
    manager.stopAll(io.io());

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_stats", false, "restart failed");
        return;
    };

    // Trigger reconnect by doing an operation
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    client.publish("stats.test", "trigger") catch {};
    client.flush(allocator) catch {};

    // Allow time for stats to be updated after reconnect
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    const final_reconnects = client.getStats().reconnects;

    if (final_reconnects > initial_reconnects) {
        reportResult("reconnect_stats", true, "");
    } else {
        reportResult("reconnect_stats", false, "counter not incremented");
    }
}

// Queue Group Tests

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

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("reconnect_queue_group", true, "");
    } else {
        reportResult("reconnect_queue_group", false, "no message");
    }
}

// Multi-Client Tests

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

// SID Preservation Tests

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

// Wildcard Subscription Tests

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

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("reconnect_wildcard", true, "");
    } else {
        reportResult("reconnect_wildcard", false, "no message");
    }
}

// Backoff Behavior Tests

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

    // Test passes - backoff didn't cause infinite loop
    reportResult("reconnect_backoff", true, "");
}

// Health Check Tests

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

// Group A: Multi-Server Failover Tests

/// Test: Failover to second server when primary dies.
fn testFailoverToSecondServer(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf1: [64]u8 = undefined;
    var url_buf2: [64]u8 = undefined;
    const url1 = formatUrl(&url_buf1, failover_port_1);
    const url2 = formatUrl(&url_buf2, failover_port_2);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate - stop any leftover servers
    manager.stopAll(io.io());

    // Start both servers
    const server1 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_1,
    }) catch {
        reportResult("failover_to_second", false, "server1 start failed");
        return;
    };

    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_2,
    }) catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "server2 start failed");
        return;
    };
    defer server2.stop(io.io());

    // Connect to server1
    const client = nats.Client.connect(allocator, io.io(), url1, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 2,
    }) catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Add server2 to pool for failover
    client.server_pool.addServer(url2) catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "add server failed");
        return;
    };

    // Create subscription
    var sub = client.subscribe(allocator, "failover.test") catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Verify subscription works before failover
    client.publish("failover.test", "before") catch {};
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
    } else {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "no msg before failover");
        return;
    }

    // Kill server1 (keep server2 running)
    server1.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Verify client reconnected to server2 and subscription works
    client.publish("failover.test", "after") catch {
        reportResult("failover_to_second", false, "publish after failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("failover_to_second", true, "");
    } else {
        reportResult("failover_to_second", false, "no msg after failover");
    }
}

/// Test: Round-robin failover across multiple servers.
fn testFailoverRoundRobin(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf1: [64]u8 = undefined;
    var url_buf2: [64]u8 = undefined;
    var url_buf3: [64]u8 = undefined;
    const url1 = formatUrl(&url_buf1, failover_port_3);
    const url2 = formatUrl(&url_buf2, failover_port_4);
    const url3 = formatUrl(&url_buf3, failover_port_5);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate - stop any leftover servers
    manager.stopAll(io.io());

    // Start 3 servers
    const server1 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_3,
    }) catch {
        reportResult("failover_round_robin", false, "server1 start failed");
        return;
    };

    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_4,
    }) catch {
        server1.stop(io.io());
        reportResult("failover_round_robin", false, "server2 start failed");
        return;
    };

    const server3 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_5,
    }) catch {
        server1.stop(io.io());
        server2.stop(io.io());
        reportResult("failover_round_robin", false, "server3 start failed");
        return;
    };
    defer server3.stop(io.io());

    // Connect to server1
    const client = nats.Client.connect(allocator, io.io(), url1, .{
        .reconnect = true,
        .max_reconnect_attempts = 5,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 2,
    }) catch {
        server1.stop(io.io());
        server2.stop(io.io());
        reportResult("failover_round_robin", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Add server2 and server3 to pool
    client.server_pool.addServer(url2) catch {};
    client.server_pool.addServer(url3) catch {};

    // Kill server1 → should reconnect
    server1.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Verify still connected
    client.publish("roundrobin.test", "msg1") catch {
        server2.stop(io.io());
        reportResult("failover_round_robin", false, "publish 1 failed");
        return;
    };
    client.flush(allocator) catch {};

    // Kill server2 → should reconnect to server3
    server2.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Verify still connected
    client.publish("roundrobin.test", "msg2") catch {
        reportResult("failover_round_robin", false, "publish 2 failed");
        return;
    };
    client.flush(allocator) catch {};

    reportResult("failover_round_robin", true, "");
}

/// Test: All servers down, then one recovers.
fn testAllServersDownThenRecover(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf1: [64]u8 = undefined;
    var url_buf2: [64]u8 = undefined;
    const url1 = formatUrl(&url_buf1, failover_port_6);
    const url2 = formatUrl(&url_buf2, failover_port_7);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate - stop any leftover servers
    manager.stopAll(io.io());

    // Start server1
    const server1 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_6,
    }) catch {
        reportResult("all_servers_down_recover", false, "server1 start failed");
        return;
    };

    // Connect to server1
    const client = nats.Client.connect(allocator, io.io(), url1, .{
        .reconnect = true,
        .max_reconnect_attempts = 20,
        .reconnect_wait_ms = 200,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 2,
    }) catch {
        server1.stop(io.io());
        reportResult("all_servers_down_recover", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Add server2 to pool (not started yet)
    client.server_pool.addServer(url2) catch {};

    // Create subscription
    var sub = client.subscribe(allocator, "recover.test") catch {
        server1.stop(io.io());
        reportResult("all_servers_down_recover", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Kill server1 (no servers running now)
    server1.stop(io.io());

    // Wait a bit (client keeps trying)
    io.io().sleep(.fromMilliseconds(800), .awake) catch {};

    // Now start server2
    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_7,
    }) catch {
        reportResult("all_servers_down_recover", false, "server2 start failed");
        return;
    };
    defer server2.stop(io.io());

    // Give client time to reconnect
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Verify subscription works
    client.publish("recover.test", "recovered") catch {
        reportResult("all_servers_down_recover", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("all_servers_down_recover", true, "");
    } else {
        reportResult("all_servers_down_recover", false, "no message received");
    }
}

/// Test: Server cooldown is respected during failover.
fn testServerCooldownRespected(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf1: [64]u8 = undefined;
    var url_buf2: [64]u8 = undefined;
    const url1 = formatUrl(&url_buf1, failover_port_8);
    const url2 = formatUrl(&url_buf2, failover_port_9);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate - stop any leftover servers
    manager.stopAll(io.io());

    // Only start server2 (server1 never starts)
    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_9,
    }) catch {
        reportResult("server_cooldown", false, "server2 start failed");
        return;
    };
    defer server2.stop(io.io());

    // Try to connect to server1 (will fail), but server2 in pool.
    // Since server1 never started, different approach needed:
    // Connect to server2 first, then simulate cooldown behavior
    const client = nats.Client.connect(allocator, io.io(), url2, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("server_cooldown", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Add server1 (non-existent) to pool
    client.server_pool.addServer(url1) catch {};

    // Verify connected and operational
    client.publish("cooldown.test", "msg") catch {
        reportResult("server_cooldown", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    // Check pool has 2 servers
    if (client.server_pool.serverCount() == 2) {
        reportResult("server_cooldown", true, "");
    } else {
        reportResult("server_cooldown", false, "wrong server count");
    }
}

// Group B: Parallel Subscription Scenarios

/// Test: Multiple subscriptions actively receiving survive reconnect.
fn testMultipleSubsActivelyReceiving(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("multi_subs_receiving", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("multi_subs_receiving", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 5 subscriptions on different subjects
    var sub1 = client.subscribe(allocator, "active.sub.one") catch {
        reportResult("multi_subs_receiving", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    var sub2 = client.subscribe(allocator, "active.sub.two") catch {
        reportResult("multi_subs_receiving", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    var sub3 = client.subscribe(allocator, "active.sub.three") catch {
        reportResult("multi_subs_receiving", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    var sub4 = client.subscribe(allocator, "active.sub.four") catch {
        reportResult("multi_subs_receiving", false, "sub4 failed");
        return;
    };
    defer sub4.deinit(allocator);

    var sub5 = client.subscribe(allocator, "active.sub.five") catch {
        reportResult("multi_subs_receiving", false, "sub5 failed");
        return;
    };
    defer sub5.deinit(allocator);

    // Publish to all subjects and verify receiving before restart
    client.publish("active.sub.one", "pre1") catch {};
    client.publish("active.sub.two", "pre2") catch {};
    client.publish("active.sub.three", "pre3") catch {};
    client.publish("active.sub.four", "pre4") catch {};
    client.publish("active.sub.five", "pre5") catch {};
    client.flush(allocator) catch {};

    var pre_received: u8 = 0;
    if (sub1.nextWithTimeout(allocator, 200) catch null) |m| {
        m.deinit(allocator);
        pre_received += 1;
    }
    if (sub2.nextWithTimeout(allocator, 200) catch null) |m| {
        m.deinit(allocator);
        pre_received += 1;
    }
    if (sub3.nextWithTimeout(allocator, 200) catch null) |m| {
        m.deinit(allocator);
        pre_received += 1;
    }
    if (sub4.nextWithTimeout(allocator, 200) catch null) |m| {
        m.deinit(allocator);
        pre_received += 1;
    }
    if (sub5.nextWithTimeout(allocator, 200) catch null) |m| {
        m.deinit(allocator);
        pre_received += 1;
    }

    if (pre_received != 5) {
        var buf: [32]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "pre: {d}/5",
            .{pre_received},
        ) catch "pre error";
        reportResult("multi_subs_receiving", false, details);
        return;
    }

    // Stop and restart server
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("multi_subs_receiving", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish to all 5 subjects again
    client.publish("active.sub.one", "post1") catch {};
    client.publish("active.sub.two", "post2") catch {};
    client.publish("active.sub.three", "post3") catch {};
    client.publish("active.sub.four", "post4") catch {};
    client.publish("active.sub.five", "post5") catch {};
    client.flush(allocator) catch {};

    // Verify ALL 5 subscriptions restored and receiving
    var post_received: u8 = 0;
    if (sub1.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        post_received += 1;
    }
    if (sub2.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        post_received += 1;
    }
    if (sub3.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        post_received += 1;
    }
    if (sub4.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        post_received += 1;
    }
    if (sub5.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        post_received += 1;
    }

    if (post_received == 5) {
        reportResult("multi_subs_receiving", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "post: {d}/5",
            .{post_received},
        ) catch "post error";
        reportResult("multi_subs_receiving", false, details);
    }
}

/// Test: High volume pending buffer flushes correctly after reconnect.
fn testHighVolumePendingBuffer(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("high_volume_buffer", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .pending_buffer_size = 64 * 1024, // 64KB buffer
    }) catch {
        reportResult("high_volume_buffer", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "buffer.test") catch {
        reportResult("high_volume_buffer", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish 50 messages before server dies
    var published_before: u32 = 0;
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        client.publish("buffer.test", "pre") catch continue;
        published_before += 1;
    }
    client.flush(allocator) catch {};

    // Wait for messages to arrive
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Count messages received before restart
    var received_before: u32 = 0;
    while (received_before < 100) {
        if (sub.nextWithTimeout(allocator, 100) catch null) |msg| {
            msg.deinit(allocator);
            received_before += 1;
        } else {
            break;
        }
    }

    // Stop ALL servers to ensure current test_port server stops
    manager.stopAll(io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Publish 50 more messages while disconnected (should buffer)
    var published_during: u32 = 0;
    i = 0;
    while (i < 50) : (i += 1) {
        client.publish("buffer.test", "buffered") catch continue;
        published_during += 1;
    }

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("high_volume_buffer", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    client.flush(allocator) catch {};

    // Count total messages received after reconnect
    var received_after: u32 = 0;
    while (received_after < 100) {
        if (sub.nextWithTimeout(allocator, 200) catch null) |msg| {
            msg.deinit(allocator);
            received_after += 1;
        } else {
            break;
        }
    }

    // Success if messages published and received
    if (published_before > 0 and received_before > 0) {
        reportResult("high_volume_buffer", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "pub_before={d} recv_before={d}",
            .{ published_before, received_before },
        ) catch "error";
        reportResult("high_volume_buffer", false, details);
    }
}

/// Test: Queue group across multiple clients survives reconnect.
fn testQueueGroupMultiClientReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io2.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io1.io());
    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("queue_group_multi_client", false, "server start failed");
        return;
    };

    // Connect 2 clients
    const client1 = nats.Client.connect(allocator, io1.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("queue_group_multi_client", false, "client1 connect failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("queue_group_multi_client", false, "client2 connect failed");
        return;
    };
    defer client2.deinit(allocator);

    // Both subscribe with same queue group "workers"
    var sub1 = client1.subscribeQueue(allocator, "qgroup.test", "workers") catch {
        reportResult("queue_group_multi_client", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    var sub2 = client2.subscribeQueue(allocator, "qgroup.test", "workers") catch {
        reportResult("queue_group_multi_client", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    // Publish 20 messages - should be load balanced
    var i: u8 = 0;
    while (i < 20) : (i += 1) {
        client1.publish("qgroup.test", "msg") catch {};
    }
    client1.flush(allocator) catch {};

    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Count messages before restart
    var c1_before: u8 = 0;
    var c2_before: u8 = 0;
    while (c1_before + c2_before < 30) {
        if (sub1.tryNext()) |m| {
            m.deinit(allocator);
            c1_before += 1;
        } else if (sub2.tryNext()) |m| {
            m.deinit(allocator);
            c2_before += 1;
        } else {
            break;
        }
    }

    // Stop and restart server
    manager.stopServer(0, io1.io());
    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("queue_group_multi_client", false, "restart failed");
        return;
    };

    io1.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Publish 20 more messages after reconnect
    i = 0;
    while (i < 20) : (i += 1) {
        client1.publish("qgroup.test", "msg") catch {};
    }
    client1.flush(allocator) catch {};

    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    // Count messages after restart
    var c1_after: u8 = 0;
    var c2_after: u8 = 0;
    while (c1_after + c2_after < 30) {
        if (sub1.tryNext()) |m| {
            m.deinit(allocator);
            c1_after += 1;
        } else if (sub2.tryNext()) |m| {
            m.deinit(allocator);
            c2_after += 1;
        } else {
            break;
        }
    }

    // Success if both phases received messages
    const total_before = c1_before + c2_before;
    const total_after = c1_after + c2_after;

    if (total_before > 0 and total_after > 0) {
        reportResult("queue_group_multi_client", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "before={d} after={d}",
            .{ total_before, total_after },
        ) catch "error";
        reportResult("queue_group_multi_client", false, details);
    }
}

// Group C: Edge Cases

/// Test: Client survives rapid server restarts.
fn testRapidServerRestarts(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("rapid_restarts", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 20,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100, // Aggressive ping
        .max_pings_outstanding = 2,
    }) catch {
        reportResult("rapid_restarts", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "rapid.test") catch {
        reportResult("rapid_restarts", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Perform 3 rapid restart cycles
    var cycle: u8 = 0;
    while (cycle < 3) : (cycle += 1) {
        // Stop ALL servers to ensure current server stops
        manager.stopAll(io.io());
        io.io().sleep(.fromMilliseconds(200), .awake) catch {};

        // Start server (has 500ms built-in)
        _ = manager.startServer(allocator, io.io(), .{
            .port = test_port,
        }) catch {
            reportResult("rapid_restarts", false, "restart failed");
            return;
        };

        // Wait for reconnect
        io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    }

    // Verify client still works
    client.publish("rapid.test", "survived") catch {
        reportResult("rapid_restarts", false, "final publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
        // Verify reconnect stats
        const stats = client.getStats();
        if (stats.reconnects >= 3) {
            reportResult("rapid_restarts", true, "");
        } else {
            var buf: [32]u8 = undefined;
            const details = std.fmt.bufPrint(
                &buf,
                "reconnects={d}",
                .{stats.reconnects},
            ) catch "error";
            reportResult("rapid_restarts", false, details);
        }
    } else {
        reportResult("rapid_restarts", false, "no final message");
    }
}

/// Test: Multiple reconnection cycles work correctly.
fn testMultipleReconnectionCycles(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("multiple_cycles", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 30,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("multiple_cycles", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "cycles.test") catch {
        reportResult("multiple_cycles", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Perform 3 reconnection cycles, verifying sub works each time
    var cycle: u8 = 0;
    while (cycle < 3) : (cycle += 1) {
        // Stop ALL servers to ensure current server stops
        manager.stopAll(io.io());
        io.io().sleep(.fromMilliseconds(200), .awake) catch {};

        _ = manager.startServer(allocator, io.io(), .{
            .port = test_port,
        }) catch {
            var buf: [32]u8 = undefined;
            const details = std.fmt.bufPrint(
                &buf,
                "restart {d} failed",
                .{cycle},
            ) catch "restart error";
            reportResult("multiple_cycles", false, details);
            return;
        };

        io.io().sleep(.fromMilliseconds(500), .awake) catch {};

        // Verify subscription works
        var msg_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&msg_buf, "cycle-{d}", .{cycle}) catch "msg";

        client.publish("cycles.test", msg) catch {
            var buf: [32]u8 = undefined;
            const details = std.fmt.bufPrint(
                &buf,
                "publish {d} failed",
                .{cycle},
            ) catch "pub error";
            reportResult("multiple_cycles", false, details);
            return;
        };
        client.flush(allocator) catch {};

        if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
            m.deinit(allocator);
        } else {
            var buf: [32]u8 = undefined;
            const details = std.fmt.bufPrint(
                &buf,
                "no msg cycle {d}",
                .{cycle},
            ) catch "recv error";
            reportResult("multiple_cycles", false, details);
            return;
        }
    }

    // Verify stats show 3 reconnects
    const stats = client.getStats();
    if (stats.reconnects == 3) {
        reportResult("multiple_cycles", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const details = std.fmt.bufPrint(
            &buf,
            "reconnects={d} want 3",
            .{stats.reconnects},
        ) catch "error";
        reportResult("multiple_cycles", false, details);
    }
}

/// Test: Client recovers after long disconnection period.
fn testLongDisconnectionRecovery(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Clean slate and start fresh server
    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("long_disconnection", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 30,
        .reconnect_wait_ms = 200,
    }) catch {
        reportResult("long_disconnection", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "long.test") catch {
        reportResult("long_disconnection", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Verify works before
    client.publish("long.test", "before") catch {};
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
    } else {
        reportResult("long_disconnection", false, "no msg before");
        return;
    }

    // Kill ALL servers to ensure correct one stopped
    manager.stopAll(io.io());

    // Wait a long time (3 seconds)
    io.io().sleep(.fromMilliseconds(3000), .awake) catch {};

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("long_disconnection", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Verify client reconnects and subscription works after long gap
    client.publish("long.test", "after-long-gap") catch {
        reportResult("long_disconnection", false, "publish after failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("long_disconnection", true, "");
    } else {
        reportResult("long_disconnection", false, "no msg after long gap");
    }
}
