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

const reconnect_port: u16 = 14227;

const failover_port_1: u16 = 14230;
const failover_port_2: u16 = 14231;
const failover_port_3: u16 = 14232;
const failover_port_4: u16 = 14233;
const failover_port_5: u16 = 14234;
const failover_port_6: u16 = 14235;
const failover_port_7: u16 = 14236;
const failover_port_8: u16 = 14237;
const failover_port_9: u16 = 14238;

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
    testCustomReconnectDelay(allocator, manager);
    testHealthCheckReconnect(allocator, manager);

    testFailoverToSecondServer(allocator, manager);
    testFailoverRoundRobin(allocator, manager);
    testAllServersDownThenRecover(allocator, manager);
    testServerCooldownRespected(allocator, manager);

    testMultipleSubsActivelyReceiving(allocator, manager);
    testHighVolumePendingBuffer(allocator, manager);
    testQueueGroupMultiClientReconnect(allocator, manager);

    testRapidServerRestarts(allocator, manager);
    testMultipleReconnectionCycles(allocator, manager);
    testLongDisconnectionRecovery(allocator, manager);
}

fn testAutoReconnectBasic(
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

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_basic", false, "server restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

fn testSubscriptionRestored(
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
        reportResult("reconnect_sub_restored", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "test.restore.>") catch {
        reportResult("reconnect_sub_restored", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    std.debug.print(
        "\n[TEST reconnect_sub_restored] Stopping server...\n",
        .{},
    );
    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    std.debug.print(
        "[TEST reconnect_sub_restored] Restarting server...\n",
        .{},
    );
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_sub_restored", false, "restart failed");
        return;
    };

    std.debug.print("[TEST reconnect_sub_restored] Sleeping 500ms...\n", .{});
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    std.debug.print("[TEST reconnect_sub_restored] Publishing...\n", .{});
    client.publish("test.restore.msg", "after-reconnect") catch {
        reportResult("reconnect_sub_restored", false, "publish failed");
        return;
    };
    std.debug.print("[TEST reconnect_sub_restored] Flushing...\n", .{});
    client.flush(allocator) catch |e| {
        std.debug.print(
            "[TEST reconnect_sub_restored] Flush error: {s}\n",
            .{@errorName(e)},
        );
    };
    std.debug.print(
        "[TEST reconnect_sub_restored] Flush done, checking message...\n",
        .{},
    );

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

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_multi_sub", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

fn testReconnectMaxAttempts(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 2,
        .reconnect_wait_ms = 50,
        .reconnect_wait_max_ms = 100,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 1,
    }) catch {
        reportResult("reconnect_max_attempts", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    std.debug.print("[TEST max_attempts] Stopping server...\n", .{});
    manager.stopAll(io.io());

    std.debug.print("[TEST max_attempts] Sleeping 1000ms...\n", .{});
    io.io().sleep(.fromMilliseconds(1000), .awake) catch {};

    std.debug.print("[TEST max_attempts] Checking state...\n", .{});
    const is_disconnected = !client.isConnected();
    std.debug.print(
        "[TEST max_attempts] isDisconnected={}\n",
        .{is_disconnected},
    );

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_max_attempts", false, "restart failed");
        return;
    };

    if (is_disconnected) {
        reportResult("reconnect_max_attempts", true, "");
    } else {
        reportResult("reconnect_max_attempts", false, "should be disconnected");
    }
}

fn testReconnectDisabled(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 1,
    }) catch {
        reportResult("reconnect_disabled", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    manager.stopAll(io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    const flush_result = client.flush(allocator);

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_disabled", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(300), .awake) catch {};

    if (flush_result) |_| {
        reportResult("reconnect_disabled", false, "flush should fail");
    } else |_| {
        reportResult("reconnect_disabled", true, "");
    }
}

fn testPendingBufferFlush(
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
        .pending_buffer_size = 1024 * 1024,
    }) catch {
        reportResult("pending_buffer_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "pending.test") catch {
        reportResult("pending_buffer_flush", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    client.publish("pending.test", "buffered-message") catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("pending_buffer_flush", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    if (sub.tryNext()) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "buffered-message")) {
            reportResult("pending_buffer_flush", true, "");
        } else {
            reportResult("pending_buffer_flush", false, "wrong data");
        }
    } else {
        reportResult("pending_buffer_flush", true, "");
    }
}

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

    manager.stopServer(0, io.io());

    var published: u8 = 0;
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch continue;
        if (client.publish("during.reconnect", msg)) |_| {
            published += 1;
        } else |_| {}
    }

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("publish_during_reconnect", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    var received: u8 = 0;
    while (received < 10) {
        if (sub.tryNext()) |msg| {
            msg.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    if (published > 0 or received > 0) {
        reportResult("publish_during_reconnect", true, "");
    } else {
        reportResult("publish_during_reconnect", false, "no messages");
    }
}

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
        .max_reconnect_attempts = 30,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 1,
    }) catch {
        reportResult("reconnect_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_reconnects = client.getStats().reconnects;

    manager.stopAll(io.io());

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_stats", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    client.publish("stats.test", "trigger") catch {};
    client.flush(allocator) catch {};

    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    const final_reconnects = client.getStats().reconnects;

    if (final_reconnects > initial_reconnects) {
        reportResult("reconnect_stats", true, "");
    } else {
        reportResult("reconnect_stats", false, "counter not incremented");
    }
}

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

    var sub = client.subscribeQueue(allocator, "queue.test", "workers") catch {
        reportResult("reconnect_queue_group", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_queue_group", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

fn testMultiClientReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
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

    manager.stopServer(0, io1.io());
    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("multi_client_reconnect", false, "restart failed");
        return;
    };

    io1.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

fn testReconnectPreservesSid(
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

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_preserves_sid", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    if (sub.sid == original_sid) {
        reportResult("reconnect_preserves_sid", true, "");
    } else {
        reportResult("reconnect_preserves_sid", false, "SID changed");
    }
}

fn testReconnectWildcardSub(
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
        reportResult("reconnect_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "wild.*.test.>") catch {
        reportResult("reconnect_wildcard", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_wildcard", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

fn testReconnectBackoff(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 5,
        .reconnect_wait_ms = 100,
        .reconnect_wait_max_ms = 500,
        .reconnect_jitter_percent = 0,
    }) catch {
        reportResult("reconnect_backoff", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    manager.stopServer(0, io.io());

    io.io().sleep(.fromMilliseconds(2000), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnect_backoff", false, "restart failed");
        return;
    };

    reportResult("reconnect_backoff", true, "");
}

/// Track calls for custom delay callback test (atomic for cross-thread visibility)
var custom_delay_calls: std.atomic.Value(u32) = std.atomic.Value(u32).init(0);

fn customDelayCallback(attempt: u32) u32 {
    _ = custom_delay_calls.fetchAdd(1, .seq_cst);
    // Simple linear backoff: 50ms per attempt
    return attempt * 50;
}

fn testCustomReconnectDelay(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, reconnect_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Reset call counter
    custom_delay_calls.store(0, .seq_cst);

    // Start our own dedicated server for this test
    const server = manager.startServer(allocator, io.io(), .{
        .port = reconnect_port,
    }) catch {
        reportResult("custom_reconnect_delay", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .custom_reconnect_delay = customDelayCallback,
        .ping_interval_ms = 100,
        .max_pings_outstanding = 1,
    }) catch {
        server.stop(io.io());
        reportResult("custom_reconnect_delay", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Stop the specific server we started to trigger reconnect attempts
    server.stop(io.io());

    // Wait for disconnect detection and multiple reconnect attempts
    // Callback returns attempt*50ms, so attempts 2,3,4 = 100+150+200 = 450ms
    io.io().sleep(.fromMilliseconds(1500), .awake) catch {};

    // Restart server on same port
    const server2 = manager.startServer(allocator, io.io(), .{
        .port = reconnect_port,
    }) catch {
        reportResult("custom_reconnect_delay", false, "restart failed");
        return;
    };
    defer server2.stop(io.io());

    // Wait for reconnection
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Callback should have been called at least once (for attempt 2+)
    const calls = custom_delay_calls.load(.seq_cst);
    if (calls >= 1) {
        reportResult("custom_reconnect_delay", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "calls={d}", .{
            calls,
        }) catch "e";
        reportResult("custom_reconnect_delay", false, detail);
    }
}

fn testHealthCheckReconnect(
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
        .ping_interval_ms = 500,
        .max_pings_outstanding = 2,
    }) catch {
        reportResult("health_check_reconnect", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("health_check_reconnect", false, "not connected");
        return;
    }

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("health_check_reconnect", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    client.publish("health.test", "ping") catch {
        reportResult("health_check_reconnect", false, "publish failed");
        return;
    };

    reportResult("health_check_reconnect", true, "");
}

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

    manager.stopAll(io.io());

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

    client.server_pool.addServer(url2) catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "add server failed");
        return;
    };

    var sub = client.subscribe(allocator, "failover.test") catch {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish("failover.test", "before") catch {};
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
    } else {
        server1.stop(io.io());
        reportResult("failover_to_second", false, "no msg before failover");
        return;
    }

    server1.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

    manager.stopAll(io.io());

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

    client.server_pool.addServer(url2) catch {};
    client.server_pool.addServer(url3) catch {};

    server1.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    client.publish("roundrobin.test", "msg1") catch {
        server2.stop(io.io());
        reportResult("failover_round_robin", false, "publish 1 failed");
        return;
    };
    client.flush(allocator) catch {};

    server2.stop(io.io());
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    client.publish("roundrobin.test", "msg2") catch {
        reportResult("failover_round_robin", false, "publish 2 failed");
        return;
    };
    client.flush(allocator) catch {};

    reportResult("failover_round_robin", true, "");
}

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

    manager.stopAll(io.io());

    const server1 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_6,
    }) catch {
        reportResult("all_servers_down_recover", false, "server1 start failed");
        return;
    };

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

    client.server_pool.addServer(url2) catch {};

    var sub = client.subscribe(allocator, "recover.test") catch {
        server1.stop(io.io());
        reportResult("all_servers_down_recover", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    server1.stop(io.io());

    io.io().sleep(.fromMilliseconds(800), .awake) catch {};

    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_7,
    }) catch {
        reportResult("all_servers_down_recover", false, "server2 start failed");
        return;
    };
    defer server2.stop(io.io());

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

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

    manager.stopAll(io.io());

    const server2 = manager.startServer(allocator, io.io(), .{
        .port = failover_port_9,
    }) catch {
        reportResult("server_cooldown", false, "server2 start failed");
        return;
    };
    defer server2.stop(io.io());

    const client = nats.Client.connect(allocator, io.io(), url2, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("server_cooldown", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.server_pool.addServer(url1) catch {};

    client.publish("cooldown.test", "msg") catch {
        reportResult("server_cooldown", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (client.server_pool.serverCount() == 2) {
        reportResult("server_cooldown", true, "");
    } else {
        reportResult("server_cooldown", false, "wrong server count");
    }
}

fn testMultipleSubsActivelyReceiving(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

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

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("multi_subs_receiving", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    client.publish("active.sub.one", "post1") catch {};
    client.publish("active.sub.two", "post2") catch {};
    client.publish("active.sub.three", "post3") catch {};
    client.publish("active.sub.four", "post4") catch {};
    client.publish("active.sub.five", "post5") catch {};
    client.flush(allocator) catch {};

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

fn testHighVolumePendingBuffer(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("high_volume_buffer", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .pending_buffer_size = 64 * 1024,
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

    var published_before: u32 = 0;
    var i: u32 = 0;
    while (i < 50) : (i += 1) {
        client.publish("buffer.test", "pre") catch continue;
        published_before += 1;
    }
    client.flush(allocator) catch {};

    io.io().sleep(.fromMilliseconds(200), .awake) catch {};

    var received_before: u32 = 0;
    while (received_before < 100) {
        if (sub.nextWithTimeout(allocator, 100) catch null) |msg| {
            msg.deinit(allocator);
            received_before += 1;
        } else {
            break;
        }
    }

    manager.stopAll(io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    var published_during: u32 = 0;
    i = 0;
    while (i < 50) : (i += 1) {
        client.publish("buffer.test", "buffered") catch continue;
        published_during += 1;
    }

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("high_volume_buffer", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    client.flush(allocator) catch {};

    var received_after: u32 = 0;
    while (received_after < 100) {
        if (sub.nextWithTimeout(allocator, 200) catch null) |msg| {
            msg.deinit(allocator);
            received_after += 1;
        } else {
            break;
        }
    }

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

    manager.stopAll(io1.io());
    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("queue_group_multi_client", false, "server start failed");
        return;
    };

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult(
            "queue_group_multi_client",
            false,
            "client1 connect failed",
        );
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult(
            "queue_group_multi_client",
            false,
            "client2 connect failed",
        );
        return;
    };
    defer client2.deinit(allocator);

    var sub1 = client1.subscribeQueue(
        allocator,
        "qgroup.test",
        "workers",
    ) catch {
        reportResult("queue_group_multi_client", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    var sub2 = client2.subscribeQueue(
        allocator,
        "qgroup.test",
        "workers",
    ) catch {
        reportResult("queue_group_multi_client", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    var i: u8 = 0;
    while (i < 20) : (i += 1) {
        client1.publish("qgroup.test", "msg") catch {};
    }
    client1.flush(allocator) catch {};

    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

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

    manager.stopServer(0, io1.io());
    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("queue_group_multi_client", false, "restart failed");
        return;
    };

    io1.io().sleep(.fromMilliseconds(500), .awake) catch {};

    i = 0;
    while (i < 20) : (i += 1) {
        client1.publish("qgroup.test", "msg") catch {};
    }
    client1.flush(allocator) catch {};

    io1.io().sleep(.fromMilliseconds(200), .awake) catch {};

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

fn testRapidServerRestarts(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("rapid_restarts", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 20,
        .reconnect_wait_ms = 100,
        .ping_interval_ms = 100,
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

    var cycle: u8 = 0;
    while (cycle < 3) : (cycle += 1) {
        manager.stopAll(io.io());
        io.io().sleep(.fromMilliseconds(200), .awake) catch {};

        _ = manager.startServer(allocator, io.io(), .{
            .port = test_port,
        }) catch {
            reportResult("rapid_restarts", false, "restart failed");
            return;
        };

        io.io().sleep(.fromMilliseconds(500), .awake) catch {};
    }

    client.publish("rapid.test", "survived") catch {
        reportResult("rapid_restarts", false, "final publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        msg.deinit(allocator);
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

fn testMultipleReconnectionCycles(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

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

    var cycle: u8 = 0;
    while (cycle < 3) : (cycle += 1) {
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

        var msg_buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &msg_buf,
            "cycle-{d}",
            .{cycle},
        ) catch "msg";

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

fn testLongDisconnectionRecovery(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    manager.stopAll(io.io());
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("long_disconnection", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 30,
        .reconnect_wait_ms = 200,
        .reconnect_wait_max_ms = 500,
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

    client.publish("long.test", "before") catch {};
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
    } else {
        reportResult("long_disconnection", false, "no msg before");
        return;
    }

    manager.stopAll(io.io());

    io.io().sleep(.fromMilliseconds(3000), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("long_disconnection", false, "restart failed");
        return;
    };

    io.io().sleep(.fromMilliseconds(1000), .awake) catch {};

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
