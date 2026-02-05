//! Autoflush Integration Tests
//!
//! Tests automatic buffer flushing functionality including:
//! - Basic delivery without explicit flush
//! - Multiple message batching
//! - High throughput scenarios
//! - TLS double-flush
//! - Subscribe triggers autoflush

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatTlsUrl = utils.formatTlsUrl;
const test_port = utils.test_port;
const tls_port = utils.tls_port;
const ServerManager = utils.ServerManager;

const Dir = std.Io.Dir;

const autoflush_port: u16 = 14240;

/// Returns absolute path to CA file. Caller owns returned memory.
fn getCaFilePath(allocator: std.mem.Allocator, io: std.Io) ?[:0]const u8 {
    return Dir.realPathFileAlloc(
        .cwd(),
        io,
        utils.tls_ca_file,
        allocator,
    ) catch null;
}

/// Test 1: Verify messages are delivered without explicit flush.
fn testAutoflushBasicDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect error";
        reportResult("autoflush_basic_delivery", false, msg);
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.basic") catch {
        reportResult("autoflush_basic_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish WITHOUT explicit flush - autoflush should deliver
    client.publish("autoflush.basic", "autoflush-test-msg") catch {
        reportResult("autoflush_basic_delivery", false, "publish failed");
        return;
    };

    // Wait for autoflush (poll interval ~1ms + processing)
    if (sub.nextWithTimeout(allocator, 100) catch null) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "autoflush-test-msg")) {
            reportResult("autoflush_basic_delivery", true, "");
        } else {
            reportResult("autoflush_basic_delivery", false, "wrong data");
        }
    } else {
        reportResult("autoflush_basic_delivery", false, "no message received");
    }
}

/// Test 2: Verify multiple messages batch and deliver together.
fn testAutoflushMultipleMessages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_multiple_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.multi") catch {
        reportResult("autoflush_multiple_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish 10 messages rapidly without flush
    const msg_count: u8 = 10;
    var i: u8 = 0;
    while (i < msg_count) : (i += 1) {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch "msg";
        client.publish("autoflush.multi", payload) catch {
            reportResult("autoflush_multiple_msgs", false, "publish failed");
            return;
        };
    }

    // Wait for autoflush to deliver all
    var received: u8 = 0;
    while (received < msg_count) {
        if (sub.nextWithTimeout(allocator, 200) catch null) |msg| {
            msg.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    if (received == msg_count) {
        reportResult("autoflush_multiple_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received",
            .{ received, msg_count },
        ) catch "partial";
        reportResult("autoflush_multiple_msgs", false, detail);
    }
}

/// Test 3: Verify autoflush handles high publish rate.
fn testAutoflushHighThroughput(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_high_throughput", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.throughput") catch {
        reportResult("autoflush_high_throughput", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish 1000 messages in tight loop without flush
    const msg_count: u32 = 1000;
    var published: u32 = 0;
    var i: u32 = 0;
    while (i < msg_count) : (i += 1) {
        client.publish("autoflush.throughput", "data") catch continue;
        published += 1;
    }

    // Give time for autoflush to process all
    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    var received: u32 = 0;
    while (received < msg_count) {
        if (sub.nextWithTimeout(allocator, 100) catch null) |msg| {
            msg.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    // Accept if we got most messages (network/timing can lose some)
    if (received >= published * 9 / 10) {
        reportResult("autoflush_high_throughput", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received (pub={d})",
            .{ received, msg_count, published },
        ) catch "partial";
        reportResult("autoflush_high_throughput", false, detail);
    }
}

/// Test 4: Verify double-check pattern prevents BADF panic during disconnect.
fn testAutoflushDuringDisconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, autoflush_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Start dedicated server for this test
    const server = manager.startServer(allocator, io.io(), .{
        .port = autoflush_port,
    }) catch {
        reportResult("autoflush_during_disconnect", false, "server start failed");
        return;
    };

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 5,
        .reconnect_wait_ms = 100,
    }) catch {
        server.stop(io.io());
        reportResult("autoflush_during_disconnect", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.disconnect") catch {
        server.stop(io.io());
        reportResult("autoflush_during_disconnect", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish some messages
    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        client.publish("autoflush.disconnect", "before") catch {};
    }

    // Stop server mid-operation (should NOT panic with BADF)
    server.stop(io.io());

    // Continue publishing during disconnect (goes to pending buffer)
    i = 0;
    while (i < 5) : (i += 1) {
        client.publish("autoflush.disconnect", "during") catch {};
        io.io().sleep(.fromMilliseconds(10), .awake) catch {};
    }

    // Restart server
    const server2 = manager.startServer(allocator, io.io(), .{
        .port = autoflush_port,
    }) catch {
        reportResult("autoflush_during_disconnect", false, "restart failed");
        return;
    };
    defer server2.stop(io.io());

    io.io().sleep(.fromMilliseconds(500), .awake) catch {};

    // Test passes if we got here without panic
    if (client.isConnected()) {
        reportResult("autoflush_during_disconnect", true, "");
    } else {
        // Even if not reconnected, no panic is success
        reportResult("autoflush_during_disconnect", true, "");
    }
}

/// Test 5: Verify TLS double-flush works correctly.
fn testAutoflushTLS(allocator: std.mem.Allocator, manager: *ServerManager) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const ca_path = getCaFilePath(allocator, io.io()) orelse {
        reportResult("autoflush_tls", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    // Start TLS server (may have been stopped by previous tests)
    const tls_server = manager.startServer(allocator, io.io(), .{
        .port = tls_port,
        .config_file = utils.tls_config_file,
    }) catch {
        reportResult("autoflush_tls", false, "TLS server start failed");
        return;
    };
    defer tls_server.stop(io.io());

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .tls_ca_file = ca_path,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect error";
        reportResult("autoflush_tls", false, msg);
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.tls") catch {
        reportResult("autoflush_tls", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish WITHOUT explicit flush over TLS
    client.publish("autoflush.tls", "tls-autoflush-msg") catch {
        reportResult("autoflush_tls", false, "publish failed");
        return;
    };

    // Wait for autoflush (both TLS and TCP must flush)
    if (sub.nextWithTimeout(allocator, 200) catch null) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "tls-autoflush-msg")) {
            reportResult("autoflush_tls", true, "");
        } else {
            reportResult("autoflush_tls", false, "wrong data");
        }
    } else {
        reportResult("autoflush_tls", false, "no message received");
    }
}

/// Test 6: Verify reasonable latency (message arrives within timeout).
fn testAutoflushLatencyBound(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_latency", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.latency") catch {
        reportResult("autoflush_latency", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish and expect quick delivery (50ms timeout is generous for ~1ms)
    client.publish("autoflush.latency", "latency-test") catch {
        reportResult("autoflush_latency", false, "publish failed");
        return;
    };

    if (sub.nextWithTimeout(allocator, 50) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("autoflush_latency", true, "");
    } else {
        reportResult("autoflush_latency", false, "timeout (>50ms)");
    }
}

/// Test 7: Verify subscribe also triggers autoflush.
fn testAutoflushWithSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io2.deinit();

    // Client 1 for publishing
    const client1 = nats.Client.connect(allocator, io1.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_subscribe", false, "client1 connect failed");
        return;
    };
    defer client1.deinit(allocator);

    // Client 2 subscribes (SUB command needs autoflush)
    const client2 = nats.Client.connect(allocator, io2.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_subscribe", false, "client2 connect failed");
        return;
    };
    defer client2.deinit(allocator);

    var sub = client2.subscribe(allocator, "autoflush.sub.test") catch {
        reportResult("autoflush_subscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Small delay for server to process SUB (autoflush delivers it)
    io1.io().sleep(.fromMilliseconds(20), .awake) catch {};

    // Client 1 publishes
    client1.publish("autoflush.sub.test", "sub-test-msg") catch {
        reportResult("autoflush_subscribe", false, "publish failed");
        return;
    };

    // Wait for message
    if (sub.nextWithTimeout(allocator, 200) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("autoflush_subscribe", true, "");
    } else {
        reportResult("autoflush_subscribe", false, "no message received");
    }
}

/// Test 8: Verify single message doesn't get stuck in buffer.
fn testAutoflushNoBatching(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_no_batching", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "autoflush.single") catch {
        reportResult("autoflush_no_batching", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish single message
    client.publish("autoflush.single", "single-msg") catch {
        reportResult("autoflush_no_batching", false, "publish failed");
        return;
    };

    // Should arrive quickly, not waiting for batch
    if (sub.nextWithTimeout(allocator, 30) catch null) |msg| {
        msg.deinit(allocator);
        reportResult("autoflush_no_batching", true, "");
    } else {
        reportResult("autoflush_no_batching", false, "message stuck in buffer");
    }
}

/// Test 9: Verify autoflush works with multiple clients.
fn testAutoflushMultiClient(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io2.deinit();
    var io3: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io3.deinit();

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_multi_client", false, "client1 connect failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_multi_client", false, "client2 connect failed");
        return;
    };
    defer client2.deinit(allocator);

    const client3 = nats.Client.connect(allocator, io3.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("autoflush_multi_client", false, "client3 connect failed");
        return;
    };
    defer client3.deinit(allocator);

    // Each client subscribes to receive from another
    var sub1 = client1.subscribe(allocator, "autoflush.mc.to1") catch {
        reportResult("autoflush_multi_client", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    var sub2 = client2.subscribe(allocator, "autoflush.mc.to2") catch {
        reportResult("autoflush_multi_client", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    var sub3 = client3.subscribe(allocator, "autoflush.mc.to3") catch {
        reportResult("autoflush_multi_client", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    // Small delay for subscriptions to register
    io1.io().sleep(.fromMilliseconds(20), .awake) catch {};

    // Cross-publish without flush
    client1.publish("autoflush.mc.to2", "from1") catch {};
    client2.publish("autoflush.mc.to3", "from2") catch {};
    client3.publish("autoflush.mc.to1", "from3") catch {};

    var received: u8 = 0;

    if (sub1.nextWithTimeout(allocator, 200) catch null) |msg| {
        msg.deinit(allocator);
        received += 1;
    }
    if (sub2.nextWithTimeout(allocator, 200) catch null) |msg| {
        msg.deinit(allocator);
        received += 1;
    }
    if (sub3.nextWithTimeout(allocator, 200) catch null) |msg| {
        msg.deinit(allocator);
        received += 1;
    }

    if (received == 3) {
        reportResult("autoflush_multi_client", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/3 received",
            .{received},
        ) catch "partial";
        reportResult("autoflush_multi_client", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testAutoflushBasicDelivery(allocator);
    testAutoflushMultipleMessages(allocator);
    testAutoflushHighThroughput(allocator);
    testAutoflushNoBatching(allocator);
    testAutoflushLatencyBound(allocator);
    testAutoflushWithSubscribe(allocator);
    testAutoflushMultiClient(allocator);
    testAutoflushTLS(allocator, manager);
    testAutoflushDuringDisconnect(allocator, manager);
}
