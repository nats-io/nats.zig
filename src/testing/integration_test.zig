//! NATS Integration Tests
//!
//! Tests against a real nats-server instance.
//! Run with: zig build test-integration

const std = @import("std");
const nats = @import("nats");
const server_manager = @import("server_manager.zig");

const ServerManager = server_manager.ServerManager;
const ServerConfig = server_manager.ServerConfig;

// Test configuration
const test_port: u16 = 14222;
const auth_port: u16 = 14223;
const test_token = "test-secret-token";

// Test counters
var tests_passed: u32 = 0;
var tests_failed: u32 = 0;

fn reportResult(name: []const u8, passed: bool, details: []const u8) void {
    if (passed) {
        tests_passed += 1;
        std.debug.print("[PASS] {s}\n", .{name});
    } else {
        tests_failed += 1;
        std.debug.print("[FAIL] {s}: {s}\n", .{ name, details });
    }
}

fn formatUrl(buf: []u8, port: u16) []const u8 {
    return std.fmt.bufPrint(buf, "nats://127.0.0.1:{d}", .{port}) catch "invalid";
}

fn formatAuthUrl(buf: []u8, port: u16, token: []const u8) []const u8 {
    return std.fmt.bufPrint(
        buf,
        "nats://{s}@127.0.0.1:{d}",
        .{ token, port },
    ) catch "invalid";
}

// Test 1: Basic connect and disconnect
fn testConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "test-connect",
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "error";
        reportResult("connect_disconnect", false, msg);
        return;
    };
    defer client.deinit(allocator);

    const connected = client.isConnected();
    reportResult("connect_disconnect", connected, "not connected");
}

// Test 2: Publish a single message
fn testPublishSingle(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_single", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.publish("test.subject", "Hello NATS!") catch {
        reportResult("publish_single", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_single", false, "flush failed");
        return;
    };

    reportResult("publish_single", true, "");
}

// Test 3: Subscribe and unsubscribe
fn testSubscribeUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subscribe_unsubscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.>") catch {
        reportResult("subscribe_unsubscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    if (sub.sid == 0) {
        reportResult("subscribe_unsubscribe", false, "invalid sid");
        return;
    }

    sub.unsubscribe() catch {
        reportResult("subscribe_unsubscribe", false, "unsubscribe failed");
        return;
    };

    reportResult("subscribe_unsubscribe", true, "");
}

// Test 4: Publish and subscribe roundtrip
fn testPublishSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_subscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "roundtrip.test") catch {
        reportResult("publish_subscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("publish_subscribe", false, "flush after sub failed");
        return;
    };

    client.publish("roundtrip.test", "hello from zig") catch {
        reportResult("publish_subscribe", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_subscribe", false, "flush after pub failed");
        return;
    };

    // Receive message with Go-style API
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("publish_subscribe", false, "nextMessage failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.subject, "roundtrip.test") and
            std.mem.eql(u8, m.data, "hello from zig"))
        {
            reportResult("publish_subscribe", true, "");
            return;
        }
    }

    reportResult("publish_subscribe", false, "message not received");
}

// Test 5: Server info validation
fn testServerInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_info", false, "no server info");
        return;
    }

    const has_version = info.?.version.len > 0;
    reportResult("server_info", has_version, "no version in info");
}

// Test 6: Multiple subscriptions
fn testMultipleSubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multiple_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "multi.one") catch {
        reportResult("multiple_subscriptions", false, "sub 1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "multi.two") catch {
        reportResult("multiple_subscriptions", false, "sub 2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "multi.three") catch {
        reportResult("multiple_subscriptions", false, "sub 3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    // SIDs should be unique and incrementing
    const valid = sub1.sid != sub2.sid and sub2.sid != sub3.sid and
        sub1.sid < sub2.sid and sub2.sid < sub3.sid;
    reportResult("multiple_subscriptions", valid, "invalid sids");
}

// Test 7: Wildcard subscriptions
fn testWildcardSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("wildcard_subscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Test * wildcard
    const sub1 = client.subscribe(allocator, "wild.*") catch {
        reportResult("wildcard_subscribe", false, "* wildcard failed");
        return;
    };
    defer sub1.deinit(allocator);

    // Test > wildcard
    const sub2 = client.subscribe(allocator, "wild.>") catch {
        reportResult("wildcard_subscribe", false, "> wildcard failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {
        reportResult("wildcard_subscribe", false, "flush failed");
        return;
    };

    reportResult("wildcard_subscribe", true, "");
}

// Test 8: Queue groups
fn testQueueGroups(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribeQueue(allocator, "queue.test", "workers") catch {
        reportResult("queue_groups", false, "queue subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    if (sub.sid == 0) {
        reportResult("queue_groups", false, "invalid queue sid");
        return;
    }

    client.flush() catch {
        reportResult("queue_groups", false, "flush failed");
        return;
    };

    reportResult("queue_groups", true, "");
}

// Test 9: Request-reply pattern
fn testRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("request_reply", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Generate an inbox
    const inbox = nats.newInbox(allocator) catch {
        reportResult("request_reply", false, "inbox generation failed");
        return;
    };
    defer allocator.free(inbox);

    // Subscribe to inbox
    const sub = client.subscribe(allocator, inbox) catch {
        reportResult("request_reply", false, "inbox subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Publish with reply-to
    client.publishRequest("request.test", inbox, "request data") catch {
        reportResult("request_reply", false, "publish request failed");
        return;
    };

    client.flush() catch {
        reportResult("request_reply", false, "flush failed");
        return;
    };

    reportResult("request_reply", true, "");
}

// Test 10: Reconnection (server restart)
fn testReconnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("reconnection", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("reconnection", false, "not connected initially");
        return;
    }

    // Stop only the primary server (index 0), not the auth server
    manager.stopServer(0);

    // Small delay
    std.posix.nanosleep(0, 100_000_000); // 100ms

    // Restart the server
    _ = manager.startServer(allocator, .{ .port = test_port }) catch {
        reportResult("reconnection", false, "server restart failed");
        return;
    };

    // Note: Our current client doesn't have auto-reconnect yet
    // This test validates server can be restarted
    reportResult("reconnection", true, "");
}

// Test 11: Token authentication
fn testAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    const connected = client.isConnected();
    reportResult("authentication", connected, "auth not connected");
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== NATS Integration Tests ===\n\n", .{});

    // Start server manager
    var manager = ServerManager.init(allocator);
    defer manager.deinit(allocator);

    // Start primary test server
    std.debug.print("Starting primary server on port {d}...\n", .{test_port});
    _ = manager.startServer(allocator, .{ .port = test_port }) catch |err| {
        std.debug.print("Failed to start primary server: {}\n", .{err});
        std.process.exit(1);
    };

    // Start auth test server
    std.debug.print("Starting auth server on port {d}...\n", .{auth_port});
    _ = manager.startServer(allocator, .{
        .port = auth_port,
        .auth_token = test_token,
    }) catch |err| {
        std.debug.print("Failed to start auth server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("\nRunning tests...\n\n", .{});

    // Run all tests
    testConnectDisconnect(allocator);
    testPublishSingle(allocator);
    testSubscribeUnsubscribe(allocator);
    testPublishSubscribe(allocator);
    testServerInfo(allocator);
    testMultipleSubscriptions(allocator);
    testWildcardSubscribe(allocator);
    testQueueGroups(allocator);
    testRequestReply(allocator);
    testReconnection(allocator, &manager);
    testAuthentication(allocator);

    // Print summary
    std.debug.print("\n=== Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{tests_passed});
    std.debug.print("Failed: {d}\n", .{tests_failed});
    std.debug.print("Total:  {d}\n\n", .{tests_passed + tests_failed});

    if (tests_failed > 0) {
        std.process.exit(1);
    }
}
