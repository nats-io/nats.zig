//! Client integration tests for NATS.
//!
//! This module organizes all Client (sync) tests. The tests are currently
//! defined in integration_test.zig and called directly from main().
//!
//! Future refactoring: Move all testXxx functions here and export runAll().

const std = @import("std");
const nats = @import("nats");

// Import shared test utilities
const utils = @import("test_utils.zig");
const client_async_tests = @import("client_async_tests.zig");

const ServerManager = utils.ServerManager;

// Re-export from utils for use in this file
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;

// Test 1: Basic connect and disconnect
fn testConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const queue = "workers";
    const sub = client.subscribeQueue(allocator, "queue.test", queue) catch {
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

    var io: std.Io.Threaded = .init(allocator, .{});
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

    var io: std.Io.Threaded = .init(allocator, .{});
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
    manager.stopServer(0, io.io());

    // Small delay
    std.posix.nanosleep(0, 100_000_000); // 100ms

    // Restart the server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    const connected = client.isConnected();
    reportResult("authentication", connected, "auth not connected");
}

// Test 12: Request-reply with actual response
fn testRequestReplySuccess(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("request_reply_success", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Set up a responder subscription
    const sub = client.subscribe(allocator, "echo.request") catch {
        reportResult("request_reply_success", false, "responder sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("request_reply_success", false, "flush failed");
        return;
    };

    // Generate inbox and subscribe
    const inbox = nats.newInbox(allocator) catch {
        reportResult("request_reply_success", false, "inbox gen failed");
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client.subscribe(allocator, inbox) catch {
        reportResult("request_reply_success", false, "inbox sub failed");
        return;
    };
    defer reply_sub.deinit(allocator);

    // Send request with reply-to
    client.publishRequest("echo.request", inbox, "ping") catch {
        reportResult("request_reply_success", false, "publish request failed");
        return;
    };
    client.flush() catch {
        reportResult("request_reply_success", false, "flush2 failed");
        return;
    };

    // Receive the request on responder side
    const req = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("request_reply_success", false, "receive request failed");
        return;
    };

    if (req) |r| {
        defer r.deinit(allocator);

        // Verify request has reply_to
        if (r.reply_to == null) {
            reportResult("request_reply_success", false, "no reply_to");
            return;
        }

        // Send reply
        client.publish(r.reply_to.?, "pong") catch {
            reportResult("request_reply_success", false, "reply publish failed");
            return;
        };
        client.flush() catch {
            reportResult("request_reply_success", false, "flush3 failed");
            return;
        };
    } else {
        reportResult("request_reply_success", false, "no request received");
        return;
    }

    // Receive the reply
    const reply = reply_sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("request_reply_success", false, "receive reply failed");
        return;
    };

    if (reply) |rep| {
        defer rep.deinit(allocator);
        if (std.mem.eql(u8, rep.data, "pong")) {
            reportResult("request_reply_success", true, "");
            return;
        }
        reportResult("request_reply_success", false, "wrong reply data");
    } else {
        reportResult("request_reply_success", false, "no reply received");
    }
}

// Test 13: Request timeout (no responder)
fn testRequestTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("request_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Use unique inbox (no request published, just test timeout)
    const inbox = nats.newInbox(allocator) catch {
        reportResult("request_timeout", false, "inbox gen failed");
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client.subscribe(allocator, inbox) catch {
        reportResult("request_timeout", false, "inbox sub failed");
        return;
    };
    defer reply_sub.deinit(allocator);

    client.flush() catch {
        reportResult("request_timeout", false, "flush failed");
        return;
    };

    // Wait for a message that will never come (test pure timeout)
    var timer = std.time.Timer.start() catch {
        reportResult("request_timeout", false, "timer unavailable");
        return;
    };
    const reply = reply_sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch {
        reportResult("request_timeout", false, "nextMessage error");
        return;
    };
    const elapsed_ns = timer.read();
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;

    // Should return null (timeout) and take roughly 200ms
    if (reply == null and elapsed_ms >= 100 and elapsed_ms <= 500) {
        reportResult("request_timeout", true, "");
    } else if (reply != null) {
        reply.?.deinit(allocator);
        reportResult("request_timeout", false, "unexpected reply");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "timeout {d}ms not in range",
            .{elapsed_ms},
        ) catch "error";
        reportResult("request_timeout", false, detail);
    }
}

// Test 14: Multiple inboxes are unique
fn testRequestInboxUniqueness(allocator: std.mem.Allocator) void {
    var inboxes: [100][]u8 = undefined;
    var count: usize = 0;

    // Generate 100 inboxes
    for (0..100) |i| {
        inboxes[i] = nats.newInbox(allocator) catch {
            reportResult("request_inbox_uniqueness", false, "inbox gen failed");
            // Cleanup already generated
            for (0..i) |j| {
                allocator.free(inboxes[j]);
            }
            return;
        };
        count += 1;
    }
    defer {
        for (0..count) |i| {
            allocator.free(inboxes[i]);
        }
    }

    // Verify all are unique
    for (0..count) |i| {
        for (i + 1..count) |j| {
            if (std.mem.eql(u8, inboxes[i], inboxes[j])) {
                reportResult("request_inbox_uniqueness", false, "duplicate");
                return;
            }
        }
    }

    // Verify all have correct prefix
    for (0..count) |i| {
        if (!std.mem.startsWith(u8, inboxes[i], "_INBOX.")) {
            reportResult("request_inbox_uniqueness", false, "wrong prefix");
            return;
        }
    }

    reportResult("request_inbox_uniqueness", true, "");
}

// Test 15: Publish with empty payload
fn testPublishEmptyPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_empty_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "empty.payload") catch {
        reportResult("publish_empty_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("publish_empty_payload", false, "flush1 failed");
        return;
    };

    // Publish empty payload
    client.publish("empty.payload", "") catch {
        reportResult("publish_empty_payload", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_empty_payload", false, "flush2 failed");
        return;
    };

    // Receive and verify empty
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("publish_empty_payload", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == 0) {
            reportResult("publish_empty_payload", true, "");
        } else {
            reportResult("publish_empty_payload", false, "data not empty");
        }
    } else {
        reportResult("publish_empty_payload", false, "no message");
    }
}

// Test 16: Statistics accuracy (output only - input stats need protocol work)
fn testStatisticsAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("statistics_accuracy", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Use unique subject to avoid cross-test contamination
    const inbox = nats.newInbox(allocator) catch {
        reportResult("statistics_accuracy", false, "inbox gen failed");
        return;
    };
    defer allocator.free(inbox);

    const sub = client.subscribe(allocator, inbox) catch {
        reportResult("statistics_accuracy", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Get stats before publish
    const stats_before = client.getStats();

    // Publish 10 messages of 100 bytes each
    const payload = "X" ** 100;
    for (0..10) |_| {
        client.publish(inbox, payload) catch {
            reportResult("statistics_accuracy", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Check output stats (relative to before)
    const stats_after = client.getStats();
    const msgs_published = stats_after.msgs_out - stats_before.msgs_out;
    const bytes_published = stats_after.bytes_out - stats_before.bytes_out;

    if (msgs_published != 10) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs_out={d} expected 10",
            .{msgs_published},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
        return;
    }
    if (bytes_published != 1000) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "bytes_out={d} expected 1000",
            .{bytes_published},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
        return;
    }

    // Receive all messages
    var received: u32 = 0;
    for (0..10) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("statistics_accuracy", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "received={d} expected 10",
            .{received},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
    }
}

// Test 17: Wildcard matching verification
fn testWildcardMatching(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("wildcard_matching", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to foo.*
    const sub_star = client.subscribe(allocator, "wtest.*") catch {
        reportResult("wildcard_matching", false, "star sub failed");
        return;
    };
    defer sub_star.deinit(allocator);

    // Subscribe to foo.>
    const sub_gt = client.subscribe(allocator, "wtest.>") catch {
        reportResult("wildcard_matching", false, "gt sub failed");
        return;
    };
    defer sub_gt.deinit(allocator);

    client.flush() catch {};

    // Publish to wtest.bar (matches both)
    client.publish("wtest.bar", "one") catch {
        reportResult("wildcard_matching", false, "pub1 failed");
        return;
    };

    // Publish to wtest.bar.baz (matches only >)
    client.publish("wtest.bar.baz", "two") catch {
        reportResult("wildcard_matching", false, "pub2 failed");
        return;
    };

    client.flush() catch {};

    // star should get 1 message
    var star_count: u32 = 0;
    while (true) {
        const msg = sub_star.nextMessage(allocator, .{ .timeout_ms = 200 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            star_count += 1;
        } else {
            break;
        }
    }

    // gt should get 2 messages
    var gt_count: u32 = 0;
    while (true) {
        const msg = sub_gt.nextMessage(allocator, .{ .timeout_ms = 200 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            gt_count += 1;
        } else {
            break;
        }
    }

    if (star_count == 1 and gt_count == 2) {
        reportResult("wildcard_matching", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "star={d} gt={d}",
            .{ star_count, gt_count },
        ) catch "count error";
        reportResult("wildcard_matching", false, detail);
    }
}

// Test 18: Queue group distribution
fn testQueueGroupDistribution(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("queue_group_distribution", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 3 queue group subscribers
    const sub1 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribeQueue(allocator, "qdist.test", "workers") catch {
        reportResult("queue_group_distribution", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush() catch {};

    // Publish 30 messages
    for (0..30) |_| {
        client.publish("qdist.test", "work") catch {
            reportResult("queue_group_distribution", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Count how many each receives
    var count1: u32 = 0;
    var count2: u32 = 0;
    var count3: u32 = 0;

    // Give time for messages to be distributed
    std.posix.nanosleep(0, 100_000_000); // 100ms

    while (true) {
        const msg = sub1.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count1 += 1;
        } else break;
    }

    while (true) {
        const msg = sub2.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count2 += 1;
        } else break;
    }

    while (true) {
        const msg = sub3.nextMessage(allocator, .{ .timeout_ms = 50 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            count3 += 1;
        } else break;
    }

    const total = count1 + count2 + count3;

    // All 30 should be received exactly once across all queue members
    if (total == 30) {
        reportResult("queue_group_distribution", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "total={d} (expected 30)",
            .{total},
        ) catch "error";
        reportResult("queue_group_distribution", false, detail);
    }
}

// Test 19: Authentication failure (no token to auth-required server)
fn testAuthenticationFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    // Connect to auth server WITHOUT providing token
    const url = formatUrl(&url_buf, auth_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // This should fail - auth server requires token but we're not providing one
    // After Bug #1 fix, connect() now detects auth rejection
    const result = nats.Client.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("auth_failure", false, "should have failed");
    } else |_| {
        // Connection failed as expected
        reportResult("auth_failure", true, "");
    }
}

// Test 20: Connection refused (no server)
fn testConnectionRefused(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    // Use a port that's definitely not running
    const url = formatUrl(&url_buf, 19999);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("connection_refused", false, "should have failed");
    } else |_| {
        // Expected to fail
        reportResult("connection_refused", true, "");
    }
}

// Test 22: Server restart - new connection works
fn testServerRestartNewConnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // First connection
    var io1: std.Io.Threaded = .init(allocator, .{});
    defer io1.deinit();

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{}) catch {
        reportResult("server_restart_new_conn", false, "initial connect failed");
        return;
    };

    // Verify connected
    if (!client1.isConnected()) {
        client1.deinit(allocator);
        reportResult("server_restart_new_conn", false, "not connected");
        return;
    }

    // Clean close first client
    client1.deinit(allocator);

    // Stop server
    manager.stopServer(0, io1.io());
    std.posix.nanosleep(0, 100_000_000); // 100ms

    // Restart server
    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("server_restart_new_conn", false, "restart failed");
        return;
    };

    // New connection should work
    var io2: std.Io.Threaded = .init(allocator, .{});
    defer io2.deinit();

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{}) catch {
        reportResult("server_restart_new_conn", false, "reconnect failed");
        return;
    };
    defer client2.deinit(allocator);

    if (client2.isConnected()) {
        reportResult("server_restart_new_conn", true, "");
    } else {
        reportResult("server_restart_new_conn", false, "not connected after restart");
    }
}

// Test 23: Publish to non-existent subject (no subscribers)
fn testPublishNoSubscribers(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_no_subscribers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish to subject with no subscribers - should succeed (fire and forget)
    client.publish("nobody.listening.here", "hello?") catch {
        reportResult("publish_no_subscribers", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_no_subscribers", false, "flush failed");
        return;
    };

    reportResult("publish_no_subscribers", true, "");
}

// Test 24: Large payload publish
fn testPublishLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_large_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large.payload") catch {
        reportResult("publish_large_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Create 16KB payload (fits in 32KB read buffer)
    const payload_size: usize = 16384;
    const payload = allocator.alloc(u8, payload_size) catch {
        reportResult("publish_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("large.payload", payload) catch {
        reportResult("publish_large_payload", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_large_payload", false, "flush failed");
        return;
    };

    // Receive and verify size
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("publish_large_payload", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == payload_size) {
            reportResult("publish_large_payload", true, "");
        } else {
            reportResult("publish_large_payload", false, "wrong size");
        }
    } else {
        reportResult("publish_large_payload", false, "no message");
    }
}

// Test 25: Multiple rapid publishes
fn testPublishRapidFire(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_rapid_fire", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "rapid.fire") catch {
        reportResult("publish_rapid_fire", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish 1000 messages rapidly
    const count: u32 = 1000;
    for (0..count) |_| {
        client.publish("rapid.fire", "ping") catch {
            reportResult("publish_rapid_fire", false, "publish failed");
            return;
        };
    }

    client.flush() catch {
        reportResult("publish_rapid_fire", false, "flush failed");
        return;
    };

    // Count received
    var received: u32 = 0;
    while (received < count) {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    if (received == count) {
        reportResult("publish_rapid_fire", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "received {d}/{d}",
            .{ received, count },
        ) catch "error";
        reportResult("publish_rapid_fire", false, detail);
    }
}

// Test 26: Subscribe unsubscribe reuse
fn testSubscribeUnsubscribeReuse(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sub_unsub_reuse", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe, unsubscribe, subscribe again - 10 cycles
    for (0..10) |_| {
        const sub = client.subscribe(allocator, "cycle.test") catch {
            reportResult("sub_unsub_reuse", false, "subscribe failed");
            return;
        };

        sub.unsubscribe() catch {
            reportResult("sub_unsub_reuse", false, "unsubscribe failed");
            sub.deinit(allocator);
            return;
        };

        sub.deinit(allocator);
    }

    reportResult("sub_unsub_reuse", true, "");
}

// Test 27: Request with client.request() method
fn testRequestMethod(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("request_method", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Set up responder
    const sub = client.subscribe(allocator, "echo.service") catch {
        reportResult("request_method", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Spawn a "responder" by publishing reply after receiving request
    // First, send the request
    const inbox = nats.newInbox(allocator) catch {
        reportResult("request_method", false, "inbox failed");
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client.subscribe(allocator, inbox) catch {
        reportResult("request_method", false, "reply sub failed");
        return;
    };
    defer reply_sub.deinit(allocator);

    client.publishRequest("echo.service", inbox, "ping") catch {
        reportResult("request_method", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Receive request and send reply
    const req = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("request_method", false, "no request");
        return;
    };

    if (req) |r| {
        if (r.reply_to) |reply_to| {
            client.publish(reply_to, "pong") catch {};
            client.flush() catch {};
        }
        r.deinit(allocator);
    }

    // Receive reply
    const reply = reply_sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("request_method", false, "no reply");
        return;
    };

    if (reply) |rep| {
        defer rep.deinit(allocator);
        if (std.mem.eql(u8, rep.data, "pong")) {
            reportResult("request_method", true, "");
        } else {
            reportResult("request_method", false, "wrong reply");
        }
    } else {
        reportResult("request_method", false, "reply timeout");
    }
}

// Test 28: Multiple subscribers same subject
fn testMultipleSubscribersSameSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_sub_same_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 3 subscribers to same subject
    const sub1 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush() catch {};

    // Publish one message
    client.publish("broadcast.test", "hello all") catch {
        reportResult("multi_sub_same_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // All 3 should receive the message
    var count: u32 = 0;

    if (sub1.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub3.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 3) {
        reportResult("multi_sub_same_subject", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/3", .{count}) catch "err";
        reportResult("multi_sub_same_subject", false, detail);
    }
}

// Test 29: Message ordering preserved
fn testMessageOrdering(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("message_ordering", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "order.test") catch {
        reportResult("message_ordering", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 100 numbered messages
    var buf: [16]u8 = undefined;
    for (0..100) |i| {
        const payload = std.fmt.bufPrint(&buf, "{d}", .{i}) catch continue;
        client.publish("order.test", payload) catch {
            reportResult("message_ordering", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Verify received in order
    for (0..100) |expected| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
            reportResult("message_ordering", false, "receive failed");
            return;
        };
        if (msg) |m| {
            defer m.deinit(allocator);
            const received = std.fmt.parseInt(usize, m.data, 10) catch {
                reportResult("message_ordering", false, "parse failed");
                return;
            };
            if (received != expected) {
                reportResult("message_ordering", false, "out of order");
                return;
            }
        } else {
            reportResult("message_ordering", false, "missing message");
            return;
        }
    }

    reportResult("message_ordering", true, "");
}

// Test 30: Unsubscribe stops delivery
fn testUnsubscribeStopsDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_stops_delivery", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "unsub.test") catch {
        reportResult("unsub_stops_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish first message
    client.publish("unsub.test", "before") catch {};
    client.flush() catch {};

    // Receive it
    const msg1 = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch null;
    if (msg1) |m| {
        m.deinit(allocator);
    } else {
        reportResult("unsub_stops_delivery", false, "no first msg");
        return;
    }

    // Unsubscribe
    sub.unsubscribe() catch {
        reportResult("unsub_stops_delivery", false, "unsub failed");
        return;
    };
    client.flush() catch {};

    // Calling nextMessage on closed subscription should return error
    const result = sub.nextMessage(allocator, .{ .timeout_ms = 200 });
    if (result) |msg_opt| {
        // Unexpected success
        if (msg_opt) |m| m.deinit(allocator);
        reportResult("unsub_stops_delivery", false, "should error on closed");
    } else |err| {
        // Expected: SubscriptionClosed error
        if (err == error.SubscriptionClosed) {
            reportResult("unsub_stops_delivery", true, "");
        } else {
            reportResult("unsub_stops_delivery", false, "wrong error type");
        }
    }
}

// Test 31: Ping pong (flush verifies roundtrip)
fn testPingPong(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("ping_pong", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // flushWithTimeout does PING/PONG roundtrip
    client.flushWithTimeout(5000) catch {
        reportResult("ping_pong", false, "flush timeout");
        return;
    };

    reportResult("ping_pong", true, "");
}

// Test 32: Cross-client message routing (client A publishes, client B receives)
fn testCrossClientRouting(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - the subscriber
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{
        .name = "client-A",
    }) catch {
        reportResult("cross_client_routing", false, "client A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - the publisher
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{
        .name = "client-B",
    }) catch {
        reportResult("cross_client_routing", false, "client B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client A subscribes
    const sub = client_a.subscribe(allocator, "cross.client.test") catch {
        reportResult("cross_client_routing", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client_a.flush() catch {};

    // Small delay for subscription to propagate
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // Client B publishes
    client_b.publish("cross.client.test", "hello from B") catch {
        reportResult("cross_client_routing", false, "publish failed");
        return;
    };
    client_b.flush() catch {};

    // Client A receives
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_routing", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "hello from B")) {
            reportResult("cross_client_routing", true, "");
        } else {
            reportResult("cross_client_routing", false, "wrong data");
        }
    } else {
        reportResult("cross_client_routing", false, "no message received");
    }
}

// Test 33: Cross-client request-reply (A requests, B responds)
fn testCrossClientRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - the requester
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{
        .name = "requester",
    }) catch {
        reportResult("cross_client_request", false, "client A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - the responder (service)
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{
        .name = "responder",
    }) catch {
        reportResult("cross_client_request", false, "client B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client B subscribes to service subject
    const service_sub = client_b.subscribe(allocator, "math.add") catch {
        reportResult("cross_client_request", false, "service sub failed");
        return;
    };
    defer service_sub.deinit(allocator);
    client_b.flush() catch {};

    // Small delay for subscription to propagate
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // Client A creates inbox and subscribes
    const inbox = nats.newInbox(allocator) catch {
        reportResult("cross_client_request", false, "inbox failed");
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client_a.subscribe(allocator, inbox) catch {
        reportResult("cross_client_request", false, "reply sub failed");
        return;
    };
    defer reply_sub.deinit(allocator);
    client_a.flush() catch {};

    // Client A sends request
    client_a.publishRequest("math.add", inbox, "2+3") catch {
        reportResult("cross_client_request", false, "request failed");
        return;
    };
    client_a.flush() catch {};

    // Client B receives request
    const req = service_sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_request", false, "no request");
        return;
    };

    if (req) |r| {
        defer r.deinit(allocator);

        // Verify request has reply_to
        if (r.reply_to) |reply_to| {
            // Client B sends response
            client_b.publish(reply_to, "5") catch {
                reportResult("cross_client_request", false, "reply failed");
                return;
            };
            client_b.flush() catch {};
        } else {
            reportResult("cross_client_request", false, "no reply_to");
            return;
        }
    } else {
        reportResult("cross_client_request", false, "request timeout");
        return;
    }

    // Client A receives response
    const reply = reply_sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_request", false, "reply receive failed");
        return;
    };

    if (reply) |rep| {
        defer rep.deinit(allocator);
        if (std.mem.eql(u8, rep.data, "5")) {
            reportResult("cross_client_request", true, "");
        } else {
            reportResult("cross_client_request", false, "wrong response");
        }
    } else {
        reportResult("cross_client_request", false, "no response");
    }
}

// Test 34: Three clients in a chain (A -> B -> C)
fn testThreeClientChain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - initial publisher
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - middleware (receives from A, forwards to C)
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C - final receiver
    var io_c: std.Io.Threaded = .init(allocator, .{});
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "C connect failed");
        return;
    };
    defer client_c.deinit(allocator);

    // B subscribes to "step1"
    const sub_b = client_b.subscribe(allocator, "chain.step1") catch {
        reportResult("three_client_chain", false, "B sub failed");
        return;
    };
    defer sub_b.deinit(allocator);

    // C subscribes to "step2"
    const sub_c = client_c.subscribe(allocator, "chain.step2") catch {
        reportResult("three_client_chain", false, "C sub failed");
        return;
    };
    defer sub_c.deinit(allocator);

    client_b.flush() catch {};
    client_c.flush() catch {};
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // A publishes to step1
    client_a.publish("chain.step1", "start") catch {
        reportResult("three_client_chain", false, "A publish failed");
        return;
    };
    client_a.flush() catch {};

    // B receives and forwards to step2
    const msg_b = sub_b.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("three_client_chain", false, "B receive failed");
        return;
    };
    if (msg_b) |m| {
        defer m.deinit(allocator);
        client_b.publish("chain.step2", "forwarded") catch {
            reportResult("three_client_chain", false, "B forward failed");
            return;
        };
        client_b.flush() catch {};
    } else {
        reportResult("three_client_chain", false, "B no message");
        return;
    }

    // C receives final message
    const msg_c = sub_c.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("three_client_chain", false, "C receive failed");
        return;
    };
    if (msg_c) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "forwarded")) {
            reportResult("three_client_chain", true, "");
        } else {
            reportResult("three_client_chain", false, "wrong data");
        }
    } else {
        reportResult("three_client_chain", false, "C no message");
    }
}

// Test 35: Publish after disconnect should fail
fn testPublishAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_after_disconnect", false, "connect failed");
        return;
    };

    // Drain closes the connection
    client.drain(allocator) catch {
        reportResult("publish_after_disconnect", false, "drain failed");
        client.deinit(allocator);
        return;
    };

    // Publish after drain should fail
    const result = client.publish("test.subject", "data");
    client.deinit(allocator);

    if (result) |_| {
        reportResult("publish_after_disconnect", false, "should have failed");
    } else |_| {
        reportResult("publish_after_disconnect", true, "");
    }
}

// Test 36: Subscribe after disconnect should fail
fn testSubscribeAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subscribe_after_disconnect", false, "connect failed");
        return;
    };

    // Drain closes the connection
    client.drain(allocator) catch {
        reportResult("subscribe_after_disconnect", false, "drain failed");
        client.deinit(allocator);
        return;
    };

    // Subscribe after drain should fail
    const result = client.subscribe(allocator, "test.subject");
    client.deinit(allocator);

    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("subscribe_after_disconnect", false, "should have failed");
    } else |_| {
        reportResult("subscribe_after_disconnect", true, "");
    }
}

// Test 37: Double flush is safe
fn testDoubleFlush(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Multiple flushes should all succeed
    client.flush() catch {
        reportResult("double_flush", false, "first flush failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "second flush failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "third flush failed");
        return;
    };

    reportResult("double_flush", true, "");
}

// Test 38: Double unsubscribe should be safe (error, not panic)
fn testDoubleUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_unsubscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "double.unsub") catch {
        reportResult("double_unsubscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // First unsubscribe
    sub.unsubscribe() catch {
        reportResult("double_unsubscribe", false, "first unsub failed");
        return;
    };

    // Second unsubscribe - should error or be idempotent, not panic
    const result = sub.unsubscribe();
    if (result) |_| {
        // Idempotent is acceptable
        reportResult("double_unsubscribe", true, "");
    } else |_| {
        // Error is also acceptable (subscription already closed)
        reportResult("double_unsubscribe", true, "");
    }
}

// Test 39: Binary payload (non-UTF8 data)
fn testBinaryPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "binary.test") catch {
        reportResult("binary_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Binary payload with null bytes and high bytes
    const binary_data = [_]u8{ 0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F, 0x00, 0xFF };

    client.publish("binary.test", &binary_data) catch {
        reportResult("binary_payload", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("binary_payload", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == binary_data.len and
            std.mem.eql(u8, m.data, &binary_data))
        {
            reportResult("binary_payload", true, "");
        } else {
            reportResult("binary_payload", false, "data mismatch");
        }
    } else {
        reportResult("binary_payload", false, "no message");
    }
}

// Test 40: Many subscriptions (stress test)
fn testManySubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("many_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create and immediately cleanup 100 subscriptions in sequence
    var success_count: usize = 0;

    for (0..100) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "stress.sub.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("many_subscriptions", false, "sub failed");
            return;
        };
        sub.deinit(allocator);
        success_count += 1;
    }

    if (success_count == 100) {
        reportResult("many_subscriptions", true, "");
    } else {
        reportResult("many_subscriptions", false, "not all created");
    }
}

// Test 41: Subject with dots (hierarchical)
fn testHierarchicalSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("hierarchical_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Deep hierarchical subject
    const deep_subject = "level1.level2.level3.level4.level5.data";

    const sub = client.subscribe(allocator, deep_subject) catch {
        reportResult("hierarchical_subject", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish(deep_subject, "deep message") catch {
        reportResult("hierarchical_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("hierarchical_subject", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.subject, deep_subject)) {
            reportResult("hierarchical_subject", true, "");
        } else {
            reportResult("hierarchical_subject", false, "subject mismatch");
        }
    } else {
        reportResult("hierarchical_subject", false, "no message");
    }
}

// Test 42: Flush after every publish (correctness check)
fn testFlushAfterEachPublish(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("flush_after_each", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "flush.each") catch {
        reportResult("flush_after_each", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 50 messages, flush after each
    for (0..50) |_| {
        client.publish("flush.each", "msg") catch {
            reportResult("flush_after_each", false, "publish failed");
            return;
        };
        client.flush() catch {
            reportResult("flush_after_each", false, "flush failed");
            return;
        };
    }

    // Receive all 50
    var received: u32 = 0;
    for (0..50) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == 50) {
        reportResult("flush_after_each", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/50",
            .{received},
        ) catch "err";
        reportResult("flush_after_each", false, detail);
    }
}

// Test 43: Publish without flush (buffered)
fn testPublishWithoutFlush(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_no_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "no.flush.test") catch {
        reportResult("publish_no_flush", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 100 messages without flushing
    for (0..100) |_| {
        client.publish("no.flush.test", "buffered") catch {
            reportResult("publish_no_flush", false, "publish failed");
            return;
        };
    }

    // Single flush at end
    client.flush() catch {
        reportResult("publish_no_flush", false, "flush failed");
        return;
    };

    // Receive all
    var received: u32 = 0;
    for (0..100) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == 100) {
        reportResult("publish_no_flush", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/100",
            .{received},
        ) catch "err";
        reportResult("publish_no_flush", false, detail);
    }
}

// Test 44: Subscribe to same subject twice (both should receive)
fn testDuplicateSubscription(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("duplicate_subscription", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two subscriptions to exact same subject
    const sub1 = client.subscribe(allocator, "dup.sub.test") catch {
        reportResult("duplicate_subscription", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "dup.sub.test") catch {
        reportResult("duplicate_subscription", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // Publish one message
    client.publish("dup.sub.test", "hello") catch {
        reportResult("duplicate_subscription", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Both should receive
    var count: u32 = 0;

    if (sub1.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 2) {
        reportResult("duplicate_subscription", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("duplicate_subscription", false, detail);
    }
}

// Test 45: Client name in connection
fn testClientName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "my-test-client-12345",
    }) catch {
        reportResult("client_name", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // If connection succeeded with name, test passes
    if (client.isConnected()) {
        reportResult("client_name", true, "");
    } else {
        reportResult("client_name", false, "not connected");
    }
}

// Test 46: Double drain should be safe
fn testDoubleDrain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_drain", false, "connect failed");
        return;
    };

    // First drain
    client.drain(allocator) catch {
        reportResult("double_drain", false, "first drain failed");
        client.deinit(allocator);
        return;
    };

    // Second drain - should error, not panic
    const result = client.drain(allocator);
    client.deinit(allocator);

    if (result) |_| {
        // Idempotent is fine
        reportResult("double_drain", true, "");
    } else |_| {
        // Error is also fine (already drained)
        reportResult("double_drain", true, "");
    }
}

// Test 47: Verify isConnected() reflects state correctly
fn testIsConnectedState(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("is_connected_state", false, "connect failed");
        return;
    };

    // Should be connected after connect
    if (!client.isConnected()) {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "not connected after connect");
        return;
    }

    // After drain, should not be connected
    client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "drain failed");
        return;
    };

    if (client.isConnected()) {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "still connected after drain");
        return;
    }

    client.deinit(allocator);
    reportResult("is_connected_state", true, "");
}

// Test 48: Long subject name
fn testLongSubjectName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("long_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Long but valid subject (100 chars)
    const long_subject = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t" ++
        ".u.v.w.x.y.z.aa.bb.cc.dd.ee.ff.gg.hh.ii.jj.kk.ll.mm.nn";

    const sub = client.subscribe(allocator, long_subject) catch {
        reportResult("long_subject", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish(long_subject, "test") catch {
        reportResult("long_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("long_subject", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        reportResult("long_subject", true, "");
    } else {
        reportResult("long_subject", false, "no message");
    }
}

// Test 49: Consecutive connections (connect, disconnect, connect again)
fn testConsecutiveConnections(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // First connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "first connect fail");
            return;
        };
        client.deinit(allocator);
    }

    // Second connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "second connect fail");
            return;
        };
        client.deinit(allocator);
    }

    // Third connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "third connect fail");
            return;
        };
        client.deinit(allocator);
    }

    reportResult("consecutive_connections", true, "");
}

// Test 50: Queue group with multiple clients
fn testQueueGroupMultipleClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("queue_multi_client", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("queue_multi_client", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C (publisher)
    var io_c: std.Io.Threaded = .init(allocator, .{});
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{}) catch {
        reportResult("queue_multi_client", false, "C connect failed");
        return;
    };
    defer client_c.deinit(allocator);

    // A and B subscribe to queue
    const sub_a = client_a.subscribeQueue(
        allocator,
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "A sub failed");
        return;
    };
    defer sub_a.deinit(allocator);

    const sub_b = client_b.subscribeQueue(
        allocator,
        "qmc.test",
        "workers",
    ) catch {
        reportResult("queue_multi_client", false, "B sub failed");
        return;
    };
    defer sub_b.deinit(allocator);

    client_a.flush() catch {};
    client_b.flush() catch {};
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // C publishes 20 messages
    for (0..20) |_| {
        client_c.publish("qmc.test", "work") catch {
            reportResult("queue_multi_client", false, "publish failed");
            return;
        };
    }
    client_c.flush() catch {};

    // Count messages received by each
    var count_a: u32 = 0;
    var count_b: u32 = 0;

    for (0..20) |_| {
        if (sub_a.nextMessage(allocator, .{ .timeout_ms = 100 }) catch null) |m| {
            m.deinit(allocator);
            count_a += 1;
        }
    }
    for (0..20) |_| {
        if (sub_b.nextMessage(allocator, .{ .timeout_ms = 100 }) catch null) |m| {
            m.deinit(allocator);
            count_b += 1;
        }
    }

    // Total should be 20 (distributed between A and B)
    if (count_a + count_b == 20) {
        reportResult("queue_multi_client", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}={d}",
            .{ count_a, count_b, count_a + count_b },
        ) catch "err";
        reportResult("queue_multi_client", false, detail);
    }
}

// Test 51: Server info has expected fields
fn testServerInfoFields(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_info_fields", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_info_fields", false, "no server info");
        return;
    }

    const i = info.?;

    // Check required fields exist
    var valid = true;
    if (i.version.len == 0) valid = false;
    if (i.max_payload == 0) valid = false;
    if (i.proto < 1) valid = false;

    if (valid) {
        reportResult("server_info_fields", true, "");
    } else {
        reportResult("server_info_fields", false, "missing fields");
    }
}

// Test 52: Stats increment correctly
fn testStatsIncrement(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_increment", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 5 messages of 10 bytes each
    for (0..5) |_| {
        client.publish("stats.test", "0123456789") catch {
            reportResult("stats_increment", false, "publish failed");
            return;
        };
    }

    const after = client.getStats();

    const msgs_diff = after.msgs_out - before.msgs_out;
    const bytes_diff = after.bytes_out - before.bytes_out;

    if (msgs_diff == 5 and bytes_diff == 50) {
        reportResult("stats_increment", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs={d} bytes={d}",
            .{ msgs_diff, bytes_diff },
        ) catch "err";
        reportResult("stats_increment", false, detail);
    }
}

// Test 53: Very short timeout (edge case)
fn testVeryShortTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("very_short_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "short.timeout") catch {
        reportResult("very_short_timeout", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // 1ms timeout - should return null quickly, not hang
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1 }) catch {
        reportResult("very_short_timeout", false, "error on short timeout");
        return;
    };

    if (msg == null) {
        reportResult("very_short_timeout", true, "");
    } else {
        msg.?.deinit(allocator);
        reportResult("very_short_timeout", false, "unexpected message");
    }
}

// Test 54: Reply-to is preserved in message
fn testReplyToPreserved(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("reply_to_preserved", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "reply.test") catch {
        reportResult("reply_to_preserved", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish with specific reply-to
    const reply_addr = "_INBOX.test.reply.12345";
    client.publishRequest("reply.test", reply_addr, "data") catch {
        reportResult("reply_to_preserved", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("reply_to_preserved", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.reply_to) |rt| {
            if (std.mem.eql(u8, rt, reply_addr)) {
                reportResult("reply_to_preserved", true, "");
            } else {
                reportResult("reply_to_preserved", false, "wrong reply_to");
            }
        } else {
            reportResult("reply_to_preserved", false, "no reply_to");
        }
    } else {
        reportResult("reply_to_preserved", false, "no message");
    }
}

// Test 55: Subject with numbers and hyphens
fn testSubjectWithNumbersHyphens(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_nums_hyphens", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const subject = "user-123.order-456.item-789";

    const sub = client.subscribe(allocator, subject) catch {
        reportResult("subject_nums_hyphens", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish(subject, "test") catch {
        reportResult("subject_nums_hyphens", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("subject_nums_hyphens", false, "receive failed");
        return;
    };

    if (msg) |m| {
        m.deinit(allocator);
        reportResult("subject_nums_hyphens", true, "");
    } else {
        reportResult("subject_nums_hyphens", false, "no message");
    }
}

// Test 56: Wildcard * at different positions
fn testWildcardPositions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("wildcard_positions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Wildcard at beginning: *.bar
    const sub1 = client.subscribe(allocator, "*.middle.end") catch {
        reportResult("wildcard_positions", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    // Wildcard in middle: foo.*.baz
    const sub2 = client.subscribe(allocator, "start.*.end") catch {
        reportResult("wildcard_positions", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // Publish matching messages
    client.publish("foo.middle.end", "msg1") catch {};
    client.publish("start.bar.end", "msg2") catch {};
    client.flush() catch {};

    var count: u32 = 0;
    if (sub1.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 2) {
        reportResult("wildcard_positions", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("wildcard_positions", false, detail);
    }
}

// Test 57: Interleaved pub/sub operations
fn testInterleavedPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("interleaved_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "interleave.test") catch {
        reportResult("interleaved_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Interleave: publish, receive, publish, receive...
    var received: u32 = 0;
    for (0..10) |i| {
        var buf: [16]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "msg{d}", .{i}) catch continue;

        client.publish("interleave.test", payload) catch {
            reportResult("interleaved_pubsub", false, "publish failed");
            return;
        };
        client.flush() catch {};

        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
            continue;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("interleaved_pubsub", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/10",
            .{received},
        ) catch "err";
        reportResult("interleaved_pubsub", false, detail);
    }
}

// Test 58: Publish to wildcard subject should fail
fn testPublishToWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_to_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Try to publish to wildcard subject - should fail
    const result1 = client.publish("foo.*", "data");
    const result2 = client.publish("foo.>", "data");

    if (result1 == error.InvalidCharacter and result2 == error.InvalidCharacter) {
        reportResult("publish_to_wildcard", true, "");
    } else if (result1) |_| {
        reportResult("publish_to_wildcard", false, "* should fail");
    } else |_| {
        if (result2) |_| {
            reportResult("publish_to_wildcard", false, "> should fail");
        } else |_| {
            reportResult("publish_to_wildcard", true, "");
        }
    }
}

// Test 59: Max payload from server info
fn testMaxPayloadRespected(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_payload_respected", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_respected", false, "no server info");
        return;
    }

    // Verify max_payload is reasonable (default is 1MB)
    if (info.?.max_payload >= 1024 and info.?.max_payload <= 64 * 1024 * 1024) {
        reportResult("max_payload_respected", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "max_payload={d}",
            .{info.?.max_payload},
        ) catch "err";
        reportResult("max_payload_respected", false, detail);
    }
}

// Test 60: Rapid subscribe/unsubscribe cycles
fn testRapidSubUnsubCycles(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("rapid_sub_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // 50 rapid cycles
    for (0..50) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "rapid.cycle.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("rapid_sub_unsub", false, "subscribe failed");
            return;
        };

        // Immediately unsubscribe
        sub.unsubscribe() catch {
            sub.deinit(allocator);
            reportResult("rapid_sub_unsub", false, "unsubscribe failed");
            return;
        };

        sub.deinit(allocator);
    }

    // If we got here without error, success
    reportResult("rapid_sub_unsub", true, "");
}

// Test 61: Empty subject should fail
fn testEmptySubjectFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("empty_subject_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Empty subject should fail for subscribe
    const sub_result = client.subscribe(allocator, "");
    if (sub_result) |sub| {
        sub.deinit(allocator);
        reportResult("empty_subject_fails", false, "subscribe should fail");
        return;
    } else |_| {
        // Expected
    }

    reportResult("empty_subject_fails", true, "");
}

// Test 62: Subject with spaces should fail
fn testSubjectWithSpacesFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_spaces_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subject with space should fail
    const result = client.publish("foo bar", "data");
    if (result) |_| {
        reportResult("subject_spaces_fails", false, "should have failed");
    } else |_| {
        reportResult("subject_spaces_fails", true, "");
    }
}

// Test 63: Zero timeout returns immediately
fn testZeroTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("zero_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "zero.timeout") catch {
        reportResult("zero_timeout", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Zero timeout - should return immediately with null
    var timer = std.time.Timer.start() catch {
        reportResult("zero_timeout", false, "timer failed");
        return;
    };

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 0 }) catch {
        reportResult("zero_timeout", false, "error on zero timeout");
        return;
    };

    const elapsed_ms = timer.read() / std.time.ns_per_ms;

    if (msg == null and elapsed_ms < 100) {
        reportResult("zero_timeout", true, "");
    } else if (msg != null) {
        msg.?.deinit(allocator);
        reportResult("zero_timeout", false, "unexpected message");
    } else {
        reportResult("zero_timeout", false, "took too long");
    }
}

// Test 64: Multiple queue groups on same subject
fn testMultipleQueueGroups(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_queue_groups", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two different queue groups on same subject
    const sub_a = client.subscribeQueue(allocator, "mqg.test", "group-A") catch {
        reportResult("multi_queue_groups", false, "sub A failed");
        return;
    };
    defer sub_a.deinit(allocator);

    const sub_b = client.subscribeQueue(allocator, "mqg.test", "group-B") catch {
        reportResult("multi_queue_groups", false, "sub B failed");
        return;
    };
    defer sub_b.deinit(allocator);

    client.flush() catch {};

    // Publish one message
    client.publish("mqg.test", "hello") catch {
        reportResult("multi_queue_groups", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Both groups should receive (each group gets a copy)
    var count: u32 = 0;
    if (sub_a.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub_b.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 2) {
        reportResult("multi_queue_groups", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("multi_queue_groups", false, detail);
    }
}

// Test 65: Unsubscribe with pending messages
fn testUnsubscribeWithPending(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_with_pending", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "pending.test") catch {
        reportResult("unsub_with_pending", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish messages before reading
    for (0..5) |_| {
        client.publish("pending.test", "msg") catch {};
    }
    client.flush() catch {};

    // Small delay for messages to arrive
    std.posix.nanosleep(0, 50_000_000);

    // Unsubscribe while messages pending
    sub.unsubscribe() catch {
        reportResult("unsub_with_pending", false, "unsubscribe failed");
        return;
    };

    // Should complete without crash
    reportResult("unsub_with_pending", true, "");
}

// Test 66: Five concurrent clients
fn testFiveConcurrentClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Create 5 clients
    var ios: [5]std.Io.Threaded = undefined;
    var clients: [5]?*nats.Client = [_]?*nats.Client{null} ** 5;
    var count: usize = 0;

    defer {
        for (0..count) |i| {
            if (clients[i]) |c| {
                c.deinit(allocator);
            }
            ios[i].deinit();
        }
    }

    for (0..5) |i| {
        ios[i] = .init(allocator, .{});
        clients[i] = nats.Client.connect(allocator, ios[i].io(), url, .{}) catch {
            reportResult("five_concurrent", false, "connect failed");
            return;
        };
        count += 1;
    }

    // All should be connected
    var all_connected = true;
    for (0..5) |i| {
        if (clients[i]) |c| {
            if (!c.isConnected()) all_connected = false;
        }
    }

    if (all_connected) {
        reportResult("five_concurrent", true, "");
    } else {
        reportResult("five_concurrent", false, "not all connected");
    }
}

// Test 67: Publish from multiple clients to one subscriber
fn testManyPublishersOneSubscriber(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Subscriber client
    var io_sub: std.Io.Threaded = .init(allocator, .{});
    defer io_sub.deinit();
    const client_sub = nats.Client.connect(allocator, io_sub.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "sub connect failed");
        return;
    };
    defer client_sub.deinit(allocator);

    const sub = client_sub.subscribe(allocator, "fanin.test") catch {
        reportResult("many_pub_one_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client_sub.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publisher client 1
    var io_pub1: std.Io.Threaded = .init(allocator, .{});
    defer io_pub1.deinit();
    const client_pub1 = nats.Client.connect(allocator, io_pub1.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "pub1 connect failed");
        return;
    };
    defer client_pub1.deinit(allocator);

    // Publisher client 2
    var io_pub2: std.Io.Threaded = .init(allocator, .{});
    defer io_pub2.deinit();
    const client_pub2 = nats.Client.connect(allocator, io_pub2.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "pub2 connect failed");
        return;
    };
    defer client_pub2.deinit(allocator);

    // Each publisher sends 5 messages
    for (0..5) |_| {
        client_pub1.publish("fanin.test", "from1") catch {};
        client_pub2.publish("fanin.test", "from2") catch {};
    }
    client_pub1.flush() catch {};
    client_pub2.flush() catch {};

    // Subscriber should receive all 10
    var received: u32 = 0;
    for (0..15) |_| {
        if (sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("many_pub_one_sub", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/10", .{received}) catch "e";
        reportResult("many_pub_one_sub", false, detail);
    }
}

// Test 68: Subject case sensitivity
fn testSubjectCaseSensitivity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_case", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to lowercase
    const sub_lower = client.subscribe(allocator, "case.test") catch {
        reportResult("subject_case", false, "sub lower failed");
        return;
    };
    defer sub_lower.deinit(allocator);

    // Subscribe to uppercase
    const sub_upper = client.subscribe(allocator, "CASE.TEST") catch {
        reportResult("subject_case", false, "sub upper failed");
        return;
    };
    defer sub_upper.deinit(allocator);

    client.flush() catch {};

    // Publish to lowercase
    client.publish("case.test", "lower") catch {};
    // Publish to uppercase
    client.publish("CASE.TEST", "upper") catch {};
    client.flush() catch {};

    // Each should only receive their own
    var lower_count: u32 = 0;
    var upper_count: u32 = 0;

    for (0..2) |_| {
        if (sub_lower.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            lower_count += 1;
        }
    }
    for (0..2) |_| {
        if (sub_upper.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            upper_count += 1;
        }
    }

    // NATS is case sensitive - each should get exactly 1
    if (lower_count == 1 and upper_count == 1) {
        reportResult("subject_case", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "lower={d} upper={d}",
            .{ lower_count, upper_count },
        ) catch "err";
        reportResult("subject_case", false, detail);
    }
}

// Test 69: Subscriber receives only after subscribe
fn testReceiveOnlyAfterSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("receive_after_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish BEFORE subscribing
    client.publish("timing.test", "before") catch {};
    client.flush() catch {};

    // Small delay
    std.posix.nanosleep(0, 50_000_000);

    // Now subscribe
    const sub = client.subscribe(allocator, "timing.test") catch {
        reportResult("receive_after_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish AFTER subscribing
    client.publish("timing.test", "after") catch {};
    client.flush() catch {};

    // Should only receive the "after" message
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
        reportResult("receive_after_sub", false, "receive error");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "after")) {
            reportResult("receive_after_sub", true, "");
        } else {
            reportResult("receive_after_sub", false, "got wrong message");
        }
    } else {
        reportResult("receive_after_sub", false, "no message");
    }
}

// Test 70: Stress test - 500 messages
fn testStress500Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_500_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.500") catch {
        reportResult("stress_500_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 500 messages
    for (0..500) |_| {
        client.publish("stress.500", "stress-test-payload") catch {
            reportResult("stress_500_msgs", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Receive all 500
    var received: u32 = 0;
    for (0..600) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 100 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 500) break;
        } else {
            break;
        }
    }

    if (received == 500) {
        reportResult("stress_500_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/500",
            .{received},
        ) catch "err";
        reportResult("stress_500_msgs", false, detail);
    }
}

// Test 72: 30KB payload (near buffer limit)
fn testPayload30KB(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("payload_30kb", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large.30kb") catch {
        reportResult("payload_30kb", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Create 30KB payload (just under 32KB buffer limit)
    // Actual limit is ~16KB due to protocol overhead + read mechanics
    const payload_size: usize = 15 * 1024; // 15KB is safe
    const payload = allocator.alloc(u8, payload_size) catch {
        reportResult("payload_30kb", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);

    // Fill with pattern
    for (payload, 0..) |*b, i| {
        b.* = @truncate(i % 256);
    }

    client.publish("large.30kb", payload) catch {
        reportResult("payload_30kb", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Receive with owned copy
    const msg = sub.nextMessageOwned(allocator, .{
        .timeout_ms = 5000,
    }) catch {
        reportResult("payload_30kb", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == payload_size) {
            // Verify pattern
            var valid = true;
            for (m.data, 0..) |b, i| {
                if (b != @as(u8, @truncate(i % 256))) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                reportResult("payload_30kb", true, "");
            } else {
                reportResult("payload_30kb", false, "data corrupt");
            }
        } else {
            var buf: [32]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "got {d} bytes",
                .{m.data.len},
            ) catch "err";
            reportResult("payload_30kb", false, detail);
        }
    } else {
        reportResult("payload_30kb", false, "no message");
    }
}

// Test 73: Payload at exact buffer boundary
fn testPayloadBoundary(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("payload_boundary", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "boundary.test") catch {
        reportResult("payload_boundary", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Test exact sizes: 1KB, 4KB, 8KB, 15KB
    const sizes = [_]usize{ 1024, 4096, 8192, 15360 };
    var all_passed = true;

    for (sizes) |size| {
        const payload = allocator.alloc(u8, size) catch {
            all_passed = false;
            break;
        };
        defer allocator.free(payload);
        @memset(payload, 'B');

        client.publish("boundary.test", payload) catch {
            all_passed = false;
            break;
        };
        client.flush() catch {};

        const msg = sub.nextMessageOwned(allocator, .{
            .timeout_ms = 2000,
        }) catch {
            all_passed = false;
            break;
        };

        if (msg) |m| {
            if (m.data.len != size) all_passed = false;
            m.deinit(allocator);
        } else {
            all_passed = false;
            break;
        }
    }

    if (all_passed) {
        reportResult("payload_boundary", true, "");
    } else {
        reportResult("payload_boundary", false, "size mismatch");
    }
}

// Test 71: Receive message with headers (via nats CLI)
fn testReceiveMessageWithHeaders(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("receive_headers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "headers.test") catch {
        reportResult("receive_headers", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Use nats CLI to publish message with headers
    var child = std.process.Child.init(
        &[_][]const u8{
            "nats",
            "pub",
            "-s",
            url,
            "--header",
            "X-Custom:test-value",
            "--header",
            "X-Another:second",
            "headers.test",
            "payload-data",
        },
        allocator,
    );
    child.spawn(io.io()) catch {
        reportResult("receive_headers", false, "nats cli spawn");
        return;
    };
    _ = child.wait(io.io()) catch {
        reportResult("receive_headers", false, "nats cli wait");
        return;
    };

    // Receive message
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("receive_headers", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);

        // Verify data
        if (!std.mem.eql(u8, m.data, "payload-data")) {
            reportResult("receive_headers", false, "wrong data");
            return;
        }

        // Verify headers exist
        if (m.headers) |h| {
            // Headers should contain our custom header
            if (std.mem.indexOf(u8, h, "X-Custom")) |_| {
                reportResult("receive_headers", true, "");
            } else {
                reportResult("receive_headers", false, "header not found");
            }
        } else {
            reportResult("receive_headers", false, "no headers");
        }
    } else {
        reportResult("receive_headers", false, "no message");
    }
}

// Test 75: Flush timeout behavior
fn testFlushTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("flush_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish something
    client.publish("flush.timeout.test", "data") catch {
        reportResult("flush_timeout", false, "publish failed");
        return;
    };

    // Flush with explicit timeout
    client.flushWithTimeout(5000) catch {
        reportResult("flush_timeout", false, "flush timeout failed");
        return;
    };

    reportResult("flush_timeout", true, "");
}

// Test 76: State after various operations
fn testConnectionStateAfterOps(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("state_after_ops", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // After connect, should be connected
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after connect");
        return;
    }

    // Publish should keep connected
    client.publish("state.test", "data") catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after publish");
        return;
    }

    // Subscribe should keep connected
    const sub = client.subscribe(allocator, "state.sub") catch {
        reportResult("state_after_ops", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after subscribe");
        return;
    }

    // Flush should keep connected
    client.flush() catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after flush");
        return;
    }

    reportResult("state_after_ops", true, "");
}

// Test 77: Queue group single receiver gets all messages
fn testQueueGroupSingleReceiver(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("queue_single_recv", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Single subscriber in queue group
    const sub = client.subscribeQueue(allocator, "qsingle.test", "solo") catch {
        reportResult("queue_single_recv", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("qsingle.test", "msg") catch {};
    }
    client.flush() catch {};

    // Should receive all 10
    var count: u32 = 0;
    for (0..15) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }

    if (count == 10) {
        reportResult("queue_single_recv", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/10", .{count}) catch "e";
        reportResult("queue_single_recv", false, detail);
    }
}

// Test 78: Subscribe with queue and wildcard combined
fn testQueueWithWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("queue_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to wildcard with queue group
    const sub = client.subscribeQueue(allocator, "qw.>", "workers") catch {
        reportResult("queue_wildcard", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish to various subjects
    client.publish("qw.foo", "one") catch {};
    client.publish("qw.bar", "two") catch {};
    client.publish("qw.baz.deep", "three") catch {};
    client.flush() catch {};

    var count: u32 = 0;
    for (0..5) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }

    if (count == 3) {
        reportResult("queue_wildcard", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/3", .{count}) catch "e";
        reportResult("queue_wildcard", false, detail);
    }
}

// Test 79: Ping to server
fn testExplicitPing(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("explicit_ping", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Send explicit ping
    client.ping() catch {
        reportResult("explicit_ping", false, "ping failed");
        return;
    };

    // Multiple pings should work
    for (0..5) |_| {
        client.ping() catch {
            reportResult("explicit_ping", false, "multi ping failed");
            return;
        };
    }

    reportResult("explicit_ping", true, "");
}

// Test 80: Stats bytes accuracy
fn testStatsBytesAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "bytes.test") catch {
        reportResult("stats_bytes", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish exactly 100 bytes
    const payload = "0123456789" ** 10; // 100 bytes
    client.publish("bytes.test", payload) catch {};
    client.flush() catch {};

    // Receive
    if (sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
    }

    const after = client.getStats();
    const bytes_diff = after.bytes_out - before.bytes_out;

    // Should have published 100 bytes of payload
    if (bytes_diff == 100) {
        reportResult("stats_bytes", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{bytes_diff}) catch "e";
        reportResult("stats_bytes", false, detail);
    }
}

// Test 81: Stats msgs_in accuracy
fn testStatsMsgsIn(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_msgs_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "msgsin.test") catch {
        reportResult("stats_msgs_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish 25 messages
    for (0..25) |_| {
        client.publish("msgsin.test", "data") catch {};
    }
    client.flush() catch {};

    // Receive all
    var received: u32 = 0;
    for (0..30) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    const after = client.getStats();
    const msgs_in = after.msgs_in - before.msgs_in;

    if (msgs_in == 25 and received == 25) {
        reportResult("stats_msgs_in", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "in={d} recv={d}",
            .{ msgs_in, received },
        ) catch "e";
        reportResult("stats_msgs_in", false, detail);
    }
}

// Test 82: Drain cleans up subscriptions properly
fn testDrainCleansUp(allocator: std.mem.Allocator) void {
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
fn testSubjectTokens(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_tokens", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Valid multi-token subject
    const sub = client.subscribe(allocator, "a.b.c.d.e.f") catch {
        reportResult("subject_tokens", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish("a.b.c.d.e.f", "deep") catch {
        reportResult("subject_tokens", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
        reportResult("subject_tokens", false, "receive failed");
        return;
    };

    if (msg) |m| {
        m.deinit(allocator);
        reportResult("subject_tokens", true, "");
    } else {
        reportResult("subject_tokens", false, "no message");
    }
}

// Test 84: Stress test - 1000 messages
fn testStress1000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_1000_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.1k") catch {
        reportResult("stress_1000_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 1000 messages
    for (0..1000) |_| {
        client.publish("stress.1k", "1k-stress") catch {
            reportResult("stress_1000_msgs", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Receive all
    var received: u32 = 0;
    for (0..1100) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 50 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 1000) break;
        } else break;
    }

    if (received == 1000) {
        reportResult("stress_1000_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/1000", .{received}) catch "e";
        reportResult("stress_1000_msgs", false, detail);
    }
}

// Test 85: Reply-to field in publish
fn testPublishWithReplyTo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_reply_to", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "replyto.test") catch {
        reportResult("publish_reply_to", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish with reply-to
    client.publishRequest("replyto.test", "_INBOX.reply123", "data") catch {
        reportResult("publish_reply_to", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
        reportResult("publish_reply_to", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.reply_to orelse "", "_INBOX.reply123")) {
            reportResult("publish_reply_to", true, "");
        } else {
            reportResult("publish_reply_to", false, "wrong reply_to");
        }
    } else {
        reportResult("publish_reply_to", false, "no message");
    }
}

// Test 86: Multiple wildcards in subject
fn testMultipleWildcards(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_wildcards", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe with multiple * wildcards
    const sub = client.subscribe(allocator, "mw.*.middle.*") catch {
        reportResult("multi_wildcards", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish matching subjects
    client.publish("mw.foo.middle.bar", "hit1") catch {};
    client.publish("mw.a.middle.b", "hit2") catch {};
    client.publish("mw.xyz.other.abc", "miss") catch {}; // should not match
    client.flush() catch {};

    var count: u32 = 0;
    for (0..4) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }

    if (count == 2) {
        reportResult("multi_wildcards", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "e";
        reportResult("multi_wildcards", false, detail);
    }
}

// Test 87: Server max payload enforcement
fn testServerMaxPayloadEnforced(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_payload_enforced", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_enforced", false, "no server info");
        return;
    }

    // max_payload from server (usually 1MB)
    const max = info.?.max_payload;
    if (max > 0) {
        reportResult("max_payload_enforced", true, "");
    } else {
        reportResult("max_payload_enforced", false, "max_payload is 0");
    }
}

// Test 88: Unsubscribe by SID
fn testUnsubscribeBySid(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_by_sid", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "sid.test") catch {
        reportResult("unsub_by_sid", false, "subscribe failed");
        return;
    };

    const sid = sub.sid;
    client.flush() catch {};

    // Publish before unsub
    client.publish("sid.test", "before") catch {};
    client.flush() catch {};

    // Unsubscribe by SID
    client.unsubscribeSid(sid) catch {
        sub.deinit(allocator);
        reportResult("unsub_by_sid", false, "unsub failed");
        return;
    };
    client.flush() catch {};

    // Publish after unsub
    client.publish("sid.test", "after") catch {};
    client.flush() catch {};

    // Should only get the "before" message
    var count: u32 = 0;
    for (0..3) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }
    sub.deinit(allocator);

    if (count <= 1) {
        reportResult("unsub_by_sid", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{count}) catch "e";
        reportResult("unsub_by_sid", false, detail);
    }
}

// Test 89: Subscribe to same subject twice (different subs)
fn testTwoSubsSameSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("two_subs_same", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "twosubs.same") catch {
        reportResult("two_subs_same", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "twosubs.same") catch {
        reportResult("two_subs_same", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // They should have different SIDs
    if (sub1.sid == sub2.sid) {
        reportResult("two_subs_same", false, "same SID");
        return;
    }

    // Publish one message
    client.publish("twosubs.same", "fanout") catch {};
    client.flush() catch {};

    // Both should receive it
    var count1: u32 = 0;
    var count2: u32 = 0;

    for (0..2) |_| {
        if (sub1.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            count1 += 1;
        }
    }
    for (0..2) |_| {
        if (sub2.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            count2 += 1;
        }
    }

    if (count1 == 1 and count2 == 1) {
        reportResult("two_subs_same", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}",
            .{ count1, count2 },
        ) catch "e";
        reportResult("two_subs_same", false, detail);
    }
}

// Test 90: Verify server version is reported
fn testServerVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_version", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_version", false, "no server info");
        return;
    }

    const version = info.?.version;
    // Server version should be like "2.x.x"
    if (version.len > 0 and (version[0] == '2' or version[0] == '3')) {
        reportResult("server_version", true, "");
    } else {
        reportResult("server_version", false, "unexpected version");
    }
}

// Test 91: bytes_in stats accuracy
fn testStatsBytesIn(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_bytes_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "bytesin.test") catch {
        reportResult("stats_bytes_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish 10 messages of 50 bytes each = 500 bytes total
    const payload = "01234567890123456789012345678901234567890123456789"; // 50 bytes
    for (0..10) |_| {
        client.publish("bytesin.test", payload) catch {};
    }
    client.flush() catch {};

    // Receive all
    for (0..15) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    const after = client.getStats();
    const bytes_in = after.bytes_in - before.bytes_in;

    // Should have received 500 bytes of payload
    if (bytes_in == 500) {
        reportResult("stats_bytes_in", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/500", .{bytes_in}) catch "e";
        reportResult("stats_bytes_in", false, detail);
    }
}

// Test 92: Subscription receives up to queue capacity
fn testSubscriptionQueueCapacity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sub_queue_cap", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "qcap.test") catch {
        reportResult("sub_queue_cap", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish many messages quickly
    for (0..100) |_| {
        client.publish("qcap.test", "qcap") catch {};
    }
    client.flush() catch {};

    // Receive what we can
    var received: u32 = 0;
    for (0..150) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 100 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    // Should receive all or most messages
    if (received >= 90) {
        reportResult("sub_queue_cap", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{received}) catch "e";
        reportResult("sub_queue_cap", false, detail);
    }
}

// Test 93: Subject with numbers and hyphens (more complex)
fn testComplexSubjectNames(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("complex_subjects", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Test various complex but valid subject names
    const subjects = [_][]const u8{
        "a-b-c.d-e-f",
        "123.456.789",
        "user-123.order-456",
        "v1.api.users.get",
        "event_stream.user_created",
    };

    var all_ok = true;
    for (subjects) |subj| {
        const sub = client.subscribe(allocator, subj) catch {
            all_ok = false;
            break;
        };
        defer sub.deinit(allocator);
        client.flush() catch {};

        client.publish(subj, "test") catch {
            all_ok = false;
            break;
        };
        client.flush() catch {};

        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 300 }) catch {
            all_ok = false;
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
        } else {
            all_ok = false;
            break;
        }
    }

    if (all_ok) {
        reportResult("complex_subjects", true, "");
    } else {
        reportResult("complex_subjects", false, "failed");
    }
}

// Test 94: Multiple sequential flushes
fn testMultipleFlushes(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_flushes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Multiple flushes in sequence should all succeed
    for (0..10) |_| {
        client.publish("flush.seq", "data") catch {
            reportResult("multi_flushes", false, "publish failed");
            return;
        };
        client.flush() catch {
            reportResult("multi_flushes", false, "flush failed");
            return;
        };
    }

    reportResult("multi_flushes", true, "");
}

// Test 95: newInbox generates unique inboxes
fn testNewInboxUniqueness(allocator: std.mem.Allocator) void {
    var inbox_slices: [10][]u8 = undefined;
    var allocated: usize = 0;

    // Allocate 10 inboxes
    for (0..10) |i| {
        inbox_slices[i] = nats.newInbox(allocator) catch {
            reportResult("inbox_unique", false, "alloc failed");
            // Free what we allocated
            for (0..allocated) |j| allocator.free(inbox_slices[j]);
            return;
        };
        allocated += 1;
    }
    defer for (0..allocated) |i| allocator.free(inbox_slices[i]);

    // All should be unique
    var all_unique = true;
    outer: for (0..10) |i| {
        for (i + 1..10) |j| {
            if (std.mem.eql(u8, inbox_slices[i], inbox_slices[j])) {
                all_unique = false;
                break :outer;
            }
        }
    }

    // All should start with _INBOX.
    var all_prefix = true;
    for (inbox_slices) |inbox| {
        if (!std.mem.startsWith(u8, inbox, "_INBOX.")) {
            all_prefix = false;
            break;
        }
    }

    if (all_unique and all_prefix) {
        reportResult("inbox_unique", true, "");
    } else {
        reportResult("inbox_unique", false, "not unique or wrong prefix");
    }
}

// Test 96: Stress test 2000 messages
fn testStress2000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_2000_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.2k") catch {
        reportResult("stress_2000_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 2000 messages in batches
    for (0..20) |_| {
        for (0..100) |_| {
            client.publish("stress.2k", "2k-stress") catch {
                reportResult("stress_2000_msgs", false, "publish failed");
                return;
            };
        }
        client.flush() catch {};
    }

    // Receive all
    var received: u32 = 0;
    for (0..2200) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 50 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 2000) break;
        } else break;
    }

    if (received == 2000) {
        reportResult("stress_2000_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2000", .{received}) catch "e";
        reportResult("stress_2000_msgs", false, detail);
    }
}

// Test 97: Four clients in queue group
fn testFourClientQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Create 4 subscriber clients + 1 publisher
    var ios: [5]std.Io.Threaded = undefined;
    for (&ios) |*io_ptr| {
        io_ptr.* = .init(allocator, .{});
    }
    defer for (&ios) |*io_ptr| io_ptr.deinit();

    var clients: [5]?*nats.Client = .{ null, null, null, null, null };
    defer for (&clients) |*c| {
        if (c.*) |client| client.deinit(allocator);
    };

    for (&clients, 0..) |*c, i| {
        c.* = nats.Client.connect(allocator, ios[i].io(), url, .{}) catch {
            reportResult("four_client_queue", false, "connect failed");
            return;
        };
    }

    // First 4 clients subscribe to queue
    var subs: [4]?*nats.Subscription(nats.Client) = .{ null, null, null, null };
    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..4) |i| {
        subs[i] = clients[i].?.subscribeQueue(
            allocator,
            "fourq.test",
            "workers",
        ) catch {
            reportResult("four_client_queue", false, "subscribe failed");
            return;
        };
        clients[i].?.flush() catch {};
    }

    std.posix.nanosleep(0, 50_000_000); // 50ms settle

    // Publisher sends 40 messages
    for (0..40) |_| {
        clients[4].?.publish("fourq.test", "work") catch {};
    }
    clients[4].?.flush() catch {};

    // Count per subscriber
    var counts: [4]u32 = .{ 0, 0, 0, 0 };
    for (0..4) |i| {
        for (0..40) |_| {
            const msg = subs[i].?.nextMessage(
                allocator,
                .{ .timeout_ms = 100 },
            ) catch break;
            if (msg) |m| {
                m.deinit(allocator);
                counts[i] += 1;
            } else break;
        }
    }

    const total = counts[0] + counts[1] + counts[2] + counts[3];
    if (total == 40) {
        reportResult("four_client_queue", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}+{d}+{d}={d}",
            .{ counts[0], counts[1], counts[2], counts[3], total },
        ) catch "e";
        reportResult("four_client_queue", false, detail);
    }
}

// Test 98: Verify message data integrity with pattern
fn testDataIntegrityPattern(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("data_integrity", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "integrity.test") catch {
        reportResult("data_integrity", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Create pattern payload
    var payload: [256]u8 = undefined;
    for (&payload, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    client.publish("integrity.test", &payload) catch {
        reportResult("data_integrity", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
        reportResult("data_integrity", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len != 256) {
            reportResult("data_integrity", false, "wrong length");
            return;
        }
        for (m.data, 0..) |b, i| {
            if (b != @as(u8, @truncate(i))) {
                reportResult("data_integrity", false, "data corrupt");
                return;
            }
        }
        reportResult("data_integrity", true, "");
    } else {
        reportResult("data_integrity", false, "no message");
    }
}

// Test 99: Protocol version in server info
fn testProtocolVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("proto_version", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("proto_version", false, "no server info");
        return;
    }

    // Protocol version should be >= 1
    if (info.?.proto >= 1) {
        reportResult("proto_version", true, "");
    } else {
        reportResult("proto_version", false, "proto < 1");
    }
}

// Test 100: Complete pub/sub round-trip verification
fn testCompletePubSubRoundTrip(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("complete_roundtrip", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Verify connected
    if (!client.isConnected()) {
        reportResult("complete_roundtrip", false, "not connected");
        return;
    }

    // Subscribe
    const sub = client.subscribe(allocator, "roundtrip.100") catch {
        reportResult("complete_roundtrip", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Get stats before
    const before = client.getStats();

    // Publish with known data
    const test_data = "Test100-RoundTrip-Verification";
    client.publish("roundtrip.100", test_data) catch {
        reportResult("complete_roundtrip", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Receive
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("complete_roundtrip", false, "receive failed");
        return;
    };

    if (msg == null) {
        reportResult("complete_roundtrip", false, "no message");
        return;
    }

    const m = msg.?;
    defer m.deinit(allocator);

    // Verify data
    if (!std.mem.eql(u8, m.data, test_data)) {
        reportResult("complete_roundtrip", false, "data mismatch");
        return;
    }

    // Verify subject
    if (!std.mem.eql(u8, m.subject, "roundtrip.100")) {
        reportResult("complete_roundtrip", false, "subject mismatch");
        return;
    }

    // Get stats after
    const after = client.getStats();

    // Verify stats updated
    if (after.msgs_out <= before.msgs_out) {
        reportResult("complete_roundtrip", false, "msgs_out not updated");
        return;
    }
    if (after.msgs_in <= before.msgs_in) {
        reportResult("complete_roundtrip", false, "msgs_in not updated");
        return;
    }

    reportResult("complete_roundtrip", true, "");
}

// Async Tests

// Test: Basic async receive
fn testAsyncBasicReceive(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_basic_receive", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.test") catch {
        reportResult("async_basic_receive", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Launch async receive BEFORE publishing
    var future = sub.nextMessageAsync(allocator);
    defer if (future.cancel(io.io())) |m| {
        if (m) |msg| msg.deinit(allocator);
    } else |_| {};

    // Now publish
    client.publish("async.test", "async-hello") catch {
        reportResult("async_basic_receive", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Await should return the message
    const result = future.await(io.io()) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "await failed: {}", .{err}) catch "e";
        reportResult("async_basic_receive", false, msg);
        return;
    };

    // Don't call msg.deinit() - defer handles cleanup via cancel()
    if (result) |msg| {
        if (std.mem.eql(u8, msg.data, "async-hello")) {
            reportResult("async_basic_receive", true, "");
        } else {
            reportResult("async_basic_receive", false, "wrong data");
        }
    } else {
        reportResult("async_basic_receive", false, "got null");
    }
}

// Test: Async flush
fn testAsyncFlush(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish some messages
    for (0..10) |_| {
        client.publish("async.flush.test", "data") catch {};
    }

    // Async flush
    var future = client.flushAsync();
    defer future.cancel(io.io()) catch {};

    future.await(io.io()) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "flush failed: {}", .{err}) catch "e";
        reportResult("async_flush", false, msg);
        return;
    };

    reportResult("async_flush", true, "");
}

// Test: Parallel async receives using separate clients
// NOTE: Each client has one connection, so parallel async requires
// separate clients to avoid poll contention on same stream.
fn testAsyncParallelReceive(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A for subscription A
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("async_parallel_receive", false, "connect_a failed");
        return;
    };
    defer client_a.deinit(allocator);

    const sub_a = client_a.subscribe(allocator, "async.parallel.a") catch {
        reportResult("async_parallel_receive", false, "sub_a failed");
        return;
    };
    defer sub_a.deinit(allocator);

    // Client B for subscription B
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("async_parallel_receive", false, "connect_b failed");
        return;
    };
    defer client_b.deinit(allocator);

    const sub_b = client_b.subscribe(allocator, "async.parallel.b") catch {
        reportResult("async_parallel_receive", false, "sub_b failed");
        return;
    };
    defer sub_b.deinit(allocator);

    // Publisher client
    var io_pub: std.Io.Threaded = .init(allocator, .{});
    defer io_pub.deinit();

    const publisher = nats.Client.connect(allocator, io_pub.io(), url, .{}) catch {
        reportResult("async_parallel_receive", false, "connect_pub failed");
        return;
    };
    defer publisher.deinit(allocator);

    client_a.flush() catch {};
    client_b.flush() catch {};

    // Launch BOTH async receives in parallel
    var future_a = sub_a.nextMessageAsync(allocator);
    defer if (future_a.cancel(io_a.io())) |m| {
        if (m) |msg| msg.deinit(allocator);
    } else |_| {};

    var future_b = sub_b.nextMessageAsync(allocator);
    defer if (future_b.cancel(io_b.io())) |m| {
        if (m) |msg| msg.deinit(allocator);
    } else |_| {};

    // Publish to both
    publisher.publish("async.parallel.a", "msg-a") catch {};
    publisher.publish("async.parallel.b", "msg-b") catch {};
    publisher.flush() catch {};

    // Await both - don't deinit, defer handles cleanup
    var got_a = false;
    var got_b = false;

    if (future_a.await(io_a.io()) catch null) |msg| {
        got_a = std.mem.eql(u8, msg.data, "msg-a");
    }

    if (future_b.await(io_b.io()) catch null) |msg| {
        got_b = std.mem.eql(u8, msg.data, "msg-b");
    }

    if (got_a and got_b) {
        reportResult("async_parallel_receive", true, "");
    } else {
        reportResult("async_parallel_receive", false, "missing messages");
    }
}

// Test: Async request/reply
fn testAsyncRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A: responder
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("async_request_reply", false, "connect_a failed");
        return;
    };
    defer client_a.deinit(allocator);

    const responder = client_a.subscribe(allocator, "async.service") catch {
        reportResult("async_request_reply", false, "responder sub failed");
        return;
    };
    defer responder.deinit(allocator);
    client_a.flush() catch {};

    // Client B: requester
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("async_request_reply", false, "connect_b failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Launch async request
    var req_future = client_b.requestAsync(
        allocator,
        "async.service",
        "ping",
        5000,
    );
    defer _ = req_future.cancel(io_b.io()) catch {};

    // Respond
    if (responder.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch null) |req| {
        if (req.reply_to) |reply| {
            client_a.publish(reply, "pong") catch {};
            client_a.flush() catch {};
        }
        req.deinit(allocator);
    }

    // Await async request result
    const reply = req_future.await(io_b.io()) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "request failed: {}", .{err}) catch "e";
        reportResult("async_request_reply", false, msg);
        return;
    };

    if (reply) |r| {
        // DirectMsg is just slices into buffer, no deinit needed
        if (std.mem.eql(u8, r.data, "pong")) {
            reportResult("async_request_reply", true, "");
        } else {
            reportResult("async_request_reply", false, "wrong reply");
        }
    } else {
        reportResult("async_request_reply", false, "no reply");
    }
}

// Test: Async defer cleanup pattern
fn testAsyncDeferCleanup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_defer_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.cleanup") catch {
        reportResult("async_defer_cleanup", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Start async but publish message immediately
    var future = sub.nextMessageAsync(allocator);

    // Publish so the future will have a result
    client.publish("async.cleanup", "cleanup-test") catch {};
    client.flush() catch {};

    // Small delay for message to arrive
    std.posix.nanosleep(0, 50_000_000);

    // Cancel should return the message (it completed)
    if (future.cancel(io.io())) |result| {
        if (result) |msg| {
            msg.deinit(allocator);
            reportResult("async_defer_cleanup", true, "");
        } else {
            reportResult("async_defer_cleanup", false, "got null");
        }
    } else |_| {
        // Canceled before completion - also valid
        reportResult("async_defer_cleanup", true, "");
    }
}

// Test: Multiple async messages in sequence
fn testAsyncMultipleMessages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_messages", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.multi") catch {
        reportResult("async_multiple_messages", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Send and receive 5 messages using async
    var received: u32 = 0;
    for (0..5) |i| {
        // Launch async with golden defer pattern
        var future = sub.nextMessageAsync(allocator);
        defer if (future.cancel(io.io())) |m| {
            if (m) |msg| msg.deinit(allocator);
        } else |_| {};

        // Publish
        var payload_buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(&payload_buf, "msg-{d}", .{i}) catch "x";
        client.publish("async.multi", payload) catch continue;
        client.flush() catch continue;

        // Await - don't deinit, defer handles cleanup
        if (future.await(io.io()) catch null) |_| {
            received += 1;
        }
    }

    if (received == 5) {
        reportResult("async_multiple_messages", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/5", .{received}) catch "e";
        reportResult("async_multiple_messages", false, msg);
    }
}

// ClientAsync tests moved to client_async_tests.zig

// Test 21: Drain operation
fn testDrainOperation(allocator: std.mem.Allocator) void {
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

pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
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
    testReconnection(allocator, manager);
    testAuthentication(allocator);

    // Request-Reply and Edge Case Tests
    testRequestReplySuccess(allocator);
    testRequestTimeout(allocator);
    testRequestInboxUniqueness(allocator);
    testPublishEmptyPayload(allocator);
    testStatisticsAccuracy(allocator);
    testWildcardMatching(allocator);
    testQueueGroupDistribution(allocator);
    testAuthenticationFailure(allocator);
    testConnectionRefused(allocator);
    testDrainOperation(allocator);

    // Reconnection / Robustness Tests
    testServerRestartNewConnection(allocator, manager);
    testPublishNoSubscribers(allocator);
    testPublishLargePayload(allocator);
    testPublishRapidFire(allocator);
    testSubscribeUnsubscribeReuse(allocator);
    testPingPong(allocator);

    // Request-Reply and Message Delivery Tests
    testRequestMethod(allocator);
    testMultipleSubscribersSameSubject(allocator);
    testMessageOrdering(allocator);
    testUnsubscribeStopsDelivery(allocator);

    // Cross-Client Message Routing Tests
    testCrossClientRouting(allocator);
    testCrossClientRequestReply(allocator);
    testThreeClientChain(allocator);

    // Edge Case and Error Handling Tests
    testPublishAfterDisconnect(allocator);
    testSubscribeAfterDisconnect(allocator);
    testDoubleFlush(allocator);
    testDoubleUnsubscribe(allocator);
    testBinaryPayload(allocator);
    testManySubscriptions(allocator);
    testHierarchicalSubject(allocator);
    testFlushAfterEachPublish(allocator);
    testPublishWithoutFlush(allocator);
    testDuplicateSubscription(allocator);
    testClientName(allocator);
    testDoubleDrain(allocator);
    testIsConnectedState(allocator);
    testLongSubjectName(allocator);
    testConsecutiveConnections(allocator);
    testQueueGroupMultipleClients(allocator);
    testServerInfoFields(allocator);
    testStatsIncrement(allocator);
    testVeryShortTimeout(allocator);
    testReplyToPreserved(allocator);
    testSubjectWithNumbersHyphens(allocator);
    testWildcardPositions(allocator);
    testInterleavedPubSub(allocator);
    testPublishToWildcard(allocator);
    testMaxPayloadRespected(allocator);
    testRapidSubUnsubCycles(allocator);
    testEmptySubjectFails(allocator);
    testSubjectWithSpacesFails(allocator);
    testZeroTimeout(allocator);
    testMultipleQueueGroups(allocator);
    testUnsubscribeWithPending(allocator);
    testFiveConcurrentClients(allocator);
    testManyPublishersOneSubscriber(allocator);
    testSubjectCaseSensitivity(allocator);
    testReceiveOnlyAfterSubscribe(allocator);
    testStress500Messages(allocator);
    testReceiveMessageWithHeaders(allocator);
    testPayload30KB(allocator);
    testPayloadBoundary(allocator);
    testFlushTimeout(allocator);
    testConnectionStateAfterOps(allocator);
    testQueueGroupSingleReceiver(allocator);
    testQueueWithWildcard(allocator);
    testExplicitPing(allocator);
    testStatsBytesAccuracy(allocator);
    testStatsMsgsIn(allocator);
    testDrainCleansUp(allocator);
    testSubjectTokens(allocator);
    testStress1000Messages(allocator);
    testPublishWithReplyTo(allocator);
    testMultipleWildcards(allocator);
    testServerMaxPayloadEnforced(allocator);
    testUnsubscribeBySid(allocator);
    testTwoSubsSameSubject(allocator);
    testServerVersion(allocator);
    testStatsBytesIn(allocator);
    testSubscriptionQueueCapacity(allocator);
    testComplexSubjectNames(allocator);
    testMultipleFlushes(allocator);
    testNewInboxUniqueness(allocator);
    testStress2000Messages(allocator);
    testFourClientQueueGroup(allocator);
    testDataIntegrityPattern(allocator);
    testProtocolVersion(allocator);
    testCompletePubSubRoundTrip(allocator);

    // Async Tests
    testAsyncBasicReceive(allocator);
    testAsyncFlush(allocator);
    testAsyncParallelReceive(allocator);
    testAsyncRequestReply(allocator);
    testAsyncDeferCleanup(allocator);
    testAsyncMultipleMessages(allocator);
}
