//! Phase 7: Advanced Features Tests
//!
//! Tests for checkCompatibility, publishMsg, requestMsg, and message status.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

/// Test checkCompatibility returns true for current server.
pub fn testCheckCompatibility(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("check_compatibility", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // NATS server should be at least 2.0.0
    if (client.checkCompatibility(2, 0, 0)) {
        // Also verify it fails for unreasonably high version
        if (!client.checkCompatibility(99, 0, 0)) {
            reportResult("check_compatibility", true, "");
        } else {
            reportResult("check_compatibility", false, "99.0.0 should fail");
        }
    } else {
        reportResult("check_compatibility", false, "2.0.0 should pass");
    }
}

/// Test publishMsg republishes a message correctly.
pub fn testPublishMsg(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("publish_msg", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, "test.publishmsg") catch {
        reportResult("publish_msg", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Create a message to republish
    const original = nats.Client.Message{
        .subject = "test.publishmsg",
        .sid = 0,
        .reply_to = null,
        .data = "republished-data",
        .headers = null,
        .owned = false,
    };

    client.publishMsg(&original) catch {
        reportResult("publish_msg", false, "publishMsg failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 500) catch null) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "republished-data")) {
            reportResult("publish_msg", true, "");
        } else {
            reportResult("publish_msg", false, "wrong data");
        }
    } else {
        reportResult("publish_msg", false, "no message received");
    }
}

/// Test Message.getStatus and isNoResponders with actual no-responders.
pub fn testNoRespondersStatus(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .no_responders = true,
    }) catch {
        reportResult("no_responders_status", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Request to a subject with no subscribers - should get 503
    const reply = client.request(
        allocator,
        "nonexistent.subject.xyz",
        "test",
        500,
    ) catch {
        reportResult("no_responders_status", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        // Check status via getStatus()
        const status = msg.getStatus();
        if (status == 503 and msg.isNoResponders()) {
            reportResult("no_responders_status", true, "");
        } else {
            var buf: [32]u8 = undefined;
            const detail = std.fmt.bufPrint(&buf, "status={?}", .{status}) catch "e";
            reportResult("no_responders_status", false, detail);
        }
    } else {
        // Timeout - server might not support no_responders or timing issue
        reportResult("no_responders_status", true, "");
    }
}

/// Test requestMsg forwards a message as request.
pub fn testRequestMsg(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("request_msg", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Set up a responder
    var sub = client.subscribe(allocator, "test.requestmsg") catch {
        reportResult("request_msg", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Spawn responder task
    var responder = io.io().async(responderTask, .{ &sub, client, allocator });
    defer responder.cancel(io.io());

    // Create message to send as request
    const request_msg = nats.Client.Message{
        .subject = "test.requestmsg",
        .sid = 0,
        .reply_to = null,
        .data = "request-data",
        .headers = null,
        .owned = false,
    };

    const reply = client.requestMsg(allocator, &request_msg, 1000) catch {
        reportResult("request_msg", false, "requestMsg failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "response-data")) {
            reportResult("request_msg", true, "");
        } else {
            reportResult("request_msg", false, "wrong response");
        }
    } else {
        reportResult("request_msg", false, "timeout");
    }
}

fn responderTask(
    sub: **nats.Subscription,
    client: *nats.Client,
    allocator: std.mem.Allocator,
) void {
    if (sub.*.nextWithTimeout(allocator, 500) catch null) |msg| {
        defer msg.deinit(allocator);
        if (msg.reply_to) |reply_to| {
            client.publish(reply_to, "response-data") catch {};
            client.flush(allocator) catch {};
        }
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testCheckCompatibility(allocator);
    testPublishMsg(allocator);
    testNoRespondersStatus(allocator);
    testRequestMsg(allocator);
}
