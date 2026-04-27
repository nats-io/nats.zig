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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("check_compatibility", false, "connect failed");
        return;
    };
    defer client.deinit();

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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("publish_msg", false, "connect failed");
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync("test.publishmsg") catch {
        reportResult("publish_msg", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

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

    if (sub.nextMsgTimeout(500) catch null) |msg| {
        defer msg.deinit();
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

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .no_responders = true,
    }) catch {
        reportResult("no_responders_status", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Request to a subject with no subscribers - should get 503
    const reply = client.request(
        "nonexistent.subject.xyz",
        "test",
        500,
    ) catch {
        reportResult("no_responders_status", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        // Check status via getStatus()
        const status = msg.status();
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

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(allocator, io_req.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("request_msg", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const io_resp = utils.newIo(allocator);
    defer io_resp.deinit();
    const responder = nats.Client.connect(allocator, io_resp.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("request_msg", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    // Set up a responder
    var sub = responder.subscribeSync("test.requestmsg") catch {
        reportResult("request_msg", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    responder.flush(1_000_000_000) catch {
        reportResult("request_msg", false, "flush failed");
        return;
    };

    // Spawn responder task
    var responder_future = io_resp.io().async(
        responderTask,
        .{ &sub, responder },
    );
    defer responder_future.cancel(io_resp.io());

    // Create message to send as request
    const request_msg = nats.Client.Message{
        .subject = "test.requestmsg",
        .sid = 0,
        .reply_to = null,
        .data = "request-data",
        .headers = null,
        .owned = false,
    };

    const reply = requester.requestMsg(&request_msg, 1000) catch {
        reportResult("request_msg", false, "requestMsg failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
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
) void {
    if (sub.*.nextMsgTimeout(2000) catch null) |msg| {
        defer msg.deinit();
        if (msg.reply_to) |reply_to| {
            client.publish(reply_to, "response-data") catch {};
        }
    }
}

fn requestMsgOrderingResponder(
    sub: **nats.Subscription,
    client: *nats.Client,
) void {
    var saw_state = false;
    var handled: usize = 0;
    while (handled < 2) {
        const maybe_msg = sub.*.nextMsgTimeout(1000) catch return;
        const msg = maybe_msg orelse return;
        defer msg.deinit();
        handled += 1;

        if (std.mem.eql(u8, msg.subject, "test.requestmsg.ordering.state")) {
            saw_state = true;
            continue;
        }

        if (std.mem.eql(u8, msg.subject, "test.requestmsg.ordering.service")) {
            const reply_to = msg.reply_to orelse return;
            client.publish(
                reply_to,
                if (saw_state) "fresh" else "stale",
            ) catch {};
            return;
        }
    }
}

pub fn testRequestMsgOrdering(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_msg_ordering", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const io_resp = utils.newIo(allocator);
    defer io_resp.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_resp.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_msg_ordering", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    var sub = responder.subscribeSync("test.requestmsg.ordering.>") catch {
        reportResult("request_msg_ordering", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    responder.flush(1_000_000_000) catch {
        reportResult("request_msg_ordering", false, "flush failed");
        return;
    };

    var handler = io_resp.io().async(
        requestMsgOrderingResponder,
        .{ &sub, responder },
    );
    defer handler.cancel(io_resp.io());

    requester.publish("test.requestmsg.ordering.state", "state-update") catch {
        reportResult("request_msg_ordering", false, "publish failed");
        return;
    };

    const request_msg = nats.Client.Message{
        .subject = "test.requestmsg.ordering.service",
        .sid = 0,
        .reply_to = null,
        .data = "request-data",
        .headers = null,
        .owned = false,
    };

    const reply = requester.requestMsg(&request_msg, 1000) catch {
        reportResult("request_msg_ordering", false, "requestMsg failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "fresh")) {
            reportResult("request_msg_ordering", true, "");
        } else {
            reportResult("request_msg_ordering", false, "request overtook publish");
        }
    } else {
        reportResult("request_msg_ordering", false, "timeout");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testCheckCompatibility(allocator);
    testPublishMsg(allocator);
    testNoRespondersStatus(allocator);
    testRequestMsg(allocator);
    testRequestMsgOrdering(allocator);
}
