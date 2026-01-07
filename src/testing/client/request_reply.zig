//! Request-Reply Tests for NATS Client
//!
//! Tests for request-reply pattern, inbox generation, and timeouts.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testRequestReply(allocator: std.mem.Allocator) void {
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

pub fn testRequestReplySuccess(allocator: std.mem.Allocator) void {
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

pub fn testRequestTimeout(allocator: std.mem.Allocator) void {
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

pub fn testRequestInboxUniqueness(allocator: std.mem.Allocator) void {
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

pub fn testRequestMethod(allocator: std.mem.Allocator) void {
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

pub fn testReplyToPreserved(allocator: std.mem.Allocator) void {
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

pub fn testNewInboxUniqueness(allocator: std.mem.Allocator) void {
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

/// Runs all request-reply tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testRequestReply(allocator);
    testRequestReplySuccess(allocator);
    testRequestTimeout(allocator);
    testRequestInboxUniqueness(allocator);
    testRequestMethod(allocator);
    testReplyToPreserved(allocator);
    testNewInboxUniqueness(allocator);
}
