//! Publish Tests for NATS Client
//!
//! Tests for publishing messages, payload handling, and flush behavior.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testPublishSingle(allocator: std.mem.Allocator) void {
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

pub fn testPublishEmptyPayload(allocator: std.mem.Allocator) void {
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

    client.publish("empty.payload", "") catch {
        reportResult("publish_empty_payload", false, "publish failed");
        return;
    };

    client.flush() catch {
        reportResult("publish_empty_payload", false, "flush2 failed");
        return;
    };

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

pub fn testPublishNoSubscribers(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_no_subscribers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

pub fn testPublishLargePayload(allocator: std.mem.Allocator) void {
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

pub fn testPublishRapidFire(allocator: std.mem.Allocator) void {
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

pub fn testPublishAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_after_disconnect", false, "connect failed");
        return;
    };

    client.drain(allocator) catch {
        reportResult("publish_after_disconnect", false, "drain failed");
        client.deinit(allocator);
        return;
    };

    const result = client.publish("test.subject", "data");
    client.deinit(allocator);

    if (result) |_| {
        reportResult("publish_after_disconnect", false, "should have failed");
    } else |_| {
        reportResult("publish_after_disconnect", true, "");
    }
}

pub fn testFlushAfterEachPublish(allocator: std.mem.Allocator) void {
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

pub fn testPublishWithoutFlush(allocator: std.mem.Allocator) void {
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

    for (0..100) |_| {
        client.publish("no.flush.test", "buffered") catch {
            reportResult("publish_no_flush", false, "publish failed");
            return;
        };
    }

    client.flush() catch {
        reportResult("publish_no_flush", false, "flush failed");
        return;
    };

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

pub fn testPublishWithReplyTo(allocator: std.mem.Allocator) void {
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

pub fn testPublishToWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("publish_to_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

/// Runs all publish tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testPublishSingle(allocator);
    testPublishEmptyPayload(allocator);
    testPublishNoSubscribers(allocator);
    testPublishLargePayload(allocator);
    testPublishRapidFire(allocator);
    testPublishAfterDisconnect(allocator);
    testFlushAfterEachPublish(allocator);
    testPublishWithoutFlush(allocator);
    testPublishWithReplyTo(allocator);
    testPublishToWildcard(allocator);
}
