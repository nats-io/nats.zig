//! Publish Tests for NATS Client

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const ServerManager = utils.ServerManager;

pub fn testClientPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "pubsub") catch {
        reportResult("client_pubsub", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    client.publish("pubsub", "test-message") catch {
        reportResult("client_pubsub", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |msg| msg.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "test-message")) {
            reportResult("client_pubsub", true, "");
            return;
        }
    } else |_| {}

    reportResult("client_pubsub", false, "no message received");
}

pub fn testClientPublishReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_pub_reply", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "req") catch {
        reportResult("client_pub_reply", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publishRequest("req", "reply.inbox", "request") catch {
        reportResult("client_pub_reply", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "reply.inbox")) {
                reportResult("client_pub_reply", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("client_pub_reply", false, "no reply_to");
}

pub fn testPublishEmptyPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_empty_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "empty") catch {
        reportResult("publish_empty_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish("empty", "") catch {
        reportResult("publish_empty_payload", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 0) {
            reportResult("publish_empty_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("publish_empty_payload", false, "no empty message");
}

pub fn testPublishLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_large_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large") catch {
        reportResult("publish_large_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    const payload = allocator.alloc(u8, 8 * 1024) catch {
        reportResult("publish_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("large", payload) catch {
        reportResult("publish_large_payload", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 8 * 1024) {
            reportResult("publish_large_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("publish_large_payload", false, "wrong size");
}

pub fn testPublishRapidFire(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_rapid_fire", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    for (0..1000) |_| {
        client.publish("rapid", "msg") catch {
            reportResult("publish_rapid_fire", false, "pub failed");
            return;
        };
    }
    client.flush(allocator) catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1000) {
        reportResult("publish_rapid_fire", true, "");
    } else {
        reportResult("publish_rapid_fire", false, "not all published");
    }
}

pub fn testPublishNoSubscribers(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_no_subscribers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.publish("nosub", "message") catch {
        reportResult("publish_no_subscribers", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    reportResult("publish_no_subscribers", true, "");
}

pub fn testPublishAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_after_disconnect", false, "connect failed");
        return;
    };

    _ = client.drain(allocator) catch {
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

pub fn testPublishBatching(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("publish_batching", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "batch.test") catch {
        reportResult("publish_batching", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    client.publish("batch.test", "data1") catch {};
    client.publish("batch.test", "data2") catch {};
    client.publish("batch.test", "data3") catch {};
    client.flush(allocator) catch {};

    var received: u32 = 0;
    for (0..3) |_| {
        if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 3) {
        reportResult("publish_batching", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail =
            std.fmt.bufPrint(&buf, "got {d}/3", .{received}) catch "e";
        reportResult("publish_batching", false, detail);
    }
}

pub fn testFlushAfterEachPublish(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("flush_after_each", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "flush.each") catch {
        reportResult("flush_after_each", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    for (0..50) |_| {
        client.publish("flush.each", "msg") catch {
            reportResult("flush_after_each", false, "publish failed");
            return;
        };
        client.flush(allocator) catch {
            reportResult("flush_after_each", false, "flush failed");
            return;
        };
    }

    var received: u32 = 0;
    for (0..50) |_| {
        const msg = sub.nextWithTimeout(allocator, 500) catch {
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

pub fn testPublishToWildcardFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("pub_wildcard_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Wildcards are only valid for subscribe, not publish
    const result1 = client.publish("foo.*", "data");
    const result2 = client.publish("foo.>", "data");

    const star_failed = if (result1) |_| false else |_| true;
    const gt_failed = if (result2) |_| false else |_| true;

    if (star_failed and gt_failed) {
        reportResult("pub_wildcard_fails", true, "");
    } else if (!star_failed) {
        reportResult("pub_wildcard_fails", false, "* should fail");
    } else {
        reportResult("pub_wildcard_fails", false, "> should fail");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testClientPubSub(allocator);
    testClientPublishReply(allocator);
    testPublishEmptyPayload(allocator);
    testPublishLargePayload(allocator);
    testPublishRapidFire(allocator);
    testPublishNoSubscribers(allocator);
    testPublishAfterDisconnect(allocator);
    testPublishBatching(allocator);
    testFlushAfterEachPublish(allocator);
    testPublishToWildcardFails(allocator);
}
