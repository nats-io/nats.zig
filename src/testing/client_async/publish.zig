//! Publish Tests for NATS Async Client
//!
//! Tests for async publishing.

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

pub fn testClientAsyncPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.pubsub") catch {
        reportResult("client_async_pubsub", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Flush subscription and wait for server to register it
    client.flush() catch {};
    std.posix.nanosleep(0, 10_000_000); // 10ms delay

    client.publish("async.pubsub", "test-message") catch {
        reportResult("client_async_pubsub", false, "pub failed");
        return;
    };
    client.flush() catch {};

    // True async/await - reader task routes messages automatically!
    // defer handles cleanup via cancel() - DON'T deinit in success path!
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |msg| msg.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        // cancel() and await() are idempotent - defer handles cleanup
        if (std.mem.eql(u8, msg.data, "test-message")) {
            reportResult("client_async_pubsub", true, "");
            return;
        }
    } else |_| {}

    reportResult("client_async_pubsub", false, "no message received");
}

// ClientAsync Test 5: Wildcard subscription

pub fn testClientAsyncPublishReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_pub_reply", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.req") catch {
        reportResult("client_async_pub_reply", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publishRequest("async.req", "reply.inbox", "request") catch {
        reportResult("client_async_pub_reply", false, "pub failed");
        return;
    };
    client.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "reply.inbox")) {
                reportResult("client_async_pub_reply", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("client_async_pub_reply", false, "no reply_to");
}

// ClientAsync Test 12: Queue group support

pub fn testAsyncPublishEmptyPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_empty_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.empty") catch {
        reportResult("async_publish_empty_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish("async.empty", "") catch {
        reportResult("async_publish_empty_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 0) {
            reportResult("async_publish_empty_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_publish_empty_payload", false, "no empty message");
}

// Test: Publish large payload (within buffer limits)

pub fn testAsyncPublishLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_large_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.large") catch {
        reportResult("async_publish_large_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Create 8KB payload (safe within buffer limits)
    const payload = allocator.alloc(u8, 8 * 1024) catch {
        reportResult("async_publish_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("async.large", payload) catch {
        reportResult("async_publish_large_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 8 * 1024) {
            reportResult("async_publish_large_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_publish_large_payload", false, "wrong size");
}

// Test: Rapid fire publishing

pub fn testAsyncPublishRapidFire(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_rapid_fire", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish 1000 messages rapidly
    for (0..1000) |_| {
        client.publish("async.rapid", "msg") catch {
            reportResult("async_publish_rapid_fire", false, "pub failed");
            return;
        };
    }
    client.flush() catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1000) {
        reportResult("async_publish_rapid_fire", true, "");
    } else {
        reportResult("async_publish_rapid_fire", false, "not all published");
    }
}

// Test: Publish with no subscribers

pub fn testAsyncPublishNoSubscribers(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_no_subscribers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Should succeed even with no subscribers
    client.publish("async.nosub", "message") catch {
        reportResult("async_publish_no_subscribers", false, "pub failed");
        return;
    };
    client.flush() catch {};

    reportResult("async_publish_no_subscribers", true, "");
}

// NEW TESTS: Subscription Patterns

// Test: Wildcard matching with *

/// Runs all async publish tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testClientAsyncPubSub(allocator);
    testClientAsyncPublishReply(allocator);
    testAsyncPublishEmptyPayload(allocator);
    testAsyncPublishLargePayload(allocator);
    testAsyncPublishRapidFire(allocator);
    testAsyncPublishNoSubscribers(allocator);
}
