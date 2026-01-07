//! Request-Reply Tests for NATS Async Client
//!
//! Tests for async request-reply pattern.

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

pub fn testAsyncRequestMethod(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_request_method", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Verify the request method exists and can be called
    // We expect it to return null (timeout) since no responder exists
    // Use a short timeout to keep tests fast
    const result = client.request(
        allocator,
        "nonexistent.service.test",
        "ping",
        50, // 50ms timeout
    ) catch {
        reportResult("async_request_method", false, "request error");
        return;
    };

    // Either null (timeout) or a message (if somehow routed) is acceptable
    // The important thing is that the method works without crashing
    if (result) |msg| {
        msg.deinit(allocator);
    }

    // Test passes if we got here without error
    if (client.isConnected()) {
        reportResult("async_request_method", true, "");
    } else {
        reportResult("async_request_method", false, "disconnected after request");
    }
}

/// Test: Async request times out eventually
pub fn testAsyncRequestReturns(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_request_returns", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const start = std.time.Instant.now() catch {
        reportResult("async_request_returns", false, "timer failed");
        return;
    };

    // Request to non-existent service with 100ms timeout
    const result = client.request(
        allocator,
        "nonexistent.service.test2",
        "data",
        100, // 100ms timeout
    ) catch {
        reportResult("async_request_returns", false, "request error");
        return;
    };

    const now = std.time.Instant.now() catch {
        reportResult("async_request_returns", false, "timer failed");
        return;
    };
    const elapsed_ns = now.since(start);
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;

    // Clean up result if any
    if (result) |msg| {
        msg.deinit(allocator);
    }

    // Test that the function returns within reasonable time (< 5 seconds)
    // This verifies the timeout mechanism works, even if not perfectly precise
    if (elapsed_ms < 5000) {
        reportResult("async_request_returns", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "took too long: {d}ms",
            .{elapsed_ms},
        ) catch "timing error";
        reportResult("async_request_returns", false, msg);
    }
}

// Drain Tests

/// Test: Drain operation closes connection
pub fn testAsyncReplyToPreserved(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_reply_preserved", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.reply.test") catch {
        reportResult("async_reply_preserved", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publishRequest("async.reply.test", "my.reply.inbox", "data") catch {
        reportResult("async_reply_preserved", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "my.reply.inbox")) {
                reportResult("async_reply_preserved", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("async_reply_preserved", false, "reply_to not preserved");
}

// Test: Hierarchical subject names

/// Runs all async request-reply tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncRequestMethod(allocator);
    testAsyncRequestReturns(allocator);
    testAsyncReplyToPreserved(allocator);
}
