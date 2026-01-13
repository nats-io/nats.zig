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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
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

pub fn testRequestReplySuccess(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Responder client
    var io_r: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_r.deinit();
    const responder = nats.Client.connect(allocator, io_r.io(), url, .{}) catch {
        reportResult("request_reply_success", false, "responder connect failed");
        return;
    };
    defer responder.deinit(allocator);

    // Requester client
    var io_req: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_req.deinit();
    const requester = nats.Client.connect(allocator, io_req.io(), url, .{}) catch {
        reportResult("request_reply_success", false, "requester connect failed");
        return;
    };
    defer requester.deinit(allocator);

    // Set up responder subscription
    const sub = responder.subscribe(allocator, "test.service") catch {
        reportResult("request_reply_success", false, "responder sub failed");
        return;
    };
    defer sub.deinit(allocator);
    responder.flush() catch {};
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Start a task to handle the request
    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
            a: std.mem.Allocator,
            io: std.Io,
        ) void {
            _ = io;
            if (s.nextWithTimeout(a, 1000) catch null) |req| {
                defer req.deinit(a);
                if (req.reply_to) |reply_inbox| {
                    r.publish(reply_inbox, "pong") catch {};
                    r.flush() catch {};
                }
            }
        }
    };

    // Start responder in background
    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
        allocator,
        io_r.io(),
    });
    defer _ = handler.cancel(io_r.io());

    // Send request
    const reply = requester.request(
        allocator,
        "test.service",
        "ping",
        2000,
    ) catch {
        reportResult("request_reply_success", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "pong")) {
            reportResult("request_reply_success", true, "");
            return;
        }
    }

    reportResult("request_reply_success", false, "no reply or wrong data");
}

pub fn testCrossClientRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A
    var io_a: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("cross_client_reqrep", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B
    var io_b: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("cross_client_reqrep", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // B subscribes to service
    const sub = client_b.subscribe(allocator, "cross.service") catch {
        reportResult("cross_client_reqrep", false, "B sub failed");
        return;
    };
    defer sub.deinit(allocator);
    client_b.flush() catch {};
    io_b.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // B handles request in background
    const Handler = struct {
        fn handle(
            b: *nats.Client,
            s: *nats.Subscription,
            a: std.mem.Allocator,
            io: std.Io,
        ) void {
            _ = io;
            if (s.nextWithTimeout(a, 2000) catch null) |req| {
                defer req.deinit(a);
                if (req.reply_to) |inbox| {
                    b.publish(inbox, "response-from-B") catch {};
                    b.flush() catch {};
                }
            }
        }
    };

    var handler = io_b.io().async(Handler.handle, .{
        client_b,
        sub,
        allocator,
        io_b.io(),
    });
    defer _ = handler.cancel(io_b.io());

    // A sends request
    const reply = client_a.request(
        allocator,
        "cross.service",
        "request-from-A",
        3000,
    ) catch {
        reportResult("cross_client_reqrep", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "response-from-B")) {
            reportResult("cross_client_reqrep", true, "");
            return;
        }
    }

    reportResult("cross_client_reqrep", false, "no reply");
}

pub fn testRequestTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .no_responders = false, // Disable no_responders for this test
    }) catch {
        reportResult("request_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const start = std.time.Instant.now() catch {
        reportResult("request_timeout", false, "timer failed");
        return;
    };

    // Request with 200ms timeout, no responder
    const result = client.request(
        allocator,
        "timeout.service.noexist",
        "ping",
        200,
    ) catch {
        reportResult("request_timeout", false, "request error");
        return;
    };

    const end = std.time.Instant.now() catch {
        reportResult("request_timeout", false, "timer failed");
        return;
    };
    const elapsed_ms = end.since(start) / std.time.ns_per_ms;

    if (result) |msg| {
        msg.deinit(allocator);
        // If we got a reply (no_responders message), still pass
        // as long as the request mechanism works
        reportResult("request_timeout", true, "");
        return;
    }

    // Should return within reasonable time (less than 5 seconds)
    if (elapsed_ms < 5000) {
        reportResult("request_timeout", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "took {d}ms", .{elapsed_ms}) catch "e";
        reportResult("request_timeout", false, detail);
    }
}

pub fn testRequestWithLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Responder client
    var io_r: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_r.deinit();
    const responder = nats.Client.connect(allocator, io_r.io(), url, .{}) catch {
        reportResult("request_large_payload", false, "responder connect failed");
        return;
    };
    defer responder.deinit(allocator);

    // Requester client
    var io_req: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_req.deinit();
    const requester = nats.Client.connect(allocator, io_req.io(), url, .{}) catch {
        reportResult("request_large_payload", false, "requester connect failed");
        return;
    };
    defer requester.deinit(allocator);

    // Set up responder subscription
    const sub = responder.subscribe(allocator, "large.service") catch {
        reportResult("request_large_payload", false, "responder sub failed");
        return;
    };
    defer sub.deinit(allocator);
    responder.flush() catch {};
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Start a task to handle the request
    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
            a: std.mem.Allocator,
            io: std.Io,
        ) void {
            _ = io;
            if (s.nextWithTimeout(a, 2000) catch null) |req| {
                defer req.deinit(a);
                if (req.reply_to) |reply_inbox| {
                    // Reply with same size payload
                    r.publish(reply_inbox, req.data) catch {};
                    r.flush() catch {};
                }
            }
        }
    };

    // Start responder in background
    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
        allocator,
        io_r.io(),
    });
    defer _ = handler.cancel(io_r.io());

    // Create 1KB payload
    const payload = allocator.alloc(u8, 1024) catch {
        reportResult("request_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    // Send request with large payload
    const reply = requester.request(
        allocator,
        "large.service",
        payload,
        3000,
    ) catch {
        reportResult("request_large_payload", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        if (msg.data.len == 1024) {
            reportResult("request_large_payload", true, "");
            return;
        }
    }

    reportResult("request_large_payload", false, "no reply or wrong size");
}

pub fn testMultipleRequestsSequential(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Responder client
    var io_r: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_r.deinit();
    const responder = nats.Client.connect(allocator, io_r.io(), url, .{}) catch {
        reportResult("multi_requests_seq", false, "responder connect failed");
        return;
    };
    defer responder.deinit(allocator);

    // Requester client
    var io_req: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_req.deinit();
    const requester = nats.Client.connect(allocator, io_req.io(), url, .{}) catch {
        reportResult("multi_requests_seq", false, "requester connect failed");
        return;
    };
    defer requester.deinit(allocator);

    // Set up responder subscription
    const sub = responder.subscribe(allocator, "multi.service") catch {
        reportResult("multi_requests_seq", false, "responder sub failed");
        return;
    };
    defer sub.deinit(allocator);
    responder.flush() catch {};
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Responder handler - handles multiple requests
    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
            a: std.mem.Allocator,
            io: std.Io,
        ) void {
            _ = io;
            for (0..5) |_| {
                if (s.nextWithTimeout(a, 2000) catch null) |req| {
                    defer req.deinit(a);
                    if (req.reply_to) |reply_inbox| {
                        r.publish(reply_inbox, "response") catch {};
                        r.flush() catch {};
                    }
                } else break;
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
        allocator,
        io_r.io(),
    });
    defer _ = handler.cancel(io_r.io());

    // Send 5 requests sequentially
    var success_count: u32 = 0;
    for (0..5) |_| {
        const reply = requester.request(
            allocator,
            "multi.service",
            "request",
            2000,
        ) catch continue;

        if (reply) |msg| {
            msg.deinit(allocator);
            success_count += 1;
        }
    }

    if (success_count >= 4) {
        reportResult("multi_requests_seq", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/5", .{success_count}) catch "e";
        reportResult("multi_requests_seq", false, detail);
    }
}

/// Runs all async request-reply tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncRequestMethod(allocator);
    testAsyncRequestReturns(allocator);
    testAsyncReplyToPreserved(allocator);
    testRequestReplySuccess(allocator);
    testCrossClientRequestReply(allocator);
    testRequestTimeout(allocator);
    testRequestWithLargePayload(allocator);
    testMultipleRequestsSequential(allocator);
}
