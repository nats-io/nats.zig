//! Async Helpers Tests for NATS Client
//!
//! Tests for async helper patterns.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testAsyncBasicReceive(allocator: std.mem.Allocator) void {
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

pub fn testAsyncFlush(allocator: std.mem.Allocator) void {
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

pub fn testAsyncParallelReceive(allocator: std.mem.Allocator) void {
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

pub fn testAsyncRequestReply(allocator: std.mem.Allocator) void {
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

pub fn testAsyncDeferCleanup(allocator: std.mem.Allocator) void {
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

pub fn testAsyncMultipleMessages(allocator: std.mem.Allocator) void {
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

/// Runs all async helper tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncBasicReceive(allocator);
    testAsyncFlush(allocator);
    testAsyncParallelReceive(allocator);
    testAsyncRequestReply(allocator);
    testAsyncDeferCleanup(allocator);
    testAsyncMultipleMessages(allocator);
}
