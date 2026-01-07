//! Edge Cases Tests for NATS Async Client
//!
//! Tests for async edge cases.

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

pub fn testAsyncDoubleUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_double_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.double.unsub") catch {
        reportResult("async_double_unsub", false, "sub failed");
        return;
    };

    // Unsubscribe twice
    sub.unsubscribe() catch {};
    sub.unsubscribe() catch {}; // Should not crash

    sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("async_double_unsub", true, "");
    } else {
        reportResult("async_double_unsub", false, "disconnected");
    }
}

// Test: Message ordering (FIFO)

pub fn testAsyncMessageOrdering(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_message_ordering", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.order") catch {
        reportResult("async_message_ordering", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish numbered messages
    var pub_buf: [5][8]u8 = undefined;
    for (0..5) |i| {
        const payload = std.fmt.bufPrint(&pub_buf[i], "msg-{d}", .{i}) catch "e";
        client.publish("async.order", payload) catch {};
    }
    client.flush() catch {};

    // Receive and verify order
    var in_order = true;
    for (0..5) |expected| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |msg| {
            var exp_buf: [8]u8 = undefined;
            const exp = std.fmt.bufPrint(&exp_buf, "msg-{d}", .{expected}) catch "e";
            if (!std.mem.eql(u8, msg.data, exp)) {
                in_order = false;
            }
        } else |_| {
            in_order = false;
            break;
        }
    }

    if (in_order) {
        reportResult("async_message_ordering", true, "");
    } else {
        reportResult("async_message_ordering", false, "out of order");
    }
}

// NEW TESTS: Misc Edge Cases

// Test: Binary payload handling

pub fn testAsyncBinaryPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.binary") catch {
        reportResult("async_binary_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Binary data with null bytes
    const binary = [_]u8{ 0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x03 };

    client.publish("async.binary", &binary) catch {
        reportResult("async_binary_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, &binary)) {
            reportResult("async_binary_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_binary_payload", false, "binary mismatch");
}

// Test: Reply-to preserved in message

/// Runs all async edge case tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncDoubleUnsubscribe(allocator);
    testAsyncMessageOrdering(allocator);
    testAsyncBinaryPayload(allocator);
}
