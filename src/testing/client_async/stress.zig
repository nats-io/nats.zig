//! Stress Tests for NATS Async Client
//!
//! Tests for async stress testing.

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

pub fn testAsyncStress500Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stress_500", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .async_queue_size = 512,
    }) catch {
        reportResult("async_stress_500", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.stress500") catch {
        reportResult("async_stress_500", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publish 500 messages
    const NUM_MSGS = 500;
    for (0..NUM_MSGS) |_| {
        publisher.publish("async.stress500", "stress-msg") catch {};
    }
    publisher.flush() catch {};

    // Receive messages
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {
            break;
        }
    }

    // Should receive most messages
    if (received >= 450) {
        reportResult("async_stress_500", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/500", .{received}) catch "e";
        reportResult("async_stress_500", false, msg);
    }
}

// NEW TESTS: Error Handling

// Test: Double unsubscribe is safe

/// Runs all async stress tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncStress500Messages(allocator);
}
