//! Multi-Client Tests for NATS Async Client
//!
//! Tests for async cross-client messaging.

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

pub fn testAsyncCrossClientRouting(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Publisher (regular client)
    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_cross_client", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    // Subscriber (async client)
    const subscriber = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_cross_client", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit(allocator);

    const sub = subscriber.subscribe(allocator, "async.cross") catch {
        reportResult("async_cross_client", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    subscriber.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publish from regular client
    publisher.publish("async.cross", "cross-message") catch {};
    publisher.flush() catch {};

    // Receive on async client
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "cross-message")) {
            reportResult("async_cross_client", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_cross_client", false, "no message");
}

// Test: Multiple async clients

pub fn testAsyncMultipleClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Create 3 async clients
    const client1 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client1 failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client2 failed");
        return;
    };
    defer client2.deinit(allocator);

    const client3 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client3 failed");
        return;
    };
    defer client3.deinit(allocator);

    if (client1.isConnected() and client2.isConnected() and client3.isConnected()) {
        reportResult("async_multiple_clients", true, "");
    } else {
        reportResult("async_multiple_clients", false, "not all connected");
    }
}

// NEW TESTS: Statistics & Metadata

// Test: Stats increment correctly

pub fn testClientAsyncHighRate(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_high_rate", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .async_queue_size = 512,
    }) catch {
        reportResult("client_async_high_rate", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.highrate") catch {
        reportResult("client_async_high_rate", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish 100 messages
    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        publisher.publish("async.highrate", "msg") catch {};
    }
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    // Try to receive at least 50 messages
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {
            break; // Stop on first error (likely queue closed)
        }
    }

    // Should get most messages (some may be dropped if queue fills)
    if (received >= 50) {
        reportResult("client_async_high_rate", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}", .{received}) catch "e";
        reportResult("client_async_high_rate", false, msg);
    }
}

// ClientAsync Test 11: Publish with reply-to

/// Runs all async multi-client tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncCrossClientRouting(allocator);
    testAsyncMultipleClients(allocator);
    testClientAsyncHighRate(allocator);
}
