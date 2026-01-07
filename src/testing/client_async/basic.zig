//! Basic Tests for NATS Async Client
//!
//! Tests for basic async client operations.

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

pub fn testClientAsyncBasic(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .name = "async-client-test",
        .async_queue_size = 64,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "error";
        reportResult("client_async_basic", false, msg);
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("client_async_basic", false, "not connected");
        return;
    }

    const sub = client.subscribe(allocator, "async.basic") catch {
        reportResult("client_async_basic", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    reportResult("client_async_basic", true, "");
}

// ClientAsync Test 2: Multiple concurrent subscriptions

pub fn testClientAsyncTryNext(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_try_next", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.trynext") catch {
        reportResult("client_async_try_next", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // No messages yet - should return null immediately
    if (sub.tryNext() != null) {
        reportResult("client_async_try_next", false, "expected null");
        return;
    }

    reportResult("client_async_try_next", true, "");
}

// ClientAsync Test 4: Publish and receive using async/await

pub fn testClientAsyncServerInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.getServerInfo()) |info| {
        if (info.port == test_port) {
            reportResult("client_async_server_info", true, "");
            return;
        }
    }
    reportResult("client_async_server_info", false, "no server info");
}

// ClientAsync Test 9: Rapid subscribe/unsubscribe

pub fn testClientAsyncRapidSubUnsub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_rapid_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Rapid sub/unsub 20 times
    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(&buf, "rapid.{d}", .{i}) catch "e";
        const sub = client.subscribe(allocator, subj) catch {
            reportResult("client_async_rapid_sub", false, "sub failed");
            return;
        };
        sub.deinit(allocator);
    }

    // Client should still work
    const sub = client.subscribe(allocator, "rapid.final") catch {
        reportResult("client_async_rapid_sub", false, "final sub failed");
        return;
    };
    defer sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("client_async_rapid_sub", true, "");
    } else {
        reportResult("client_async_rapid_sub", false, "disconnected");
    }
}

// ClientAsync Test 10: High message rate

/// Runs all basic async tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testClientAsyncBasic(allocator);
    testClientAsyncTryNext(allocator);
    testClientAsyncServerInfo(allocator);
    testClientAsyncRapidSubUnsub(allocator);
}
