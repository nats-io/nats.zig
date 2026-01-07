//! Auth Tests for NATS Async Client
//!
//! Tests for async authentication.

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

pub fn testAsyncAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("async_authentication", true, "");
    } else {
        reportResult("async_authentication", false, "not connected");
    }
}

/// Test: Authentication failure without token
pub fn testAsyncAuthenticationFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, auth_port); // No token!

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const result = nats.ClientAsync.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("async_auth_failure", false, "should have failed");
    } else |_| {
        reportResult("async_auth_failure", true, "");
    }
}

/// Test: Server restart behavior
/// Runs all async auth tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncAuthentication(allocator);
    testAsyncAuthenticationFailure(allocator);
}
