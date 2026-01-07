//! Authentication Tests for NATS Client
//!
//! Tests for token authentication and authentication failures.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;

pub fn testAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    const connected = client.isConnected();
    reportResult("authentication", connected, "auth not connected");
}

pub fn testAuthenticationFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    // Connect to auth server WITHOUT providing token
    const url = formatUrl(&url_buf, auth_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // This should fail - auth server requires token but we're not providing one
    // After Bug #1 fix, connect() now detects auth rejection
    const result = nats.Client.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("auth_failure", false, "should have failed");
    } else |_| {
        // Connection failed as expected
        reportResult("auth_failure", true, "");
    }
}

/// Runs all authentication tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAuthentication(allocator);
    testAuthenticationFailure(allocator);
}
