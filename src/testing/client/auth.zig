//! Auth Tests for NATS Client

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

pub fn testAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("authentication", true, "");
    } else {
        reportResult("authentication", false, "not connected");
    }
}

pub fn testAuthenticationFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, auth_port); // No token!

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("auth_failure", false, "should have failed");
    } else |_| {
        reportResult("auth_failure", true, "");
    }
}

pub fn testAuthenticatedPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("auth_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "auth.test.subject") catch {
        reportResult("auth_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    client.publish("auth.test.subject", "auth message") catch {
        reportResult("auth_pubsub", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("auth_pubsub", true, "");
    } else {
        reportResult("auth_pubsub", false, "no message");
    }
}

pub fn testEmptyToken(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, "");

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false });

    if (result) |client| {
        if (client.isConnected()) {
            client.deinit(allocator);
            reportResult("empty_token", false, "should fail auth");
            return;
        }
        client.deinit(allocator);
    } else |_| {
        // Expected - auth failed
    }

    reportResult("empty_token", true, "");
}

pub fn testTokenSpecialChars(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("token_special_chars", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("token_special_chars", true, "");
    } else {
        reportResult("token_special_chars", false, "not connected");
    }
}

pub fn testAuthRejectionRecovery(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    var bad_url_buf: [128]u8 = undefined;
    const bad_url = formatAuthUrl(&bad_url_buf, auth_port, "wrong-token");

    const bad_result = nats.Client.connect(allocator, io.io(), bad_url, .{ .reconnect = false });
    if (bad_result) |client| {
        client.deinit(allocator);
        // If it connected, that's unexpected but not a failure of this test
    } else |_| {
        // Expected - auth failed
    }

    // Second: succeed with correct token
    var good_url_buf: [128]u8 = undefined;
    const good_url = formatAuthUrl(&good_url_buf, auth_port, test_token);

    const good_result = nats.Client.connect(allocator, io.io(), good_url, .{ .reconnect = false });
    if (good_result) |client| {
        defer client.deinit(allocator);
        if (client.isConnected()) {
            reportResult("auth_rejection_recovery", true, "");
            return;
        }
    } else |_| {
        reportResult("auth_rejection_recovery", false, "good connect failed");
        return;
    }

    reportResult("auth_rejection_recovery", false, "not connected");
}

pub fn testMultipleAuthAttempts(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    var url_buf: [128]u8 = undefined;
    const bad_url = formatAuthUrl(&url_buf, auth_port, "wrong");

    var failures: u32 = 0;
    for (0..5) |_| {
        const result = nats.Client.connect(allocator, io.io(), bad_url, .{ .reconnect = false });
        if (result) |client| {
            client.deinit(allocator);
        } else |_| {
            failures += 1;
        }
    }

    if (failures == 5) {
        reportResult("multiple_auth_attempts", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "{d}/5 failed", .{failures}) catch "e";
        reportResult("multiple_auth_attempts", false, detail);
    }
}

pub fn testAuthRequiredDetection(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("auth_required_detect", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("auth_required_detect", false, "no server info");
        return;
    }

    // Server should have auth_required = true
    if (info.?.auth_required) {
        reportResult("auth_required_detect", true, "");
    } else {
        reportResult("auth_required_detect", false, "auth not required");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testAuthentication(allocator);
    testAuthenticationFailure(allocator);
    testAuthenticatedPubSub(allocator);
    testEmptyToken(allocator);
    testTokenSpecialChars(allocator);
    testAuthRejectionRecovery(allocator);
    testMultipleAuthAttempts(allocator);
    testAuthRequiredDetection(allocator);
}
