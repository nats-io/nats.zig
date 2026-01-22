//! JWT/Credentials Authentication Tests for NATS Client
//!
//! Tests JWT authentication with credentials files against nats-server.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const jwt_port = utils.jwt_port;
const test_creds_file = utils.test_creds_file;

/// Tests successful JWT authentication with credentials file.
pub fn testJwtCredsFile(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds_file = test_creds_file,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("jwt_creds_file", false, detail);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("jwt_creds_file", true, "");
    } else {
        reportResult("jwt_creds_file", false, "not connected");
    }
}

/// Tests JWT authentication with in-memory credentials content.
pub fn testJwtCredsContent(allocator: std.mem.Allocator) void {
    const creds_content = @embedFile("../configs/TestUser.creds");

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds = creds_content,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("jwt_creds_content", false, detail);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("jwt_creds_content", true, "");
    } else {
        reportResult("jwt_creds_content", false, "not connected");
    }
}

/// Tests pub/sub works after JWT authentication.
pub fn testJwtPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds_file = test_creds_file,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("jwt_pub_sub", false, detail);
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "jwt.test.subject") catch {
        reportResult("jwt_pub_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    client.publish("jwt.test.subject", "jwt message") catch {
        reportResult("jwt_pub_sub", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("jwt_pub_sub", true, "");
    } else {
        reportResult("jwt_pub_sub", false, "no message");
    }
}

pub fn testJwtInvalidCreds(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Wrong seed that won't match the JWT's public key
    const wrong_creds =
        \\-----BEGIN NATS USER JWT-----
        \\eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJMN1dBT1hJU0tPSUZNM1QyNEhMQ09ENzJRT1czQkNVWEdETjRKVU1SSUtHTlQ3RzdZVFRRIiwiaWF0IjoxNjUxNzkwOTgyLCJpc3MiOiJBRFRRUzdaQ0ZWSk5XNTcyNkdPWVhXNVRTQ1pGTklRU0hLMlpHWVVCQ0Q1RDc3T1ROTE9PS1pPWiIsIm5hbWUiOiJUZXN0VXNlciIsInN1YiI6IlVBRkhHNkZVRDJVVTRTREZWQUZVTDVMREZPMlhNNFdZTTc2VU5YVFBKWUpLN0VFTVlSQkhUMlZFIiwibmF0cyI6eyJwdWIiOnt9LCJzdWIiOnt9LCJzdWJzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ0eXBlIjoidXNlciIsInZlcnNpb24iOjJ9fQ.bp2-Jsy33l4ayF7Ku1MNdJby4WiMKUrG-rSVYGBusAtV3xP4EdCa-zhSNUaBVIL3uYPPCQYCEoM1pCUdOnoJBg
        \\------END NATS USER JWT------
        \\-----BEGIN USER NKEY SEED-----
        \\SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4
        \\------END USER NKEY SEED------
    ;

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds = wrong_creds,
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("jwt_invalid_creds", false, "should have failed");
    } else |_| {
        reportResult("jwt_invalid_creds", true, "");
    }
}

pub fn testJwtMalformedCreds(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const malformed_creds = "not a valid creds file";

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds = malformed_creds,
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("jwt_malformed_creds", false, "should have failed");
    } else |_| {
        reportResult("jwt_malformed_creds", true, "");
    }
}

pub fn testJwtMissingFile(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, jwt_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .creds_file = "/nonexistent/path/to/creds.creds",
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("jwt_missing_file", false, "should have failed");
    } else |_| {
        reportResult("jwt_missing_file", true, "");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testJwtCredsFile(allocator);
    testJwtCredsContent(allocator);
    testJwtPubSub(allocator);
    testJwtInvalidCreds(allocator);
    testJwtMalformedCreds(allocator);
    testJwtMissingFile(allocator);
}
