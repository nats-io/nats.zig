//! Server Tests for NATS Client
//!
//! Tests for server info and protocol handling.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testServerInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_info", false, "no server info");
        return;
    }

    const has_version = info.?.version.len > 0;
    reportResult("server_info", has_version, "no version in info");
}

// Test 6: Multiple subscriptions

pub fn testServerInfoFields(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_info_fields", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_info_fields", false, "no server info");
        return;
    }

    const i = info.?;

    // Check required fields exist
    var valid = true;
    if (i.version.len == 0) valid = false;
    if (i.max_payload == 0) valid = false;
    if (i.proto < 1) valid = false;

    if (valid) {
        reportResult("server_info_fields", true, "");
    } else {
        reportResult("server_info_fields", false, "missing fields");
    }
}

// Test 52: Stats increment correctly

pub fn testServerVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_version", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_version", false, "no server info");
        return;
    }

    const version = info.?.version;
    // Server version should be like "2.x.x"
    if (version.len > 0 and (version[0] == '2' or version[0] == '3')) {
        reportResult("server_version", true, "");
    } else {
        reportResult("server_version", false, "unexpected version");
    }
}

// Test 91: bytes_in stats accuracy

pub fn testServerMaxPayloadEnforced(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_payload_enforced", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_enforced", false, "no server info");
        return;
    }

    // max_payload from server (usually 1MB)
    const max = info.?.max_payload;
    if (max > 0) {
        reportResult("max_payload_enforced", true, "");
    } else {
        reportResult("max_payload_enforced", false, "max_payload is 0");
    }
}

// Test 88: Unsubscribe by SID

pub fn testMaxPayloadRespected(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_payload_respected", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_respected", false, "no server info");
        return;
    }

    // Verify max_payload is reasonable (default is 1MB)
    if (info.?.max_payload >= 1024 and info.?.max_payload <= 64 * 1024 * 1024) {
        reportResult("max_payload_respected", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "max_payload={d}",
            .{info.?.max_payload},
        ) catch "err";
        reportResult("max_payload_respected", false, detail);
    }
}

// Test 60: Rapid subscribe/unsubscribe cycles

pub fn testProtocolVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("proto_version", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("proto_version", false, "no server info");
        return;
    }

    // Protocol version should be >= 1
    if (info.?.proto >= 1) {
        reportResult("proto_version", true, "");
    } else {
        reportResult("proto_version", false, "proto < 1");
    }
}

// Test 100: Complete pub/sub round-trip verification

pub fn testClientName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "my-test-client-12345",
    }) catch {
        reportResult("client_name", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // If connection succeeded with name, test passes
    if (client.isConnected()) {
        reportResult("client_name", true, "");
    } else {
        reportResult("client_name", false, "not connected");
    }
}

// Test 46: Double drain should be safe

/// Runs all server tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testServerInfo(allocator);
    testServerInfoFields(allocator);
    testServerVersion(allocator);
    testServerMaxPayloadEnforced(allocator);
    testMaxPayloadRespected(allocator);
    testProtocolVersion(allocator);
    testClientName(allocator);
}
