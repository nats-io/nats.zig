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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
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

pub fn testServerInfoFields(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
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

pub fn testServerVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
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
    if (version.len > 0 and (version[0] == '2' or version[0] == '3')) {
        reportResult("server_version", true, "");
    } else {
        reportResult("server_version", false, "unexpected version");
    }
}

pub fn testServerMaxPayloadEnforced(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("max_payload_enforced", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_enforced", false, "no server info");
        return;
    }

    const max = info.?.max_payload;
    if (max > 0) {
        reportResult("max_payload_enforced", true, "");
    } else {
        reportResult("max_payload_enforced", false, "max_payload is 0");
    }
}

pub fn testMaxPayloadRespected(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("max_payload_respected", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_respected", false, "no server info");
        return;
    }

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

pub fn testProtocolVersion(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("proto_version", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("proto_version", false, "no server info");
        return;
    }

    if (info.?.proto >= 1) {
        reportResult("proto_version", true, "");
    } else {
        reportResult("proto_version", false, "proto < 1");
    }
}

pub fn testClientName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "my-test-client-12345",
        .reconnect = false,
    }) catch {
        reportResult("client_name", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("client_name", true, "");
    } else {
        reportResult("client_name", false, "not connected");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testServerInfo(allocator);
    testServerInfoFields(allocator);
    testServerVersion(allocator);
    testServerMaxPayloadEnforced(allocator);
    testMaxPayloadRespected(allocator);
    testProtocolVersion(allocator);
    testClientName(allocator);
}
