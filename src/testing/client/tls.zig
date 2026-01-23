//! TLS Tests for NATS Client
//!
//! Tests TLS connection functionality including:
//! - TLS connection with CA certificate
//! - Insecure skip verify mode
//! - Pub/sub over TLS
//! - TLS reconnection

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatTlsUrl = utils.formatTlsUrl;
const tls_port = utils.tls_port;
const ServerManager = utils.ServerManager;

const Dir = std.Io.Dir;

/// Returns absolute path to CA file. Caller owns returned memory.
fn getCaFilePath(allocator: std.mem.Allocator, io: std.Io) ?[:0]const u8 {
    return Dir.realPathFileAlloc(.cwd(), io, utils.tls_ca_file, allocator) catch null;
}

pub fn testTlsConnection(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const ca_path = getCaFilePath(allocator, io) orelse {
        reportResult("tls_connection", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .tls_ca_file = ca_path,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect failed";
        reportResult("tls_connection", false, err_msg);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        const info = client.getServerInfo();
        if (info != null and info.?.tls_required) {
            reportResult("tls_connection", true, "");
        } else {
            reportResult("tls_connection", false, "server not TLS required");
        }
    } else {
        reportResult("tls_connection", false, "not connected");
    }
}

pub fn testTlsInsecureSkipVerify(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .tls_insecure_skip_verify = true,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const err_msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect failed";
        reportResult("tls_insecure_skip_verify", false, err_msg);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("tls_insecure_skip_verify", true, "");
    } else {
        reportResult("tls_insecure_skip_verify", false, "not connected");
    }
}

pub fn testTlsPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const ca_path = getCaFilePath(allocator, io) orelse {
        reportResult("tls_pubsub", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .tls_ca_file = ca_path,
    }) catch {
        reportResult("tls_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "tls.test.subject") catch {
        reportResult("tls_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const test_msg = "encrypted message over TLS";
    client.publish("tls.test.subject", test_msg) catch {
        reportResult("tls_pubsub", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, test_msg)) {
            reportResult("tls_pubsub", true, "");
        } else {
            reportResult("tls_pubsub", false, "message mismatch");
        }
    } else {
        reportResult("tls_pubsub", false, "no message received");
    }
}

pub fn testTlsReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const ca_path = getCaFilePath(allocator, io) orelse {
        reportResult("tls_reconnect", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
        .reconnect_wait_max_ms = 1000,
        .tls_ca_file = ca_path,
    }) catch {
        reportResult("tls_reconnect", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("tls_reconnect", false, "not connected initially");
        return;
    }

    // Find TLS server index (last started server)
    const tls_server_idx = manager.count() - 1;

    manager.stopServer(tls_server_idx, io);
    io.sleep(.fromMilliseconds(200), .awake) catch {};

    _ = manager.startServer(allocator, io, .{
        .port = tls_port,
        .config_file = utils.tls_config_file,
    }) catch {
        reportResult("tls_reconnect", false, "server restart failed");
        return;
    };

    io.sleep(.fromMilliseconds(500), .awake) catch {};

    client.publish("tls.reconnect.test", "ping") catch {
        reportResult("tls_reconnect", false, "publish after restart failed");
        return;
    };

    if (client.isConnected()) {
        reportResult("tls_reconnect", true, "");
    } else {
        reportResult("tls_reconnect", false, "not reconnected");
    }
}

pub fn testTlsServerInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const ca_path = getCaFilePath(allocator, io) orelse {
        reportResult("tls_server_info", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .tls_ca_file = ca_path,
    }) catch {
        reportResult("tls_server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("tls_server_info", false, "no server info");
        return;
    }

    if (info.?.tls_required) {
        reportResult("tls_server_info", true, "");
    } else {
        reportResult("tls_server_info", false, "tls_required not set");
    }
}

pub fn testTlsMultipleMessages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const ca_path = getCaFilePath(allocator, io) orelse {
        reportResult("tls_multiple_msgs", false, "CA file not found");
        return;
    };
    defer allocator.free(ca_path);

    const client = nats.Client.connect(allocator, io, url, .{
        .reconnect = false,
        .tls_ca_file = ca_path,
    }) catch {
        reportResult("tls_multiple_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "tls.multi.>") catch {
        reportResult("tls_multiple_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const msg_count: usize = 100;
    for (0..msg_count) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "tls.multi.{d}",
            .{i},
        ) catch "tls.multi.x";
        client.publish(subject, "data") catch {
            reportResult("tls_multiple_msgs", false, "publish failed");
            return;
        };
    }
    client.flush(allocator) catch {};

    var received: usize = 0;
    for (0..msg_count) |_| {
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        } else {
            break;
        }
    }

    if (received == msg_count) {
        reportResult("tls_multiple_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received",
            .{ received, msg_count },
        ) catch "partial";
        reportResult("tls_multiple_msgs", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testTlsConnection(allocator);
    testTlsInsecureSkipVerify(allocator);
    testTlsPubSub(allocator);
    testTlsServerInfo(allocator);
    testTlsMultipleMessages(allocator);
    testTlsReconnect(allocator, manager);
}
