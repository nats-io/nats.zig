//! Connection Tests for NATS Client
//!
//! Tests for connection lifecycle, reconnection, and connection state.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;
const ServerManager = utils.ServerManager;

pub fn testConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "test-connect",
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "error";
        reportResult("connect_disconnect", false, msg);
        return;
    };
    defer client.deinit(allocator);

    const connected = client.isConnected();
    reportResult("connect_disconnect", connected, "not connected");
}

pub fn testConnectionRefused(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, 19999);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("connection_refused", false, "should have failed");
    } else |_| {
        reportResult("connection_refused", true, "");
    }
}

pub fn testConsecutiveConnections(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // First connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "first connect fail");
            return;
        };
        client.deinit(allocator);
    }

    // Second connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "second connect fail");
            return;
        };
        client.deinit(allocator);
    }

    // Third connection
    {
        var io: std.Io.Threaded = .init(allocator, .{});
        defer io.deinit();

        const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
            reportResult("consecutive_connections", false, "third connect fail");
            return;
        };
        client.deinit(allocator);
    }

    reportResult("consecutive_connections", true, "");
}

pub fn testIsConnectedState(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("is_connected_state", false, "connect failed");
        return;
    };

    if (!client.isConnected()) {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "not connected after connect");
        return;
    }

    client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "drain failed");
        return;
    };

    if (client.isConnected()) {
        client.deinit(allocator);
        reportResult("is_connected_state", false, "still connected after drain");
        return;
    }

    client.deinit(allocator);
    reportResult("is_connected_state", true, "");
}

pub fn testConnectionStateAfterOps(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("state_after_ops", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after connect");
        return;
    }

    client.publish("state.test", "data") catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after publish");
        return;
    }

    const sub = client.subscribe(allocator, "state.sub") catch {
        reportResult("state_after_ops", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after subscribe");
        return;
    }

    client.flush() catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after flush");
        return;
    }

    reportResult("state_after_ops", true, "");
}

pub fn testReconnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("reconnection", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("reconnection", false, "not connected initially");
        return;
    }

    manager.stopServer(0, io.io());
    std.posix.nanosleep(0, 100_000_000);

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnection", false, "server restart failed");
        return;
    };

    reportResult("reconnection", true, "");
}

pub fn testServerRestartNewConnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{});
    defer io1.deinit();

    const client1 = nats.Client.connect(allocator, io1.io(), url, .{}) catch {
        reportResult("server_restart_new_conn", false, "initial connect failed");
        return;
    };

    if (!client1.isConnected()) {
        client1.deinit(allocator);
        reportResult("server_restart_new_conn", false, "not connected");
        return;
    }

    client1.deinit(allocator);

    manager.stopServer(0, io1.io());
    std.posix.nanosleep(0, 100_000_000);

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("server_restart_new_conn", false, "restart failed");
        return;
    };

    var io2: std.Io.Threaded = .init(allocator, .{});
    defer io2.deinit();

    const client2 = nats.Client.connect(allocator, io2.io(), url, .{}) catch {
        reportResult("server_restart_new_conn", false, "reconnect failed");
        return;
    };
    defer client2.deinit(allocator);

    if (client2.isConnected()) {
        reportResult("server_restart_new_conn", true, "");
    } else {
        reportResult("server_restart_new_conn", false, "not connected after restart");
    }
}

/// Runs all connection tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testConnectDisconnect(allocator);
    testConnectionRefused(allocator);
    testConsecutiveConnections(allocator);
    testIsConnectedState(allocator);
    testConnectionStateAfterOps(allocator);
    testReconnection(allocator, manager);
    testServerRestartNewConnection(allocator, manager);
}
