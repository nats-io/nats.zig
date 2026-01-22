//! Basic Tests for NATS Client

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

pub fn testClientBasic(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "client-test",
        .sub_queue_size = 64,
        .reconnect = false,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "error";
        reportResult("client_basic", false, msg);
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("client_basic", false, "not connected");
        return;
    }

    const sub = client.subscribe(allocator, "basic") catch {
        reportResult("client_basic", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    reportResult("client_basic", true, "");
}

pub fn testClientTryNext(allocator: std.mem.Allocator) void {
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
        reportResult("client_try_next", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "trynext") catch {
        reportResult("client_try_next", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    if (sub.tryNext() != null) {
        reportResult("client_try_next", false, "expected null");
        return;
    }

    reportResult("client_try_next", true, "");
}

pub fn testClientServerInfo(allocator: std.mem.Allocator) void {
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
        reportResult("client_server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.getServerInfo()) |info| {
        if (info.port == test_port) {
            reportResult("client_server_info", true, "");
            return;
        }
    }
    reportResult("client_server_info", false, "no server info");
}

pub fn testClientRapidSubUnsub(allocator: std.mem.Allocator) void {
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
        reportResult("client_rapid_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(&buf, "rapid.{d}", .{i}) catch "e";
        const sub = client.subscribe(allocator, subj) catch {
            reportResult("client_rapid_sub", false, "sub failed");
            return;
        };
        sub.deinit(allocator);
    }

    const sub = client.subscribe(allocator, "rapid.final") catch {
        reportResult("client_rapid_sub", false, "final sub failed");
        return;
    };
    defer sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("client_rapid_sub", true, "");
    } else {
        reportResult("client_rapid_sub", false, "disconnected");
    }
}

pub fn testClientName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "test-client-name",
        .reconnect = false,
    }) catch {
        reportResult("client_name_opt", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("client_name_opt", true, "");
    } else {
        reportResult("client_name_opt", false, "not connected");
    }
}

pub fn testClientVerbose(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .verbose = true,
        .reconnect = false,
    }) catch {
        reportResult("client_verbose", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.publish("verbose.test", "data") catch {
        reportResult("client_verbose", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {
        reportResult("client_verbose", false, "flush failed");
        return;
    };

    reportResult("client_verbose", true, "");
}

pub fn testMultipleConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    for (0..5) |_| {
        var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
        const client = nats.Client.connect(
            allocator,
            io.io(),
            url,
            .{ .reconnect = false },
        ) catch {
            io.deinit();
            reportResult("multi_connect_disconnect", false, "connect failed");
            return;
        };
        client.deinit(allocator);
        io.deinit();
    }

    reportResult("multi_connect_disconnect", true, "");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testClientBasic(allocator);
    testClientTryNext(allocator);
    testClientServerInfo(allocator);
    testClientRapidSubUnsub(allocator);
    testClientName(allocator);
    testClientVerbose(allocator);
    testMultipleConnectDisconnect(allocator);
}
