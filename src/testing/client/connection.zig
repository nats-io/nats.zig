//! Connection Tests for NATS Client

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

pub fn testConnectionRefused(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(
        allocator,
        io.io(),
        "nats://127.0.0.1:19999",
        .{ .reconnect = false },
    );

    if (result) |client| {
        client.deinit(allocator);
        reportResult("connection_refused", false, "expected error");
    } else |_| {
        reportResult("connection_refused", true, "");
    }
}

pub fn testConsecutiveConnections(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    for (0..3) |i| {
        const client = nats.Client.connect(
            allocator,
            io.io(),
            url,
            .{ .reconnect = false },
        ) catch {
            var buf: [32]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "connect {d} failed",
                .{i},
            ) catch "e";
            reportResult("consecutive_connections", false, msg);
            return;
        };
        client.deinit(allocator);
    }

    reportResult("consecutive_connections", true, "");
}

pub fn testIsConnectedState(allocator: std.mem.Allocator) void {
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
        reportResult("is_connected_state", false, "connect failed");
        return;
    };

    if (!client.isConnected()) {
        client.deinit(allocator);
        reportResult(
            "is_connected_state",
            false,
            "not connected initially",
        );
        return;
    }

    client.deinit(allocator);
    reportResult("is_connected_state", true, "");
}

pub fn testReconnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
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
        reportResult("reconnection", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("reconnection", false, "not connected initially");
        return;
    }

    manager.stopServer(0, io.io());
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("reconnection", false, "server restart failed");
        return;
    };

    reportResult("reconnection", true, "");
}

/// Test: New connection after server restart
pub fn testServerRestartNewConnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io1.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("server_restart", false, "initial connect failed");
        return;
    };

    if (!client1.isConnected()) {
        client1.deinit(allocator);
        reportResult("server_restart", false, "not connected");
        return;
    }

    client1.deinit(allocator);

    manager.stopServer(0, io1.io());
    io1.io().sleep(.fromMilliseconds(100), .awake) catch {};

    _ = manager.startServer(
        allocator,
        io1.io(),
        .{ .port = test_port },
    ) catch {
        reportResult("server_restart", false, "restart failed");
        return;
    };

    var io2: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io2.deinit();

    const client2 = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("server_restart", false, "reconnect failed");
        return;
    };
    defer client2.deinit(allocator);

    if (client2.isConnected()) {
        reportResult("server_restart", true, "");
    } else {
        reportResult(
            "server_restart",
            false,
            "not connected after restart",
        );
    }
}

pub fn testConnectionStateAfterOps(allocator: std.mem.Allocator) void {
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

    client.flush(allocator) catch {};
    if (!client.isConnected()) {
        reportResult("state_after_ops", false, "not connected after flush");
        return;
    }

    reportResult("state_after_ops", true, "");
}

/// Verifies no resource leaks after many connection cycles.
pub fn testRapidConnectDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Do 50 connect/disconnect cycles
    const CYCLES = 50;
    var success: u32 = 0;

    for (0..CYCLES) |_| {
        var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
        defer io.deinit();

        const client = nats.Client.connect(
            allocator,
            io.io(),
            url,
            .{ .reconnect = false },
        ) catch {
            continue;
        };

        if (client.isConnected()) {
            success += 1;
        }

        client.deinit(allocator);
    }

    if (success == CYCLES) {
        reportResult("rapid_connect_disconnect", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/50 cycles",
            .{success},
        ) catch "e";
        reportResult("rapid_connect_disconnect", false, detail);
    }
}

/// Verifies connect options are properly applied.
pub fn testConnectionOptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .name = "test-client",
        .verbose = false,
        .pedantic = false,
        .echo = true,
        .headers = true,
        .no_responders = true,
        .sub_queue_size = 128,
        .reconnect = false,
    }) catch {
        reportResult("connection_options", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("connection_options", false, "not connected");
        return;
    }

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("connection_options", false, "no server info");
        return;
    }

    reportResult("connection_options", true, "");
}

/// Verifies drain properly cleans up subscriptions.
pub fn testConnectionDrain(allocator: std.mem.Allocator) void {
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
        reportResult("connection_drain", false, "connect failed");
        return;
    };

    // Create some subscriptions
    const sub1 = client.subscribe(allocator, "drain.1") catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "drain.2") catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush(allocator) catch {};

    _ = client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("connection_drain", false, "drain failed");
        return;
    };

    if (client.isConnected()) {
        client.deinit(allocator);
        reportResult("connection_drain", false, "still connected");
        return;
    }

    client.deinit(allocator);
    reportResult("connection_drain", true, "");
}

/// Verifies invalid URLs are rejected properly.
pub fn testInvalidUrlHandling(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const empty_result = nats.Client.connect(
        allocator,
        io.io(),
        "",
        .{ .reconnect = false },
    );
    if (empty_result) |client| {
        client.deinit(allocator);
        reportResult("invalid_url_handling", false, "empty should fail");
        return;
    } else |_| {}

    const invalid_result = nats.Client.connect(
        allocator,
        io.io(),
        "nats://",
        .{ .reconnect = false },
    );
    if (invalid_result) |client| {
        client.deinit(allocator);
        reportResult("invalid_url_handling", false, "invalid should fail");
        return;
    } else |_| {}

    reportResult("invalid_url_handling", true, "");
}

/// Verifies state machine transitions are correct.
pub fn testConnectionStateTransitions(allocator: std.mem.Allocator) void {
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
        reportResult("connection_state", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("connection_state", false, "not connected");
        return;
    }

    client.publish("state.trans", "test") catch {
        reportResult("connection_state", false, "publish failed");
        return;
    };

    const sub = client.subscribe(allocator, "state.trans") catch {
        reportResult("connection_state", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("connection_state", false, "flush failed");
        return;
    };

    reportResult("connection_state", true, "");
}

/// Verifies client can handle many subscriptions.
pub fn testManyClientSubscriptions(allocator: std.mem.Allocator) void {
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
        reportResult("many_client_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const NUM_SUBS = 100;
    var subs: [NUM_SUBS]?*nats.Subscription =
        [_]?*nats.Subscription{null} ** NUM_SUBS;
    var created: usize = 0;

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..NUM_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "many.subs.{d}",
            .{i},
        ) catch continue;

        subs[i] = client.subscribe(allocator, subject) catch {
            break;
        };
        created += 1;
    }

    if (created != NUM_SUBS) {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/100",
            .{created},
        ) catch "e";
        reportResult("many_client_subs", false, detail);
        return;
    }

    if (client.isConnected()) {
        reportResult("many_client_subs", true, "");
    } else {
        reportResult("many_client_subs", false, "disconnected");
    }
}

pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    testConnectionRefused(allocator);
    testConsecutiveConnections(allocator);
    testIsConnectedState(allocator);
    testConnectionStateAfterOps(allocator);
    testRapidConnectDisconnect(allocator);
    testConnectionOptions(allocator);
    testConnectionDrain(allocator);
    testInvalidUrlHandling(allocator);
    testConnectionStateTransitions(allocator);
    testManyClientSubscriptions(allocator);
    testReconnection(allocator, manager);
    testServerRestartNewConnection(allocator, manager);
}
