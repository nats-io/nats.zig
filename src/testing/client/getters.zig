//! Getters Tests for NATS Client
//!
//! Tests for connection info and subscription info getters.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testConnectionInfoGetters(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false, .name = "test-client" },
    ) catch {
        reportResult("connection_info_getters", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Test name()
    const name = client.name();
    if (name == null or !std.mem.eql(u8, name.?, "test-client")) {
        reportResult("connection_info_getters", false, "name failed");
        return;
    }

    // Test connectedUrl()
    const conn_url = client.connectedUrl();
    if (conn_url == null) {
        reportResult("connection_info_getters", false, "connectedUrl null");
        return;
    }

    // Test connectedServerId() - should have a value
    const server_id = client.connectedServerId();
    if (server_id == null or server_id.?.len == 0) {
        reportResult("connection_info_getters", false, "connectedServerId");
        return;
    }

    // Test connectedServerVersion() - should have a value
    const version = client.connectedServerVersion();
    if (version == null or version.?.len == 0) {
        reportResult("connection_info_getters", false, "connectedServerVersion");
        return;
    }

    // Test headersSupported() - NATS 2.x supports headers
    if (!client.headersSupported()) {
        reportResult("connection_info_getters", false, "headersSupported");
        return;
    }

    // Test maxPayload() - should be > 0
    if (client.maxPayload() == 0) {
        reportResult("connection_info_getters", false, "maxPayload");
        return;
    }

    reportResult("connection_info_getters", true, "");
}

pub fn testServerInfoGetters(allocator: std.mem.Allocator) void {
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
        reportResult("server_info_getters", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Test tlsRequired() - default server doesn't require TLS
    // (this may vary by server config, just check it doesn't crash)
    _ = client.tlsRequired();

    // Test authRequired() - default server may or may not require auth
    _ = client.authRequired();

    // Test clientId() - should be set by server
    const client_id = client.clientId();
    if (client_id == null or client_id.? == 0) {
        reportResult("server_info_getters", false, "clientId");
        return;
    }

    // Test serverCount() - should be at least 1
    if (client.serverCount() < 1) {
        reportResult("server_info_getters", false, "serverCount");
        return;
    }

    reportResult("server_info_getters", true, "");
}

pub fn testConnectedAddrGetter(allocator: std.mem.Allocator) void {
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
        reportResult("connected_addr_getter", false, "connect failed");
        return;
    };
    defer client.deinit();

    var addr_buf: [64]u8 = undefined;
    const addr = client.connectedAddr(&addr_buf);
    if (addr == null) {
        reportResult("connected_addr_getter", false, "connectedAddr null");
        return;
    }

    // Should contain a colon (host:port format)
    if (std.mem.indexOf(u8, addr.?, ":") == null) {
        reportResult("connected_addr_getter", false, "no colon in addr");
        return;
    }

    reportResult("connected_addr_getter", true, "");
}

pub fn testUrlRedaction(allocator: std.mem.Allocator) void {
    // Test URL redaction with credentials
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
        reportResult("url_redaction", false, "connect failed");
        return;
    };
    defer client.deinit();

    var redact_buf: [256]u8 = undefined;
    const redacted = client.connectedUrlRedacted(&redact_buf);
    if (redacted == null) {
        reportResult("url_redaction", false, "connectedUrlRedacted null");
        return;
    }

    // URL without credentials should be unchanged
    const orig = client.connectedUrl().?;
    if (!std.mem.eql(u8, redacted.?, orig)) {
        reportResult("url_redaction", false, "mismatch for no-creds url");
        return;
    }

    reportResult("url_redaction", true, "");
}

pub fn testSubscriptionInfoGetters(allocator: std.mem.Allocator) void {
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
        reportResult("subscription_info_getters", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Create a regular subscription
    const sub = client.subscribeSync("test.getters") catch {
        reportResult("subscription_info_getters", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Test getSid() - should be > 0
    if (sub.getSid() == 0) {
        reportResult("subscription_info_getters", false, "getSid");
        return;
    }

    // Test getSubject()
    if (!std.mem.eql(u8, sub.getSubject(), "test.getters")) {
        reportResult("subscription_info_getters", false, "getSubject");
        return;
    }

    // Test queueGroup() - should be null for regular sub
    if (sub.queueGroup() != null) {
        reportResult("subscription_info_getters", false, "queueGroup");
        return;
    }

    // Test isDraining() - should be false initially
    if (sub.isDraining()) {
        reportResult("subscription_info_getters", false, "isDraining");
        return;
    }

    reportResult("subscription_info_getters", true, "");
}

pub fn testQueueSubGetters(allocator: std.mem.Allocator) void {
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
        reportResult("queue_sub_getters", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Create a queue subscription
    const sub = client.queueSubscribeSync(
        "test.queue.getters",
        "workers",
    ) catch {
        reportResult("queue_sub_getters", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Test queueGroup() - should return "workers"
    const qg = sub.queueGroup();
    if (qg == null or !std.mem.eql(u8, qg.?, "workers")) {
        reportResult("queue_sub_getters", false, "queueGroup");
        return;
    }

    reportResult("queue_sub_getters", true, "");
}

pub fn testDrainingState(allocator: std.mem.Allocator) void {
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
        reportResult("draining_state", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("test.draining") catch {
        reportResult("draining_state", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Initially not draining
    if (sub.isDraining()) {
        reportResult("draining_state", false, "initially draining");
        return;
    }

    // Start drain
    sub.drain() catch {
        reportResult("draining_state", false, "drain failed");
        return;
    };

    // Now should be draining
    if (!sub.isDraining()) {
        reportResult("draining_state", false, "not draining after drain()");
        return;
    }

    reportResult("draining_state", true, "");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testConnectionInfoGetters(allocator);
    testServerInfoGetters(allocator);
    testConnectedAddrGetter(allocator);
    testUrlRedaction(allocator);
    testSubscriptionInfoGetters(allocator);
    testQueueSubGetters(allocator);
    testDrainingState(allocator);
}
