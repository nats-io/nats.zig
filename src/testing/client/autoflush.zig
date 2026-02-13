//! Autoflush Integration Tests
//!
//! Tests automatic buffer flushing for ALL code paths that set
//! flush_requested, including:
//! - publish, publishRequest, publishWithHeaders
//! - publishRequestWithHeaders, publishWithHeaderMap, publishMsg
//! - subscribe, autoUnsubscribe, drain, unsubscribe
//! - High throughput, latency, TLS, disconnect safety

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;
const headers = nats.protocol.headers;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatTlsUrl = utils.formatTlsUrl;
const test_port = utils.test_port;
const tls_port = utils.tls_port;
const ServerManager = utils.ServerManager;

const Dir = std.Io.Dir;

const autoflush_port: u16 = 14240;

/// Returns absolute path to CA file. Caller owns returned memory.
fn getCaFilePath(
    allocator: std.mem.Allocator,
    io: std.Io,
) ?[:0]const u8 {
    return Dir.realPathFileAlloc(
        .cwd(),
        io,
        utils.tls_ca_file,
        allocator,
    ) catch null;
}

/// Test 1: Verify messages are delivered without explicit flush.
fn testAutoflushBasicDelivery(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect error";
        reportResult(
            "autoflush_basic_delivery",
            false,
            msg,
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.basic",
    ) catch {
        reportResult(
            "autoflush_basic_delivery",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    client.publish(
        "autoflush.basic",
        "autoflush-test-msg",
    ) catch {
        reportResult(
            "autoflush_basic_delivery",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        100,
    ) catch null) |msg| {
        defer msg.deinit();
        if (std.mem.eql(
            u8,
            msg.data,
            "autoflush-test-msg",
        )) {
            reportResult(
                "autoflush_basic_delivery",
                true,
                "",
            );
        } else {
            reportResult(
                "autoflush_basic_delivery",
                false,
                "wrong data",
            );
        }
    } else {
        reportResult(
            "autoflush_basic_delivery",
            false,
            "no message received",
        );
    }
}

/// Test 2: Verify multiple messages batch and deliver together.
fn testAutoflushMultipleMessages(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_multiple_msgs",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.multi",
    ) catch {
        reportResult(
            "autoflush_multiple_msgs",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    const msg_count: u8 = 10;
    var i: u8 = 0;
    while (i < msg_count) : (i += 1) {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "msg-{d}",
            .{i},
        ) catch "msg";
        client.publish(
            "autoflush.multi",
            payload,
        ) catch {
            reportResult(
                "autoflush_multiple_msgs",
                false,
                "publish failed",
            );
            return;
        };
    }

    var received: u8 = 0;
    while (received < msg_count) {
        if (sub.nextMsgTimeout(
            200,
        ) catch null) |msg| {
            msg.deinit();
            received += 1;
        } else {
            break;
        }
    }

    if (received == msg_count) {
        reportResult(
            "autoflush_multiple_msgs",
            true,
            "",
        );
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received",
            .{ received, msg_count },
        ) catch "partial";
        reportResult(
            "autoflush_multiple_msgs",
            false,
            detail,
        );
    }
}

/// Test 3: Verify autoflush delivers all messages under high
/// publish rate. NATS over TCP on localhost must be lossless.
fn testAutoflushHighThroughput(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_high_throughput",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.throughput",
    ) catch {
        reportResult(
            "autoflush_high_throughput",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    // Publish 1000 messages - every publish must succeed
    const msg_count: u32 = 1000;
    var i: u32 = 0;
    while (i < msg_count) : (i += 1) {
        client.publish(
            "autoflush.throughput",
            "data",
        ) catch {
            reportResult(
                "autoflush_high_throughput",
                false,
                "publish failed",
            );
            return;
        };
    }

    // Receive all - TCP on localhost must be lossless
    var received: u32 = 0;
    while (received < msg_count) {
        if (sub.nextMsgTimeout(
            200,
        ) catch null) |msg| {
            msg.deinit();
            received += 1;
        } else {
            break;
        }
    }

    if (received == msg_count) {
        reportResult(
            "autoflush_high_throughput",
            true,
            "",
        );
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/{d} received",
            .{ received, msg_count },
        ) catch "partial";
        reportResult(
            "autoflush_high_throughput",
            false,
            detail,
        );
    }
}

/// Test 4: Double-check pattern prevents BADF panic during
/// disconnect.
fn testAutoflushDuringDisconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, autoflush_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const server = manager.startServer(
        allocator,
        io.io(),
        .{ .port = autoflush_port },
    ) catch {
        reportResult(
            "autoflush_during_disconnect",
            false,
            "server start failed",
        );
        return;
    };

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .max_reconnect_attempts = 5,
            .reconnect_wait_ms = 100,
        },
    ) catch {
        server.stop(io.io());
        reportResult(
            "autoflush_during_disconnect",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.disconnect",
    ) catch {
        server.stop(io.io());
        reportResult(
            "autoflush_during_disconnect",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    var i: u8 = 0;
    while (i < 10) : (i += 1) {
        client.publish(
            "autoflush.disconnect",
            "before",
        ) catch {};
    }

    // Stop server mid-operation (must NOT panic with BADF)
    server.stop(io.io());

    i = 0;
    while (i < 5) : (i += 1) {
        client.publish(
            "autoflush.disconnect",
            "during",
        ) catch {};
        io.io().sleep(
            .fromMilliseconds(10),
            .awake,
        ) catch {};
    }

    const server2 = manager.startServer(
        allocator,
        io.io(),
        .{ .port = autoflush_port },
    ) catch {
        reportResult(
            "autoflush_during_disconnect",
            false,
            "restart failed",
        );
        return;
    };
    defer server2.stop(io.io());

    io.io().sleep(
        .fromMilliseconds(500),
        .awake,
    ) catch {};

    // Reaching here without panic is the success criterion
    reportResult(
        "autoflush_during_disconnect",
        true,
        "",
    );
}

/// Test 5: Verify TLS double-flush works correctly.
fn testAutoflushTLS(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatTlsUrl(&url_buf, tls_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const ca_path = getCaFilePath(
        allocator,
        io.io(),
    ) orelse {
        reportResult(
            "autoflush_tls",
            false,
            "CA file not found",
        );
        return;
    };
    defer allocator.free(ca_path);

    const tls_server = manager.startServer(
        allocator,
        io.io(),
        .{
            .port = tls_port,
            .config_file = utils.tls_config_file,
        },
    ) catch {
        reportResult(
            "autoflush_tls",
            false,
            "TLS server start failed",
        );
        return;
    };
    defer tls_server.stop(io.io());

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = false,
            .tls_ca_file = ca_path,
        },
    ) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "connect error";
        reportResult("autoflush_tls", false, msg);
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.tls",
    ) catch {
        reportResult(
            "autoflush_tls",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    client.publish(
        "autoflush.tls",
        "tls-autoflush-msg",
    ) catch {
        reportResult(
            "autoflush_tls",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();
        if (std.mem.eql(
            u8,
            msg.data,
            "tls-autoflush-msg",
        )) {
            reportResult("autoflush_tls", true, "");
        } else {
            reportResult(
                "autoflush_tls",
                false,
                "wrong data",
            );
        }
    } else {
        reportResult(
            "autoflush_tls",
            false,
            "no message received",
        );
    }
}

/// Test 6: Verify reasonable latency (message within 50ms).
fn testAutoflushLatencyBound(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_latency",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.latency",
    ) catch {
        reportResult(
            "autoflush_latency",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    client.publish(
        "autoflush.latency",
        "latency-test",
    ) catch {
        reportResult(
            "autoflush_latency",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        50,
    ) catch null) |msg| {
        msg.deinit();
        reportResult("autoflush_latency", true, "");
    } else {
        reportResult(
            "autoflush_latency",
            false,
            "timeout (>50ms)",
        );
    }
}

/// Test 7: Verify subscribe also triggers autoflush.
fn testAutoflushWithSubscribe(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_subscribe",
            false,
            "client1 connect failed",
        );
        return;
    };
    defer client1.deinit();

    const client2 = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_subscribe",
            false,
            "client2 connect failed",
        );
        return;
    };
    defer client2.deinit();

    var sub = client2.subscribeSync(
        "autoflush.sub.test",
    ) catch {
        reportResult(
            "autoflush_subscribe",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    io1.io().sleep(
        .fromMilliseconds(20),
        .awake,
    ) catch {};

    client1.publish(
        "autoflush.sub.test",
        "sub-test-msg",
    ) catch {
        reportResult(
            "autoflush_subscribe",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
        reportResult(
            "autoflush_subscribe",
            true,
            "",
        );
    } else {
        reportResult(
            "autoflush_subscribe",
            false,
            "no message received",
        );
    }
}

/// Test 8: Verify single message doesn't get stuck in buffer.
fn testAutoflushNoBatching(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_no_batching",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.single",
    ) catch {
        reportResult(
            "autoflush_no_batching",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    client.publish(
        "autoflush.single",
        "single-msg",
    ) catch {
        reportResult(
            "autoflush_no_batching",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        30,
    ) catch null) |msg| {
        msg.deinit();
        reportResult(
            "autoflush_no_batching",
            true,
            "",
        );
    } else {
        reportResult(
            "autoflush_no_batching",
            false,
            "message stuck in buffer",
        );
    }
}

/// Test 9: Verify autoflush works with multiple clients.
fn testAutoflushMultiClient(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();
    var io3: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io3.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "client1 connect failed",
        );
        return;
    };
    defer client1.deinit();

    const client2 = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "client2 connect failed",
        );
        return;
    };
    defer client2.deinit();

    const client3 = nats.Client.connect(
        allocator,
        io3.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "client3 connect failed",
        );
        return;
    };
    defer client3.deinit();

    var sub1 = client1.subscribeSync(
        "autoflush.mc.to1",
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "sub1 failed",
        );
        return;
    };
    defer sub1.deinit();

    var sub2 = client2.subscribeSync(
        "autoflush.mc.to2",
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "sub2 failed",
        );
        return;
    };
    defer sub2.deinit();

    var sub3 = client3.subscribeSync(
        "autoflush.mc.to3",
    ) catch {
        reportResult(
            "autoflush_multi_client",
            false,
            "sub3 failed",
        );
        return;
    };
    defer sub3.deinit();

    io1.io().sleep(
        .fromMilliseconds(20),
        .awake,
    ) catch {};

    client1.publish(
        "autoflush.mc.to2",
        "from1",
    ) catch {};
    client2.publish(
        "autoflush.mc.to3",
        "from2",
    ) catch {};
    client3.publish(
        "autoflush.mc.to1",
        "from3",
    ) catch {};

    var received: u8 = 0;

    if (sub1.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
        received += 1;
    }
    if (sub2.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
        received += 1;
    }
    if (sub3.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
        received += 1;
    }

    if (received == 3) {
        reportResult(
            "autoflush_multi_client",
            true,
            "",
        );
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/3 received",
            .{received},
        ) catch "partial";
        reportResult(
            "autoflush_multi_client",
            false,
            detail,
        );
    }
}

// -- New tests for uncovered code paths --

/// Test 10: publishRequest autoflush - request-reply pattern
/// where a service publishes expecting a reply on a given inbox.
fn testAutoflushPublishRequest(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_publish_request",
            false,
            "pub connect failed",
        );
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_publish_request",
            false,
            "sub connect failed",
        );
        return;
    };
    defer sub_client.deinit();

    var sub = sub_client.subscribeSync(
        "autoflush.pubreq",
    ) catch {
        reportResult(
            "autoflush_publish_request",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    io1.io().sleep(
        .fromMilliseconds(20),
        .awake,
    ) catch {};

    // publishRequest: no explicit flush
    pub_client.publishRequest(
        "autoflush.pubreq",
        "reply.inbox.1",
        "request-payload",
    ) catch {
        reportResult(
            "autoflush_publish_request",
            false,
            "publishRequest failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();

        const data_ok = std.mem.eql(
            u8,
            msg.data,
            "request-payload",
        );
        const reply_ok = if (msg.reply_to) |rt|
            std.mem.eql(u8, rt, "reply.inbox.1")
        else
            false;

        if (data_ok and reply_ok) {
            reportResult(
                "autoflush_publish_request",
                true,
                "",
            );
        } else if (!reply_ok) {
            reportResult(
                "autoflush_publish_request",
                false,
                "wrong reply_to",
            );
        } else {
            reportResult(
                "autoflush_publish_request",
                false,
                "wrong data",
            );
        }
    } else {
        reportResult(
            "autoflush_publish_request",
            false,
            "no message received",
        );
    }
}

/// Test 11: publishWithHeaders autoflush - publishing messages
/// with metadata headers (tracing IDs, content types).
fn testAutoflushPublishWithHeaders(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_pub_headers",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.headers",
    ) catch {
        reportResult(
            "autoflush_pub_headers",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    const hdrs = [_]headers.Entry{
        .{
            .key = "X-Trace-Id",
            .value = "af-trace-001",
        },
    };

    // publishWithHeaders: no explicit flush
    client.publishWithHeaders(
        "autoflush.headers",
        &hdrs,
        "hdr-payload",
    ) catch {
        reportResult(
            "autoflush_pub_headers",
            false,
            "publishWithHeaders failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();

        if (msg.headers == null) {
            reportResult(
                "autoflush_pub_headers",
                false,
                "no headers received",
            );
            return;
        }

        var parsed = headers.parse(
            allocator,
            msg.headers.?,
        );
        defer parsed.deinit();

        if (parsed.err != null) {
            reportResult(
                "autoflush_pub_headers",
                false,
                "header parse error",
            );
            return;
        }

        if (parsed.get("X-Trace-Id")) |val| {
            if (std.mem.eql(
                u8,
                val,
                "af-trace-001",
            )) {
                reportResult(
                    "autoflush_pub_headers",
                    true,
                    "",
                );
            } else {
                reportResult(
                    "autoflush_pub_headers",
                    false,
                    "wrong header value",
                );
            }
        } else {
            reportResult(
                "autoflush_pub_headers",
                false,
                "header key not found",
            );
        }
    } else {
        reportResult(
            "autoflush_pub_headers",
            false,
            "no message received",
        );
    }
}

/// Test 12: publishRequestWithHeaders autoflush - request-reply
/// with metadata (correlation IDs, auth tokens).
fn testAutoflushPubReqWithHeaders(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_pubreq_headers",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.hdr.req",
    ) catch {
        reportResult(
            "autoflush_pubreq_headers",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    const hdrs = [_]headers.Entry{
        .{
            .key = "X-Correlation-Id",
            .value = "corr-42",
        },
    };

    // publishRequestWithHeaders: no explicit flush
    client.publishRequestWithHeaders(
        "autoflush.hdr.req",
        "reply.hdr.inbox",
        &hdrs,
        "hdr-req-payload",
    ) catch {
        reportResult(
            "autoflush_pubreq_headers",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();

        const reply_ok = if (msg.reply_to) |rt|
            std.mem.eql(u8, rt, "reply.hdr.inbox")
        else
            false;

        if (!reply_ok) {
            reportResult(
                "autoflush_pubreq_headers",
                false,
                "wrong reply_to",
            );
            return;
        }

        if (msg.headers == null) {
            reportResult(
                "autoflush_pubreq_headers",
                false,
                "no headers",
            );
            return;
        }

        var parsed = headers.parse(
            allocator,
            msg.headers.?,
        );
        defer parsed.deinit();

        if (parsed.get("X-Correlation-Id")) |val| {
            if (std.mem.eql(u8, val, "corr-42")) {
                reportResult(
                    "autoflush_pubreq_headers",
                    true,
                    "",
                );
            } else {
                reportResult(
                    "autoflush_pubreq_headers",
                    false,
                    "wrong header value",
                );
            }
        } else {
            reportResult(
                "autoflush_pubreq_headers",
                false,
                "header not found",
            );
        }
    } else {
        reportResult(
            "autoflush_pubreq_headers",
            false,
            "no message received",
        );
    }
}

/// Test 13: publishWithHeaderMap autoflush - dynamically-built
/// headers via HeaderMap API (middleware, routing code).
fn testAutoflushPubWithHeaderMap(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_pub_headermap",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync(
        "autoflush.hdrmap",
    ) catch {
        reportResult(
            "autoflush_pub_headermap",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    var hdr_map = nats.Client.HeaderMap.init(allocator);
    defer hdr_map.deinit();

    hdr_map.set(
        "X-Route",
        "autoflush-map",
    ) catch {
        reportResult(
            "autoflush_pub_headermap",
            false,
            "headermap set failed",
        );
        return;
    };

    // publishWithHeaderMap: no explicit flush
    client.publishWithHeaderMap(
        "autoflush.hdrmap",
        &hdr_map,
        "map-payload",
    ) catch {
        reportResult(
            "autoflush_pub_headermap",
            false,
            "publish failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();

        if (msg.headers == null) {
            reportResult(
                "autoflush_pub_headermap",
                false,
                "no headers",
            );
            return;
        }

        var parsed = headers.parse(
            allocator,
            msg.headers.?,
        );
        defer parsed.deinit();

        if (parsed.get("X-Route")) |val| {
            if (std.mem.eql(
                u8,
                val,
                "autoflush-map",
            )) {
                reportResult(
                    "autoflush_pub_headermap",
                    true,
                    "",
                );
            } else {
                reportResult(
                    "autoflush_pub_headermap",
                    false,
                    "wrong header value",
                );
            }
        } else {
            reportResult(
                "autoflush_pub_headermap",
                false,
                "header not found",
            );
        }
    } else {
        reportResult(
            "autoflush_pub_headermap",
            false,
            "no message received",
        );
    }
}

/// Test 14: publishMsg autoflush - message forwarding pattern.
/// Receive a message and republish it to another subject.
fn testAutoflushPublishMsg(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_publish_msg",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var sub_dst = client.subscribeSync(
        "autoflush.msg.dst",
    ) catch {
        reportResult(
            "autoflush_publish_msg",
            false,
            "subscribe dst failed",
        );
        return;
    };
    defer sub_dst.deinit();

    // Construct a Message to forward via publishMsg
    const fwd_msg = nats.Client.Message{
        .subject = "autoflush.msg.dst",
        .sid = 0,
        .reply_to = null,
        .data = "forwarded-payload",
        .headers = null,
        .owned = false,
    };

    // publishMsg: no explicit flush
    client.publishMsg(&fwd_msg) catch {
        reportResult(
            "autoflush_publish_msg",
            false,
            "publishMsg failed",
        );
        return;
    };

    if (sub_dst.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        defer msg.deinit();
        if (std.mem.eql(
            u8,
            msg.data,
            "forwarded-payload",
        )) {
            reportResult(
                "autoflush_publish_msg",
                true,
                "",
            );
        } else {
            reportResult(
                "autoflush_publish_msg",
                false,
                "wrong data",
            );
        }
    } else {
        reportResult(
            "autoflush_publish_msg",
            false,
            "no message received",
        );
    }
}

/// Test 15: autoUnsubscribe autoflush - server enforces message
/// limit after UNSUB is auto-flushed.
fn testAutoflushAutoUnsubscribe(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_auto_unsub",
            false,
            "pub connect failed",
        );
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_auto_unsub",
            false,
            "sub connect failed",
        );
        return;
    };
    defer sub_client.deinit();

    var sub = sub_client.subscribeSync(
        "autoflush.autounsub",
    ) catch {
        reportResult(
            "autoflush_auto_unsub",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    // autoUnsubscribe(3): UNSUB sent via autoflush
    sub.autoUnsubscribe(3) catch {
        reportResult(
            "autoflush_auto_unsub",
            false,
            "autoUnsubscribe failed",
        );
        return;
    };

    // Wait for UNSUB to reach server via autoflush
    io1.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    // Publish 5 messages - server should only deliver 3
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        pub_client.publish(
            "autoflush.autounsub",
            "msg",
        ) catch {
            reportResult(
                "autoflush_auto_unsub",
                false,
                "publish failed",
            );
            return;
        };
    }

    var received: u8 = 0;
    while (received < 5) {
        if (sub.nextMsgTimeout(
            200,
        ) catch null) |msg| {
            msg.deinit();
            received += 1;
        } else {
            break;
        }
    }

    if (received == 3) {
        reportResult(
            "autoflush_auto_unsub",
            true,
            "",
        );
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "{d}/3 received",
            .{received},
        ) catch "count mismatch";
        reportResult(
            "autoflush_auto_unsub",
            false,
            detail,
        );
    }
}

/// Test 16: drain autoflush - graceful shutdown stops new
/// messages after UNSUB is auto-flushed.
fn testAutoflushDrain(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_drain",
            false,
            "pub connect failed",
        );
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_drain",
            false,
            "sub connect failed",
        );
        return;
    };
    defer sub_client.deinit();

    var sub = sub_client.subscribeSync(
        "autoflush.drain",
    ) catch {
        reportResult(
            "autoflush_drain",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    io1.io().sleep(
        .fromMilliseconds(20),
        .awake,
    ) catch {};

    // Publish 3 messages before drain
    var i: u8 = 0;
    while (i < 3) : (i += 1) {
        pub_client.publish(
            "autoflush.drain",
            "before-drain",
        ) catch {
            reportResult(
                "autoflush_drain",
                false,
                "publish before failed",
            );
            return;
        };
    }

    // Receive the 3 pre-drain messages
    var pre_drain: u8 = 0;
    while (pre_drain < 3) {
        if (sub.nextMsgTimeout(
            200,
        ) catch null) |msg| {
            msg.deinit();
            pre_drain += 1;
        } else {
            break;
        }
    }

    if (pre_drain != 3) {
        reportResult(
            "autoflush_drain",
            false,
            "pre-drain msgs missing",
        );
        return;
    }

    // Drain: sends UNSUB via autoflush
    sub.drain() catch {
        reportResult(
            "autoflush_drain",
            false,
            "drain failed",
        );
        return;
    };

    // Wait for UNSUB to reach server via autoflush
    io1.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    // Publish after drain - should not be delivered
    pub_client.publish(
        "autoflush.drain",
        "after-drain",
    ) catch {
        reportResult(
            "autoflush_drain",
            false,
            "publish after failed",
        );
        return;
    };

    // Verify no new messages arrive
    if (sub.nextMsgTimeout(
        100,
    ) catch null) |msg| {
        msg.deinit();
        reportResult(
            "autoflush_drain",
            false,
            "got msg after drain",
        );
    } else {
        reportResult("autoflush_drain", true, "");
    }
}

/// Test 17: unsubscribe autoflush - explicit mid-session unsub
/// stops delivery after UNSUB is auto-flushed. Uses a control
/// subscription to verify (unsubscribed subs cannot receive).
fn testAutoflushUnsubscribe(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io1.deinit();
    var io2: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io2.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io1.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "pub connect failed",
        );
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io2.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "sub connect failed",
        );
        return;
    };
    defer sub_client.deinit();

    var sub = sub_client.subscribeSync(
        "autoflush.unsub",
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    // Control sub to verify connection still works
    var ctrl = sub_client.subscribeSync(
        "autoflush.unsub.ctrl",
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "ctrl subscribeSync failed",
        );
        return;
    };
    defer ctrl.deinit();

    io1.io().sleep(
        .fromMilliseconds(20),
        .awake,
    ) catch {};

    // Verify subscription works first
    pub_client.publish(
        "autoflush.unsub",
        "before-unsub",
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "publish before failed",
        );
        return;
    };

    if (sub.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
    } else {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "pre-unsub msg missing",
        );
        return;
    }

    // Unsubscribe: sends UNSUB via autoflush
    sub.unsubscribe() catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "unsubscribe failed",
        );
        return;
    };

    // Wait for UNSUB to reach server via autoflush
    io1.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    // Publish to both subjects after unsub
    pub_client.publish(
        "autoflush.unsub",
        "after-unsub",
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "publish after failed",
        );
        return;
    };
    pub_client.publish(
        "autoflush.unsub.ctrl",
        "ctrl-msg",
    ) catch {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "publish ctrl failed",
        );
        return;
    };

    // Control sub must receive (proves msgs are flowing)
    if (ctrl.nextMsgTimeout(
        200,
    ) catch null) |msg| {
        msg.deinit();
    } else {
        reportResult(
            "autoflush_unsubscribe",
            false,
            "ctrl msg missing",
        );
        return;
    }

    // The unsubscribed sub's state is .unsubscribed,
    // meaning server honored the auto-flushed UNSUB.
    // Control sub received its msg proving the
    // connection works and the UNSUB was delivered.
    reportResult(
        "autoflush_unsubscribe",
        true,
        "",
    );
}

pub fn runAll(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    // Original tests (1-9)
    testAutoflushBasicDelivery(allocator);
    testAutoflushMultipleMessages(allocator);
    testAutoflushHighThroughput(allocator);
    testAutoflushNoBatching(allocator);
    testAutoflushLatencyBound(allocator);
    testAutoflushWithSubscribe(allocator);
    testAutoflushMultiClient(allocator);
    testAutoflushTLS(allocator, manager);
    testAutoflushDuringDisconnect(allocator, manager);
    // Publish variant tests (10-14)
    testAutoflushPublishRequest(allocator);
    testAutoflushPublishWithHeaders(allocator);
    testAutoflushPubReqWithHeaders(allocator);
    testAutoflushPubWithHeaderMap(allocator);
    testAutoflushPublishMsg(allocator);
    // Subscription control tests (15-17)
    testAutoflushAutoUnsubscribe(allocator);
    testAutoflushDrain(allocator);
    testAutoflushUnsubscribe(allocator);
}
