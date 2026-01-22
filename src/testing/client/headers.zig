//! Headers API Integration Tests

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;
const headers = nats.protocol.headers;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testHeadersPublishSingle(allocator: std.mem.Allocator) void {
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
        reportResult("headers_publish_single", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.single") catch {
        reportResult("headers_publish_single", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Test", .value = "hello" },
    };
    client.publishWithHeaders("test.headers.single", &hdrs, "payload") catch {
        reportResult("headers_publish_single", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_publish_single", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_publish_single", false, "parse error");
            return;
        }
        if (parsed.get("X-Test")) |val| {
            if (std.mem.eql(u8, val, "hello")) {
                reportResult("headers_publish_single", true, "");
                return;
            }
        }
        reportResult("headers_publish_single", false, "header mismatch");
    } else |_| {
        reportResult("headers_publish_single", false, "receive failed");
    }
}

pub fn testHeadersPublishMultiple(allocator: std.mem.Allocator) void {
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
        reportResult("headers_publish_multiple", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.multi") catch {
        reportResult("headers_publish_multiple", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-First", .value = "one" },
        .{ .key = "X-Second", .value = "two" },
        .{ .key = "X-Third", .value = "three" },
    };
    client.publishWithHeaders("test.headers.multi", &hdrs, "data") catch {
        reportResult("headers_publish_multiple", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_publish_multiple", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_publish_multiple", false, "parse error");
            return;
        }
        if (parsed.count != 3) {
            reportResult("headers_publish_multiple", false, "wrong count");
            return;
        }
        const has_first = parsed.get("X-First") != null;
        const has_second = parsed.get("X-Second") != null;
        const has_third = parsed.get("X-Third") != null;
        if (has_first and has_second and has_third) {
            reportResult("headers_publish_multiple", true, "");
            return;
        }
        reportResult("headers_publish_multiple", false, "missing headers");
    } else |_| {
        reportResult("headers_publish_multiple", false, "receive failed");
    }
}

pub fn testHeadersPublishEmptyPayload(allocator: std.mem.Allocator) void {
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
        reportResult("headers_empty_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.empty") catch {
        reportResult("headers_empty_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Empty", .value = "yes" },
    };
    client.publishWithHeaders("test.headers.empty", &hdrs, "") catch {
        reportResult("headers_empty_payload", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_empty_payload", false, "no headers");
            return;
        }
        if (msg.data.len != 0) {
            reportResult("headers_empty_payload", false, "payload not empty");
            return;
        }
        reportResult("headers_empty_payload", true, "");
    } else |_| {
        reportResult("headers_empty_payload", false, "receive failed");
    }
}

pub fn testHeadersPublishRequest(allocator: std.mem.Allocator) void {
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
        reportResult("headers_publish_request", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.req") catch {
        reportResult("headers_publish_request", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Request-Id", .value = "req-123" },
    };
    client.publishRequestWithHeaders(
        "test.headers.req",
        "reply.inbox",
        &hdrs,
        "request-data",
    ) catch {
        reportResult("headers_publish_request", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_publish_request", false, "no headers");
            return;
        }
        if (msg.reply_to == null) {
            reportResult("headers_publish_request", false, "no reply_to");
            return;
        }
        if (!std.mem.eql(u8, msg.reply_to.?, "reply.inbox")) {
            reportResult("headers_publish_request", false, "wrong reply_to");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.get("X-Request-Id")) |val| {
            if (std.mem.eql(u8, val, "req-123")) {
                reportResult("headers_publish_request", true, "");
                return;
            }
        }
        reportResult("headers_publish_request", false, "header mismatch");
    } else |_| {
        reportResult("headers_publish_request", false, "receive failed");
    }
}

pub fn testHeadersRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Responder client
    var io_r: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("headers_request_reply", false, "responder connect failed");
        return;
    };
    defer responder.deinit(allocator);

    var io_req: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("headers_request_reply", false, "requester connect failed");
        return;
    };
    defer requester.deinit(allocator);

    const sub = responder.subscribe(allocator, "svc.headers") catch {
        reportResult("headers_request_reply", false, "responder sub failed");
        return;
    };
    defer sub.deinit(allocator);
    responder.flush(allocator) catch {};
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
            a: std.mem.Allocator,
            io: std.Io,
        ) void {
            _ = io;
            if (s.nextWithTimeout(a, 2000) catch null) |req| {
                defer req.deinit(a);
                if (req.reply_to) |reply_inbox| {
                    // Verify headers received
                    if (req.headers != null) {
                        r.publish(reply_inbox, "headers-received") catch {};
                    } else {
                        r.publish(reply_inbox, "no-headers") catch {};
                    }
                    r.flush(a) catch {};
                }
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
        allocator,
        io_r.io(),
    });
    defer _ = handler.cancel(io_r.io());

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Request-Id", .value = "test-123" },
    };
    const reply = requester.requestWithHeaders(
        allocator,
        "svc.headers",
        &hdrs,
        "ping",
        2000,
    ) catch {
        reportResult("headers_request_reply", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit(allocator);
        if (std.mem.eql(u8, msg.data, "headers-received")) {
            reportResult("headers_request_reply", true, "");
            return;
        }
        reportResult("headers_request_reply", false, "headers not received");
        return;
    }

    reportResult("headers_request_reply", false, "no reply");
}

pub fn testHeadersRequestTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .no_responders = false,
        .reconnect = false,
    }) catch {
        reportResult("headers_request_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Test", .value = "timeout" },
    };

    const start = std.time.Instant.now() catch {
        reportResult("headers_request_timeout", false, "timer failed");
        return;
    };

    const result = client.requestWithHeaders(
        allocator,
        "nonexistent.headers.service",
        &hdrs,
        "ping",
        200,
    ) catch {
        reportResult("headers_request_timeout", false, "request error");
        return;
    };

    const end = std.time.Instant.now() catch {
        reportResult("headers_request_timeout", false, "timer failed");
        return;
    };
    const elapsed_ms = end.since(start) / std.time.ns_per_ms;

    if (result) |msg| {
        msg.deinit(allocator);
    }

    if (elapsed_ms < 5000) {
        reportResult("headers_request_timeout", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "took {d}ms",
            .{elapsed_ms},
        ) catch "e";
        reportResult("headers_request_timeout", false, detail);
    }
}

pub fn testHeadersCrossClient(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io_a: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_a.deinit();
    const client_a = nats.Client.connect(
        allocator,
        io_a.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("headers_cross_client", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    var io_b: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_b.deinit();
    const client_b = nats.Client.connect(
        allocator,
        io_b.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("headers_cross_client", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    const sub = client_b.subscribe(allocator, "cross.headers") catch {
        reportResult("headers_cross_client", false, "B sub failed");
        return;
    };
    defer sub.deinit(allocator);
    client_b.flush(allocator) catch {};
    io_b.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-From", .value = "client-A" },
        .{ .key = "X-Correlation-Id", .value = "corr-456" },
    };
    client_a.publishWithHeaders("cross.headers", &hdrs, "cross-data") catch {
        reportResult("headers_cross_client", false, "A publish failed");
        return;
    };
    client_a.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 2000) catch null) |msg| {
        defer msg.deinit(allocator);
        if (msg.headers == null) {
            reportResult("headers_cross_client", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_cross_client", false, "parse error");
            return;
        }
        const from = parsed.get("X-From");
        const corr = parsed.get("X-Correlation-Id");
        if (from != null and corr != null) {
            if (std.mem.eql(u8, from.?, "client-A") and
                std.mem.eql(u8, corr.?, "corr-456"))
            {
                reportResult("headers_cross_client", true, "");
                return;
            }
        }
        reportResult("headers_cross_client", false, "header values wrong");
        return;
    }

    reportResult("headers_cross_client", false, "no message received");
}

pub fn testHeadersManyEntries(allocator: std.mem.Allocator) void {
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
        reportResult("headers_many_entries", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.many") catch {
        reportResult("headers_many_entries", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "H00", .value = "v00" }, .{ .key = "H01", .value = "v01" },
        .{ .key = "H02", .value = "v02" }, .{ .key = "H03", .value = "v03" },
        .{ .key = "H04", .value = "v04" }, .{ .key = "H05", .value = "v05" },
        .{ .key = "H06", .value = "v06" }, .{ .key = "H07", .value = "v07" },
        .{ .key = "H08", .value = "v08" }, .{ .key = "H09", .value = "v09" },
        .{ .key = "H10", .value = "v10" }, .{ .key = "H11", .value = "v11" },
        .{ .key = "H12", .value = "v12" }, .{ .key = "H13", .value = "v13" },
        .{ .key = "H14", .value = "v14" }, .{ .key = "H15", .value = "v15" },
        .{ .key = "H16", .value = "v16" }, .{ .key = "H17", .value = "v17" },
        .{ .key = "H18", .value = "v18" }, .{ .key = "H19", .value = "v19" },
        .{ .key = "H20", .value = "v20" }, .{ .key = "H21", .value = "v21" },
        .{ .key = "H22", .value = "v22" }, .{ .key = "H23", .value = "v23" },
        .{ .key = "H24", .value = "v24" }, .{ .key = "H25", .value = "v25" },
        .{ .key = "H26", .value = "v26" }, .{ .key = "H27", .value = "v27" },
        .{ .key = "H28", .value = "v28" }, .{ .key = "H29", .value = "v29" },
        .{ .key = "H30", .value = "v30" }, .{ .key = "H31", .value = "v31" },
        .{ .key = "H32", .value = "v32" }, .{ .key = "H33", .value = "v33" },
        .{ .key = "H34", .value = "v34" }, .{ .key = "H35", .value = "v35" },
        .{ .key = "H36", .value = "v36" }, .{ .key = "H37", .value = "v37" },
        .{ .key = "H38", .value = "v38" }, .{ .key = "H39", .value = "v39" },
    };
    client.publishWithHeaders("test.headers.many", &hdrs, "many-test") catch {
        reportResult("headers_many_entries", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_many_entries", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_many_entries", false, "parse error");
            return;
        }
        if (parsed.count != 40) {
            var buf: [32]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "got {d}/40",
                .{parsed.count},
            ) catch "e";
            reportResult("headers_many_entries", false, detail);
            return;
        }
        const first = parsed.get("H00");
        const last = parsed.get("H39");
        if (first != null and last != null) {
            if (std.mem.eql(u8, first.?, "v00") and
                std.mem.eql(u8, last.?, "v39"))
            {
                reportResult("headers_many_entries", true, "");
                return;
            }
        }
        reportResult("headers_many_entries", false, "header mismatch");
    } else |_| {
        reportResult("headers_many_entries", false, "receive failed");
    }
}

pub fn testHeadersLargeValues(allocator: std.mem.Allocator) void {
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
        reportResult("headers_large_values", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.large") catch {
        reportResult("headers_large_values", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    var large_value: [200]u8 = undefined;
    @memset(&large_value, 'X');

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Large", .value = &large_value },
    };
    client.publishWithHeaders("test.headers.large", &hdrs, "payload") catch {
        reportResult("headers_large_values", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_large_values", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_large_values", false, "parse error");
            return;
        }
        if (parsed.get("X-Large")) |val| {
            if (val.len == 200) {
                reportResult("headers_large_values", true, "");
                return;
            }
        }
        reportResult("headers_large_values", false, "value mismatch");
    } else |_| {
        reportResult("headers_large_values", false, "receive failed");
    }
}

pub fn testHeadersSpecialChars(allocator: std.mem.Allocator) void {
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
        reportResult("headers_special_chars", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.special") catch {
        reportResult("headers_special_chars", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "X-Timestamp", .value = "2026-01-21T10:30:00Z" },
        .{ .key = "X-URL", .value = "http://example.com:8080/path" },
    };
    client.publishWithHeaders("test.headers.special", &hdrs, "data") catch {
        reportResult("headers_special_chars", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_special_chars", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_special_chars", false, "parse error");
            return;
        }
        const ts = parsed.get("X-Timestamp");
        const url_val = parsed.get("X-URL");
        if (ts != null and url_val != null) {
            const ts_ok = std.mem.eql(u8, ts.?, "2026-01-21T10:30:00Z");
            const url_ok = std.mem.eql(
                u8,
                url_val.?,
                "http://example.com:8080/path",
            );
            if (ts_ok and url_ok) {
                reportResult("headers_special_chars", true, "");
                return;
            }
        }
        reportResult("headers_special_chars", false, "value mismatch");
    } else |_| {
        reportResult("headers_special_chars", false, "receive failed");
    }
}

pub fn testHeadersBinaryPayload(allocator: std.mem.Allocator) void {
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
        reportResult("headers_binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.binary") catch {
        reportResult("headers_binary_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const binary_payload = [_]u8{ 0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80, 0x00, 0xFF };

    const hdrs = [_]headers.Entry{
        .{ .key = "Content-Type", .value = "application/octet-stream" },
    };
    client.publishWithHeaders(
        "test.headers.binary",
        &hdrs,
        &binary_payload,
    ) catch {
        reportResult("headers_binary_payload", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_binary_payload", false, "no headers");
            return;
        }
        if (msg.data.len != 8) {
            reportResult("headers_binary_payload", false, "wrong payload len");
            return;
        }
        if (std.mem.eql(u8, msg.data, &binary_payload)) {
            reportResult("headers_binary_payload", true, "");
            return;
        }
        reportResult("headers_binary_payload", false, "payload mismatch");
    } else |_| {
        reportResult("headers_binary_payload", false, "receive failed");
    }
}

pub fn testHeadersWellKnown(allocator: std.mem.Allocator) void {
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
        reportResult("headers_well_known", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.wellknown") catch {
        reportResult("headers_well_known", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = headers.HeaderName.msg_id, .value = "unique-msg-001" },
    };
    client.publishWithHeaders("test.headers.wellknown", &hdrs, "data") catch {
        reportResult("headers_well_known", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_well_known", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_well_known", false, "parse error");
            return;
        }
        if (parsed.get(headers.HeaderName.msg_id)) |val| {
            if (std.mem.eql(u8, val, "unique-msg-001")) {
                reportResult("headers_well_known", true, "");
                return;
            }
        }
        reportResult("headers_well_known", false, "header not found");
    } else |_| {
        reportResult("headers_well_known", false, "receive failed");
    }
}

pub fn testHeadersCaseInsensitive(allocator: std.mem.Allocator) void {
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
        reportResult("headers_case_insensitive", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.headers.case") catch {
        reportResult("headers_case_insensitive", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    const hdrs = [_]headers.Entry{
        .{ .key = "Content-Type", .value = "application/json" },
    };
    client.publishWithHeaders("test.headers.case", &hdrs, "{}") catch {
        reportResult("headers_case_insensitive", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.headers == null) {
            reportResult("headers_case_insensitive", false, "no headers");
            return;
        }
        var parsed = headers.parse(allocator, msg.headers.?);
        defer parsed.deinit();
        if (parsed.err != null) {
            reportResult("headers_case_insensitive", false, "parse error");
            return;
        }
        // Lookup with different case
        const val1 = parsed.get("content-type");
        const val2 = parsed.get("CONTENT-TYPE");
        const val3 = parsed.get("Content-Type");
        if (val1 != null and val2 != null and val3 != null) {
            reportResult("headers_case_insensitive", true, "");
            return;
        }
        reportResult("headers_case_insensitive", false, "case mismatch");
    } else |_| {
        reportResult("headers_case_insensitive", false, "receive failed");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testHeadersPublishSingle(allocator);
    testHeadersPublishMultiple(allocator);
    testHeadersPublishEmptyPayload(allocator);
    testHeadersPublishRequest(allocator);
    testHeadersRequestReply(allocator);
    testHeadersRequestTimeout(allocator);
    testHeadersCrossClient(allocator);
    testHeadersManyEntries(allocator);
    testHeadersLargeValues(allocator);
    testHeadersSpecialChars(allocator);
    testHeadersBinaryPayload(allocator);
    testHeadersWellKnown(allocator);
    testHeadersCaseInsensitive(allocator);
}
