//! Microservices Integration Tests

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.micro_port;
const ServerManager = utils.ServerManager;
const TestServer = utils.server_manager.TestServer;

const ParseOpts: std.json.ParseOptions = .{
    .ignore_unknown_fields = true,
};

const EchoHandler = struct {
    pub fn onRequest(_: *@This(), req: *nats.micro.Request) void {
        req.respond(req.data()) catch {};
    }
};

const CountingHandler = struct {
    count: *u32,

    pub fn onRequest(self: *@This(), req: *nats.micro.Request) void {
        self.count.* += 1;
        req.respond(req.data()) catch {};
    }
};

fn echoFn(req: *nats.micro.Request) void {
    req.respond(req.data()) catch {};
}

const ErrorHandler = struct {
    pub fn onRequest(_: *@This(), req: *nats.micro.Request) void {
        req.respondError(503, "unavailable", "detail") catch {};
    }
};

pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    _ = manager;

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = TestServer.start(allocator, io.io(), .{
        .port = test_port,
    }) catch {
        reportResult("micro_server", false, "start server failed");
        return;
    };
    defer server.deinit(io.io());

    testMicroBasicRequest(allocator);
    testMicroBasicRequestHandlerFn(allocator);
    testMicroRespondError(allocator);
    testMicroRespondJson(allocator);
    testMicroPing(allocator);
    testMicroPingById(allocator);
    testMicroInfo(allocator);
    testMicroStats(allocator);
    testMicroStatsErrorCount(allocator);
    testMicroStatsStartedRfc3339(allocator);
    testMicroReset(allocator);
    testMicroNoQueueFanout(allocator);
    testMicroQueueGroupLoadBalance(allocator);
    testMicroCustomServiceQueueGroup(allocator);
    testMicroEndpointQueueGroupOverride(allocator);
    testMicroNestedGroups(allocator);
    testMicroMultipleEndpoints(allocator);
    testMicroMetadataInInfo(allocator);
    testMicroServiceIdUnique(allocator);
    testMicroStop(allocator);
    testMicroStopIdempotent(allocator);
    testMicroDrainOnStop(allocator);
    testMicroReconnect(allocator, &server);
}

fn waitForConnected(
    io: std.Io,
    client: *nats.Client,
    timeout_ms: u32,
) bool {
    var waited: u32 = 0;
    while (waited < timeout_ms) : (waited += 25) {
        if (client.isConnected()) return true;
        io.sleep(.fromMilliseconds(25), .awake) catch {};
    }
    return client.isConnected();
}

fn testMicroBasicRequest(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroBasicRequest", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "echo-basic",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "echo.basic",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroBasicRequest", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("echo.basic", "hello", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.data, "hello")) {
            reportResult("testMicroBasicRequest", true, "");
        } else {
            reportResult("testMicroBasicRequest", false, "wrong payload");
        }
    } else {
        reportResult("testMicroBasicRequest", false, "no response");
    }
}

fn testMicroBasicRequestHandlerFn(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroBasicRequestHandlerFn", false, "connect failed");
        return;
    };
    defer client.deinit();

    const service = nats.micro.addService(client, .{
        .name = "echo-fn",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "echo.fn",
            .handler = nats.micro.Handler.fromFn(echoFn),
        },
    }) catch {
        reportResult("testMicroBasicRequestHandlerFn", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("echo.fn", "hello", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.data, "hello")) {
            reportResult("testMicroBasicRequestHandlerFn", true, "");
        } else {
            reportResult("testMicroBasicRequestHandlerFn", false, "wrong payload");
        }
    } else {
        reportResult("testMicroBasicRequestHandlerFn", false, "no response");
    }
}

fn testMicroRespondError(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroRespondError", false, "connect failed");
        return;
    };
    defer client.deinit();

    var handler = ErrorHandler{};
    const service = nats.micro.addService(client, .{
        .name = "err-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "err.svc",
            .handler = nats.micro.Handler.init(ErrorHandler, &handler),
        },
    }) catch {
        reportResult("testMicroRespondError", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("err.svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (m.headers) |raw_headers| {
            var parsed = nats.protocol.headers.parse(allocator, raw_headers);
            defer parsed.deinit();
            const err_hdr = parsed.get("Nats-Service-Error");
            const code_hdr = parsed.get("Nats-Service-Error-Code");
            if (err_hdr != null and code_hdr != null and
                std.mem.eql(u8, err_hdr.?, "unavailable") and
                std.mem.eql(u8, code_hdr.?, "503") and
                std.mem.eql(u8, m.data, "detail"))
            {
                reportResult("testMicroRespondError", true, "");
                return;
            }
        }
        reportResult("testMicroRespondError", false, "missing error headers");
    } else {
        reportResult("testMicroRespondError", false, "no response");
    }
}

fn testMicroPing(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroPing", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "ping-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "ping.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroPing", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.PING.ping-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Ping,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroPing", false, "json parse failed");
            return;
        };
        defer parsed.deinit();

        if (std.mem.eql(u8, parsed.value.name, "ping-svc") and
            std.mem.eql(u8, parsed.value.version, "1.0.0") and
            std.mem.eql(u8, parsed.value.type, nats.micro.protocol.Type.ping))
        {
            reportResult("testMicroPing", true, "");
        } else {
            reportResult("testMicroPing", false, "wrong ping response");
        }
    } else {
        reportResult("testMicroPing", false, "no response");
    }
}

fn testMicroInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroInfo", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "info-svc",
        .version = "1.0.0",
        .metadata = &.{.{ .key = "team", .value = "core" }},
        .endpoint = .{
            .subject = "info.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
            .metadata = &.{.{ .key = "kind", .value = "echo" }},
        },
    }) catch {
        reportResult("testMicroInfo", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.INFO.info-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Info,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroInfo", false, "json parse failed");
            return;
        };
        defer parsed.deinit();

        if (parsed.value.endpoints.len == 1 and
            std.mem.eql(u8, parsed.value.endpoints[0].subject, "info.echo"))
        {
            reportResult("testMicroInfo", true, "");
        } else {
            reportResult("testMicroInfo", false, "bad info payload");
        }
    } else {
        reportResult("testMicroInfo", false, "no response");
    }
}

fn testMicroStats(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroStats", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "stats-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "stats.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroStats", false, "addService failed");
        return;
    };
    defer service.deinit();

    for (0..3) |_| {
        const msg = client.request("stats.echo", "x", 1000) catch null;
        if (msg) |m| m.deinit();
    }

    const stats_msg = client.request("$SRV.STATS.stats-svc", "", 1000) catch null;
    if (stats_msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.StatsResponse,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroStats", false, "json parse failed");
            return;
        };
        defer parsed.deinit();

        if (parsed.value.endpoints.len == 1 and
            parsed.value.endpoints[0].num_requests == 3 and
            parsed.value.endpoints[0].processing_time > 0)
        {
            reportResult("testMicroStats", true, "");
        } else {
            reportResult("testMicroStats", false, "bad stats payload");
        }
    } else {
        reportResult("testMicroStats", false, "no response");
    }
}

fn testMicroNoQueueFanout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io_a = utils.newIo(allocator);
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroNoQueueFanout", false, "client_a failed");
        return;
    };
    defer client_a.deinit();

    const io_b = utils.newIo(allocator);
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroNoQueueFanout", false, "client_b failed");
        return;
    };
    defer client_b.deinit();

    var count_a: u32 = 0;
    var count_b: u32 = 0;
    var handler_a = CountingHandler{ .count = &count_a };
    var handler_b = CountingHandler{ .count = &count_b };

    const service_a = nats.micro.addService(client_a, .{
        .name = "fanout-svc",
        .version = "1.0.0",
        .queue_policy = .no_queue,
        .endpoint = .{
            .subject = "fanout.echo",
            .handler = nats.micro.Handler.init(CountingHandler, &handler_a),
        },
    }) catch {
        reportResult("testMicroNoQueueFanout", false, "service_a failed");
        return;
    };
    defer service_a.deinit();

    const service_b = nats.micro.addService(client_b, .{
        .name = "fanout-svc",
        .version = "1.0.0",
        .queue_policy = .no_queue,
        .endpoint = .{
            .subject = "fanout.echo",
            .handler = nats.micro.Handler.init(CountingHandler, &handler_b),
        },
    }) catch {
        reportResult("testMicroNoQueueFanout", false, "service_b failed");
        return;
    };
    defer service_b.deinit();

    for (0..5) |_| {
        const msg = client_a.request("fanout.echo", "x", 1000) catch null;
        if (msg) |m| m.deinit();
    }

    io_a.io().sleep(.fromMilliseconds(200), .awake) catch {};

    if (count_a == 5 and count_b == 5) {
        reportResult("testMicroNoQueueFanout", true, "");
    } else {
        reportResult("testMicroNoQueueFanout", false, "fanout count mismatch");
    }
}

fn testMicroStop(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{ .reconnect = false }) catch {
        reportResult("testMicroStop", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "stop-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "stop.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroStop", false, "addService failed");
        return;
    };
    defer service.deinit();

    service.stop(null) catch {
        reportResult("testMicroStop", false, "stop failed");
        return;
    };

    const msg = client.request("stop.echo", "x", 200) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (m.isNoResponders()) {
            reportResult("testMicroStop", true, "");
        } else {
            reportResult("testMicroStop", false, "request still answered");
        }
    } else {
        reportResult("testMicroStop", true, "");
    }
}

fn testMicroReconnect(
    allocator: std.mem.Allocator,
    server: *TestServer,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = true,
        .max_reconnect_attempts = 10,
        .reconnect_wait_ms = 100,
    }) catch {
        reportResult("testMicroReconnect", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "reconnect-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "reconnect.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroReconnect", false, "addService failed");
        return;
    };
    defer service.deinit();

    server.stop(io.io());
    client.forceReconnect() catch {};
    server.* = TestServer.start(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("testMicroReconnect", false, "restart failed");
        return;
    };
    if (!waitForConnected(io.io(), client, 5000)) {
        reportResult("testMicroReconnect", false, "reconnect timeout");
        return;
    }

    const msg = client.request("reconnect.echo", "again", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.data, "again")) {
            reportResult("testMicroReconnect", true, "");
        } else {
            reportResult("testMicroReconnect", false, "wrong payload");
        }
    } else {
        reportResult("testMicroReconnect", false, "no response");
    }
}

// ---------- New tests added to close v1 coverage gaps ----------

const JsonResp = struct {
    answer: u32,
    note: []const u8,
};

const JsonHandler = struct {
    pub fn onRequest(_: *@This(), req: *nats.micro.Request) void {
        const value = JsonResp{ .answer = 42, .note = "ok" };
        req.respondJson(value) catch {};
    }
};

fn testMicroRespondJson(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroRespondJson", false, "connect failed");
        return;
    };
    defer client.deinit();

    var handler = JsonHandler{};
    const service = nats.micro.addService(client, .{
        .name = "json-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "json.svc",
            .handler = nats.micro.Handler.init(JsonHandler, &handler),
        },
    }) catch {
        reportResult("testMicroRespondJson", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("json.svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            JsonResp,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroRespondJson", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (parsed.value.answer == 42 and
            std.mem.eql(u8, parsed.value.note, "ok"))
        {
            reportResult("testMicroRespondJson", true, "");
        } else {
            reportResult("testMicroRespondJson", false, "wrong fields");
        }
    } else {
        reportResult("testMicroRespondJson", false, "no response");
    }
}

fn testMicroPingById(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroPingById", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "ping-id-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "ping.id.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroPingById", false, "addService failed");
        return;
    };
    defer service.deinit();

    var subject_buf: [128]u8 = undefined;
    const subject = std.fmt.bufPrint(
        &subject_buf,
        "$SRV.PING.ping-id-svc.{s}",
        .{service.id},
    ) catch {
        reportResult("testMicroPingById", false, "subject format failed");
        return;
    };

    const msg = client.request(subject, "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Ping,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroPingById", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (std.mem.eql(u8, parsed.value.id, service.id)) {
            reportResult("testMicroPingById", true, "");
        } else {
            reportResult("testMicroPingById", false, "id mismatch");
        }
    } else {
        reportResult("testMicroPingById", false, "no response");
    }
}

fn testMicroStatsErrorCount(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroStatsErrorCount", false, "connect failed");
        return;
    };
    defer client.deinit();

    var handler = ErrorHandler{};
    const service = nats.micro.addService(client, .{
        .name = "stats-err-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "stats.err.svc",
            .handler = nats.micro.Handler.init(ErrorHandler, &handler),
        },
    }) catch {
        reportResult("testMicroStatsErrorCount", false, "addService failed");
        return;
    };
    defer service.deinit();

    for (0..3) |_| {
        const m = client.request("stats.err.svc", "", 1000) catch null;
        if (m) |x| x.deinit();
    }

    const stats_msg = client.request(
        "$SRV.STATS.stats-err-svc",
        "",
        1000,
    ) catch null;
    if (stats_msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.StatsResponse,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroStatsErrorCount", false, "parse failed");
            return;
        };
        defer parsed.deinit();

        if (parsed.value.endpoints.len != 1) {
            reportResult("testMicroStatsErrorCount", false, "endpoint count");
            return;
        }
        const ep = parsed.value.endpoints[0];
        if (ep.num_requests == 3 and ep.num_errors == 3 and
            ep.last_error != null and ep.last_error.?.code == 503)
        {
            reportResult("testMicroStatsErrorCount", true, "");
        } else {
            reportResult("testMicroStatsErrorCount", false, "wrong counts");
        }
    } else {
        reportResult("testMicroStatsErrorCount", false, "no stats");
    }
}

fn isAsciiDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

fn looksLikeRfc3339(s: []const u8) bool {
    // Expected: YYYY-MM-DDTHH:MM:SS.mmmZ -> 24 chars
    if (s.len != 24) return false;
    if (s[4] != '-' or s[7] != '-' or s[10] != 'T' or
        s[13] != ':' or s[16] != ':' or s[19] != '.' or s[23] != 'Z')
        return false;
    for (s[0..4]) |c| if (!isAsciiDigit(c)) return false;
    for (s[5..7]) |c| if (!isAsciiDigit(c)) return false;
    for (s[8..10]) |c| if (!isAsciiDigit(c)) return false;
    for (s[11..13]) |c| if (!isAsciiDigit(c)) return false;
    for (s[14..16]) |c| if (!isAsciiDigit(c)) return false;
    for (s[17..19]) |c| if (!isAsciiDigit(c)) return false;
    for (s[20..23]) |c| if (!isAsciiDigit(c)) return false;
    return true;
}

fn testMicroStatsStartedRfc3339(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroStatsStartedRfc3339", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "started-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "started.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroStatsStartedRfc3339", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.STATS.started-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.StatsResponse,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroStatsStartedRfc3339", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (looksLikeRfc3339(parsed.value.started)) {
            reportResult("testMicroStatsStartedRfc3339", true, "");
        } else {
            reportResult("testMicroStatsStartedRfc3339", false, "bad format");
        }
    } else {
        reportResult("testMicroStatsStartedRfc3339", false, "no response");
    }
}

fn testMicroReset(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroReset", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "reset-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "reset.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroReset", false, "addService failed");
        return;
    };
    defer service.deinit();

    for (0..3) |_| {
        const m = client.request("reset.echo", "x", 1000) catch null;
        if (m) |x| x.deinit();
    }

    service.reset();

    const stats_msg = client.request("$SRV.STATS.reset-svc", "", 1000) catch null;
    if (stats_msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.StatsResponse,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroReset", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (parsed.value.endpoints.len == 1 and
            parsed.value.endpoints[0].num_requests == 0 and
            parsed.value.endpoints[0].processing_time == 0)
        {
            reportResult("testMicroReset", true, "");
        } else {
            reportResult("testMicroReset", false, "stats not zero");
        }
    } else {
        reportResult("testMicroReset", false, "no stats");
    }
}

fn testMicroQueueGroupLoadBalance(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io_a = utils.newIo(allocator);
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroQueueGroupLoadBalance", false, "client_a failed");
        return;
    };
    defer client_a.deinit();

    const io_b = utils.newIo(allocator);
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroQueueGroupLoadBalance", false, "client_b failed");
        return;
    };
    defer client_b.deinit();

    var count_a: u32 = 0;
    var count_b: u32 = 0;
    var handler_a = CountingHandler{ .count = &count_a };
    var handler_b = CountingHandler{ .count = &count_b };

    const svc_a = nats.micro.addService(client_a, .{
        .name = "lb-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "lb.echo",
            .handler = nats.micro.Handler.init(CountingHandler, &handler_a),
        },
    }) catch {
        reportResult("testMicroQueueGroupLoadBalance", false, "svc_a failed");
        return;
    };
    defer svc_a.deinit();

    const svc_b = nats.micro.addService(client_b, .{
        .name = "lb-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "lb.echo",
            .handler = nats.micro.Handler.init(CountingHandler, &handler_b),
        },
    }) catch {
        reportResult("testMicroQueueGroupLoadBalance", false, "svc_b failed");
        return;
    };
    defer svc_b.deinit();

    const N = 20;
    for (0..N) |_| {
        const m = client_a.request("lb.echo", "x", 1000) catch null;
        if (m) |x| x.deinit();
    }

    io_a.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Both should receive >0 and combined == N (queue group → load split).
    if (count_a + count_b == N and count_a > 0 and count_b > 0) {
        reportResult("testMicroQueueGroupLoadBalance", true, "");
    } else {
        reportResult("testMicroQueueGroupLoadBalance", false, "unbalanced");
    }
}

fn testMicroCustomServiceQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroCustomServiceQueueGroup", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "custom-q-svc",
        .version = "1.0.0",
        .queue_policy = .{ .queue = "svc-q" },
        .endpoint = .{
            .subject = "custom.q.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroCustomServiceQueueGroup", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.INFO.custom-q-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Info,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroCustomServiceQueueGroup", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (parsed.value.endpoints.len == 1 and
            parsed.value.endpoints[0].queue_group != null and
            std.mem.eql(u8, parsed.value.endpoints[0].queue_group.?, "svc-q"))
        {
            reportResult("testMicroCustomServiceQueueGroup", true, "");
        } else {
            reportResult("testMicroCustomServiceQueueGroup", false, "wrong queue");
        }
    } else {
        reportResult("testMicroCustomServiceQueueGroup", false, "no info");
    }
}

fn testMicroEndpointQueueGroupOverride(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroEndpointQueueGroupOverride", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "ep-override-svc",
        .version = "1.0.0",
        .queue_policy = .{ .queue = "svc-q" },
        .endpoint = .{
            .subject = "ep.override.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
            .queue_policy = .{ .queue = "ep-q" },
        },
    }) catch {
        reportResult("testMicroEndpointQueueGroupOverride", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.INFO.ep-override-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Info,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroEndpointQueueGroupOverride", false, "parse failed");
            return;
        };
        defer parsed.deinit();
        if (parsed.value.endpoints.len == 1 and
            parsed.value.endpoints[0].queue_group != null and
            std.mem.eql(u8, parsed.value.endpoints[0].queue_group.?, "ep-q"))
        {
            reportResult("testMicroEndpointQueueGroupOverride", true, "");
        } else {
            reportResult("testMicroEndpointQueueGroupOverride", false, "wrong queue");
        }
    } else {
        reportResult("testMicroEndpointQueueGroupOverride", false, "no info");
    }
}

fn testMicroNestedGroups(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroNestedGroups", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "nested-svc",
        .version = "1.0.0",
    }) catch {
        reportResult("testMicroNestedGroups", false, "addService failed");
        return;
    };
    defer service.deinit();

    var v1 = service.addGroup("v1") catch {
        reportResult("testMicroNestedGroups", false, "addGroup v1 failed");
        return;
    };
    var users = v1.group("users") catch {
        reportResult("testMicroNestedGroups", false, "nested group failed");
        return;
    };
    _ = users.addEndpoint(.{
        .subject = "get",
        .handler = nats.micro.Handler.init(EchoHandler, &echo),
    }) catch {
        reportResult("testMicroNestedGroups", false, "addEndpoint failed");
        return;
    };

    const msg = client.request("v1.users.get", "hello", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.data, "hello")) {
            reportResult("testMicroNestedGroups", true, "");
        } else {
            reportResult("testMicroNestedGroups", false, "wrong payload");
        }
    } else {
        reportResult("testMicroNestedGroups", false, "no response");
    }
}

fn testMicroMultipleEndpoints(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroMultipleEndpoints", false, "connect failed");
        return;
    };
    defer client.deinit();

    var count_a: u32 = 0;
    var count_b: u32 = 0;
    var handler_a = CountingHandler{ .count = &count_a };
    var handler_b = CountingHandler{ .count = &count_b };

    const service = nats.micro.addService(client, .{
        .name = "multi-ep-svc",
        .version = "1.0.0",
    }) catch {
        reportResult("testMicroMultipleEndpoints", false, "addService failed");
        return;
    };
    defer service.deinit();

    _ = service.addEndpoint(.{
        .subject = "multi.a",
        .handler = nats.micro.Handler.init(CountingHandler, &handler_a),
    }) catch {
        reportResult("testMicroMultipleEndpoints", false, "addEndpoint a failed");
        return;
    };
    _ = service.addEndpoint(.{
        .subject = "multi.b",
        .handler = nats.micro.Handler.init(CountingHandler, &handler_b),
    }) catch {
        reportResult("testMicroMultipleEndpoints", false, "addEndpoint b failed");
        return;
    };

    for (0..3) |_| {
        const m = client.request("multi.a", "x", 1000) catch null;
        if (m) |x| x.deinit();
    }
    const m_only = client.request("multi.b", "y", 1000) catch null;
    if (m_only) |x| x.deinit();

    if (count_a == 3 and count_b == 1) {
        reportResult("testMicroMultipleEndpoints", true, "");
    } else {
        reportResult("testMicroMultipleEndpoints", false, "wrong split");
    }
}

fn testMicroMetadataInInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroMetadataInInfo", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "meta-svc",
        .version = "1.0.0",
        .metadata = &.{
            .{ .key = "team", .value = "core" },
            .{ .key = "env", .value = "test" },
        },
        .endpoint = .{
            .subject = "meta.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
            .metadata = &.{
                .{ .key = "kind", .value = "echo" },
            },
        },
    }) catch {
        reportResult("testMicroMetadataInInfo", false, "addService failed");
        return;
    };
    defer service.deinit();

    const msg = client.request("$SRV.INFO.meta-svc", "", 1000) catch null;
    if (msg) |m| {
        defer m.deinit();
        var parsed = std.json.parseFromSlice(
            nats.micro.protocol.Info,
            allocator,
            m.data,
            ParseOpts,
        ) catch {
            reportResult("testMicroMetadataInInfo", false, "parse failed");
            return;
        };
        defer parsed.deinit();

        const svc_md = parsed.value.metadata orelse {
            reportResult("testMicroMetadataInInfo", false, "no svc metadata");
            return;
        };
        if (svc_md.len != 2) {
            reportResult("testMicroMetadataInInfo", false, "svc md count");
            return;
        }
        if (parsed.value.endpoints.len != 1) {
            reportResult("testMicroMetadataInInfo", false, "ep count");
            return;
        }
        const ep_md = parsed.value.endpoints[0].metadata orelse {
            reportResult("testMicroMetadataInInfo", false, "no ep metadata");
            return;
        };
        if (ep_md.len != 1 or
            !std.mem.eql(u8, ep_md[0].key, "kind") or
            !std.mem.eql(u8, ep_md[0].value, "echo"))
        {
            reportResult("testMicroMetadataInInfo", false, "ep md wrong");
            return;
        }
        reportResult("testMicroMetadataInInfo", true, "");
    } else {
        reportResult("testMicroMetadataInInfo", false, "no info");
    }
}

fn testMicroServiceIdUnique(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroServiceIdUnique", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const svc_a = nats.micro.addService(client, .{
        .name = "uniq-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "uniq.a",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroServiceIdUnique", false, "svc_a failed");
        return;
    };
    defer svc_a.deinit();

    const svc_b = nats.micro.addService(client, .{
        .name = "uniq-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "uniq.b",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroServiceIdUnique", false, "svc_b failed");
        return;
    };
    defer svc_b.deinit();

    if (svc_a.id.len == 16 and svc_b.id.len == 16 and
        !std.mem.eql(u8, svc_a.id, svc_b.id))
    {
        reportResult("testMicroServiceIdUnique", true, "");
    } else {
        reportResult("testMicroServiceIdUnique", false, "id collision or wrong len");
    }
}

fn testMicroStopIdempotent(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroStopIdempotent", false, "connect failed");
        return;
    };
    defer client.deinit();

    var echo = EchoHandler{};
    const service = nats.micro.addService(client, .{
        .name = "stop-twice-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "stop.twice.echo",
            .handler = nats.micro.Handler.init(EchoHandler, &echo),
        },
    }) catch {
        reportResult("testMicroStopIdempotent", false, "addService failed");
        return;
    };
    defer service.deinit();

    service.stop(null) catch {
        reportResult("testMicroStopIdempotent", false, "first stop failed");
        return;
    };
    service.stop(null) catch {
        reportResult("testMicroStopIdempotent", false, "second stop failed");
        return;
    };
    if (service.stopped()) {
        reportResult("testMicroStopIdempotent", true, "");
    } else {
        reportResult("testMicroStopIdempotent", false, "not stopped");
    }
}

const BlockingEcho = struct {
    started: *std.atomic.Value(bool),
    release: *std.atomic.Value(bool),
    finished: *std.atomic.Value(bool),

    pub fn onRequest(self: *@This(), req: *nats.micro.Request) void {
        self.started.store(true, .release);
        while (!self.release.load(.acquire)) {
            req.client.io.sleep(.fromMilliseconds(1), .awake) catch {};
        }
        req.respond("done") catch {};
        self.finished.store(true, .release);
    }
};

const StopState = struct {
    done: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    err: ?anyerror = null,
};

fn stopService(
    service: *nats.micro.Service,
    state: *StopState,
) void {
    service.stop(null) catch |err| {
        state.err = err;
    };
    state.done.store(true, .release);
}

fn drainRequester(
    client: *nats.Client,
    out: *?nats.Client.Message,
) void {
    out.* = client.request("drain.echo", "x", 5000) catch null;
}

fn testMicroDrainOnStop(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);
    const service_io = utils.newIo(allocator);
    defer service_io.deinit();

    const service_client = nats.Client.connect(allocator, service_io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroDrainOnStop", false, "connect failed");
        return;
    };
    defer service_client.deinit();

    var started = std.atomic.Value(bool).init(false);
    var release = std.atomic.Value(bool).init(false);
    var finished = std.atomic.Value(bool).init(false);
    var blocking = BlockingEcho{
        .started = &started,
        .release = &release,
        .finished = &finished,
    };

    const service = nats.micro.addService(service_client, .{
        .name = "drain-svc",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "drain.echo",
            .handler = nats.micro.Handler.init(BlockingEcho, &blocking),
        },
    }) catch {
        reportResult("testMicroDrainOnStop", false, "addService failed");
        return;
    };
    defer service.deinit();

    const requester_io = utils.newIo(allocator);
    defer requester_io.deinit();

    const requester = nats.Client.connect(allocator, requester_io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("testMicroDrainOnStop", false, "requester failed");
        return;
    };
    defer requester.deinit();

    var resp: ?nats.Client.Message = null;
    var fut = requester_io.io().async(drainRequester, .{ requester, &resp });

    // Wait until the handler has actually entered.
    var spins: u32 = 0;
    while (!started.load(.acquire) and spins < 200) {
        service_io.io().sleep(.fromMilliseconds(5), .awake) catch {};
        spins += 1;
    }
    if (!started.load(.acquire)) {
        _ = fut.cancel(requester_io.io());
        reportResult("testMicroDrainOnStop", false, "handler never entered");
        return;
    }

    var stop_state = StopState{};
    var stop_fut = service_io.io().concurrent(stopService, .{ service, &stop_state }) catch
        service_io.io().async(stopService, .{ service, &stop_state });

    spins = 0;
    while (!service.stopping.load(.acquire) and spins < 200) {
        service_io.io().sleep(.fromMilliseconds(1), .awake) catch {};
        spins += 1;
    }
    if (!service.stopping.load(.acquire)) {
        release.store(true, .release);
        stop_fut.await(service_io.io());
        fut.await(requester_io.io());
        defer if (resp) |m| m.deinit();
        reportResult("testMicroDrainOnStop", false, "stop never started");
        return;
    }

    service_io.io().sleep(.fromMilliseconds(50), .awake) catch {};
    if (stop_state.done.load(.acquire)) {
        release.store(true, .release);
        stop_fut.await(service_io.io());
        fut.await(requester_io.io());
        defer if (resp) |m| m.deinit();
        reportResult("testMicroDrainOnStop", false, "stop returned early");
        return;
    }

    release.store(true, .release);
    stop_fut.await(service_io.io());
    if (stop_state.err != null) {
        _ = fut.cancel(requester_io.io());
        reportResult("testMicroDrainOnStop", false, "stop failed");
        return;
    }

    fut.await(requester_io.io());
    defer if (resp) |m| m.deinit();

    if (resp != null and finished.load(.acquire) and service.stopped()) {
        reportResult("testMicroDrainOnStop", true, "");
    } else {
        reportResult("testMicroDrainOnStop", false, "drain incomplete");
    }
}
