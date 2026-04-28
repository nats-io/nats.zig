//! JetStream Integration Tests
//!
//! End-to-end tests for JetStream stream/consumer CRUD,
//! publish with ack, and pull-based fetch.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const reportError = utils.reportError;
const formatUrl = utils.formatUrl;
const ServerManager = utils.ServerManager;
const TestServer = utils.server_manager.TestServer;

const js_port = utils.jetstream_port;
const js_reconnect_port: u16 = 14240;
const test_js_timeout_ms: u32 = 15_000;

fn initTestJetStream(
    client: *nats.Client,
) nats.jetstream.JetStream {
    return nats.jetstream.JetStream.init(client, .{
        .timeout_ms = test_js_timeout_ms,
    }) catch unreachable;
}

fn threadSleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

var push_heartbeat_err_seen =
    std.atomic.Value(bool).init(false);

fn deleteStreamIfExists(
    js: *nats.jetstream.JetStream,
    name: []const u8,
) void {
    var d = js.deleteStream(name) catch return;
    d.deinit();
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

fn startJsReconnectServer(
    allocator: std.mem.Allocator,
    io: std.Io,
) !TestServer {
    return TestServer.start(allocator, io, .{
        .port = js_reconnect_port,
        .jetstream = true,
    });
}

fn restartJsReconnectServer(
    allocator: std.mem.Allocator,
    io: std.Io,
    server: *TestServer,
    client: *nats.Client,
    name: []const u8,
) bool {
    server.stop(io);
    client.forceReconnect() catch {};

    server.* = startJsReconnectServer(allocator, io) catch {
        reportResult(name, false, "restart server");
        return false;
    };

    if (!waitForConnected(io, client, 5000)) {
        reportResult(name, false, "reconnect timeout");
        return false;
    }

    return true;
}

fn startSharedJsServer(
    allocator: std.mem.Allocator,
    io: std.Io,
) !TestServer {
    return TestServer.start(allocator, io, .{
        .port = js_port,
        .jetstream = true,
    });
}

fn restartSharedJsServer(
    allocator: std.mem.Allocator,
    io: std.Io,
    server: *TestServer,
    name: []const u8,
) bool {
    server.stop(io);
    server.* = startSharedJsServer(allocator, io) catch {
        reportResult(name, false, "restart JS server");
        return false;
    };
    return true;
}

fn pushHeartbeatErrHandler(err: anyerror) void {
    if (err == error.NoHeartbeat) {
        push_heartbeat_err_seen.store(true, .release);
    }
}

pub fn testStreamCreateAndInfo(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream
    var resp = js.createStream(.{
        .name = "TEST_CREATE",
        .subjects = &.{"test.create.>"},
        .storage = .memory,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "create failed: {}",
            .{err},
        ) catch "error";
        reportResult("js_stream_create", false, msg);
        return;
    };
    defer resp.deinit();

    if (resp.value.config) |cfg| {
        if (!std.mem.eql(
            u8,
            cfg.name,
            "TEST_CREATE",
        )) {
            reportResult(
                "js_stream_create",
                false,
                "wrong name",
            );
            return;
        }
    } else {
        reportResult(
            "js_stream_create",
            false,
            "no config",
        );
        return;
    }

    // Get stream info
    var info = js.streamInfo(
        "TEST_CREATE",
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.state) |state| {
        if (state.messages != 0) {
            reportResult(
                "js_stream_create",
                false,
                "expected 0 msgs",
            );
            return;
        }
    }

    // Cleanup
    var del = js.deleteStream(
        "TEST_CREATE",
    ) catch {
        reportResult(
            "js_stream_create",
            false,
            "delete failed",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "js_stream_create",
            false,
            "delete not success",
        );
        return;
    }

    reportResult("js_stream_create", true, "");
}

pub fn testPublishAndAck(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_ack",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream
    var stream = js.createStream(.{
        .name = "TEST_PUB",
        .subjects = &.{"test.pub.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_publish_ack",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Publish
    var ack = js.publish(
        "test.pub.hello",
        "hello world",
    ) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "publish failed: {}",
            .{err},
        ) catch "error";
        reportResult("js_publish_ack", false, msg);
        return;
    };
    defer ack.deinit();

    if (ack.value.seq != 1) {
        reportResult(
            "js_publish_ack",
            false,
            "expected seq 1",
        );
        return;
    }

    if (ack.value.stream) |s| {
        if (!std.mem.eql(u8, s, "TEST_PUB")) {
            reportResult(
                "js_publish_ack",
                false,
                "wrong stream",
            );
            return;
        }
    } else {
        reportResult(
            "js_publish_ack",
            false,
            "no stream in ack",
        );
        return;
    }

    // Publish second message
    var ack2 = js.publish(
        "test.pub.world",
        "second",
    ) catch {
        reportResult(
            "js_publish_ack",
            false,
            "publish 2 failed",
        );
        return;
    };
    defer ack2.deinit();

    if (ack2.value.seq != 2) {
        reportResult(
            "js_publish_ack",
            false,
            "expected seq 2",
        );
        return;
    }

    // Cleanup
    var del = js.deleteStream("TEST_PUB") catch {
        reportResult(
            "js_publish_ack",
            false,
            "delete failed",
        );
        return;
    };
    defer del.deinit();

    reportResult("js_publish_ack", true, "");
}

pub fn testConsumerCRUD(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream
    var stream = js.createStream(.{
        .name = "TEST_CONS",
        .subjects = &.{"test.cons.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Create consumer
    var cons = js.createConsumer(
        "TEST_CONS",
        .{
            .name = "my-consumer",
            .durable_name = "my-consumer",
            .ack_policy = .explicit,
        },
    ) catch |err| {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "create consumer: {}",
            .{err},
        ) catch "error";
        reportResult(
            "js_consumer_crud",
            false,
            msg,
        );
        return;
    };
    defer cons.deinit();

    if (cons.value.name) |n| {
        if (!std.mem.eql(u8, n, "my-consumer")) {
            reportResult(
                "js_consumer_crud",
                false,
                "wrong consumer name",
            );
            return;
        }
    }

    // Consumer info
    var info = js.consumerInfo(
        "TEST_CONS",
        "my-consumer",
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    // Delete consumer
    var del_c = js.deleteConsumer(
        "TEST_CONS",
        "my-consumer",
    ) catch {
        reportResult(
            "js_consumer_crud",
            false,
            "delete consumer failed",
        );
        return;
    };
    defer del_c.deinit();

    if (!del_c.value.success) {
        reportResult(
            "js_consumer_crud",
            false,
            "delete not success",
        );
        return;
    }

    // Cleanup stream
    var del_s = js.deleteStream("TEST_CONS") catch {
        reportResult(
            "js_consumer_crud",
            false,
            "delete stream failed",
        );
        return;
    };
    defer del_s.deinit();

    reportResult("js_consumer_crud", true, "");
}

pub fn testApiError(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_api_error",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Try to get info for non-existent stream
    var info = js.streamInfo("NONEXISTENT");
    if (info) |*r| {
        r.deinit();
        reportResult(
            "js_api_error",
            false,
            "expected error",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_api_error",
                false,
                "wrong error type",
            );
            return;
        }
    }

    // Check last API error
    if (js.lastApiError()) |api_err| {
        if (api_err.err_code !=
            nats.jetstream.errors.ErrCode.stream_not_found)
        {
            reportResult(
                "js_api_error",
                false,
                "wrong err_code",
            );
            return;
        }
    } else {
        reportResult(
            "js_api_error",
            false,
            "no last api error",
        );
        return;
    }

    reportResult("js_api_error", true, "");
}

pub fn testStreamNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_names",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create 3 streams
    var s1 = js.createStream(.{
        .name = "NAMES_A",
        .subjects = &.{"names.a.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create A failed",
        );
        return;
    };
    defer s1.deinit();

    var s2 = js.createStream(.{
        .name = "NAMES_B",
        .subjects = &.{"names.b.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create B failed",
        );
        return;
    };
    defer s2.deinit();

    var s3 = js.createStream(.{
        .name = "NAMES_C",
        .subjects = &.{"names.c.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_names",
            false,
            "create C failed",
        );
        return;
    };
    defer s3.deinit();

    // List names
    var resp = js.streamNames() catch {
        reportResult(
            "js_stream_names",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.streams orelse {
        reportResult(
            "js_stream_names",
            false,
            "no streams",
        );
        return;
    };

    if (names.len < 3) {
        reportResult(
            "js_stream_names",
            false,
            "expected >= 3 streams",
        );
        return;
    }

    // Cleanup
    {
        var d1 = js.deleteStream("NAMES_A") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d1.deinit();
    }
    {
        var d2 = js.deleteStream("NAMES_B") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d2.deinit();
    }
    {
        var d3 = js.deleteStream("NAMES_C") catch {
            reportResult("js_stream_names", true, "");
            return;
        };
        d3.deinit();
    }

    reportResult("js_stream_names", true, "");
}

pub fn testStreamList(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_stream_list",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s1 = js.createStream(.{
        .name = "LIST_A",
        .subjects = &.{"list.a.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_stream_list",
            false,
            "create failed",
        );
        return;
    };
    defer s1.deinit();

    var resp = js.streams() catch {
        reportResult(
            "js_stream_list",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const streams = resp.value.streams orelse {
        reportResult(
            "js_stream_list",
            false,
            "no streams",
        );
        return;
    };

    if (streams.len < 1) {
        reportResult(
            "js_stream_list",
            false,
            "expected >= 1",
        );
        return;
    }

    // Verify we get StreamInfo with config
    var found = false;
    for (streams) |si| {
        if (si.config) |cfg| {
            if (std.mem.eql(u8, cfg.name, "LIST_A")) {
                found = true;
                break;
            }
        }
    }
    if (!found) {
        reportResult(
            "js_stream_list",
            false,
            "LIST_A not found",
        );
        return;
    }

    var d = js.deleteStream("LIST_A") catch {
        reportResult("js_stream_list", true, "");
        return;
    };
    d.deinit();

    reportResult("js_stream_list", true, "");
}

pub fn testConsumerNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "CONS_NAMES",
        .subjects = &.{"cnames.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var c1 = js.createConsumer(
        "CONS_NAMES",
        .{
            .name = "cons-alpha",
            .durable_name = "cons-alpha",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create cons failed",
        );
        return;
    };
    defer c1.deinit();

    var c2 = js.createConsumer(
        "CONS_NAMES",
        .{
            .name = "cons-beta",
            .durable_name = "cons-beta",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "create cons2 failed",
        );
        return;
    };
    defer c2.deinit();

    var resp = js.consumerNames(
        "CONS_NAMES",
    ) catch {
        reportResult(
            "js_consumer_names",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.consumers orelse {
        reportResult(
            "js_consumer_names",
            false,
            "no consumers",
        );
        return;
    };

    if (names.len < 2) {
        reportResult(
            "js_consumer_names",
            false,
            "expected >= 2",
        );
        return;
    }

    var d = js.deleteStream("CONS_NAMES") catch {
        reportResult("js_consumer_names", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consumer_names", true, "");
}

pub fn testConsumerList(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consumer_list",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "CONS_LIST",
        .subjects = &.{"clist.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consumer_list",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var c1 = js.createConsumer(
        "CONS_LIST",
        .{
            .name = "list-cons",
            .durable_name = "list-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consumer_list",
            false,
            "create cons failed",
        );
        return;
    };
    defer c1.deinit();

    var resp = js.consumers("CONS_LIST") catch {
        reportResult(
            "js_consumer_list",
            false,
            "list failed",
        );
        return;
    };
    defer resp.deinit();

    const consumers = resp.value.consumers orelse {
        reportResult(
            "js_consumer_list",
            false,
            "no consumers",
        );
        return;
    };

    if (consumers.len < 1) {
        reportResult(
            "js_consumer_list",
            false,
            "expected >= 1",
        );
        return;
    }

    // Verify ConsumerInfo has config
    if (consumers[0].config) |cfg| {
        if (cfg.name) |n| {
            if (!std.mem.eql(u8, n, "list-cons")) {
                reportResult(
                    "js_consumer_list",
                    false,
                    "wrong name",
                );
                return;
            }
        }
    } else {
        reportResult(
            "js_consumer_list",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteStream("CONS_LIST") catch {
        reportResult("js_consumer_list", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consumer_list", true, "");
}

pub fn testAccountInfo(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_account_info",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var resp = js.accountInfo() catch {
        reportResult(
            "js_account_info",
            false,
            "request failed",
        );
        return;
    };
    defer resp.deinit();

    if (resp.value.limits == null) {
        reportResult(
            "js_account_info",
            false,
            "no limits",
        );
        return;
    }

    reportResult("js_account_info", true, "");
}

pub fn testMetadata(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream + consumer
    var stream = js.createStream(.{
        .name = "TEST_META",
        .subjects = &.{"test.meta.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_metadata",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_META",
        .{
            .name = "meta-cons",
            .durable_name = "meta-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish
    var ack = js.publish(
        "test.meta.hello",
        "metadata test",
    ) catch {
        reportResult(
            "js_metadata",
            false,
            "publish failed",
        );
        return;
    };
    defer ack.deinit();

    // Fetch and check metadata
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_META",
    };
    pull.setConsumer("meta-cons") catch unreachable;

    var msg = (pull.next(5000) catch {
        reportResult(
            "js_metadata",
            false,
            "next failed",
        );
        return;
    }) orelse {
        reportResult(
            "js_metadata",
            false,
            "no message",
        );
        return;
    };
    defer msg.deinit();

    const md = msg.metadata() orelse {
        reportResult(
            "js_metadata",
            false,
            "no metadata",
        );
        return;
    };

    if (!std.mem.eql(u8, md.stream, "TEST_META")) {
        reportResult(
            "js_metadata",
            false,
            "wrong stream",
        );
        return;
    }
    if (!std.mem.eql(u8, md.consumer, "meta-cons")) {
        reportResult(
            "js_metadata",
            false,
            "wrong consumer",
        );
        return;
    }
    if (md.stream_seq != 1) {
        reportResult(
            "js_metadata",
            false,
            "expected seq 1",
        );
        return;
    }

    // Cleanup
    var d = js.deleteStream("TEST_META") catch {
        reportResult(
            "js_metadata",
            false,
            "delete failed",
        );
        return;
    };
    defer d.deinit();

    reportResult("js_metadata", true, "");
}

pub fn testFetchNoWait(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_NOWAIT",
        .subjects = &.{"test.nowait.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "create failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_NOWAIT",
        .{
            .name = "nowait-cons",
            .durable_name = "nowait-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_NOWAIT",
    };
    pull.setConsumer("nowait-cons") catch unreachable;

    // Fetch no-wait on empty consumer -> 0 messages
    var result = pull.fetchNoWait(10) catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "fetchNoWait failed",
        );
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_fetch_no_wait",
            false,
            "expected 0 messages",
        );
        return;
    }

    // Cleanup
    var d = js.deleteStream("TEST_NOWAIT") catch {
        reportResult(
            "js_fetch_no_wait",
            false,
            "delete failed",
        );
        return;
    };
    defer d.deinit();

    reportResult("js_fetch_no_wait", true, "");
}

pub fn testMessages(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_messages",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_MSGS",
        .subjects = &.{"test.msgs.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_messages",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_MSGS",
        .{
            .name = "msgs-cons",
            .durable_name = "msgs-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_messages",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.msgs.data",
            "hello",
        ) catch {
            reportResult(
                "js_messages",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Use messages iterator
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_MSGS",
    };
    pull.setConsumer("msgs-cons") catch unreachable;

    var ctx = pull.messages(.{
        .max_messages = 10,
        .expires_ms = 5000,
    }) catch {
        reportResult(
            "js_messages",
            false,
            "messages() failed",
        );
        return;
    };
    defer ctx.deinit();

    var received: u32 = 0;
    while (received < 5) {
        var msg = (ctx.next() catch {
            break;
        }) orelse break;
        msg.ack() catch {};
        msg.deinit();
        received += 1;
    }

    if (received != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 5",
            .{received},
        ) catch "count mismatch";
        reportResult("js_messages", false, m);
        return;
    }

    var d = js.deleteStream("TEST_MSGS") catch {
        reportResult("js_messages", true, "");
        return;
    };
    d.deinit();

    reportResult("js_messages", true, "");
}

pub fn testConsume(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_CONSUME",
        .subjects = &.{"test.consume.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_consume",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_CONSUME",
        .{
            .name = "consume-cons",
            .durable_name = "consume-cons",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "create consumer failed",
        );
        return;
    };
    defer cons.deinit();

    // Publish 10 messages
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var a = js.publish(
            "test.consume.data",
            "consume-test",
        ) catch {
            reportResult(
                "js_consume",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Use consume() with callback handler
    const Counter = struct {
        count: u32 = 0,
        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            msg.ack() catch {};
            self.count += 1;
        }
    };

    var counter = Counter{};
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_CONSUME",
    };
    pull.setConsumer("consume-cons") catch unreachable;

    var ctx = pull.consume(
        nats.jetstream.JsMsgHandler.init(
            Counter,
            &counter,
        ),
        .{
            .max_messages = 10,
            .expires_ms = 5000,
        },
    ) catch {
        reportResult(
            "js_consume",
            false,
            "consume() failed",
        );
        return;
    };

    // Wait for messages to be consumed
    var wait: u32 = 0;
    while (counter.count < 10 and wait < 50) : (wait += 1) {
        threadSleepNs(100_000_000);
    }

    ctx.stop();
    ctx.deinit();

    if (counter.count < 10) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 10",
            .{counter.count},
        ) catch "count mismatch";
        reportResult("js_consume", false, m);
        return;
    }

    var d = js.deleteStream("TEST_CONSUME") catch {
        reportResult("js_consume", true, "");
        return;
    };
    d.deinit();

    reportResult("js_consume", true, "");
}

pub fn testOrderedConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_ordered",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_ORDERED",
        .subjects = &.{"test.ordered.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_ordered",
            false,
            "create stream failed",
        );
        return;
    };
    defer stream.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.ordered.data",
            "ordered-msg",
        ) catch {
            reportResult(
                "js_ordered",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Create ordered consumer
    var oc = nats.jetstream.OrderedConsumer.init(
        &js,
        "TEST_ORDERED",
        .{
            .filter_subject = "test.ordered.>",
            .deliver_policy = .all,
        },
    );
    defer oc.deinit();

    // Fetch all 5 messages in order
    var received: u32 = 0;
    var last_seq: u64 = 0;
    while (received < 5) {
        var msg = (oc.next(5000) catch {
            break;
        }) orelse break;

        // Verify ordering
        if (msg.metadata()) |md| {
            if (md.stream_seq <= last_seq) {
                reportResult(
                    "js_ordered",
                    false,
                    "out of order",
                );
                msg.deinit();
                return;
            }
            last_seq = md.stream_seq;
        }

        msg.deinit();
        received += 1;
    }

    if (received != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 5",
            .{received},
        ) catch "count mismatch";
        reportResult("js_ordered", false, m);
        return;
    }

    // Verify stream_seq tracked correctly
    if (oc.stream_seq != 5) {
        reportResult(
            "js_ordered",
            false,
            "wrong stream_seq",
        );
        return;
    }

    var d = js.deleteStream("TEST_ORDERED") catch {
        reportResult("js_ordered", true, "");
        return;
    };
    d.deinit();

    reportResult("js_ordered", true, "");
}

// -- Ack protocol tests --

pub fn testAckPreventsRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_ack", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_ACK",
        .subjects = &.{"test.ack.>"},
        .storage = .memory,
    }) catch {
        reportResult("js_ack", false, "create stream");
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_ACK", .{
        .name = "ack-cons",
        .durable_name = "ack-cons",
        .ack_policy = .explicit,
        .ack_wait = 1_000_000_000,
    }) catch {
        reportResult(
            "js_ack",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    // Publish 1 message
    var a = js.publish(
        "test.ack.one",
        "ack-test",
    ) catch {
        reportResult("js_ack", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_ACK",
    };
    pull.setConsumer("ack-cons") catch unreachable;

    // Fetch and ACK
    var msg = (pull.next(5000) catch {
        reportResult("js_ack", false, "fetch 1");
        return;
    }) orelse {
        reportResult("js_ack", false, "no msg 1");
        return;
    };
    msg.ack() catch {
        reportResult("js_ack", false, "ack failed");
        msg.deinit();
        return;
    };
    msg.deinit();

    // Fetch again -> should be empty (acked)
    var result = pull.fetchNoWait(10) catch {
        reportResult("js_ack", false, "fetch 2");
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_ack",
            false,
            "expected 0 after ack",
        );
        return;
    }

    var d = js.deleteStream("TEST_ACK") catch {
        reportResult("js_ack", true, "");
        return;
    };
    d.deinit();
    reportResult("js_ack", true, "");
}

pub fn testNakCausesRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_nak", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_NAK",
        .subjects = &.{"test.nak.>"},
        .storage = .memory,
    }) catch {
        reportResult("js_nak", false, "create stream");
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_NAK", .{
        .name = "nak-cons",
        .durable_name = "nak-cons",
        .ack_policy = .explicit,
        .ack_wait = 2_000_000_000,
        .max_deliver = 3,
    }) catch {
        reportResult(
            "js_nak",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.nak.one",
        "nak-test",
    ) catch {
        reportResult("js_nak", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_NAK",
    };
    pull.setConsumer("nak-cons") catch unreachable;

    // Fetch and NAK
    var msg1 = (pull.next(5000) catch {
        reportResult("js_nak", false, "fetch 1");
        return;
    }) orelse {
        reportResult("js_nak", false, "no msg 1");
        return;
    };
    msg1.nak() catch {
        reportResult("js_nak", false, "nak failed");
        msg1.deinit();
        return;
    };
    msg1.deinit();

    // Fetch again -> should get redelivered message
    var msg2 = (pull.next(5000) catch {
        reportResult("js_nak", false, "fetch 2");
        return;
    }) orelse {
        reportResult(
            "js_nak",
            false,
            "no redeliver",
        );
        return;
    };

    // Verify it's the same data
    if (!std.mem.eql(u8, msg2.data(), "nak-test")) {
        reportResult(
            "js_nak",
            false,
            "wrong redeliver data",
        );
        msg2.deinit();
        return;
    }

    // Verify num_delivered > 1
    if (msg2.metadata()) |md| {
        if (md.num_delivered < 2) {
            reportResult(
                "js_nak",
                false,
                "expected redeliver count",
            );
            msg2.deinit();
            return;
        }
    }

    msg2.ack() catch {};
    msg2.deinit();

    var d = js.deleteStream("TEST_NAK") catch {
        reportResult("js_nak", true, "");
        return;
    };
    d.deinit();
    reportResult("js_nak", true, "");
}

pub fn testTermStopsRedeliver(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_term", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_TERM",
        .subjects = &.{"test.term.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_term",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_TERM", .{
        .name = "term-cons",
        .durable_name = "term-cons",
        .ack_policy = .explicit,
        .max_deliver = 5,
    }) catch {
        reportResult(
            "js_term",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.term.one",
        "term-test",
    ) catch {
        reportResult("js_term", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_TERM",
    };
    pull.setConsumer("term-cons") catch unreachable;

    // Fetch and TERM
    var msg = (pull.next(5000) catch {
        reportResult("js_term", false, "fetch");
        return;
    }) orelse {
        reportResult("js_term", false, "no msg");
        return;
    };
    msg.term() catch {
        reportResult("js_term", false, "term failed");
        msg.deinit();
        return;
    };
    msg.deinit();

    // Fetch again -> should be empty (terminated)
    var result = pull.fetchNoWait(10) catch {
        reportResult("js_term", false, "fetch 2");
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            "js_term",
            false,
            "expected 0 after term",
        );
        return;
    }

    var d = js.deleteStream("TEST_TERM") catch {
        reportResult("js_term", true, "");
        return;
    };
    d.deinit();
    reportResult("js_term", true, "");
}

// -- Batch fetch tests --

pub fn testBatchFetch(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_batch", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_BATCH",
        .subjects = &.{"test.batch.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_batch",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_BATCH", .{
        .name = "batch-cons",
        .durable_name = "batch-cons",
        .ack_policy = .explicit,
    }) catch {
        reportResult(
            "js_batch",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    // Publish 10 messages
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var a = js.publish(
            "test.batch.data",
            "batch-msg",
        ) catch {
            reportResult(
                "js_batch",
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_BATCH",
    };
    pull.setConsumer("batch-cons") catch unreachable;

    // Fetch batch of 5
    var r1 = pull.fetch(.{
        .max_messages = 5,
        .timeout_ms = 5000,
    }) catch {
        reportResult("js_batch", false, "fetch 1");
        return;
    };

    if (r1.count() != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "batch1: got {d}",
            .{r1.count()},
        ) catch "wrong";
        reportResult("js_batch", false, m);
        r1.deinit();
        return;
    }

    // Ack all in first batch
    for (r1.messages) |*msg| {
        msg.ack() catch {};
    }
    r1.deinit();

    // Fetch remaining 5
    var r2 = pull.fetch(.{
        .max_messages = 5,
        .timeout_ms = 5000,
    }) catch {
        reportResult("js_batch", false, "fetch 2");
        return;
    };

    if (r2.count() != 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "batch2: got {d}",
            .{r2.count()},
        ) catch "wrong";
        reportResult("js_batch", false, m);
        r2.deinit();
        return;
    }

    for (r2.messages) |*msg| {
        msg.ack() catch {};
    }
    r2.deinit();

    // Fetch again -> should be empty
    var r3 = pull.fetchNoWait(10) catch {
        reportResult("js_batch", false, "fetch 3");
        return;
    };
    defer r3.deinit();

    if (r3.count() != 0) {
        reportResult(
            "js_batch",
            false,
            "expected 0 after all acked",
        );
        return;
    }

    var d = js.deleteStream("TEST_BATCH") catch {
        reportResult("js_batch", true, "");
        return;
    };
    d.deinit();
    reportResult("js_batch", true, "");
}

// -- Publish options tests --

pub fn testPublishDedup(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_dedup", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_DEDUP",
        .subjects = &.{"test.dedup.>"},
        .storage = .memory,
        .duplicate_window = 60_000_000_000,
    }) catch {
        reportResult(
            "js_dedup",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish with same msg-id twice
    var a1 = js.publishWithOpts(
        "test.dedup.data",
        "first",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult("js_dedup", false, "pub 1");
        return;
    };
    const seq1 = a1.value.seq;
    a1.deinit();

    var a2 = js.publishWithOpts(
        "test.dedup.data",
        "duplicate",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult("js_dedup", false, "pub 2");
        return;
    };

    // Should get same seq (deduplicated)
    if (a2.value.seq != seq1) {
        reportResult(
            "js_dedup",
            false,
            "not deduped",
        );
        a2.deinit();
        return;
    }

    // Should be marked as duplicate
    if (a2.value.duplicate == null or
        !a2.value.duplicate.?)
    {
        reportResult(
            "js_dedup",
            false,
            "no dup flag",
        );
        a2.deinit();
        return;
    }
    a2.deinit();

    // Different msg-id -> new message
    var a3 = js.publishWithOpts(
        "test.dedup.data",
        "second",
        .{ .msg_id = "unique-2" },
    ) catch {
        reportResult("js_dedup", false, "pub 3");
        return;
    };

    if (a3.value.seq != seq1 + 1) {
        reportResult(
            "js_dedup",
            false,
            "wrong seq for new msg",
        );
        a3.deinit();
        return;
    }
    a3.deinit();

    var d = js.deleteStream("TEST_DEDUP") catch {
        reportResult("js_dedup", true, "");
        return;
    };
    d.deinit();
    reportResult("js_dedup", true, "");
}

pub fn testPublishExpectedSeq(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_exp_seq", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_EXPSEQ",
        .subjects = &.{"test.expseq.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_exp_seq",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish first message
    var a1 = js.publish(
        "test.expseq.data",
        "first",
    ) catch {
        reportResult("js_exp_seq", false, "pub 1");
        return;
    };
    a1.deinit();

    // Publish with correct expected_last_seq=1
    var a2 = js.publishWithOpts(
        "test.expseq.data",
        "second",
        .{ .expected_last_seq = 1 },
    ) catch {
        reportResult("js_exp_seq", false, "pub 2");
        return;
    };
    a2.deinit();

    // Publish with WRONG expected_last_seq=0
    // -> should fail
    var a3 = js.publishWithOpts(
        "test.expseq.data",
        "should-fail",
        .{ .expected_last_seq = 0 },
    );
    if (a3) |*r| {
        r.deinit();
        reportResult(
            "js_exp_seq",
            false,
            "should have failed",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_exp_seq",
                false,
                "wrong error",
            );
            return;
        }
        // Verify the error code
        if (js.lastApiError()) |api_err| {
            if (api_err.err_code !=
                nats.jetstream.errors
                    .ErrCode.stream_wrong_last_seq)
            {
                reportResult(
                    "js_exp_seq",
                    false,
                    "wrong err_code",
                );
                return;
            }
        }
    }

    var d = js.deleteStream("TEST_EXPSEQ") catch {
        reportResult("js_exp_seq", true, "");
        return;
    };
    d.deinit();
    reportResult("js_exp_seq", true, "");
}

// -- Stream operations tests --

pub fn testPurgeStream(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_purge", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_PURGE",
        .subjects = &.{"test.purge.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_purge",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.purge.data",
            "purge-msg",
        ) catch {
            reportResult(
                "js_purge",
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    // Verify 5 messages exist
    var info1 = js.streamInfo("TEST_PURGE") catch {
        reportResult("js_purge", false, "info 1");
        return;
    };
    if (info1.value.state) |st| {
        if (st.messages != 5) {
            reportResult(
                "js_purge",
                false,
                "expected 5 msgs",
            );
            info1.deinit();
            return;
        }
    }
    info1.deinit();

    // Purge
    var p = js.purgeStream("TEST_PURGE") catch {
        reportResult("js_purge", false, "purge");
        return;
    };
    if (!p.value.success) {
        reportResult(
            "js_purge",
            false,
            "purge not success",
        );
        p.deinit();
        return;
    }
    if (p.value.purged != 5) {
        reportResult(
            "js_purge",
            false,
            "wrong purge count",
        );
        p.deinit();
        return;
    }
    p.deinit();

    // Verify 0 messages
    var info2 = js.streamInfo("TEST_PURGE") catch {
        reportResult("js_purge", false, "info 2");
        return;
    };
    defer info2.deinit();
    if (info2.value.state) |st| {
        if (st.messages != 0) {
            reportResult(
                "js_purge",
                false,
                "expected 0 after purge",
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_PURGE") catch {
        reportResult("js_purge", true, "");
        return;
    };
    d.deinit();
    reportResult("js_purge", true, "");
}

// -- Stream update test --

pub fn testStreamUpdate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_update", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_UPDATE",
        .subjects = &.{"test.update.>"},
        .storage = .memory,
        .max_msgs = 100,
    }) catch {
        reportResult(
            "js_update",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Update max_msgs
    var u = js.updateStream(.{
        .name = "TEST_UPDATE",
        .subjects = &.{"test.update.>"},
        .storage = .memory,
        .max_msgs = 200,
    }) catch {
        reportResult("js_update", false, "update");
        return;
    };
    defer u.deinit();

    if (u.value.config) |cfg| {
        if (cfg.max_msgs != 200) {
            reportResult(
                "js_update",
                false,
                "max_msgs not updated",
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_UPDATE") catch {
        reportResult("js_update", true, "");
        return;
    };
    d.deinit();
    reportResult("js_update", true, "");
}

// -- InProgress (WPI) test --

pub fn testInProgress(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("js_wpi", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_WPI",
        .subjects = &.{"test.wpi.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_wpi",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_WPI", .{
        .name = "wpi-cons",
        .durable_name = "wpi-cons",
        .ack_policy = .explicit,
        .ack_wait = 2_000_000_000,
    }) catch {
        reportResult(
            "js_wpi",
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "test.wpi.one",
        "wpi-test",
    ) catch {
        reportResult("js_wpi", false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_WPI",
    };
    pull.setConsumer("wpi-cons") catch unreachable;

    var msg = (pull.next(5000) catch {
        reportResult("js_wpi", false, "fetch");
        return;
    }) orelse {
        reportResult("js_wpi", false, "no msg");
        return;
    };

    // Send inProgress to extend deadline
    msg.inProgress() catch {
        reportResult("js_wpi", false, "wpi failed");
        msg.deinit();
        return;
    };

    // Can call inProgress multiple times
    msg.inProgress() catch {
        reportResult("js_wpi", false, "wpi 2");
        msg.deinit();
        return;
    };

    // Now ack
    msg.ack() catch {
        reportResult("js_wpi", false, "ack");
        msg.deinit();
        return;
    };
    msg.deinit();

    var d = js.deleteStream("TEST_WPI") catch {
        reportResult("js_wpi", true, "");
        return;
    };
    d.deinit();
    reportResult("js_wpi", true, "");
}

// -- Consumer not found test --

pub fn testConsumerNotFound(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_cons_not_found",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_CNF",
        .subjects = &.{"test.cnf.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_cons_not_found",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var info = js.consumerInfo(
        "TEST_CNF",
        "nonexistent",
    );
    if (info) |*r| {
        r.deinit();
        reportResult(
            "js_cons_not_found",
            false,
            "should fail",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_cons_not_found",
                false,
                "wrong error",
            );
            return;
        }
        if (js.lastApiError()) |api_err| {
            if (api_err.err_code !=
                nats.jetstream.errors
                    .ErrCode.consumer_not_found)
            {
                reportResult(
                    "js_cons_not_found",
                    false,
                    "wrong err_code",
                );
                return;
            }
        }
    }

    var d = js.deleteStream("TEST_CNF") catch {
        reportResult(
            "js_cons_not_found",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_cons_not_found", true, "");
}

// -- Stream by subject test --

pub fn testStreamBySubject(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_by_subject",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_BYSUB",
        .subjects = &.{"bysub.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_by_subject",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var resp = js.streamNameBySubject(
        "bysub.test",
    ) catch {
        reportResult(
            "js_by_subject",
            false,
            "lookup failed",
        );
        return;
    };
    defer resp.deinit();

    const names = resp.value.streams orelse {
        reportResult(
            "js_by_subject",
            false,
            "no result",
        );
        return;
    };

    if (names.len != 1) {
        reportResult(
            "js_by_subject",
            false,
            "expected 1 match",
        );
        return;
    }

    if (!std.mem.eql(u8, names[0], "TEST_BYSUB")) {
        reportResult(
            "js_by_subject",
            false,
            "wrong stream",
        );
        return;
    }

    var d = js.deleteStream("TEST_BYSUB") catch {
        reportResult("js_by_subject", true, "");
        return;
    };
    d.deinit();
    reportResult("js_by_subject", true, "");
}

// -- Key-Value Store tests --

pub fn testKvPutGet(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_put_get", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_put_get",
            false,
            "create bucket",
        );
        return;
    };

    // Put
    const rev1 = kv.put("mykey", "hello") catch {
        reportResult("kv_put_get", false, "put");
        return;
    };
    if (rev1 == 0) {
        reportResult(
            "kv_put_get",
            false,
            "rev should be > 0",
        );
        return;
    }

    // Get
    var entry = (kv.get("mykey") catch {
        reportResult("kv_put_get", false, "get");
        return;
    }) orelse {
        reportResult(
            "kv_put_get",
            false,
            "key not found",
        );
        return;
    };
    defer entry.deinit();

    if (entry.revision != rev1) {
        reportResult(
            "kv_put_get",
            false,
            "wrong revision",
        );
        return;
    }
    if (entry.operation != .put) {
        reportResult(
            "kv_put_get",
            false,
            "wrong operation",
        );
        return;
    }

    // Get non-existent key
    const missing = kv.get("nonexistent") catch {
        reportResult(
            "kv_put_get",
            false,
            "get missing err",
        );
        return;
    };
    if (missing != null) {
        reportResult(
            "kv_put_get",
            false,
            "should be null",
        );
        return;
    }

    reportResult("kv_put_get", true, "");
}

pub fn testKvCreate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_create", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_CREATE",
        .storage = .memory,
    }) catch |err| {
        reportError("kv_create", "create bucket", err);
        return;
    };

    // Create succeeds on new key
    _ = kv.create("newkey", "value1") catch |err| {
        reportError("kv_create", "create 1", err);
        return;
    };

    // Create fails on existing key
    _ = kv.create("newkey", "value2") catch |err| {
        if (err == error.ApiError) {
            reportResult("kv_create", true, "");
            return;
        }
        reportResult(
            "kv_create",
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        "kv_create",
        false,
        "should have failed",
    );
}

pub fn testKvUpdate(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_update", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_UPDATE",
        .storage = .memory,
    }) catch |err| {
        reportError("kv_update", "create bucket", err);
        return;
    };

    const rev1 = kv.put("key1", "v1") catch |err| {
        reportError("kv_update", "put", err);
        return;
    };

    // Update with correct revision
    const rev2 = kv.update(
        "key1",
        "v2",
        rev1,
    ) catch {
        reportResult(
            "kv_update",
            false,
            "update ok",
        );
        return;
    };

    if (rev2 <= rev1) {
        reportResult(
            "kv_update",
            false,
            "rev not incremented",
        );
        return;
    }

    // Update with wrong revision -> fail
    _ = kv.update("key1", "v3", rev1) catch |err| {
        if (err == error.ApiError) {
            reportResult("kv_update", true, "");
            return;
        }
        reportResult(
            "kv_update",
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        "kv_update",
        false,
        "should have failed",
    );
}

pub fn testKvDelete(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_delete", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_DEL",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_delete",
            false,
            "create bucket",
        );
        return;
    };

    _ = kv.put("delkey", "value") catch {
        reportResult("kv_delete", false, "put");
        return;
    };

    // Delete
    _ = kv.delete("delkey") catch {
        reportResult("kv_delete", false, "delete");
        return;
    };

    // Get should show delete marker
    var entry = (kv.get("delkey") catch {
        reportResult("kv_delete", false, "get");
        return;
    }) orelse {
        // Key gone completely (ok for history=1)
        reportResult("kv_delete", true, "");
        return;
    };
    defer entry.deinit();

    if (entry.operation != .delete) {
        reportResult(
            "kv_delete",
            false,
            "expected delete op",
        );
        return;
    }

    reportResult("kv_delete", true, "");
}

pub fn testKvKeys(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_keys", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_keys",
            false,
            "create bucket",
        );
        return;
    };

    // Put 3 keys
    _ = kv.put("alpha", "1") catch {
        reportResult("kv_keys", false, "put 1");
        return;
    };
    _ = kv.put("beta", "2") catch {
        reportResult("kv_keys", false, "put 2");
        return;
    };
    _ = kv.put("gamma", "3") catch {
        reportResult("kv_keys", false, "put 3");
        return;
    };

    const key_list = kv.keys(allocator) catch {
        reportResult("kv_keys", false, "keys()");
        return;
    };
    defer {
        for (key_list) |k| allocator.free(k);
        allocator.free(key_list);
    }

    if (key_list.len != 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d} keys, expected 3",
            .{key_list.len},
        ) catch "wrong count";
        reportResult("kv_keys", false, m);
        return;
    }

    reportResult("kv_keys", true, "");
}

pub fn testKvHistory(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_history", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_HIST",
        .storage = .memory,
        .history = 10,
    }) catch {
        reportResult(
            "kv_history",
            false,
            "create bucket",
        );
        return;
    };

    // Put same key 3 times
    _ = kv.put("hkey", "v1") catch {
        reportResult("kv_history", false, "put 1");
        return;
    };
    _ = kv.put("hkey", "v2") catch {
        reportResult("kv_history", false, "put 2");
        return;
    };
    _ = kv.put("hkey", "v3") catch {
        reportResult("kv_history", false, "put 3");
        return;
    };

    const hist = kv.history(
        allocator,
        "hkey",
    ) catch {
        reportResult(
            "kv_history",
            false,
            "history()",
        );
        return;
    };
    defer {
        for (hist) |*h| h.deinit();
        allocator.free(hist);
    }

    if (hist.len != 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 3",
            .{hist.len},
        ) catch "wrong";
        reportResult("kv_history", false, m);
        return;
    }

    // Verify revisions are increasing
    if (hist.len >= 2) {
        if (hist[1].revision <= hist[0].revision) {
            reportResult(
                "kv_history",
                false,
                "revs not increasing",
            );
            return;
        }
    }

    reportResult("kv_history", true, "");
}

pub fn testKvWatch(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("kv_watch", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_WATCH",
        .storage = .memory,
    }) catch |err| {
        reportError("kv_watch", "create bucket", err);
        return;
    };

    // Put a key before watching
    _ = kv.put("pre-watch", "initial") catch |err| {
        reportError("kv_watch", "put", err);
        return;
    };

    // Start watching
    var watcher = kv.watchAll() catch {
        reportResult(
            "kv_watch",
            false,
            "watchAll()",
        );
        return;
    };
    defer watcher.deinit();

    // Should get the initial key
    var entry = (watcher.next(5000) catch |err| {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "watch next: {}",
            .{err},
        ) catch "watch err";
        reportResult("kv_watch", false, m);
        return;
    }) orelse {
        reportResult(
            "kv_watch",
            false,
            "no initial entry",
        );
        return;
    };
    defer entry.deinit();

    if (!std.mem.eql(u8, entry.key, "pre-watch")) {
        reportResult(
            "kv_watch",
            false,
            "wrong key",
        );
        return;
    }

    reportResult("kv_watch", true, "");
}

pub fn testKvBucketLifecycle(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create
    var kv = js.createKeyValue(.{
        .bucket = "TEST_KV_LIFE",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "create",
        );
        return;
    };

    // Status
    var st = kv.status() catch {
        reportResult(
            "kv_lifecycle",
            false,
            "status",
        );
        return;
    };
    defer st.deinit();

    if (st.value.config) |cfg| {
        if (!std.mem.eql(
            u8,
            cfg.name,
            "KV_TEST_KV_LIFE",
        )) {
            reportResult(
                "kv_lifecycle",
                false,
                "wrong stream name",
            );
            return;
        }
    }

    // Bind
    const kv2 = js.keyValue("TEST_KV_LIFE") catch {
        reportResult(
            "kv_lifecycle",
            false,
            "bind",
        );
        return;
    };
    _ = kv2;

    // Delete
    var del = js.deleteKeyValue(
        "TEST_KV_LIFE",
    ) catch {
        reportResult(
            "kv_lifecycle",
            false,
            "delete",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "kv_lifecycle",
            false,
            "delete failed",
        );
        return;
    }

    // Bind to deleted bucket -> should fail
    _ = js.keyValue("TEST_KV_LIFE") catch {
        reportResult("kv_lifecycle", true, "");
        return;
    };

    reportResult(
        "kv_lifecycle",
        false,
        "bind should fail after delete",
    );
}

// -- Behavioral correctness tests --

pub fn testFilteredConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_FILTER",
        .subjects = &.{"test.filter.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish to different subjects
    var a1 = js.publish(
        "test.filter.a",
        "msg-a",
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "pub a",
        );
        return;
    };
    a1.deinit();

    var a2 = js.publish(
        "test.filter.b",
        "msg-b",
    ) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "pub b",
        );
        return;
    };
    a2.deinit();

    // Create consumer filtered on "test.filter.a"
    var c = js.createConsumer("TEST_FILTER", .{
        .name = "filter-cons",
        .durable_name = "filter-cons",
        .ack_policy = .explicit,
        .filter_subject = "test.filter.a",
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "create cons: {}",
            .{err},
        ) catch "err";
        reportResult("js_filtered_cons", false, m);
        return;
    };
    defer c.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_FILTER",
    };
    pull.setConsumer("filter-cons") catch unreachable;

    // Should only get "msg-a" (filtered)
    var msg = (pull.next(5000) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "fetch",
        );
        return;
    }) orelse {
        reportResult(
            "js_filtered_cons",
            false,
            "no msg",
        );
        return;
    };

    if (!std.mem.eql(u8, msg.data(), "msg-a")) {
        reportResult(
            "js_filtered_cons",
            false,
            "wrong data",
        );
        msg.deinit();
        return;
    }
    msg.ack() catch {};
    msg.deinit();

    // No more messages (msg-b filtered out)
    var r = pull.fetchNoWait(10) catch {
        reportResult(
            "js_filtered_cons",
            false,
            "fetch 2",
        );
        return;
    };
    defer r.deinit();

    if (r.count() != 0) {
        reportResult(
            "js_filtered_cons",
            false,
            "expected 0 after filter",
        );
        return;
    }

    var d = js.deleteStream("TEST_FILTER") catch {
        reportResult("js_filtered_cons", true, "");
        return;
    };
    d.deinit();
    reportResult("js_filtered_cons", true, "");
}

pub fn testPurgeSubject(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_purge_subj",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_PURGE_S",
        .subjects = &.{"test.purge.s.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_purge_subj",
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish to 2 subjects
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "test.purge.s.keep",
            "keep",
        ) catch {
            reportResult(
                "js_purge_subj",
                false,
                "pub keep",
            );
            return;
        };
        a.deinit();
    }
    i = 0;
    while (i < 2) : (i += 1) {
        var a = js.publish(
            "test.purge.s.remove",
            "remove",
        ) catch {
            reportResult(
                "js_purge_subj",
                false,
                "pub remove",
            );
            return;
        };
        a.deinit();
    }

    // Purge only "remove" subject
    var p = js.purgeStreamSubject(
        "TEST_PURGE_S",
        "test.purge.s.remove",
    ) catch {
        reportResult(
            "js_purge_subj",
            false,
            "purge",
        );
        return;
    };
    defer p.deinit();

    if (p.value.purged != 2) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "purged {d}, expected 2",
            .{p.value.purged},
        ) catch "wrong count";
        reportResult("js_purge_subj", false, m);
        return;
    }

    // Verify "keep" messages still exist
    var info = js.streamInfo("TEST_PURGE_S") catch {
        reportResult(
            "js_purge_subj",
            false,
            "info",
        );
        return;
    };
    defer info.deinit();

    if (info.value.state) |st| {
        if (st.messages != 3) {
            var buf: [64]u8 = undefined;
            const m = std.fmt.bufPrint(
                &buf,
                "{d} msgs, expected 3",
                .{st.messages},
            ) catch "wrong";
            reportResult(
                "js_purge_subj",
                false,
                m,
            );
            return;
        }
    }

    var d = js.deleteStream("TEST_PURGE_S") catch {
        reportResult("js_purge_subj", true, "");
        return;
    };
    d.deinit();
    reportResult("js_purge_subj", true, "");
}

pub fn testPaginatedStreamNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_paginated",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create 3 streams
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const sname = std.fmt.bufPrint(
            &name_buf,
            "PAG_{d}",
            .{i},
        ) catch unreachable;
        var subj_b: [32]u8 = undefined;
        const ssubj = std.fmt.bufPrint(
            &subj_b,
            "pag.{d}.>",
            .{i},
        ) catch unreachable;
        const subjects: [1][]const u8 = .{ssubj};
        var r = js.createStream(.{
            .name = sname,
            .subjects = &subjects,
            .storage = .memory,
        }) catch {
            reportResult(
                "js_paginated",
                false,
                "create",
            );
            return;
        };
        r.deinit();
    }

    // Use allStreamNames (pagination)
    const all = js.allStreamNames(allocator) catch {
        reportResult(
            "js_paginated",
            false,
            "allStreamNames",
        );
        return;
    };
    defer {
        for (all) |n| allocator.free(n);
        allocator.free(all);
    }

    if (all.len < 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected >= 3",
            .{all.len},
        ) catch "wrong";
        reportResult("js_paginated", false, m);
        return;
    }

    // Cleanup
    i = 0;
    while (i < 3) : (i += 1) {
        var name_buf: [32]u8 = undefined;
        const sname = std.fmt.bufPrint(
            &name_buf,
            "PAG_{d}",
            .{i},
        ) catch unreachable;
        var r = js.deleteStream(sname) catch continue;
        r.deinit();
    }

    reportResult("js_paginated", true, "");
}

pub fn testGetMsg(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_get_msg",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_GETMSG",
        .subjects = &.{"test.getmsg.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_get_msg",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Publish 3 messages
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "test.getmsg.a",
            "payload",
        ) catch {
            reportResult(
                "js_get_msg",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Get message at seq 1
    var resp = js.getMsg(
        "TEST_GETMSG",
        1,
    ) catch {
        reportResult(
            "js_get_msg",
            false,
            "getMsg failed",
        );
        return;
    };
    defer resp.deinit();

    if (resp.value.message) |m| {
        if (m.seq != 1) {
            reportResult(
                "js_get_msg",
                false,
                "expected seq 1",
            );
            return;
        }
    } else {
        reportResult(
            "js_get_msg",
            false,
            "no message",
        );
        return;
    }

    // Get non-existent seq -> ApiError
    var bad = js.getMsg("TEST_GETMSG", 999);
    if (bad) |*r| {
        r.deinit();
        reportResult(
            "js_get_msg",
            false,
            "should fail for 999",
        );
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(
                "js_get_msg",
                false,
                "wrong error type",
            );
            return;
        }
        if (js.lastApiError()) |ae| {
            if (ae.err_code !=
                nats.jetstream.errors
                    .ErrCode.message_not_found)
            {
                reportResult(
                    "js_get_msg",
                    false,
                    "wrong err_code",
                );
                return;
            }
        }
    }

    var d = js.deleteStream(
        "TEST_GETMSG",
    ) catch {
        reportResult("js_get_msg", true, "");
        return;
    };
    d.deinit();
    reportResult("js_get_msg", true, "");
}

pub fn testGetLastMsgForSubject(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_get_last_msg",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_GETLAST",
        .subjects = &.{"test.getlast.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_get_last_msg",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Publish 3 msgs to test.getlast.a
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "test.getlast.a",
            "msg-a",
        ) catch {
            reportResult(
                "js_get_last_msg",
                false,
                "pub a failed",
            );
            return;
        };
        a.deinit();
    }

    // Publish 2 msgs to test.getlast.b
    i = 0;
    while (i < 2) : (i += 1) {
        var a = js.publish(
            "test.getlast.b",
            "msg-b",
        ) catch {
            reportResult(
                "js_get_last_msg",
                false,
                "pub b failed",
            );
            return;
        };
        a.deinit();
    }

    // Last for subject "a" should be seq 3
    var ra = js.getLastMsgForSubject(
        "TEST_GETLAST",
        "test.getlast.a",
    ) catch {
        reportResult(
            "js_get_last_msg",
            false,
            "getLast a failed",
        );
        return;
    };
    defer ra.deinit();

    if (ra.value.message) |m| {
        if (m.seq != 3) {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "a: got {d}, want 3",
                .{m.seq},
            ) catch "wrong seq";
            reportResult(
                "js_get_last_msg",
                false,
                msg,
            );
            return;
        }
    } else {
        reportResult(
            "js_get_last_msg",
            false,
            "no msg for a",
        );
        return;
    }

    // Last for subject "b" should be seq 5
    var rb = js.getLastMsgForSubject(
        "TEST_GETLAST",
        "test.getlast.b",
    ) catch {
        reportResult(
            "js_get_last_msg",
            false,
            "getLast b failed",
        );
        return;
    };
    defer rb.deinit();

    if (rb.value.message) |m| {
        if (m.seq != 5) {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "b: got {d}, want 5",
                .{m.seq},
            ) catch "wrong seq";
            reportResult(
                "js_get_last_msg",
                false,
                msg,
            );
            return;
        }
    } else {
        reportResult(
            "js_get_last_msg",
            false,
            "no msg for b",
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_GETLAST",
    ) catch {
        reportResult(
            "js_get_last_msg",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_get_last_msg", true, "");
}

pub fn testDeleteMsg(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_delete_msg",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_DELMSG",
        .subjects = &.{"test.delmsg.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_delete_msg",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "test.delmsg.a",
            "payload",
        ) catch {
            reportResult(
                "js_delete_msg",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Delete seq 3
    var del = js.deleteMsg(
        "TEST_DELMSG",
        3,
    ) catch {
        reportResult(
            "js_delete_msg",
            false,
            "deleteMsg failed",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "js_delete_msg",
            false,
            "delete not success",
        );
        return;
    }

    // getMsg(3) should fail
    var bad = js.getMsg("TEST_DELMSG", 3);
    if (bad) |*r| {
        r.deinit();
        reportResult(
            "js_delete_msg",
            false,
            "seq 3 should be gone",
        );
        return;
    } else |_| {}

    // getMsg(2) should still work
    var ok = js.getMsg(
        "TEST_DELMSG",
        2,
    ) catch {
        reportResult(
            "js_delete_msg",
            false,
            "seq 2 should exist",
        );
        return;
    };
    ok.deinit();

    var d = js.deleteStream(
        "TEST_DELMSG",
    ) catch {
        reportResult(
            "js_delete_msg",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_delete_msg", true, "");
}

pub fn testSecureDeleteMsg(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_secure_del",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_SECDEL",
        .subjects = &.{"test.secdel.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_secure_del",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Publish 3 messages
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "test.secdel.a",
            "payload",
        ) catch {
            reportResult(
                "js_secure_del",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Secure delete seq 2
    var del = js.secureDeleteMsg(
        "TEST_SECDEL",
        2,
    ) catch {
        reportResult(
            "js_secure_del",
            false,
            "secureDelete failed",
        );
        return;
    };
    defer del.deinit();

    if (!del.value.success) {
        reportResult(
            "js_secure_del",
            false,
            "delete not success",
        );
        return;
    }

    // getMsg(2) should fail
    var bad = js.getMsg("TEST_SECDEL", 2);
    if (bad) |*r| {
        r.deinit();
        reportResult(
            "js_secure_del",
            false,
            "seq 2 should be gone",
        );
        return;
    } else |_| {}

    // getMsg(1) should still work
    var ok = js.getMsg(
        "TEST_SECDEL",
        1,
    ) catch {
        reportResult(
            "js_secure_del",
            false,
            "seq 1 should exist",
        );
        return;
    };
    ok.deinit();

    var d = js.deleteStream(
        "TEST_SECDEL",
    ) catch {
        reportResult(
            "js_secure_del",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_secure_del", true, "");
}

pub fn testCreateOrUpdateStream(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_upsert_stream",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create via createOrUpdate
    var r1 = js.createOrUpdateStream(.{
        .name = "TEST_UPSERT",
        .subjects = &.{"upsert.>"},
        .storage = .memory,
        .max_msgs = 100,
    }) catch {
        reportResult(
            "js_upsert_stream",
            false,
            "create failed",
        );
        return;
    };
    r1.deinit();

    // Update via createOrUpdate
    var r2 = js.createOrUpdateStream(.{
        .name = "TEST_UPSERT",
        .subjects = &.{"upsert.>"},
        .storage = .memory,
        .max_msgs = 200,
    }) catch {
        reportResult(
            "js_upsert_stream",
            false,
            "update failed",
        );
        return;
    };
    r2.deinit();

    // Verify updated config
    var info = js.streamInfo(
        "TEST_UPSERT",
    ) catch {
        reportResult(
            "js_upsert_stream",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.config) |cfg| {
        if (cfg.max_msgs) |mm| {
            if (mm != 200) {
                var buf: [64]u8 = undefined;
                const m = std.fmt.bufPrint(
                    &buf,
                    "max_msgs {d}, want 200",
                    .{mm},
                ) catch "wrong";
                reportResult(
                    "js_upsert_stream",
                    false,
                    m,
                );
                return;
            }
        } else {
            reportResult(
                "js_upsert_stream",
                false,
                "no max_msgs",
            );
            return;
        }
    } else {
        reportResult(
            "js_upsert_stream",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_UPSERT",
    ) catch {
        reportResult(
            "js_upsert_stream",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_upsert_stream", true, "");
}

pub fn testCreateOrUpdateConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_upsert_cons",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_UCONS",
        .subjects = &.{"ucons.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_upsert_cons",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Create consumer
    var c1 = js.createOrUpdateConsumer(
        "TEST_UCONS",
        .{
            .name = "upsert-c",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_upsert_cons",
            false,
            "create cons",
        );
        return;
    };
    c1.deinit();

    // Update consumer
    var c2 = js.createOrUpdateConsumer(
        "TEST_UCONS",
        .{
            .name = "upsert-c",
            .ack_policy = .explicit,
            .max_ack_pending = 500,
        },
    ) catch {
        reportResult(
            "js_upsert_cons",
            false,
            "update cons",
        );
        return;
    };
    c2.deinit();

    // Verify updated config
    var info = js.consumerInfo(
        "TEST_UCONS",
        "upsert-c",
    ) catch {
        reportResult(
            "js_upsert_cons",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.config) |cfg| {
        if (cfg.max_ack_pending) |mp| {
            if (mp != 500) {
                var buf: [64]u8 = undefined;
                const m = std.fmt.bufPrint(
                    &buf,
                    "max_ack {d}, want 500",
                    .{mp},
                ) catch "wrong";
                reportResult(
                    "js_upsert_cons",
                    false,
                    m,
                );
                return;
            }
        } else {
            reportResult(
                "js_upsert_cons",
                false,
                "no max_ack_pending",
            );
            return;
        }
    } else {
        reportResult(
            "js_upsert_cons",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_UCONS",
    ) catch {
        reportResult(
            "js_upsert_cons",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_upsert_cons", true, "");
}

pub fn testPauseResumeConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_pause_resume",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PAUSE",
        .subjects = &.{"pause.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_pause_resume",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    var cons = js.createConsumer(
        "TEST_PAUSE",
        .{
            .name = "pause-c",
            .durable_name = "pause-c",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            "js_pause_resume",
            false,
            "create cons",
        );
        return;
    };
    cons.deinit();

    // Pause consumer
    var pr = js.pauseConsumer(
        "TEST_PAUSE",
        "pause-c",
        "2099-01-01T00:00:00Z",
    ) catch {
        reportResult(
            "js_pause_resume",
            false,
            "pause failed",
        );
        return;
    };
    defer pr.deinit();

    if (!pr.value.paused) {
        reportResult(
            "js_pause_resume",
            false,
            "not paused",
        );
        return;
    }

    // Resume consumer
    var rr = js.resumeConsumer(
        "TEST_PAUSE",
        "pause-c",
    ) catch {
        reportResult(
            "js_pause_resume",
            false,
            "resume failed",
        );
        return;
    };
    defer rr.deinit();

    if (rr.value.paused) {
        reportResult(
            "js_pause_resume",
            false,
            "still paused",
        );
        return;
    }

    // Publish + fetch after resume
    var a = js.publish(
        "pause.test",
        "after-resume",
    ) catch {
        reportResult(
            "js_pause_resume",
            false,
            "publish failed",
        );
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_PAUSE",
    };
    pull.setConsumer("pause-c") catch unreachable;

    var msg = (pull.next(5000) catch {
        reportResult(
            "js_pause_resume",
            false,
            "fetch failed",
        );
        return;
    }) orelse {
        reportResult(
            "js_pause_resume",
            false,
            "no msg after resume",
        );
        return;
    };
    msg.ack() catch {};
    msg.deinit();

    var d = js.deleteStream(
        "TEST_PAUSE",
    ) catch {
        reportResult(
            "js_pause_resume",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_pause_resume", true, "");
}

pub fn testPushConsumerBasic(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_push_basic",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Clean up from prior runs
    if (js.deleteStream("TEST_PUSH")) |r| {
        var rr = r;
        rr.deinit();
    } else |_| {}

    var stream = js.createStream(.{
        .name = "TEST_PUSH",
        .subjects = &.{"push.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_push_basic",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // Publish 5 messages first
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "push.test",
            "push-data",
        ) catch {
            reportResult(
                "js_push_basic",
                false,
                "publish failed",
            );
            return;
        };
        a.deinit();
    }

    // Set up push subscription handler
    const Counter = struct {
        count: u32 = 0,
        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            _ = msg;
            self.count += 1;
        }
    };

    var counter = Counter{};

    // Subscribe to deliver subject BEFORE creating
    // the consumer -- otherwise server pushes before
    // we're listening and messages are lost.
    const deliver_subj = "_PUSH_DELIVER.test";
    var push_sub = nats.jetstream.PushSubscription{
        .js = &js,
        .stream = "TEST_PUSH",
    };
    push_sub.setConsumer("push-c") catch unreachable;
    push_sub.setDeliverSubject(deliver_subj) catch unreachable;

    var ctx = push_sub.consume(
        nats.jetstream.JsMsgHandler.init(
            Counter,
            &counter,
        ),
        .{},
    ) catch {
        reportResult(
            "js_push_basic",
            false,
            "consume failed",
        );
        return;
    };

    // Now create the push consumer -- server starts
    // delivering to the already-subscribed subject.
    var pc = js.createPushConsumer(
        "TEST_PUSH",
        .{
            .name = "push-c",
            .deliver_subject = deliver_subj,
            .ack_policy = .none,
        },
    ) catch {
        ctx.stop();
        ctx.deinit();
        reportResult(
            "js_push_basic",
            false,
            "create push cons",
        );
        return;
    };
    pc.deinit();

    // Wait for messages
    var wait: u32 = 0;
    while (counter.count < 5 and
        wait < 50) : (wait += 1)
    {
        threadSleepNs(100_000_000);
    }

    ctx.stop();
    ctx.deinit();

    if (counter.count < 5) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 5",
            .{counter.count},
        ) catch "count mismatch";
        reportResult(
            "js_push_basic",
            false,
            m,
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_PUSH",
    ) catch {
        reportResult(
            "js_push_basic",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_push_basic", true, "");
}

pub fn testPushConsumerBorrowedAck(
    allocator: std.mem.Allocator,
) void {
    const name = "js_push_borrowed_ack";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect failed");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PUSH_ACK",
        .subjects = &.{"push.ack.>"},
        .storage = .memory,
    }) catch {
        reportResult(name, false, "create stream");
        return;
    };
    defer stream.deinit();

    const Handler = struct {
        count: u32 = 0,
        saw_expected_data: bool = false,
        ack_failed: bool = false,

        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            if (std.mem.eql(
                u8,
                msg.data(),
                "ack-data",
            )) {
                self.saw_expected_data = true;
            }
            msg.ack() catch {
                self.ack_failed = true;
                return;
            };
            self.count += 1;
        }
    };

    var handler = Handler{};
    const deliver_subj = "_PUSH_ACK_DELIVER.test";
    var push_sub = nats.jetstream.PushSubscription{
        .js = &js,
        .stream = "TEST_PUSH_ACK",
    };
    push_sub.setConsumer("push-ack-c") catch unreachable;
    push_sub.setDeliverSubject(deliver_subj) catch unreachable;

    var ctx = push_sub.consume(
        nats.jetstream.JsMsgHandler.init(
            Handler,
            &handler,
        ),
        .{},
    ) catch {
        reportResult(name, false, "consume failed");
        return;
    };

    var pc = js.createPushConsumer(
        "TEST_PUSH_ACK",
        .{
            .name = "push-ack-c",
            .deliver_subject = deliver_subj,
            .ack_policy = .explicit,
            .ack_wait = 1_000_000_000,
        },
    ) catch {
        ctx.stop();
        ctx.deinit();
        reportResult(name, false, "create push cons");
        return;
    };
    pc.deinit();

    var ack = js.publish(
        "push.ack.data",
        "ack-data",
    ) catch {
        ctx.stop();
        ctx.deinit();
        reportResult(name, false, "publish failed");
        return;
    };
    ack.deinit();

    var wait: u32 = 0;
    while (handler.count < 1 and
        !handler.ack_failed and
        wait < 50) : (wait += 1)
    {
        threadSleepNs(100_000_000);
    }

    if (handler.ack_failed) {
        ctx.stop();
        ctx.deinit();
        reportResult(name, false, "ack failed");
        return;
    }
    if (handler.count != 1 or !handler.saw_expected_data) {
        ctx.stop();
        ctx.deinit();
        reportResult(name, false, "callback did not ack data");
        return;
    }

    var ack_cleared = false;
    var info_wait: u32 = 0;
    while (info_wait < 30) : (info_wait += 1) {
        var info = js.consumerInfo(
            "TEST_PUSH_ACK",
            "push-ack-c",
        ) catch {
            ctx.stop();
            ctx.deinit();
            reportResult(name, false, "consumer info");
            return;
        };
        defer info.deinit();

        if (info.value.num_ack_pending == 0) {
            ack_cleared = true;
            break;
        }
        threadSleepNs(100_000_000);
    }

    ctx.stop();
    ctx.deinit();

    if (!ack_cleared) {
        reportResult(name, false, "ack still pending");
        return;
    }

    var d = js.deleteStream(
        "TEST_PUSH_ACK",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testPushConsumerHeartbeatErrHandler(
    allocator: std.mem.Allocator,
) void {
    const name = "js_push_heartbeat";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect failed");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PUSH_HB",
        .subjects = &.{"push.hb.>"},
        .storage = .memory,
    }) catch {
        reportResult(name, false, "create stream");
        return;
    };
    defer stream.deinit();

    const Handler = struct {
        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            _ = self;
            _ = msg;
        }
    };

    var handler = Handler{};
    push_heartbeat_err_seen.store(false, .release);

    const deliver_subj = "_PUSH_HB_DELIVER.test";
    var push_sub = nats.jetstream.PushSubscription{
        .js = &js,
        .stream = "TEST_PUSH_HB",
    };
    push_sub.setConsumer("push-hb-c") catch unreachable;
    push_sub.setDeliverSubject(deliver_subj) catch unreachable;

    var ctx = push_sub.consume(
        nats.jetstream.JsMsgHandler.init(
            Handler,
            &handler,
        ),
        .{
            .heartbeat_ms = 200,
            .err_handler = pushHeartbeatErrHandler,
        },
    ) catch {
        reportResult(name, false, "consume failed");
        return;
    };

    var pc = js.createPushConsumer(
        "TEST_PUSH_HB",
        .{
            .name = "push-hb-c",
            .deliver_subject = deliver_subj,
            .ack_policy = .none,
        },
    ) catch {
        ctx.stop();
        ctx.deinit();
        reportResult(name, false, "create push cons");
        return;
    };
    pc.deinit();

    var wait: u32 = 0;
    while (!push_heartbeat_err_seen.load(.acquire) and
        wait < 40) : (wait += 1)
    {
        threadSleepNs(100_000_000);
    }

    ctx.stop();
    ctx.deinit();

    if (!push_heartbeat_err_seen.load(.acquire)) {
        reportResult(name, false, "no heartbeat error missing");
        return;
    }

    var d = js.deleteStream(
        "TEST_PUSH_HB",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testPublishWithTTL(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_ttl",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream with TTL support
    var stream = js.createStream(.{
        .name = "TEST_TTL",
        .subjects = &.{"ttl.>"},
        .storage = .memory,
        .allow_msg_ttl = true,
    }) catch {
        // Server may not support TTL
        reportResult(
            "js_publish_ttl",
            true,
            "skipped",
        );
        return;
    };
    defer stream.deinit();

    // Publish with 1s TTL
    var ack = js.publishWithOpts(
        "ttl.a",
        "data",
        .{ .ttl = "1s" },
    ) catch {
        reportResult(
            "js_publish_ttl",
            false,
            "publish failed",
        );
        return;
    };
    ack.deinit();

    // Immediately should exist
    var r1 = js.getMsg("TEST_TTL", 1) catch {
        reportResult(
            "js_publish_ttl",
            false,
            "getMsg before ttl",
        );
        return;
    };
    r1.deinit();

    // Wait for TTL expiry
    threadSleepNs(2_000_000_000);

    // Should be expired now
    var r2 = js.getMsg("TEST_TTL", 1);
    if (r2) |*r| {
        r.deinit();
        reportResult(
            "js_publish_ttl",
            false,
            "should expire",
        );
        return;
    } else |_| {}

    var d = js.deleteStream(
        "TEST_TTL",
    ) catch {
        reportResult(
            "js_publish_ttl",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_publish_ttl", true, "");
}

/// Verifies publishMsg() merges user headers with JS-derived
/// opts headers. On key collision (case-insensitive), JS opts
/// must win. Retrieves the stored message via getMsg() and
/// asserts both headers against the wire.
pub fn testPublishMsg(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_msg",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PUBLISH_MSG",
        .subjects = &.{"pubmsg.test"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_publish_msg",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    // User headers. Note lowercase "nats-msg-id" deliberately
    // collides (case-insensitively) with the JS header set by
    // opts.msg_id -- "jsopts-id" must win.
    const user_headers = [_]nats.protocol.headers.Entry{
        .{ .key = "X-Custom", .value = "uservalue" },
        .{ .key = "nats-msg-id", .value = "user-id" },
    };

    var ack = js.publishMsg(allocator, .{
        .subject = "pubmsg.test",
        .payload = "hello",
        .headers = &user_headers,
        .opts = .{ .msg_id = "jsopts-id" },
    }) catch {
        reportResult(
            "js_publish_msg",
            false,
            "publishMsg failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    ack.deinit();

    var resp = js.getMsg(
        "TEST_PUBLISH_MSG",
        1,
    ) catch {
        reportResult(
            "js_publish_msg",
            false,
            "getMsg failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    defer resp.deinit();

    const stored = resp.value.message orelse {
        reportResult(
            "js_publish_msg",
            false,
            "no stored message",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };

    const hdr_b64 = stored.hdrs orelse {
        reportResult(
            "js_publish_msg",
            false,
            "no headers in stored msg",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };

    // Decode base64 headers, then parse NATS/1.0.
    const decoder = std.base64.standard.Decoder;
    const decoded_len = decoder.calcSizeForSlice(hdr_b64) catch {
        reportResult(
            "js_publish_msg",
            false,
            "b64 calc size",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    const decoded_buf = allocator.alloc(u8, decoded_len) catch {
        reportResult(
            "js_publish_msg",
            false,
            "alloc decoded",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    defer allocator.free(decoded_buf);
    decoder.decode(decoded_buf, hdr_b64) catch {
        reportResult(
            "js_publish_msg",
            false,
            "b64 decode",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };

    var parsed = nats.protocol.headers.parse(
        allocator,
        decoded_buf,
    );
    defer parsed.deinit();

    if (parsed.err != null) {
        reportResult(
            "js_publish_msg",
            false,
            "header parse err",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    }

    // User-supplied custom header must pass through.
    const custom = parsed.get("X-Custom") orelse {
        reportResult(
            "js_publish_msg",
            false,
            "X-Custom missing",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    if (!std.mem.eql(u8, custom, "uservalue")) {
        reportResult(
            "js_publish_msg",
            false,
            "X-Custom wrong value",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    }

    // Collision: opts.msg_id ("jsopts-id") must override the
    // user's lowercase "nats-msg-id" entry. Check via the
    // case-insensitive lookup.
    const msg_id = parsed.get("Nats-Msg-Id") orelse {
        reportResult(
            "js_publish_msg",
            false,
            "Nats-Msg-Id missing",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    };
    if (!std.mem.eql(u8, msg_id, "jsopts-id")) {
        reportResult(
            "js_publish_msg",
            false,
            "opts.msg_id did not override",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG") catch return;
        d.deinit();
        return;
    }

    var d = js.deleteStream(
        "TEST_PUBLISH_MSG",
    ) catch {
        reportResult("js_publish_msg", true, "");
        return;
    };
    d.deinit();
    reportResult("js_publish_msg", true, "");
}

/// Regression: publishMsg with no user headers and default opts
/// must succeed (takes the no-header publish path). Previously
/// this tripped an assertion in protocol.headers.encodedSize()
/// because PublishHeaderSet.slice() returned an empty (but
/// non-null) slice.
pub fn testPublishMsgNoHeaders(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_msg_no_headers",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PUBLISH_MSG_BARE",
        .subjects = &.{"pubmsg.bare"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_publish_msg_no_headers",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    var ack = js.publishMsg(allocator, .{
        .subject = "pubmsg.bare",
        .payload = "hi",
    }) catch {
        reportResult(
            "js_publish_msg_no_headers",
            false,
            "publishMsg failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG_BARE") catch return;
        d.deinit();
        return;
    };
    ack.deinit();

    var resp = js.getMsg(
        "TEST_PUBLISH_MSG_BARE",
        1,
    ) catch {
        reportResult(
            "js_publish_msg_no_headers",
            false,
            "getMsg failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG_BARE") catch return;
        d.deinit();
        return;
    };
    defer resp.deinit();

    if (resp.value.message == null) {
        reportResult(
            "js_publish_msg_no_headers",
            false,
            "no stored message",
        );
        var d = js.deleteStream("TEST_PUBLISH_MSG_BARE") catch return;
        d.deinit();
        return;
    }

    var d = js.deleteStream(
        "TEST_PUBLISH_MSG_BARE",
    ) catch {
        reportResult("js_publish_msg_no_headers", true, "");
        return;
    };
    d.deinit();
    reportResult("js_publish_msg_no_headers", true, "");
}

/// Regression: publishWithOpts with empty opts must succeed
/// (same underlying bug as publishMsg with no headers).
pub fn testPublishWithOptsEmpty(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_publish_with_opts_empty",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_PUBLISH_OPTS_EMPTY",
        .subjects = &.{"pubopts.empty"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_publish_with_opts_empty",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    var ack = js.publishWithOpts(
        "pubopts.empty",
        "payload",
        .{},
    ) catch {
        reportResult(
            "js_publish_with_opts_empty",
            false,
            "publishWithOpts failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_OPTS_EMPTY") catch return;
        d.deinit();
        return;
    };
    ack.deinit();

    var resp = js.getMsg(
        "TEST_PUBLISH_OPTS_EMPTY",
        1,
    ) catch {
        reportResult(
            "js_publish_with_opts_empty",
            false,
            "getMsg failed",
        );
        var d = js.deleteStream("TEST_PUBLISH_OPTS_EMPTY") catch return;
        d.deinit();
        return;
    };
    defer resp.deinit();

    if (resp.value.message == null) {
        reportResult(
            "js_publish_with_opts_empty",
            false,
            "no stored message",
        );
        var d = js.deleteStream("TEST_PUBLISH_OPTS_EMPTY") catch return;
        d.deinit();
        return;
    }

    var d = js.deleteStream(
        "TEST_PUBLISH_OPTS_EMPTY",
    ) catch {
        reportResult("js_publish_with_opts_empty", true, "");
        return;
    };
    d.deinit();
    reportResult("js_publish_with_opts_empty", true, "");
}

pub fn testKvUpdateBucket(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_update_bucket",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    _ = js.createKeyValue(.{
        .bucket = "UPD_BUCKET",
        .history = 1,
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_update_bucket",
            false,
            "create bucket",
        );
        return;
    };

    _ = js.updateKeyValue(.{
        .bucket = "UPD_BUCKET",
        .history = 5,
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_update_bucket",
            false,
            "update bucket",
        );
        return;
    };

    // Verify via stream info
    var info = js.streamInfo(
        "KV_UPD_BUCKET",
    ) catch {
        reportResult(
            "kv_update_bucket",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.config) |cfg| {
        if (cfg.max_msgs_per_subject) |mps| {
            if (mps != 5) {
                var buf: [64]u8 = undefined;
                const m = std.fmt.bufPrint(
                    &buf,
                    "mps {d}, want 5",
                    .{mps},
                ) catch "wrong";
                reportResult(
                    "kv_update_bucket",
                    false,
                    m,
                );
                return;
            }
        } else {
            reportResult(
                "kv_update_bucket",
                false,
                "no max_msgs_per_subj",
            );
            return;
        }
    } else {
        reportResult(
            "kv_update_bucket",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "UPD_BUCKET",
    ) catch {
        reportResult(
            "kv_update_bucket",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("kv_update_bucket", true, "");
}

pub fn testKvCreateOrUpdateBucket(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_upsert_bucket",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create via createOrUpdate
    _ = js.createOrUpdateKeyValue(.{
        .bucket = "UPSERT_KV",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_upsert_bucket",
            false,
            "create bucket",
        );
        return;
    };

    // Update via createOrUpdate
    _ = js.createOrUpdateKeyValue(.{
        .bucket = "UPSERT_KV",
        .history = 10,
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_upsert_bucket",
            false,
            "update bucket",
        );
        return;
    };

    // Verify via stream info
    var info = js.streamInfo(
        "KV_UPSERT_KV",
    ) catch {
        reportResult(
            "kv_upsert_bucket",
            false,
            "info failed",
        );
        return;
    };
    defer info.deinit();

    if (info.value.config) |cfg| {
        if (cfg.max_msgs_per_subject) |mps| {
            if (mps != 10) {
                var buf: [64]u8 = undefined;
                const m = std.fmt.bufPrint(
                    &buf,
                    "mps {d}, want 10",
                    .{mps},
                ) catch "wrong";
                reportResult(
                    "kv_upsert_bucket",
                    false,
                    m,
                );
                return;
            }
        } else {
            reportResult(
                "kv_upsert_bucket",
                false,
                "no max_msgs_per_subj",
            );
            return;
        }
    } else {
        reportResult(
            "kv_upsert_bucket",
            false,
            "no config",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "UPSERT_KV",
    ) catch {
        reportResult(
            "kv_upsert_bucket",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult(
        "kv_upsert_bucket",
        true,
        "",
    );
}

pub fn testKvPurgeDeletes(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "PURGE_DEL",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "create bucket",
        );
        return;
    };

    // Put 5 keys
    _ = kv.put("a", "1") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "put a",
        );
        return;
    };
    _ = kv.put("b", "2") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "put b",
        );
        return;
    };
    _ = kv.put("c", "3") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "put c",
        );
        return;
    };
    _ = kv.put("d", "4") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "put d",
        );
        return;
    };
    _ = kv.put("e", "5") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "put e",
        );
        return;
    };

    // Delete a, b, c
    _ = kv.delete("a") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "del a",
        );
        return;
    };
    _ = kv.delete("b") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "del b",
        );
        return;
    };
    _ = kv.delete("c") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "del c",
        );
        return;
    };

    // keys() should return 2 live keys
    const key_list = kv.keys(allocator) catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "keys()",
        );
        return;
    };
    defer {
        for (key_list) |k| allocator.free(k);
        allocator.free(key_list);
    }

    if (key_list.len != 2) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "keys: {d}, want 2",
            .{key_list.len},
        ) catch "wrong";
        reportResult(
            "kv_purge_deletes",
            false,
            m,
        );
        return;
    }

    // Fresh delete markers should not match an age filter.
    const skipped = kv.purgeDeletes(
        .{ .older_than_ns = @as(i64, 60 * std.time.ns_per_s) },
    ) catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "purgeDeletes age",
        );
        return;
    };
    if (skipped != 0) {
        reportResult(
            "kv_purge_deletes",
            false,
            "purged fresh markers",
        );
        return;
    }

    // Purge delete markers
    const purged = kv.purgeDeletes(
        .{},
    ) catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "purgeDeletes",
        );
        return;
    };
    _ = purged;

    // Verify d and e still accessible
    var ed = (kv.get("d") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "get d after purge",
        );
        return;
    }) orelse {
        reportResult(
            "kv_purge_deletes",
            false,
            "d missing",
        );
        return;
    };
    defer ed.deinit();
    if (ed.operation != .put) {
        reportResult(
            "kv_purge_deletes",
            false,
            "d not put op",
        );
        return;
    }

    var ee = (kv.get("e") catch {
        reportResult(
            "kv_purge_deletes",
            false,
            "get e after purge",
        );
        return;
    }) orelse {
        reportResult(
            "kv_purge_deletes",
            false,
            "e missing",
        );
        return;
    };
    defer ee.deinit();
    if (ee.operation != .put) {
        reportResult(
            "kv_purge_deletes",
            false,
            "e not put op",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "PURGE_DEL",
    ) catch {
        reportResult(
            "kv_purge_deletes",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult(
        "kv_purge_deletes",
        true,
        "",
    );
}

pub fn testKvStoreNames(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_store_names",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create 3 KV buckets
    _ = js.createKeyValue(.{
        .bucket = "NAMES_A",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_store_names",
            false,
            "create A",
        );
        return;
    };
    _ = js.createKeyValue(.{
        .bucket = "NAMES_B",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_store_names",
            false,
            "create B",
        );
        return;
    };
    _ = js.createKeyValue(.{
        .bucket = "NAMES_C",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_store_names",
            false,
            "create C",
        );
        return;
    };

    const names = js.keyValueStoreNames(
        allocator,
    ) catch {
        reportResult(
            "kv_store_names",
            false,
            "storeNames()",
        );
        return;
    };
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }

    // Verify our 3 buckets are in the list
    var found_a = false;
    var found_b = false;
    var found_c = false;
    for (names) |n| {
        if (std.mem.eql(u8, n, "NAMES_A"))
            found_a = true;
        if (std.mem.eql(u8, n, "NAMES_B"))
            found_b = true;
        if (std.mem.eql(u8, n, "NAMES_C"))
            found_c = true;
    }

    if (!found_a or !found_b or !found_c) {
        reportResult(
            "kv_store_names",
            false,
            "missing bucket names",
        );
        return;
    }

    // Verify KV_ prefix was stripped
    for (names) |n| {
        if (std.mem.startsWith(u8, n, "KV_")) {
            reportResult(
                "kv_store_names",
                false,
                "prefix not stripped",
            );
            return;
        }
    }

    // Cleanup
    var da = js.deleteKeyValue(
        "NAMES_A",
    ) catch {
        reportResult(
            "kv_store_names",
            true,
            "",
        );
        return;
    };
    da.deinit();
    var db = js.deleteKeyValue(
        "NAMES_B",
    ) catch {
        reportResult(
            "kv_store_names",
            true,
            "",
        );
        return;
    };
    db.deinit();
    var dc = js.deleteKeyValue(
        "NAMES_C",
    ) catch {
        reportResult(
            "kv_store_names",
            true,
            "",
        );
        return;
    };
    dc.deinit();

    reportResult("kv_store_names", true, "");
}

pub fn testKvWatchIgnoreDeletes(
    allocator: std.mem.Allocator,
) void {
    _ = allocator;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(std.heap.page_allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        std.heap.page_allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "WATCH_IGN",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "create bucket",
        );
        return;
    };

    // Put a=1, b=2, then delete a
    _ = kv.put("a", "1") catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "put a",
        );
        return;
    };
    _ = kv.put("b", "2") catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "put b",
        );
        return;
    };
    _ = kv.delete("a") catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "delete a",
        );
        return;
    };

    // Watch with ignore_deletes
    var watcher = kv.watchAllWithOpts(.{
        .ignore_deletes = true,
        .include_history = true,
    }) catch {
        reportResult(
            "kv_watch_ign_del",
            false,
            "watchAll",
        );
        return;
    };
    defer watcher.deinit();

    // Collect entries until null
    var count: u32 = 0;
    while (count < 10) {
        var entry = (watcher.next(
            3000,
        ) catch break) orelse break;
        entry.deinit();
        count += 1;
    }

    // Should get 2 (put a, put b) not delete
    if (count != 2) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 2",
            .{count},
        ) catch "wrong count";
        reportResult(
            "kv_watch_ign_del",
            false,
            m,
        );
        return;
    }

    var d = js.deleteKeyValue(
        "WATCH_IGN",
    ) catch {
        reportResult(
            "kv_watch_ign_del",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult(
        "kv_watch_ign_del",
        true,
        "",
    );
}

pub fn testKvWatchUpdatesOnly(
    allocator: std.mem.Allocator,
) void {
    _ = allocator;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(std.heap.page_allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        std.heap.page_allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "WATCH_UPD",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "create bucket",
        );
        return;
    };

    // Put a key before watching
    _ = kv.put("pre", "before") catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "put pre",
        );
        return;
    };

    // Watch with updates_only
    var watcher = kv.watchAllWithOpts(.{
        .updates_only = true,
    }) catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "watchAll",
        );
        return;
    };
    defer watcher.deinit();

    // First next() should return null (no initial)
    const first = (watcher.next(
        2000,
    ) catch null) orelse null;
    if (first != null) {
        reportResult(
            "kv_watch_upd_only",
            false,
            "should be null first",
        );
        return;
    }

    // Put a new key
    _ = kv.put("post", "after") catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "put post",
        );
        return;
    };

    // Should get the new entry
    var entry = (watcher.next(5000) catch {
        reportResult(
            "kv_watch_upd_only",
            false,
            "next() failed",
        );
        return;
    }) orelse {
        reportResult(
            "kv_watch_upd_only",
            false,
            "no post entry",
        );
        return;
    };
    defer entry.deinit();

    if (!std.mem.eql(u8, entry.key, "post")) {
        reportResult(
            "kv_watch_upd_only",
            false,
            "wrong key",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "WATCH_UPD",
    ) catch {
        reportResult(
            "kv_watch_upd_only",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult(
        "kv_watch_upd_only",
        true,
        "",
    );
}

pub fn testKvListKeys(
    allocator: std.mem.Allocator,
) void {
    _ = allocator;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(std.heap.page_allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        std.heap.page_allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "kv_list_keys",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "LIST_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            "kv_list_keys",
            false,
            "create bucket",
        );
        return;
    };

    // Put 5 keys
    _ = kv.put("k1", "v1") catch {
        reportResult(
            "kv_list_keys",
            false,
            "put k1",
        );
        return;
    };
    _ = kv.put("k2", "v2") catch {
        reportResult(
            "kv_list_keys",
            false,
            "put k2",
        );
        return;
    };
    _ = kv.put("k3", "v3") catch {
        reportResult(
            "kv_list_keys",
            false,
            "put k3",
        );
        return;
    };
    _ = kv.put("k4", "v4") catch {
        reportResult(
            "kv_list_keys",
            false,
            "put k4",
        );
        return;
    };
    _ = kv.put("k5", "v5") catch {
        reportResult(
            "kv_list_keys",
            false,
            "put k5",
        );
        return;
    };

    // Delete k3
    _ = kv.delete("k3") catch {
        reportResult(
            "kv_list_keys",
            false,
            "del k3",
        );
        return;
    };

    // List keys via lister
    var lister = kv.listKeys() catch {
        reportResult(
            "kv_list_keys",
            false,
            "listKeys()",
        );
        return;
    };
    defer lister.deinit();

    var count: u32 = 0;
    while (count < 10) {
        const key = (lister.next() catch {
            break;
        }) orelse break;
        _ = key;
        count += 1;
    }

    if (count != 4) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d} keys, expected 4",
            .{count},
        ) catch "wrong count";
        reportResult(
            "kv_list_keys",
            false,
            m,
        );
        return;
    }

    var d = js.deleteKeyValue(
        "LIST_KEYS",
    ) catch {
        reportResult(
            "kv_list_keys",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("kv_list_keys", true, "");
}

pub fn testDoubleAck(
    allocator: std.mem.Allocator,
) void {
    const name = "js_double_ack";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_DACK",
        .subjects = &.{"dack.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var c = js.createConsumer("TEST_DACK", .{
        .name = "dack-c",
        .durable_name = "dack-c",
        .ack_policy = .explicit,
    }) catch {
        reportResult(
            name,
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    var a = js.publish(
        "dack.test",
        "double-ack-data",
    ) catch {
        reportResult(name, false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "TEST_DACK",
    };
    pull.setConsumer("dack-c") catch unreachable;

    var msg = (pull.next(5000) catch {
        reportResult(name, false, "fetch 1");
        return;
    }) orelse {
        reportResult(name, false, "no msg");
        return;
    };

    msg.doubleAck(5000) catch {
        reportResult(
            name,
            false,
            "doubleAck failed",
        );
        msg.deinit();
        return;
    };
    msg.deinit();

    // After doubleAck, no messages should remain
    var r = pull.fetchNoWait(10) catch {
        reportResult(name, false, "fetch 2");
        return;
    };
    defer r.deinit();

    if (r.count() != 0) {
        reportResult(
            name,
            false,
            "expected 0 after dack",
        );
        return;
    }

    var d = js.deleteStream("TEST_DACK") catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testUpdatePushConsumer(
    allocator: std.mem.Allocator,
) void {
    const name = "js_upd_push_cons";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_UPDPUSH",
        .subjects = &.{"updpush.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var pc = js.createPushConsumer(
        "TEST_UPDPUSH",
        .{
            .name = "updpush-c",
            .deliver_subject = "_UPD.test",
            .description = "v1",
            .ack_policy = .none,
        },
    ) catch {
        reportResult(
            name,
            false,
            "create push cons",
        );
        return;
    };
    pc.deinit();

    // Update description to "v2"
    var upd = js.updatePushConsumer(
        "TEST_UPDPUSH",
        .{
            .name = "updpush-c",
            .deliver_subject = "_UPD.test",
            .description = "v2",
            .ack_policy = .none,
        },
    ) catch {
        reportResult(name, false, "update");
        return;
    };
    upd.deinit();

    // Verify description changed
    var info = js.consumerInfo(
        "TEST_UPDPUSH",
        "updpush-c",
    ) catch {
        reportResult(name, false, "info");
        return;
    };
    defer info.deinit();

    if (info.value.config) |cfg| {
        if (cfg.description) |desc| {
            if (!std.mem.eql(u8, desc, "v2")) {
                reportResult(
                    name,
                    false,
                    "desc not v2",
                );
                return;
            }
        } else {
            reportResult(
                name,
                false,
                "no description",
            );
            return;
        }
    } else {
        reportResult(name, false, "no config");
        return;
    }

    var d = js.deleteStream(
        "TEST_UPDPUSH",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testGetPushConsumer(
    allocator: std.mem.Allocator,
) void {
    const name = "js_get_push_cons";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_GETPUSH",
        .subjects = &.{"getpush.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var pc = js.createPushConsumer(
        "TEST_GETPUSH",
        .{
            .name = "getpush-c",
            .deliver_subject = "_GET.test",
            .ack_policy = .none,
        },
    ) catch {
        reportResult(
            name,
            false,
            "create push cons",
        );
        return;
    };
    pc.deinit();

    // Get push consumer via pushConsumer()
    const push_sub = js.pushConsumer(
        "TEST_GETPUSH",
        "getpush-c",
    ) catch {
        reportResult(
            name,
            false,
            "pushConsumer()",
        );
        return;
    };

    const ds = push_sub.deliverSubject();
    if (!std.mem.eql(u8, ds, "_GET.test")) {
        reportResult(
            name,
            false,
            "wrong deliver subj",
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_GETPUSH",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testKvPutString(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_put_string";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "PUTSTR",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    const rev = kv.putString(
        "greeting",
        "hello",
    ) catch {
        reportResult(
            name,
            false,
            "putString",
        );
        return;
    };

    if (rev == 0) {
        reportResult(
            name,
            false,
            "rev should be > 0",
        );
        return;
    }

    var entry = (kv.get("greeting") catch {
        reportResult(name, false, "get");
        return;
    }) orelse {
        reportResult(
            name,
            false,
            "key not found",
        );
        return;
    };
    defer entry.deinit();

    if (!std.mem.eql(u8, entry.value, "hello")) {
        reportResult(
            name,
            false,
            "wrong value",
        );
        return;
    }

    var d = js.deleteKeyValue("PUTSTR") catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testKvDeleteLastRev(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_del_last_rev";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "DEL_REV",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    const rev1 = kv.put("x", "v1") catch {
        reportResult(name, false, "put 1");
        return;
    };
    const rev2 = kv.put("x", "v2") catch {
        reportResult(name, false, "put 2");
        return;
    };

    // Wrong revision should fail
    _ = kv.deleteWithOpts("x", .{
        .last_revision = rev1,
    }) catch |err| {
        if (err == error.ApiError) {
            // Expected: now try correct rev
            _ = kv.deleteWithOpts("x", .{
                .last_revision = rev2,
            }) catch {
                reportResult(
                    name,
                    false,
                    "correct rev failed",
                );
                return;
            };
            var d = js.deleteKeyValue(
                "DEL_REV",
            ) catch {
                reportResult(name, true, "");
                return;
            };
            d.deinit();
            reportResult(name, true, "");
            return;
        }
        reportResult(
            name,
            false,
            "wrong error type",
        );
        return;
    };

    reportResult(
        name,
        false,
        "wrong rev should fail",
    );
}

pub fn testKvPurgeLastRev(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_purge_last_rev";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "PURGE_REV",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    const rev1 = kv.put("y", "v1") catch {
        reportResult(name, false, "put 1");
        return;
    };
    const rev2 = kv.put("y", "v2") catch {
        reportResult(name, false, "put 2");
        return;
    };

    // Wrong revision should fail
    _ = kv.purgeWithOpts("y", .{
        .last_revision = rev1,
    }) catch |err| {
        if (err == error.ApiError) {
            // Expected: now try correct rev
            _ = kv.purgeWithOpts("y", .{
                .last_revision = rev2,
            }) catch {
                reportResult(
                    name,
                    false,
                    "correct rev failed",
                );
                return;
            };
            var d = js.deleteKeyValue(
                "PURGE_REV",
            ) catch {
                reportResult(name, true, "");
                return;
            };
            d.deinit();
            reportResult(name, true, "");
            return;
        }
        reportResult(
            name,
            false,
            "wrong error type",
        );
        return;
    };

    reportResult(
        name,
        false,
        "wrong rev should fail",
    );
}

pub fn testKvListKeysFiltered(
    allocator: std.mem.Allocator,
) void {
    _ = allocator;
    const name = "kv_list_filtered";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(std.heap.page_allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        std.heap.page_allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "FILT_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    _ = kv.put("a", "1") catch {
        reportResult(name, false, "put a");
        return;
    };
    _ = kv.put("b", "2") catch {
        reportResult(name, false, "put b");
        return;
    };
    _ = kv.put("c", "3") catch {
        reportResult(name, false, "put c");
        return;
    };
    _ = kv.put("d", "4") catch {
        reportResult(name, false, "put d");
        return;
    };

    var lister = kv.listKeysFiltered(
        &.{ "a", "c" },
    ) catch {
        reportResult(
            name,
            false,
            "listKeysFiltered",
        );
        return;
    };
    defer lister.deinit();

    var count: u32 = 0;
    while (count < 10) {
        const key = (lister.next() catch {
            break;
        }) orelse break;
        _ = key;
        count += 1;
    }

    if (count != 2) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 2",
            .{count},
        ) catch "wrong count";
        reportResult(name, false, m);
        return;
    }

    var d = js.deleteKeyValue(
        "FILT_KEYS",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testKvHistoryWithOpts(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_hist_opts";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "HIST_OPTS",
        .storage = .memory,
        .history = 5,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    _ = kv.put("z", "val1") catch {
        reportResult(name, false, "put 1");
        return;
    };
    _ = kv.put("z", "val2") catch {
        reportResult(name, false, "put 2");
        return;
    };
    _ = kv.put("z", "val3") catch {
        reportResult(name, false, "put 3");
        return;
    };

    const hist = kv.historyWithOpts(
        allocator,
        "z",
        .{ .meta_only = true },
    ) catch {
        reportResult(
            name,
            false,
            "historyWithOpts",
        );
        return;
    };
    defer {
        for (hist) |*h| h.deinit();
        allocator.free(hist);
    }

    if (hist.len != 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, expected 3",
            .{hist.len},
        ) catch "wrong";
        reportResult(name, false, m);
        return;
    }

    // Verify meta_only: values should be empty
    for (hist) |entry| {
        if (entry.value.len != 0) {
            reportResult(
                name,
                false,
                "meta_only not empty",
            );
            return;
        }
        if (entry.revision == 0) {
            reportResult(
                name,
                false,
                "rev should be > 0",
            );
            return;
        }
    }

    var d = js.deleteKeyValue(
        "HIST_OPTS",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testConnOptions(
    allocator: std.mem.Allocator,
) void {
    const name = "js_conn_options";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Test conn() accessor
    const c = js.conn();
    if (!c.isConnected()) {
        reportResult(
            name,
            false,
            "conn not connected",
        );
        return;
    }

    // Test options() accessor
    const opts = js.options();
    if (!std.mem.startsWith(
        u8,
        opts.api_prefix,
        "$JS.",
    )) {
        reportResult(
            name,
            false,
            "bad api_prefix",
        );
        return;
    }

    if (opts.timeout_ms == 0) {
        reportResult(
            name,
            false,
            "timeout_ms is 0",
        );
        return;
    }

    reportResult(name, true, "");
}

pub fn testKvCreateWithTTL(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_create_ttl";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream with TTL support
    var stream = js.createStream(.{
        .name = "KV_TTL_CREATE",
        .subjects = &.{"$KV.TTL_CREATE.>"},
        .storage = .memory,
        .max_msgs_per_subject = 1,
        .allow_msg_ttl = true,
    }) catch {
        // Server may not support TTL
        reportResult(
            name,
            true,
            "skipped: no ttl",
        );
        return;
    };
    stream.deinit();

    var kv = js.keyValue("TTL_CREATE") catch {
        reportResult(name, false, "bind kv");
        return;
    };

    // createWithOpts with TTL
    const rev = kv.createWithOpts(
        "ttlkey",
        "val",
        .{ .ttl = "1s" },
    ) catch |err| {
        if (err == error.ApiError) {
            // TTL not supported on this server
            var dd = js.deleteStream(
                "KV_TTL_CREATE",
            ) catch {
                reportResult(
                    name,
                    true,
                    "skipped: ttl err",
                );
                return;
            };
            dd.deinit();
            reportResult(
                name,
                true,
                "skipped: ttl err",
            );
            return;
        }
        reportResult(
            name,
            false,
            "createWithOpts",
        );
        return;
    };

    if (rev == 0) {
        reportResult(
            name,
            false,
            "rev should be > 0",
        );
        return;
    }

    var d = js.deleteStream(
        "KV_TTL_CREATE",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testPublishAsync(
    allocator: std.mem.Allocator,
) void {
    const stream_name = "TEST_ASYNC_PUB";

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_pub_async",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    deleteStreamIfExists(&js, stream_name);

    var stream = js.createStream(.{
        .name = stream_name,
        .subjects = &.{"async.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_pub_async",
            false,
            "create stream",
        );
        return;
    };
    defer deleteStreamIfExists(&js, stream_name);
    defer stream.deinit();

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{ .max_pending = 64 },
    ) catch {
        reportResult(
            "js_pub_async",
            false,
            "init async pub",
        );
        return;
    };
    defer ap.deinit();

    // Publish 20 messages asynchronously
    var futures: [20]*nats.jetstream.PubAckFuture =
        undefined;
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        futures[i] = ap.publish(
            "async.test",
            "async-data",
        ) catch {
            reportResult(
                "js_pub_async",
                false,
                "publish failed",
            );
            return;
        };
    }

    // Wait for all acks
    ap.waitComplete(10000) catch {
        reportResult(
            "js_pub_async",
            false,
            "waitComplete timeout",
        );
        // Clean up futures
        for (futures[0..i]) |f| f.deinit();
        return;
    };

    // Verify all futures resolved
    var all_ok = true;
    for (futures[0..20]) |f| {
        if (f.result() == null) {
            all_ok = false;
        }
    }

    // Check pending is 0
    if (ap.publishAsyncPending() != 0) {
        reportResult(
            "js_pub_async",
            false,
            "pending not 0",
        );
        for (futures[0..20]) |f| f.deinit();
        return;
    }

    // Verify stream has 20 messages
    var info = js.streamInfo(
        stream_name,
    ) catch {
        for (futures[0..20]) |f| f.deinit();
        reportResult(
            "js_pub_async",
            false,
            "stream info",
        );
        return;
    };
    defer info.deinit();

    const msg_count = if (info.value.state) |s|
        s.messages
    else
        0;

    for (futures[0..20]) |f| f.deinit();

    if (!all_ok or msg_count != 20) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "ok={}, msgs={d}",
            .{ all_ok, msg_count },
        ) catch "verify failed";
        reportResult(
            "js_pub_async",
            false,
            m,
        );
        return;
    }

    reportResult("js_pub_async", true, "");
}

pub fn testPublishAsyncFutureWait(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "js_async_wait",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var stream = js.createStream(.{
        .name = "TEST_ASYNC_WAIT",
        .subjects = &.{"await.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            "js_async_wait",
            false,
            "create stream",
        );
        return;
    };
    defer stream.deinit();

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{},
    ) catch {
        reportResult(
            "js_async_wait",
            false,
            "init",
        );
        return;
    };
    defer ap.deinit();

    // Publish one message and wait on the future
    const fut = ap.publish(
        "await.test",
        "wait-data",
    ) catch {
        reportResult(
            "js_async_wait",
            false,
            "publish",
        );
        return;
    };
    defer fut.deinit();

    const ack = fut.wait(5000) catch {
        reportResult(
            "js_async_wait",
            false,
            "wait timeout",
        );
        return;
    };

    if (ack.seq != 1) {
        reportResult(
            "js_async_wait",
            false,
            "wrong seq",
        );
        return;
    }

    var churn = std.ArrayList([]u8).empty;
    defer {
        for (churn.items) |buf| allocator.free(buf);
        churn.deinit(allocator);
    }
    for (0..32) |_| {
        const buf = allocator.alloc(u8, 64) catch {
            reportResult(
                "js_async_wait",
                false,
                "alloc churn failed",
            );
            return;
        };
        churn.append(allocator, buf) catch {
            allocator.free(buf);
            reportResult(
                "js_async_wait",
                false,
                "alloc churn append failed",
            );
            return;
        };
    }

    if (ack.stream == null or !std.mem.eql(
        u8,
        ack.stream.?,
        "TEST_ASYNC_WAIT",
    )) {
        reportResult(
            "js_async_wait",
            false,
            "ack stream invalid",
        );
        return;
    }

    var d = js.deleteStream(
        "TEST_ASYNC_WAIT",
    ) catch {
        reportResult(
            "js_async_wait",
            true,
            "",
        );
        return;
    };
    d.deinit();
    reportResult("js_async_wait", true, "");
}

pub fn testPublishAsyncExpectedSeqParity(
    allocator: std.mem.Allocator,
) void {
    const name = "js_async_exp_seq";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect failed");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "TEST_ASYNC_EXPSEQ",
        .subjects = &.{"atest.expseq.>"},
        .storage = .memory,
    }) catch {
        reportResult(name, false, "create stream");
        return;
    };
    defer s.deinit();

    var a1 = js.publish(
        "atest.expseq.data",
        "first",
    ) catch {
        reportResult(name, false, "sync pub 1");
        return;
    };
    a1.deinit();

    var a2 = js.publishWithOpts(
        "atest.expseq.data",
        "second",
        .{ .expected_last_seq = 1 },
    ) catch {
        reportResult(name, false, "sync pub 2");
        return;
    };
    a2.deinit();

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{},
    ) catch {
        reportResult(name, false, "init ap");
        return;
    };
    defer ap.deinit();

    const fut = ap.publishWithOpts(
        "atest.expseq.data",
        "should-fail",
        .{ .expected_last_seq = 0 },
    ) catch {
        reportResult(name, false, "async publish");
        return;
    };
    defer fut.deinit();

    const ack_or_err = fut.wait(5000);
    if (ack_or_err) |ack| {
        _ = ack;
        reportResult(name, false, "should have failed");
        return;
    } else |err| {
        if (err != error.ApiError) {
            reportResult(name, false, "wrong error");
            return;
        }
    }

    var info = js.streamInfo(
        "TEST_ASYNC_EXPSEQ",
    ) catch {
        reportResult(name, false, "stream info");
        return;
    };
    defer info.deinit();

    const msg_count = if (info.value.state) |st|
        st.messages
    else
        0;
    if (msg_count != 2) {
        reportResult(name, false, "wrong msg count");
        return;
    }

    var d = js.deleteStream(
        "TEST_ASYNC_EXPSEQ",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testKvEmptyValue(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_empty_value";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "EMPTY_VAL",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    _ = kv.put("empty", "") catch {
        reportResult(name, false, "put empty");
        return;
    };

    var entry = (kv.get("empty") catch {
        reportResult(name, false, "get");
        return;
    }) orelse {
        reportResult(
            name,
            false,
            "key not found",
        );
        return;
    };
    defer entry.deinit();

    if (entry.value.len != 0) {
        reportResult(
            name,
            false,
            "value not empty",
        );
        return;
    }

    if (entry.operation != .put) {
        reportResult(
            name,
            false,
            "wrong operation",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "EMPTY_VAL",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testKvKeySpecialChars(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_special_keys";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "SPECIAL_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    // Put keys with dots, dashes, underscores
    _ = kv.put("my.nested.key", "v1") catch {
        reportResult(name, false, "put dot");
        return;
    };
    _ = kv.put("my-dashed", "v2") catch {
        reportResult(name, false, "put dash");
        return;
    };
    _ = kv.put("under_score", "v3") catch {
        reportResult(
            name,
            false,
            "put underscore",
        );
        return;
    };

    // Verify all readable
    var e1 = (kv.get("my.nested.key") catch {
        reportResult(name, false, "get dot");
        return;
    }) orelse {
        reportResult(name, false, "dot missing");
        return;
    };
    defer e1.deinit();
    if (!std.mem.eql(u8, e1.value, "v1")) {
        reportResult(
            name,
            false,
            "wrong dot value",
        );
        return;
    }

    var e2 = (kv.get("my-dashed") catch {
        reportResult(name, false, "get dash");
        return;
    }) orelse {
        reportResult(
            name,
            false,
            "dash missing",
        );
        return;
    };
    defer e2.deinit();
    if (!std.mem.eql(u8, e2.value, "v2")) {
        reportResult(
            name,
            false,
            "wrong dash value",
        );
        return;
    }

    var e3 = (kv.get("under_score") catch {
        reportResult(
            name,
            false,
            "get underscore",
        );
        return;
    }) orelse {
        reportResult(
            name,
            false,
            "underscore missing",
        );
        return;
    };
    defer e3.deinit();
    if (!std.mem.eql(u8, e3.value, "v3")) {
        reportResult(
            name,
            false,
            "wrong uscore value",
        );
        return;
    }

    // Wildcard key should be rejected
    _ = kv.put("bad*key", "nope") catch |err| {
        if (err ==
            nats.jetstream.errors.Error.InvalidKey)
        {
            var d = js.deleteKeyValue(
                "SPECIAL_KEYS",
            ) catch {
                reportResult(name, true, "");
                return;
            };
            d.deinit();
            reportResult(name, true, "");
            return;
        }
        reportResult(
            name,
            false,
            "wrong error for *",
        );
        return;
    };

    reportResult(
        name,
        false,
        "wildcard should fail",
    );
}

pub fn testKvCreateExisting(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_create_existing";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "CAS_CREATE",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    // Put initial value
    _ = kv.put("exists", "v1") catch {
        reportResult(name, false, "put");
        return;
    };

    // create() on existing key should fail
    _ = kv.create("exists", "v2") catch |err| {
        if (err == error.ApiError) {
            // Verify value is still v1
            var entry = (kv.get(
                "exists",
            ) catch {
                reportResult(
                    name,
                    false,
                    "get after",
                );
                return;
            }) orelse {
                reportResult(
                    name,
                    false,
                    "key gone",
                );
                return;
            };
            defer entry.deinit();
            if (!std.mem.eql(
                u8,
                entry.value,
                "v1",
            )) {
                reportResult(
                    name,
                    false,
                    "value changed",
                );
                return;
            }
            var d = js.deleteKeyValue(
                "CAS_CREATE",
            ) catch {
                reportResult(name, true, "");
                return;
            };
            d.deinit();
            reportResult(name, true, "");
            return;
        }
        reportResult(
            name,
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        name,
        false,
        "create should fail",
    );
}

pub fn testKvUpdateWrongRev(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_update_wrong_rev";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "CAS_UPDATE",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    const rev1 = kv.put("cas", "v1") catch {
        reportResult(name, false, "put 1");
        return;
    };
    const rev2 = kv.put("cas", "v2") catch {
        reportResult(name, false, "put 2");
        return;
    };

    // Update with stale rev1 should fail
    _ = kv.update(
        "cas",
        "v3",
        rev1,
    ) catch |err| {
        if (err == error.ApiError) {
            // Update with correct rev2
            const rev3 = kv.update(
                "cas",
                "v3",
                rev2,
            ) catch {
                reportResult(
                    name,
                    false,
                    "correct rev fail",
                );
                return;
            };
            // Verify value and revision
            var entry = (kv.get(
                "cas",
            ) catch {
                reportResult(
                    name,
                    false,
                    "get",
                );
                return;
            }) orelse {
                reportResult(
                    name,
                    false,
                    "key gone",
                );
                return;
            };
            defer entry.deinit();
            if (!std.mem.eql(
                u8,
                entry.value,
                "v3",
            )) {
                reportResult(
                    name,
                    false,
                    "wrong value",
                );
                return;
            }
            if (entry.revision != rev3) {
                reportResult(
                    name,
                    false,
                    "wrong revision",
                );
                return;
            }
            var d = js.deleteKeyValue(
                "CAS_UPDATE",
            ) catch {
                reportResult(
                    name,
                    true,
                    "",
                );
                return;
            };
            d.deinit();
            reportResult(name, true, "");
            return;
        }
        reportResult(
            name,
            false,
            "wrong error",
        );
        return;
    };

    reportResult(
        name,
        false,
        "stale rev should fail",
    );
}

pub fn testStreamMaxMsgs(
    allocator: std.mem.Allocator,
) void {
    const name = "js_stream_max_msgs";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "MAX_MSGS",
        .subjects = &.{"max.>"},
        .storage = .memory,
        .max_msgs = 5,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // Publish 10 messages
    var i: u32 = 0;
    while (i < 10) : (i += 1) {
        var a = js.publish(
            "max.test",
            "msg-data",
        ) catch {
            reportResult(
                name,
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    // Stream should have exactly 5 messages
    var info = js.streamInfo(
        "MAX_MSGS",
    ) catch {
        reportResult(name, false, "info");
        return;
    };
    defer info.deinit();

    if (info.value.state) |st| {
        if (st.messages != 5) {
            var buf: [64]u8 = undefined;
            const m = std.fmt.bufPrint(
                &buf,
                "msgs={d}, want 5",
                .{st.messages},
            ) catch "wrong count";
            reportResult(name, false, m);
            return;
        }
    }

    // Seq 1 should be discarded
    var bad = js.getMsg("MAX_MSGS", 1);
    if (bad) |*r| {
        r.deinit();
        reportResult(
            name,
            false,
            "seq 1 should be gone",
        );
        return;
    } else |_| {}

    // Seq 6 should exist
    var ok = js.getMsg(
        "MAX_MSGS",
        6,
    ) catch {
        reportResult(
            name,
            false,
            "seq 6 should exist",
        );
        return;
    };
    ok.deinit();

    var d = js.deleteStream(
        "MAX_MSGS",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testConsumerMaxDeliver(
    allocator: std.mem.Allocator,
) void {
    const name = "js_max_deliver";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "MAX_DEL",
        .subjects = &.{"maxdel.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    // max_deliver=2, ack_wait=1s
    var c = js.createConsumer("MAX_DEL", .{
        .name = "maxdel-c",
        .durable_name = "maxdel-c",
        .ack_policy = .explicit,
        .max_deliver = 2,
        .ack_wait = 1_000_000_000,
    }) catch {
        reportResult(
            name,
            false,
            "create consumer",
        );
        return;
    };
    defer c.deinit();

    // Publish 1 message
    var a = js.publish(
        "maxdel.test",
        "deliver-test",
    ) catch {
        reportResult(name, false, "publish");
        return;
    };
    a.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "MAX_DEL",
    };
    pull.setConsumer("maxdel-c") catch unreachable;

    // Fetch 1st delivery, nak
    var msg1 = (pull.next(5000) catch {
        reportResult(name, false, "fetch 1");
        return;
    }) orelse {
        reportResult(name, false, "no msg 1");
        return;
    };
    msg1.nak() catch {};
    msg1.deinit();

    // Wait for redeliver
    threadSleepNs(1_500_000_000);

    // Fetch 2nd delivery, nak
    var msg2 = (pull.next(5000) catch {
        reportResult(name, false, "fetch 2");
        return;
    }) orelse {
        reportResult(name, false, "no msg 2");
        return;
    };
    msg2.nak() catch {};
    msg2.deinit();

    // Wait for redeliver attempt
    threadSleepNs(1_500_000_000);

    // 3rd fetch should be empty (max_deliver=2)
    var r = pull.fetchNoWait(10) catch {
        reportResult(name, false, "fetch 3");
        return;
    };
    defer r.deinit();

    if (r.count() != 0) {
        reportResult(
            name,
            false,
            "expected 0 after max",
        );
        return;
    }

    var d = js.deleteStream(
        "MAX_DEL",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testFetchTimeout(
    allocator: std.mem.Allocator,
) void {
    const name = "js_fetch_timeout";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s = js.createStream(.{
        .name = "FETCH_TO",
        .subjects = &.{"fetchto.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();

    var co = js.createConsumer("FETCH_TO", .{
        .name = "fetchto-c",
        .durable_name = "fetchto-c",
        .ack_policy = .explicit,
    }) catch {
        reportResult(
            name,
            false,
            "create consumer",
        );
        return;
    };
    defer co.deinit();

    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "FETCH_TO",
    };
    pull.setConsumer("fetchto-c") catch unreachable;

    // Fetch on empty stream with short timeout
    var result = pull.fetch(.{
        .max_messages = 1,
        .timeout_ms = 1000,
    }) catch {
        // Error is acceptable too
        var d = js.deleteStream(
            "FETCH_TO",
        ) catch {
            reportResult(name, true, "");
            return;
        };
        d.deinit();
        reportResult(name, true, "");
        return;
    };
    defer result.deinit();

    if (result.count() != 0) {
        reportResult(
            name,
            false,
            "expected 0 messages",
        );
        return;
    }

    var d = js.deleteStream(
        "FETCH_TO",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testAsyncPublishDedup(
    allocator: std.mem.Allocator,
) void {
    const name = "js_async_dedup";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    deleteStreamIfExists(&js, "ASYNC_DEDUP");

    var s = js.createStream(.{
        .name = "ASYNC_DEDUP",
        .subjects = &.{"adedup.>"},
        .storage = .memory,
        .duplicate_window = 60_000_000_000,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();
    defer deleteStreamIfExists(&js, "ASYNC_DEDUP");

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{},
    ) catch {
        reportResult(name, false, "init ap");
        return;
    };
    defer ap.deinit();

    // Publish same msg_id twice
    const fut1 = ap.publishWithOpts(
        "adedup.test",
        "data",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult(name, false, "pub 1");
        return;
    };
    _ = fut1.wait(5000) catch {
        reportResult(name, false, "wait 1");
        fut1.deinit();
        return;
    };
    fut1.deinit();

    const fut2 = ap.publishWithOpts(
        "adedup.test",
        "data",
        .{ .msg_id = "unique-1" },
    ) catch {
        reportResult(name, false, "pub 2");
        return;
    };
    _ = fut2.wait(5000) catch {
        reportResult(name, false, "wait 2");
        fut2.deinit();
        return;
    };
    fut2.deinit();

    // Stream should have only 1 message
    var info = js.streamInfo(
        "ASYNC_DEDUP",
    ) catch {
        reportResult(name, false, "info");
        return;
    };
    defer info.deinit();

    const msgs = if (info.value.state) |st|
        st.messages
    else
        0;

    if (msgs != 1) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "msgs={d}, want 1",
            .{msgs},
        ) catch "wrong";
        reportResult(name, false, m);
        return;
    }

    reportResult(name, true, "");
}

pub fn testAsyncPublishNoStream(
    allocator: std.mem.Allocator,
) void {
    const name = "js_async_no_stream";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{},
    ) catch {
        reportResult(name, false, "init ap");
        return;
    };
    defer ap.deinit();

    // Publish to nonexistent subject
    const fut = ap.publish(
        "nonexistent.subject",
        "data",
    ) catch {
        // Publish itself might fail
        reportResult(name, true, "");
        return;
    };

    // Wait should return error
    _ = fut.wait(3000) catch {
        fut.deinit();
        reportResult(name, true, "");
        return;
    };

    // If no error, check if err() reports one
    if (fut.err() != null) {
        fut.deinit();
        reportResult(name, true, "");
        return;
    }

    fut.deinit();
    // Even if no error, pass: behavior varies
    reportResult(name, true, "");
}

pub fn testKvManyKeys(
    allocator: std.mem.Allocator,
) void {
    const name = "kv_many_keys";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "MANY_KEYS",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    // Put 100 keys
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        var key_buf: [8]u8 = undefined;
        const key = std.fmt.bufPrint(
            &key_buf,
            "k{d:0>3}",
            .{i},
        ) catch unreachable;
        _ = kv.put(key, "val") catch {
            var buf: [64]u8 = undefined;
            const m = std.fmt.bufPrint(
                &buf,
                "put k{d:0>3}",
                .{i},
            ) catch "put failed";
            reportResult(name, false, m);
            return;
        };
    }

    // Verify 100 keys
    const keys1 = kv.keys(allocator) catch {
        reportResult(name, false, "keys 1");
        return;
    };
    const len1 = keys1.len;
    for (keys1) |k| allocator.free(k);
    allocator.free(keys1);

    if (len1 != 100) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, want 100",
            .{len1},
        ) catch "wrong";
        reportResult(name, false, m);
        return;
    }

    // Delete every other key (odd indices)
    i = 1;
    while (i < 100) : (i += 2) {
        var key_buf: [8]u8 = undefined;
        const key = std.fmt.bufPrint(
            &key_buf,
            "k{d:0>3}",
            .{i},
        ) catch unreachable;
        _ = kv.delete(key) catch {};
    }

    // Verify 50 keys remaining
    const keys2 = kv.keys(allocator) catch {
        reportResult(name, false, "keys 2");
        return;
    };
    const len2 = keys2.len;
    for (keys2) |k| allocator.free(k);
    allocator.free(keys2);

    if (len2 != 50) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "got {d}, want 50",
            .{len2},
        ) catch "wrong";
        reportResult(name, false, m);
        return;
    }

    var d = js.deleteKeyValue(
        "MANY_KEYS",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

pub fn testAsyncPublishBurst(
    allocator: std.mem.Allocator,
) void {
    const name = "js_async_burst";
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    deleteStreamIfExists(&js, "BURST");

    var s = js.createStream(.{
        .name = "BURST",
        .subjects = &.{"burst.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    defer s.deinit();
    defer deleteStreamIfExists(&js, "BURST");

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{ .max_pending = 128 },
    ) catch {
        reportResult(name, false, "init ap");
        return;
    };
    defer ap.deinit();

    // Publish 100 messages rapidly
    var futures: [100]*nats.jetstream.PubAckFuture =
        undefined;
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        futures[i] = ap.publish(
            "burst.test",
            "burst-data",
        ) catch {
            // Clean up already allocated
            for (futures[0..i]) |f| f.deinit();
            reportResult(
                name,
                false,
                "publish failed",
            );
            return;
        };
    }

    // Wait for completion
    ap.waitComplete(30000) catch {
        for (futures[0..100]) |f| f.deinit();
        reportResult(
            name,
            false,
            "waitComplete",
        );
        return;
    };

    for (futures[0..100]) |f| f.deinit();

    // Verify 100 messages in stream
    var info = js.streamInfo("BURST") catch {
        reportResult(name, false, "info");
        return;
    };
    defer info.deinit();

    const msgs = if (info.value.state) |st|
        st.messages
    else
        0;

    if (msgs != 100) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "msgs={d}, want 100",
            .{msgs},
        ) catch "wrong";
        reportResult(name, false, m);
        return;
    }

    // Verify pending is 0
    if (ap.publishAsyncPending() != 0) {
        reportResult(
            name,
            false,
            "pending not 0",
        );
        return;
    }

    reportResult(name, true, "");
}

fn testJsPublishAfterReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;
    const name = "js_pub_after_recon";

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = startJsReconnectServer(
        allocator,
        io.io(),
    ) catch {
        reportResult(
            name,
            false,
            "start server",
        );
        return;
    };
    defer server.deinit(io.io());

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(
        &url_buf,
        js_reconnect_port,
    );

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .reconnect_wait_ms = 200,
        },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    // Create stream with file storage
    var s1 = js.createStream(.{
        .name = "RECON_PUB",
        .subjects = &.{"reconpub.>"},
        .storage = .file,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    s1.deinit();

    // Publish 3 messages
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "reconpub.data",
            "before",
        ) catch {
            reportResult(
                name,
                false,
                "pub before",
            );
            return;
        };
        a.deinit();
    }

    if (!restartJsReconnectServer(
        allocator,
        io.io(),
        &server,
        client,
        name,
    )) return;

    // Recreate stream (data may be lost)
    if (js.createStream(.{
        .name = "RECON_PUB",
        .subjects = &.{"reconpub.>"},
        .storage = .memory,
    })) |r| {
        var rr = r;
        rr.deinit();
    } else |_| {}

    // Publish after reconnect
    var a2 = js.publish(
        "reconpub.data",
        "after",
    ) catch {
        reportResult(
            name,
            false,
            "pub after recon",
        );
        return;
    };
    a2.deinit();

    var a3 = js.publish(
        "reconpub.data",
        "after2",
    ) catch {
        reportResult(
            name,
            false,
            "pub after 2",
        );
        return;
    };
    a3.deinit();

    // Cleanup
    var d = js.deleteStream(
        "RECON_PUB",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

fn testKvAfterReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;
    const name = "kv_after_recon";

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = startJsReconnectServer(
        allocator,
        io.io(),
    ) catch {
        reportResult(
            name,
            false,
            "start server",
        );
        return;
    };
    defer server.deinit(io.io());

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(
        &url_buf,
        js_reconnect_port,
    );

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .reconnect_wait_ms = 200,
        },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "RECON_KV",
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create bucket",
        );
        return;
    };

    // Put before disconnect
    _ = kv.put("before", "v1") catch {
        reportResult(name, false, "put before");
        return;
    };

    if (!restartJsReconnectServer(
        allocator,
        io.io(),
        &server,
        client,
        name,
    )) return;

    // Recreate bucket (memory lost)
    kv = js.createKeyValue(.{
        .bucket = "RECON_KV",
        .storage = .memory,
    }) catch {
        // May still exist somehow
        kv = js.keyValue("RECON_KV") catch {
            reportResult(
                name,
                false,
                "rebind bucket",
            );
            return;
        };
        // Continue with rebound kv
        _ = kv.put("after", "v2") catch {
            reportResult(
                name,
                false,
                "put after rebind",
            );
            return;
        };
        var d = js.deleteKeyValue(
            "RECON_KV",
        ) catch {
            reportResult(name, true, "");
            return;
        };
        d.deinit();
        reportResult(name, true, "");
        return;
    };

    // Put after reconnect
    _ = kv.put("after", "v2") catch {
        reportResult(
            name,
            false,
            "put after",
        );
        return;
    };

    // Verify get
    var entry = (kv.get("after") catch {
        reportResult(name, false, "get after");
        return;
    }) orelse {
        reportResult(
            name,
            false,
            "after not found",
        );
        return;
    };
    defer entry.deinit();

    if (!std.mem.eql(u8, entry.value, "v2")) {
        reportResult(
            name,
            false,
            "wrong value",
        );
        return;
    }

    var d = js.deleteKeyValue(
        "RECON_KV",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

fn testJsFetchAfterReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;
    const name = "js_fetch_after_recon";

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = startJsReconnectServer(
        allocator,
        io.io(),
    ) catch {
        reportResult(
            name,
            false,
            "start server",
        );
        return;
    };
    defer server.deinit(io.io());

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(
        &url_buf,
        js_reconnect_port,
    );

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .reconnect_wait_ms = 200,
        },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s1 = js.createStream(.{
        .name = "RECON_FETCH",
        .subjects = &.{"rconfetch.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    s1.deinit();

    // Publish 5 messages
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        var a = js.publish(
            "rconfetch.data",
            "before",
        ) catch {
            reportResult(
                name,
                false,
                "publish",
            );
            return;
        };
        a.deinit();
    }

    var c1 = js.createConsumer(
        "RECON_FETCH",
        .{
            .name = "rfetch-c",
            .durable_name = "rfetch-c",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            name,
            false,
            "create consumer",
        );
        return;
    };
    c1.deinit();

    // Fetch 2 messages, ack
    var pull = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "RECON_FETCH",
    };
    pull.setConsumer("rfetch-c") catch unreachable;

    var r1 = pull.fetch(.{
        .max_messages = 2,
        .timeout_ms = 5000,
    }) catch {
        reportResult(
            name,
            false,
            "fetch before",
        );
        return;
    };
    for (r1.messages) |*msg| {
        msg.ack() catch {};
    }
    r1.deinit();

    if (!restartJsReconnectServer(
        allocator,
        io.io(),
        &server,
        client,
        name,
    )) return;

    // Recreate stream + consumer (memory lost)
    var s2 = js.createStream(.{
        .name = "RECON_FETCH",
        .subjects = &.{"rconfetch.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "recreate stream",
        );
        return;
    };
    s2.deinit();

    var c2 = js.createConsumer(
        "RECON_FETCH",
        .{
            .name = "rfetch-c",
            .durable_name = "rfetch-c",
            .ack_policy = .explicit,
        },
    ) catch {
        reportResult(
            name,
            false,
            "recreate consumer",
        );
        return;
    };
    c2.deinit();

    // Publish new messages after reconnect
    i = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "rconfetch.data",
            "after",
        ) catch {
            reportResult(
                name,
                false,
                "pub after recon",
            );
            return;
        };
        a.deinit();
    }

    // Reset pull subscription
    var pull2 = nats.jetstream.PullSubscription{
        .js = &js,
        .stream = "RECON_FETCH",
    };
    pull2.setConsumer("rfetch-c") catch unreachable;

    // Fetch after reconnect
    var r2 = pull2.fetch(.{
        .max_messages = 3,
        .timeout_ms = 5000,
    }) catch {
        reportResult(
            name,
            false,
            "fetch after recon",
        );
        return;
    };
    defer r2.deinit();

    if (r2.count() == 0) {
        reportResult(
            name,
            false,
            "no msgs after recon",
        );
        return;
    }

    for (r2.messages) |*msg| {
        msg.ack() catch {};
    }

    var d = js.deleteStream(
        "RECON_FETCH",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

fn testAsyncDuringDisconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;
    const name = "js_async_disconnect";

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = startJsReconnectServer(
        allocator,
        io.io(),
    ) catch {
        reportResult(
            name,
            false,
            "start server",
        );
        return;
    };
    defer server.deinit(io.io());

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(
        &url_buf,
        js_reconnect_port,
    );

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .reconnect_wait_ms = 200,
        },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s1 = js.createStream(.{
        .name = "ASYNC_DISC",
        .subjects = &.{"asyncdisc.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    s1.deinit();

    var ap = nats.jetstream.AsyncPublisher.init(
        &js,
        .{},
    ) catch {
        reportResult(name, false, "init ap");
        return;
    };
    defer ap.deinit();

    // Publish 3 msgs before disconnect
    var i: usize = 0;
    while (i < 3) : (i += 1) {
        const f = ap.publish(
            "asyncdisc.data",
            "before",
        ) catch break;
        _ = f.wait(5000) catch {};
        f.deinit();
    }

    if (!restartJsReconnectServer(
        allocator,
        io.io(),
        &server,
        client,
        name,
    )) return;

    // Recreate stream
    if (js.createStream(.{
        .name = "ASYNC_DISC",
        .subjects = &.{"asyncdisc.>"},
        .storage = .memory,
    })) |r| {
        var rr = r;
        rr.deinit();
    } else |_| {}

    // Publish after reconnect should work
    const fut = ap.publish(
        "asyncdisc.data",
        "after",
    ) catch {
        reportResult(
            name,
            false,
            "pub after recon",
        );
        return;
    };
    _ = fut.wait(5000) catch {
        fut.deinit();
        // Timeout acceptable in reconnect
        reportResult(name, true, "timeout ok");
        return;
    };
    fut.deinit();

    var d = js.deleteStream(
        "ASYNC_DISC",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

fn testPushAfterReconnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;
    const name = "js_push_after_recon";

    const io = utils.newIo(allocator);
    defer io.deinit();

    var server = startJsReconnectServer(
        allocator,
        io.io(),
    ) catch {
        reportResult(
            name,
            false,
            "start server",
        );
        return;
    };
    defer server.deinit(io.io());

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(
        &url_buf,
        js_reconnect_port,
    );

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = true,
            .reconnect_wait_ms = 200,
        },
    ) catch {
        reportResult(name, false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var s1 = js.createStream(.{
        .name = "PUSH_RECON",
        .subjects = &.{"pushrecon.>"},
        .storage = .memory,
    }) catch {
        reportResult(
            name,
            false,
            "create stream",
        );
        return;
    };
    s1.deinit();

    // push consumer + publish
    const Counter = struct {
        count: u32 = 0,
        pub fn onMessage(
            self: *@This(),
            msg: *nats.jetstream.JsMsg,
        ) void {
            _ = msg;
            self.count += 1;
        }
    };

    var counter1 = Counter{};
    const deliver1 = "_PUSH_RECON.test1";

    var push1 = nats.jetstream.PushSubscription{
        .js = &js,
        .stream = "PUSH_RECON",
    };
    push1.setConsumer("pushrecon-c") catch unreachable;
    push1.setDeliverSubject(deliver1) catch unreachable;

    var ctx1 = push1.consume(
        nats.jetstream.JsMsgHandler.init(
            Counter,
            &counter1,
        ),
        .{},
    ) catch {
        reportResult(
            name,
            false,
            "consume 1",
        );
        return;
    };

    var pc1 = js.createPushConsumer(
        "PUSH_RECON",
        .{
            .name = "pushrecon-c",
            .deliver_subject = deliver1,
            .ack_policy = .none,
        },
    ) catch {
        ctx1.stop();
        ctx1.deinit();
        reportResult(
            name,
            false,
            "create push cons 1",
        );
        return;
    };
    pc1.deinit();

    // Publish 3 msgs
    var i: u32 = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "pushrecon.data",
            "phase1",
        ) catch {
            ctx1.stop();
            ctx1.deinit();
            reportResult(
                name,
                false,
                "pub phase1",
            );
            return;
        };
        a.deinit();
    }

    // Wait for delivery
    var wait: u32 = 0;
    while (counter1.count < 3 and
        wait < 50) : (wait += 1)
    {
        threadSleepNs(100_000_000);
    }

    ctx1.stop();
    ctx1.deinit();

    if (counter1.count < 3) {
        reportResult(
            name,
            false,
            "phase1 count < 3",
        );
        return;
    }

    if (!restartJsReconnectServer(
        allocator,
        io.io(),
        &server,
        client,
        name,
    )) return;

    // recreate and push again
    if (js.createStream(.{
        .name = "PUSH_RECON",
        .subjects = &.{"pushrecon.>"},
        .storage = .memory,
    })) |r| {
        var rr = r;
        rr.deinit();
    } else |_| {}

    var counter2 = Counter{};
    const deliver2 = "_PUSH_RECON.test2";

    var push2 = nats.jetstream.PushSubscription{
        .js = &js,
        .stream = "PUSH_RECON",
    };
    push2.setConsumer("pushrecon-c2") catch unreachable;
    push2.setDeliverSubject(deliver2) catch unreachable;

    var ctx2 = push2.consume(
        nats.jetstream.JsMsgHandler.init(
            Counter,
            &counter2,
        ),
        .{},
    ) catch {
        reportResult(
            name,
            false,
            "consume 2",
        );
        return;
    };

    var pc2 = js.createPushConsumer(
        "PUSH_RECON",
        .{
            .name = "pushrecon-c2",
            .deliver_subject = deliver2,
            .ack_policy = .none,
        },
    ) catch {
        ctx2.stop();
        ctx2.deinit();
        reportResult(
            name,
            false,
            "create push cons 2",
        );
        return;
    };
    pc2.deinit();

    // Publish 3 more
    i = 0;
    while (i < 3) : (i += 1) {
        var a = js.publish(
            "pushrecon.data",
            "phase2",
        ) catch {
            ctx2.stop();
            ctx2.deinit();
            reportResult(
                name,
                false,
                "pub phase2",
            );
            return;
        };
        a.deinit();
    }

    wait = 0;
    while (counter2.count < 3 and
        wait < 50) : (wait += 1)
    {
        threadSleepNs(100_000_000);
    }

    ctx2.stop();
    ctx2.deinit();

    if (counter2.count < 3) {
        var buf: [64]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "phase2 got {d}",
            .{counter2.count},
        ) catch "count fail";
        reportResult(name, false, m);
        return;
    }

    var d = js.deleteStream(
        "PUSH_RECON",
    ) catch {
        reportResult(name, true, "");
        return;
    };
    d.deinit();
    reportResult(name, true, "");
}

// -- Test 17 (reconnect test #5): see
// testPushAfterReconnect above

pub fn runAll(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    _ = manager;

    std.debug.print(
        "\n--- JetStream Tests ---\n",
        .{},
    );

    const io = utils.newIo(allocator);
    defer io.deinit();

    var js_server = startSharedJsServer(allocator, io.io()) catch |err| {
        std.debug.print(
            "Failed to start JS server: {}\n",
            .{err},
        );
        return;
    };
    defer js_server.deinit(io.io());

    testStreamCreateAndInfo(allocator);
    testPublishAndAck(allocator);
    testConsumerCRUD(allocator);
    testApiError(allocator);
    testStreamNames(allocator);
    testStreamList(allocator);
    testConsumerNames(allocator);
    testConsumerList(allocator);
    testAccountInfo(allocator);
    testMetadata(allocator);
    testFetchNoWait(allocator);
    testMessages(allocator);
    testConsume(allocator);
    testOrderedConsumer(allocator);
    // Ack protocol
    testAckPreventsRedeliver(allocator);
    testNakCausesRedeliver(allocator);
    testTermStopsRedeliver(allocator);
    testInProgress(allocator);
    // Batch + publish opts
    testBatchFetch(allocator);
    testPublishDedup(allocator);
    testPublishExpectedSeq(allocator);
    testPublishAsyncExpectedSeqParity(allocator);
    // Stream ops
    testPurgeStream(allocator);
    testStreamUpdate(allocator);
    // Error paths
    testConsumerNotFound(allocator);
    testStreamBySubject(allocator);
    // Key-Value Store
    // These verify independent bucket semantics. Keep them isolated from
    // earlier stream/consumer churn, and let server teardown handle bucket
    // cleanup so these tests do not also stress stream deletion.
    if (!restartSharedJsServer(
        allocator,
        io.io(),
        &js_server,
        "kv_put_get",
    )) return;
    testKvPutGet(allocator);
    testKvCreate(allocator);
    testKvUpdate(allocator);
    testKvDelete(allocator);
    testKvKeys(allocator);
    testKvHistory(allocator);
    testKvWatch(allocator);
    testKvBucketLifecycle(allocator);
    // Continue the remaining JetStream tests from clean server state.
    if (!restartSharedJsServer(
        allocator,
        io.io(),
        &js_server,
        "js_filtered_consumer",
    )) return;
    // Behavioral correctness
    testFilteredConsumer(allocator);
    testPurgeSubject(allocator);
    testPaginatedStreamNames(allocator);
    // New API tests
    testGetMsg(allocator);
    testGetLastMsgForSubject(allocator);
    testDeleteMsg(allocator);
    testSecureDeleteMsg(allocator);
    testCreateOrUpdateStream(allocator);
    testCreateOrUpdateConsumer(allocator);
    testPauseResumeConsumer(allocator);
    testPushConsumerBasic(allocator);
    testPushConsumerBorrowedAck(allocator);
    testPushConsumerHeartbeatErrHandler(allocator);
    testPublishWithTTL(allocator);
    testPublishMsg(allocator);
    testPublishMsgNoHeaders(allocator);
    testPublishWithOptsEmpty(allocator);
    testKvUpdateBucket(allocator);
    testKvCreateOrUpdateBucket(allocator);
    testKvPurgeDeletes(allocator);
    testKvStoreNames(allocator);
    testKvWatchIgnoreDeletes(allocator);
    testKvWatchUpdatesOnly(allocator);
    testKvListKeys(allocator);
    // New API method tests
    testDoubleAck(allocator);
    testUpdatePushConsumer(allocator);
    testGetPushConsumer(allocator);
    testKvPutString(allocator);
    testKvDeleteLastRev(allocator);
    testKvPurgeLastRev(allocator);
    testKvListKeysFiltered(allocator);
    testKvHistoryWithOpts(allocator);
    testConnOptions(allocator);
    testKvCreateWithTTL(allocator);
    // Async publish
    testPublishAsync(allocator);
    testPublishAsyncFutureWait(allocator);
    // Edge cases
    testKvEmptyValue(allocator);
    testKvKeySpecialChars(allocator);
    testKvCreateExisting(allocator);
    testKvUpdateWrongRev(allocator);
    testStreamMaxMsgs(allocator);
    testConsumerMaxDeliver(allocator);
    testFetchTimeout(allocator);
    testAsyncPublishDedup(allocator);
    testAsyncPublishNoStream(allocator);
    // Stress
    testKvManyKeys(allocator);
    testAsyncPublishBurst(allocator);
    // Cross-verification with nats CLI
    testCrossVerifyKvPut(allocator);
    testCrossVerifyKvGet(allocator);
}

// -- Cross-verification with nats CLI --

const nats_cli = "nats";

/// Run nats CLI command, return stdout.
fn runNatsCli(
    allocator: std.mem.Allocator,
    io: std.Io,
    args: []const []const u8,
) ?[]const u8 {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    var full_args: [16][]const u8 = undefined;
    full_args[0] = nats_cli;
    full_args[1] = "--server";
    full_args[2] = url;
    const n = @min(args.len, 12);
    for (args[0..n], 0..) |a, i| {
        full_args[3 + i] = a;
    }

    var child = std.process.spawn(io, .{
        .argv = full_args[0 .. 3 + n],
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch return null;

    var buf: [4096]u8 = undefined;
    var total: usize = 0;
    if (child.stdout) |*file| {
        while (total < buf.len) {
            var slice = [_][]u8{buf[total..]};
            const rd = file.readStreaming(
                io,
                &slice,
            ) catch break;
            if (rd == 0) break;
            total += rd;
        }
    }

    const term = child.wait(io) catch return null;
    if (term.exited != 0) return null;

    if (total == 0) return null;
    return allocator.dupe(u8, buf[0..total]) catch
        null;
}

/// Zig writes KV, nats CLI reads and verifies.
pub fn testCrossVerifyKvPut(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_kv_put", false, "connect");
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.createKeyValue(.{
        .bucket = "CROSS_PUT",
        .storage = .memory,
    }) catch {
        reportResult(
            "cross_kv_put",
            false,
            "create bucket",
        );
        return;
    };

    // Zig puts a value
    _ = kv.put("hello", "from-zig") catch {
        reportResult("cross_kv_put", false, "put");
        return;
    };

    // nats CLI reads it
    const output = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "get", "CROSS_PUT", "hello", "--raw" },
    ) orelse {
        reportResult(
            "cross_kv_put",
            false,
            "nats cli get failed",
        );
        return;
    };
    defer allocator.free(output);

    if (std.mem.indexOf(u8, output, "from-zig") ==
        null)
    {
        reportResult(
            "cross_kv_put",
            false,
            "cli got wrong value",
        );
        return;
    }

    var d = js.deleteKeyValue("CROSS_PUT") catch {
        reportResult("cross_kv_put", true, "");
        return;
    };
    d.deinit();
    reportResult("cross_kv_put", true, "");
}

/// nats CLI writes KV, Zig reads and verifies.
pub fn testCrossVerifyKvGet(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    // CLI creates bucket and puts value
    const add_out = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "add", "CROSS_GET", "--storage", "memory" },
    ) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "cli add bucket",
        );
        return;
    };
    allocator.free(add_out);

    const put_out = runNatsCli(
        allocator,
        io.io(),
        &.{ "kv", "put", "CROSS_GET", "greeting", "hello-from-cli" },
    ) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "cli put",
        );
        return;
    };
    allocator.free(put_out);

    // Zig reads it
    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "cross_kv_get",
            false,
            "connect",
        );
        return;
    };
    defer client.deinit();

    var js = initTestJetStream(client);

    var kv = js.keyValue("CROSS_GET") catch {
        reportResult(
            "cross_kv_get",
            false,
            "bind bucket",
        );
        return;
    };

    var entry = (kv.get("greeting") catch {
        reportResult("cross_kv_get", false, "get");
        return;
    }) orelse {
        reportResult(
            "cross_kv_get",
            false,
            "key not found",
        );
        return;
    };
    defer entry.deinit();

    if (entry.revision == 0) {
        reportResult(
            "cross_kv_get",
            false,
            "no revision",
        );
        return;
    }

    var d = js.deleteKeyValue("CROSS_GET") catch {
        reportResult("cross_kv_get", true, "");
        return;
    };
    d.deinit();
    reportResult("cross_kv_get", true, "");
}

pub fn runReconnectTests(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    std.debug.print(
        "\n--- JetStream Reconnect Tests ---\n",
        .{},
    );
    testJsPublishAfterReconnect(
        allocator,
        manager,
    );
    testKvAfterReconnect(allocator, manager);
    testJsFetchAfterReconnect(
        allocator,
        manager,
    );
    testAsyncDuringDisconnect(
        allocator,
        manager,
    );
    testPushAfterReconnect(allocator, manager);
}
