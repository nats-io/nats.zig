//! JetStream Integration Tests
//!
//! End-to-end tests for JetStream stream/consumer CRUD,
//! publish with ack, and pull-based fetch.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const ServerManager = utils.ServerManager;

const js_port = utils.jetstream_port;

pub fn testStreamCreateAndInfo(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, js_port);

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
            "js_stream_create",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

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
            "js_publish_ack",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

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
            "js_consumer_crud",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

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
            "js_api_error",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var js = nats.jetstream.JetStream.init(
        client,
        .{},
    );

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

pub fn runAll(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    std.debug.print(
        "\n--- JetStream Tests ---\n",
        .{},
    );

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    _ = manager.startServer(
        allocator,
        io.io(),
        .{ .port = js_port, .jetstream = true },
    ) catch |err| {
        std.debug.print(
            "Failed to start JS server: {}\n",
            .{err},
        );
        return;
    };

    testStreamCreateAndInfo(allocator);
    testPublishAndAck(allocator);
    testConsumerCRUD(allocator);
    testApiError(allocator);
}
