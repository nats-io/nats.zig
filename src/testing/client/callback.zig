//! Callback Subscription Tests

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

// -- MsgHandler delivery test --

const CountHandler = struct {
    count: *u32,

    pub fn onMessage(self: *@This(), _: *const nats.Message) void {
        self.count.* += 1;
    }
};

pub fn testCallbackMsgHandler(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_msg_handler",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var count: u32 = 0;
    var handler = CountHandler{ .count = &count };

    const sub = client.subscribe(
        "cb.handler.test",
        nats.MsgHandler.init(CountHandler, &handler),
    ) catch {
        reportResult(
            "callback_msg_handler",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    for (0..5) |_| {
        client.publish("cb.handler.test", "x") catch {
            reportResult(
                "callback_msg_handler",
                false,
                "publish failed",
            );
            return;
        };
    }

    // Wait for callbacks to fire
    io.io().sleep(
        .fromMilliseconds(300),
        .awake,
    ) catch {};

    if (count == 5) {
        reportResult("callback_msg_handler", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "expected 5, got {d}",
            .{count},
        ) catch "count mismatch";
        reportResult("callback_msg_handler", false, msg);
    }
}

// -- Plain fn delivery test --

var plain_fn_count: u32 = 0;

fn plainCallback(_: *const nats.Message) void {
    plain_fn_count += 1;
}

pub fn testCallbackPlainFn(
    allocator: std.mem.Allocator,
) void {
    plain_fn_count = 0;

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_plain_fn",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    const sub = client.subscribeFn(
        "cb.plainfn.test",
        plainCallback,
    ) catch {
        reportResult(
            "callback_plain_fn",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    for (0..3) |_| {
        client.publish("cb.plainfn.test", "y") catch {
            reportResult(
                "callback_plain_fn",
                false,
                "publish failed",
            );
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(300),
        .awake,
    ) catch {};

    if (plain_fn_count == 3) {
        reportResult("callback_plain_fn", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "expected 3, got {d}",
            .{plain_fn_count},
        ) catch "count mismatch";
        reportResult("callback_plain_fn", false, msg);
    }
}

// -- Queue group test --

const QueueHandler = struct {
    count: *u32,

    pub fn onMessage(self: *@This(), _: *const nats.Message) void {
        self.count.* += 1;
    }
};

pub fn testCallbackQueueGroup(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_queue_group",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var count1: u32 = 0;
    var count2: u32 = 0;
    var h1 = QueueHandler{ .count = &count1 };
    var h2 = QueueHandler{ .count = &count2 };

    const sub1 = client.queueSubscribe(
        "cb.queue.test",
        "workers",
        nats.MsgHandler.init(QueueHandler, &h1),
    ) catch {
        reportResult(
            "callback_queue_group",
            false,
            "sub1 failed",
        );
        return;
    };
    defer sub1.deinit();

    const sub2 = client.queueSubscribe(
        "cb.queue.test",
        "workers",
        nats.MsgHandler.init(QueueHandler, &h2),
    ) catch {
        reportResult(
            "callback_queue_group",
            false,
            "sub2 failed",
        );
        return;
    };
    defer sub2.deinit();

    for (0..10) |_| {
        client.publish("cb.queue.test", "z") catch {
            reportResult(
                "callback_queue_group",
                false,
                "publish failed",
            );
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(300),
        .awake,
    ) catch {};

    const total = count1 + count2;
    if (total >= 9) {
        reportResult("callback_queue_group", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "expected >=9, got {d}",
            .{total},
        ) catch "total mismatch";
        reportResult("callback_queue_group", false, msg);
    }
}

// -- Deinit cleanup test (no hang) --

pub fn testCallbackDeinitCleanup(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_deinit_cleanup",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var count: u32 = 0;
    var handler = CountHandler{ .count = &count };

    const sub = client.subscribe(
        "cb.deinit.test",
        nats.MsgHandler.init(CountHandler, &handler),
    ) catch {
        reportResult(
            "callback_deinit_cleanup",
            false,
            "subscribe failed",
        );
        return;
    };
    // Immediately deinit -- must not hang
    sub.deinit();

    reportResult("callback_deinit_cleanup", true, "");
}

// -- Mode field test --

pub fn testCallbackModeField(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_mode_field",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    // Manual sub should have .manual mode
    const manual_sub = client.subscribeSync(
        "cb.mode.manual",
    ) catch {
        reportResult(
            "callback_mode_field",
            false,
            "manual sub failed",
        );
        return;
    };
    defer manual_sub.deinit();

    var count: u32 = 0;
    var handler = CountHandler{ .count = &count };

    // Callback sub should have .callback mode
    const cb_sub = client.subscribe(
        "cb.mode.callback",
        nats.MsgHandler.init(CountHandler, &handler),
    ) catch {
        reportResult(
            "callback_mode_field",
            false,
            "callback sub failed",
        );
        return;
    };
    defer cb_sub.deinit();

    if (manual_sub.mode != .manual) {
        reportResult(
            "callback_mode_field",
            false,
            "manual sub mode wrong",
        );
        return;
    }

    if (cb_sub.mode != .callback) {
        reportResult(
            "callback_mode_field",
            false,
            "callback sub mode wrong",
        );
        return;
    }

    reportResult("callback_mode_field", true, "");
}

// -- High volume delivery test --

pub fn testCallbackHighVolume(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_high_volume",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var count: u32 = 0;
    var handler = CountHandler{ .count = &count };

    const sub = client.subscribe(
        "cb.volume.test",
        nats.MsgHandler.init(CountHandler, &handler),
    ) catch {
        reportResult(
            "callback_high_volume",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    for (0..100) |_| {
        client.publish("cb.volume.test", "payload") catch {
            reportResult(
                "callback_high_volume",
                false,
                "publish failed",
            );
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(500),
        .awake,
    ) catch {};

    if (count == 100) {
        reportResult("callback_high_volume", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "expected 100, got {d}",
            .{count},
        ) catch "count mismatch";
        reportResult(
            "callback_high_volume",
            false,
            msg,
        );
    }
}

// -- Data integrity test --

const IntegrityHandler = struct {
    /// Tracks which payload indices were received.
    seen: *[100]bool,
    count: *u32,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        const idx = std.fmt.parseInt(
            usize,
            msg.data,
            10,
        ) catch return;
        if (idx < 100) {
            self.seen.*[idx] = true;
        }
        self.count.* += 1;
    }
};

pub fn testCallbackDataIntegrity(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_data_integrity",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    var seen: [100]bool = .{false} ** 100;
    var count: u32 = 0;
    var handler = IntegrityHandler{
        .seen = &seen,
        .count = &count,
    };

    const sub = client.subscribe(
        "cb.integrity.test",
        nats.MsgHandler.init(
            IntegrityHandler,
            &handler,
        ),
    ) catch {
        reportResult(
            "callback_data_integrity",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    var pbuf: [8]u8 = undefined;
    for (0..100) |i| {
        const payload = std.fmt.bufPrint(
            &pbuf,
            "{d}",
            .{i},
        ) catch "0";
        client.publish(
            "cb.integrity.test",
            payload,
        ) catch {
            reportResult(
                "callback_data_integrity",
                false,
                "publish failed",
            );
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(500),
        .awake,
    ) catch {};

    if (count != 100) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "expected 100, got {d}",
            .{count},
        ) catch "count mismatch";
        reportResult(
            "callback_data_integrity",
            false,
            msg,
        );
        return;
    }

    for (0..100) |i| {
        if (!seen[i]) {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "missing payload {d}",
                .{i},
            ) catch "missing payload";
            reportResult(
                "callback_data_integrity",
                false,
                msg,
            );
            return;
        }
    }

    reportResult("callback_data_integrity", true, "");
}

// -- Mixed manual + callback test --

pub fn testCallbackMixedModes(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_mixed_modes",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    // Callback sub on one subject
    var cb_count: u32 = 0;
    var handler = CountHandler{ .count = &cb_count };

    const cb_sub = client.subscribe(
        "cb.mixed.auto",
        nats.MsgHandler.init(CountHandler, &handler),
    ) catch {
        reportResult(
            "callback_mixed_modes",
            false,
            "callback sub failed",
        );
        return;
    };
    defer cb_sub.deinit();

    // Manual sub on different subject
    const man_sub = client.subscribeSync(
        "cb.mixed.manual",
    ) catch {
        reportResult(
            "callback_mixed_modes",
            false,
            "manual sub failed",
        );
        return;
    };
    defer man_sub.deinit();

    // Publish to both subjects
    for (0..5) |_| {
        client.publish("cb.mixed.auto", "a") catch {
            reportResult(
                "callback_mixed_modes",
                false,
                "publish auto failed",
            );
            return;
        };
        client.publish("cb.mixed.manual", "m") catch {
            reportResult(
                "callback_mixed_modes",
                false,
                "publish manual failed",
            );
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(300),
        .awake,
    ) catch {};

    // Drain manual sub
    var manual_count: u32 = 0;
    while (man_sub.tryNextMsg()) |_| {
        manual_count += 1;
    }

    if (cb_count != 5 or manual_count != 5) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "cb={d} man={d}, expected 5/5",
            .{ cb_count, manual_count },
        ) catch "count mismatch";
        reportResult(
            "callback_mixed_modes",
            false,
            msg,
        );
        return;
    }

    reportResult("callback_mixed_modes", true, "");
}

// -- Callback request/reply test --

const EchoHandler = struct {
    client: *nats.Client,
    handled: *u32,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        self.handled.* += 1;
        msg.respond(self.client, msg.data) catch {};
    }
};

pub fn testCallbackRequestReply(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const svc_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_request_reply",
            false,
            "svc connect failed",
        );
        return;
    };
    defer svc_client.deinit();

    const req_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "callback_request_reply",
            false,
            "req connect failed",
        );
        return;
    };
    defer req_client.deinit();

    var handled: u32 = 0;
    var handler = EchoHandler{
        .client = svc_client,
        .handled = &handled,
    };

    const sub = svc_client.subscribe(
        "cb.echo.test",
        nats.MsgHandler.init(EchoHandler, &handler),
    ) catch {
        reportResult(
            "callback_request_reply",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    // Wait for sub to propagate
    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    var replies: u32 = 0;
    const payloads = [_][]const u8{ "a", "b", "c" };
    for (payloads) |payload| {
        if (req_client.request(
            "cb.echo.test",
            payload,
            1000,
        )) |maybe_reply| {
            if (maybe_reply) |reply| {
                defer reply.deinit();
                if (std.mem.eql(
                    u8,
                    reply.data,
                    payload,
                )) {
                    replies += 1;
                }
            }
        } else |_| {}
    }

    if (handled != 3 or replies != 3) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "handled={d} replies={d}",
            .{ handled, replies },
        ) catch "mismatch";
        reportResult(
            "callback_request_reply",
            false,
            msg,
        );
        return;
    }

    reportResult("callback_request_reply", true, "");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testCallbackMsgHandler(allocator);
    testCallbackPlainFn(allocator);
    testCallbackQueueGroup(allocator);
    testCallbackDeinitCleanup(allocator);
    testCallbackModeField(allocator);
    testCallbackHighVolume(allocator);
    testCallbackDataIntegrity(allocator);
    testCallbackMixedModes(allocator);
    testCallbackRequestReply(allocator);
}
