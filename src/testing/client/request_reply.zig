//! Request-Reply Tests for NATS Client
//!
//! Tests for request-reply pattern.

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

pub fn testRequestMethod(allocator: std.mem.Allocator) void {
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
        reportResult("request_method", false, "connect failed");
        return;
    };
    defer client.deinit();

    const result = client.request(
        "nonexistent.service.test",
        "ping",
        50,
    ) catch {
        reportResult("request_method", false, "request error");
        return;
    };

    if (result) |msg| {
        msg.deinit();
    }

    if (client.isConnected()) {
        reportResult("request_method", true, "");
    } else {
        reportResult("request_method", false, "disconnected after request");
    }
}

pub fn testRequestReturns(allocator: std.mem.Allocator) void {
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
        reportResult("request_returns", false, "connect failed");
        return;
    };
    defer client.deinit();

    const start = std.Io.Timestamp.now(io.io(), .awake);

    const result = client.request(
        "nonexistent.service.test2",
        "data",
        100,
    ) catch {
        reportResult("request_returns", false, "request error");
        return;
    };

    const end = std.Io.Timestamp.now(io.io(), .awake);
    const elapsed = start.durationTo(end);
    const elapsed_ns: u64 = @intCast(elapsed.nanoseconds);
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;

    if (result) |msg| {
        msg.deinit();
    }

    if (elapsed_ms < 5000) {
        reportResult("request_returns", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "took too long: {d}ms",
            .{elapsed_ms},
        ) catch "timing error";
        reportResult("request_returns", false, msg);
    }
}

pub fn testReplyToPreserved(allocator: std.mem.Allocator) void {
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
        reportResult("reply_preserved", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("reply.test") catch {
        reportResult("reply_preserved", false, "sub failed");
        return;
    };
    defer sub.deinit();

    client.publishRequest("reply.test", "my.reply.inbox", "data") catch {
        reportResult("reply_preserved", false, "pub failed");
        return;
    };

    var future = io.io().async(
        nats.Client.Sub.nextMsg,
        .{sub},
    );
    defer if (future.cancel(io.io())) |m| m.deinit() else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "my.reply.inbox")) {
                reportResult("reply_preserved", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("reply_preserved", false, "reply_to not preserved");
}

pub fn testRequestReplySuccess(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_reply_success", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_reply_success", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const sub = responder.subscribeSync("test.service") catch {
        reportResult("request_reply_success", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
        ) void {
            if (s.nextMsgTimeout(1000) catch null) |req| {
                defer req.deinit();
                if (req.reply_to) |reply_inbox| {
                    r.publish(reply_inbox, "pong") catch {};
                }
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
    });
    defer _ = handler.cancel(io_r.io());

    const reply = requester.request(
        "test.service",
        "ping",
        2000,
    ) catch {
        reportResult("request_reply_success", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "pong")) {
            reportResult("request_reply_success", true, "");
            return;
        }
    }

    reportResult("request_reply_success", false, "no reply or wrong data");
}

pub fn testCrossClientRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_a = utils.newIo(allocator);
    defer io_a.deinit();
    const client_a = nats.Client.connect(
        allocator,
        io_a.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_client_reqrep", false, "A connect failed");
        return;
    };
    defer client_a.deinit();

    const io_b = utils.newIo(allocator);
    defer io_b.deinit();
    const client_b = nats.Client.connect(
        allocator,
        io_b.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_client_reqrep", false, "B connect failed");
        return;
    };
    defer client_b.deinit();

    const sub = client_b.subscribeSync("cross.service") catch {
        reportResult("cross_client_reqrep", false, "B sub failed");
        return;
    };
    defer sub.deinit();
    io_b.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(
            b: *nats.Client,
            s: *nats.Subscription,
        ) void {
            if (s.nextMsgTimeout(2000) catch null) |req| {
                defer req.deinit();
                if (req.reply_to) |inbox| {
                    b.publish(inbox, "response-from-B") catch {};
                }
            }
        }
    };

    var handler = io_b.io().async(Handler.handle, .{
        client_b,
        sub,
    });
    defer _ = handler.cancel(io_b.io());

    const reply = client_a.request(
        "cross.service",
        "request-from-A",
        3000,
    ) catch {
        reportResult("cross_client_reqrep", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "response-from-B")) {
            reportResult("cross_client_reqrep", true, "");
            return;
        }
    }

    reportResult("cross_client_reqrep", false, "no reply");
}

pub fn testRequestTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .no_responders = false,
        .reconnect = false,
    }) catch {
        reportResult("request_timeout", false, "connect failed");
        return;
    };
    defer client.deinit();

    const start = std.Io.Timestamp.now(io.io(), .awake);

    const result = client.request(
        "timeout.service.noexist",
        "ping",
        200,
    ) catch {
        reportResult(
            "request_timeout",
            false,
            "request error",
        );
        return;
    };

    const end = std.Io.Timestamp.now(io.io(), .awake);
    const elapsed = start.durationTo(end);
    const elapsed_ns: u64 = @intCast(elapsed.nanoseconds);
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;

    if (result) |msg| {
        msg.deinit();
        reportResult("request_timeout", true, "");
        return;
    }

    if (elapsed_ms < 5000) {
        reportResult("request_timeout", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "took {d}ms",
            .{elapsed_ms},
        ) catch "e";
        reportResult("request_timeout", false, detail);
    }
}

pub fn testRequestWithLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_large_payload", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("request_large_payload", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const sub = responder.subscribeSync("large.service") catch {
        reportResult("request_large_payload", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
        ) void {
            if (s.nextMsgTimeout(2000) catch null) |req| {
                defer req.deinit();
                if (req.reply_to) |reply_inbox| {
                    r.publish(reply_inbox, req.data) catch {};
                }
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
    });
    defer _ = handler.cancel(io_r.io());

    const payload = allocator.alloc(u8, 1024) catch {
        reportResult("request_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    const reply = requester.request(
        "large.service",
        payload,
        3000,
    ) catch {
        reportResult("request_large_payload", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        if (msg.data.len == 1024) {
            reportResult("request_large_payload", true, "");
            return;
        }
    }

    reportResult("request_large_payload", false, "no reply or wrong size");
}

pub fn testMultipleRequestsSequential(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_requests_seq", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multi_requests_seq", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const sub = responder.subscribeSync("multi.service") catch {
        reportResult("multi_requests_seq", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(
            r: *nats.Client,
            s: *nats.Subscription,
        ) void {
            for (0..5) |_| {
                if (s.nextMsgTimeout(2000) catch null) |req| {
                    defer req.deinit();
                    if (req.reply_to) |reply_inbox| {
                        r.publish(reply_inbox, "response") catch {};
                    }
                } else break;
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{
        responder,
        sub,
    });
    defer _ = handler.cancel(io_r.io());

    var success_count: u32 = 0;
    for (0..5) |_| {
        const reply = requester.request(
            "multi.service",
            "request",
            2000,
        ) catch continue;

        if (reply) |msg| {
            msg.deinit();
            success_count += 1;
        }
    }

    if (success_count >= 4) {
        reportResult("multi_requests_seq", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/5",
            .{success_count},
        ) catch "e";
        reportResult("multi_requests_seq", false, detail);
    }
}

/// Helper: spawns a responder fiber that replies "pong" forever
/// to the given subject. The caller cancels the returned future
/// in defer to stop it.
const RespHandler = struct {
    fn run(client: *nats.Client, sub: *nats.Subscription) void {
        while (true) {
            const req = sub.nextMsgTimeout(2000) catch return;
            const m = req orelse return;
            defer m.deinit();
            const reply = m.reply_to orelse continue;
            client.publish(reply, "pong") catch return;
        }
    }
};

/// Muxer-specific test: proves the old per-request subscription
/// path is gone. That path carried a hardcoded 5ms latency floor;
/// instead of depending on sub-5ms wall-clock timing on CI, assert
/// that request() creates one wildcard response mux subscription
/// and reuses it for subsequent requests.
pub fn testMuxerLatencyFloor(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("muxer_latency_floor", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    const sub = responder.subscribeSync("muxer.lat.test") catch {
        reportResult("muxer_latency_floor", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    var resp_fut = io_r.io().async(RespHandler.run, .{ responder, sub });
    defer _ = resp_fut.cancel(io_r.io());

    const io_q = utils.newIo(allocator);
    defer io_q.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_q.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("muxer_latency_floor", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const before_subs = requester.numSubscriptions();
    if (before_subs != 0) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "subs before request: {d}",
            .{before_subs},
        ) catch "e";
        reportResult("muxer_latency_floor", false, detail);
        return;
    }

    for (0..6) |i| {
        const reply = requester.request(
            "muxer.lat.test",
            "ping",
            2000,
        ) catch {
            reportResult("muxer_latency_floor", false, "request failed");
            return;
        };

        if (reply) |m| {
            defer m.deinit();
            if (!std.mem.eql(u8, m.data, "pong")) {
                reportResult("muxer_latency_floor", false, "wrong reply");
                return;
            }
        } else {
            reportResult("muxer_latency_floor", false, "no reply");
            return;
        }

        const subs = requester.numSubscriptions();
        if (subs != 1) {
            var buf: [64]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "request {d}: subs={d}, want 1",
                .{ i + 1, subs },
            ) catch "e";
            reportResult("muxer_latency_floor", false, detail);
            return;
        }
    }

    reportResult("muxer_latency_floor", true, "");
}

/// Muxer-specific test: proves the muxer's PING/PONG init cost
/// is amortized to zero. Issues 100 sequential requests on the
/// same connection and asserts the average round-trip is well
/// under 1ms (the cold first call is the only one paying for
/// ensureRespMux + PING/PONG).
pub fn testMuxerRapidSequential(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("muxer_rapid_sequential", false, "responder connect failed");
        return;
    };
    defer responder.deinit();

    const sub = responder.subscribeSync("muxer.rapid.test") catch {
        reportResult("muxer_rapid_sequential", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    var resp_fut = io_r.io().async(RespHandler.run, .{ responder, sub });
    defer _ = resp_fut.cancel(io_r.io());

    const io_q = utils.newIo(allocator);
    defer io_q.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_q.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("muxer_rapid_sequential", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const N: u32 = 100;
    var success: u32 = 0;
    const start = std.Io.Timestamp.now(io_q.io(), .awake);
    var i: u32 = 0;
    while (i < N) : (i += 1) {
        const reply = requester.request(
            "muxer.rapid.test",
            "ping",
            2000,
        ) catch break;
        if (reply) |m| {
            defer m.deinit();
            if (std.mem.eql(u8, m.data, "pong")) success += 1;
        } else break;
    }
    const end = std.Io.Timestamp.now(io_q.io(), .awake);

    if (success != N) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/{d} replies",
            .{ success, N },
        ) catch "e";
        reportResult("muxer_rapid_sequential", false, detail);
        return;
    }

    const elapsed_ns: u64 = @intCast(start.durationTo(end).nanoseconds);
    const total_ms = elapsed_ns / std.time.ns_per_ms;

    // The old per-request-sub path burned at least 5ms per call
    // in the artificial sleep alone, so 100 requests would take
    // >= 500ms even before counting SUB/UNSUB churn. The muxer
    // amortizes ensureRespMux to one PING/PONG and then uses the
    // wildcard sub for every subsequent request, so total time
    // is dominated by per-call dispatch overhead. We assert well
    // under the old floor to prove the muxer is on the hot path.
    if (total_ms < 400) {
        reportResult("muxer_rapid_sequential", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "100 requests took {d}ms (expected < 400ms)",
            .{total_ms},
        ) catch "e";
        reportResult("muxer_rapid_sequential", false, detail);
    }
}

/// Muxer-specific test: proves no use-after-free or leak when a
/// request times out (waiter is removed from the resp_map by the
/// cleanup defer in requestAwaitResp). Fires N timing-out
/// requests against a nonexistent subject and asserts each
/// returns null cleanly without leaking the waiter slot.
pub fn testMuxerTimeoutCleanup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false, .no_responders = false },
    ) catch {
        reportResult("muxer_timeout_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit();

    var i: u32 = 0;
    while (i < 20) : (i += 1) {
        const reply = client.request(
            "muxer.cleanup.noexist",
            "ping",
            20,
        ) catch {
            reportResult("muxer_timeout_cleanup", false, "request error");
            return;
        };
        if (reply) |m| {
            m.deinit();
            reportResult(
                "muxer_timeout_cleanup",
                false,
                "unexpected reply",
            );
            return;
        }
    }

    if (client.isConnected()) {
        reportResult("muxer_timeout_cleanup", true, "");
    } else {
        reportResult(
            "muxer_timeout_cleanup",
            false,
            "disconnected after timeouts",
        );
    }
}

const ScatterResponder = struct {
    fn handle(
        c: *nats.Client,
        s: *nats.Subscription,
        tag: []const u8,
    ) void {
        const req = s.nextMsgTimeout(3000) catch return;
        const m = req orelse return;
        defer m.deinit();
        const reply = m.reply_to orelse return;
        c.publish(reply, tag) catch return;
    }
};

pub fn testRequestManyCount(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const responder_count: u32 = 3;

    var io_ctxs: [3]*utils.TestIo = undefined;
    var responders: [3]*nats.Client = undefined;
    var subs: [3]*nats.Subscription = undefined;
    var futs: [3]std.Io.Future(void) = undefined;

    var i: usize = 0;
    while (i < responder_count) : (i += 1) {
        io_ctxs[i] = utils.newIo(allocator);
        responders[i] = nats.Client.connect(
            allocator,
            io_ctxs[i].io(),
            url,
            .{ .reconnect = false },
        ) catch {
            reportResult(
                "request_many_count",
                false,
                "responder connect failed",
            );
            return;
        };
        subs[i] = responders[i].subscribeSync(
            "rm.count.service",
        ) catch {
            reportResult(
                "request_many_count",
                false,
                "responder sub failed",
            );
            return;
        };
    }
    defer {
        var j: usize = responder_count;
        while (j > 0) : (j -= 1) {
            _ = futs[j - 1].cancel(io_ctxs[j - 1].io());
            subs[j - 1].deinit();
            responders[j - 1].deinit();
            io_ctxs[j - 1].deinit();
        }
    }
    io_ctxs[0].io().sleep(.fromMilliseconds(50), .awake) catch {};

    i = 0;
    while (i < responder_count) : (i += 1) {
        futs[i] = io_ctxs[i].io().async(
            ScatterResponder.handle,
            .{ responders[i], subs[i], "ok" },
        );
    }

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_count",
            false,
            "requester connect failed",
        );
        return;
    };
    defer requester.deinit();

    var iter = requester.requestMany(
        "rm.count.service",
        "ping",
        .{
            .max_wait_ms = 2000,
            .max_messages = responder_count,
        },
    ) catch {
        reportResult(
            "request_many_count",
            false,
            "requestMany failed",
        );
        return;
    };
    defer iter.deinit();

    var got: u32 = 0;
    while (iter.next() catch null) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "ok")) got += 1;
    }

    if (got == responder_count and
        iter.termination == .max_messages)
    {
        reportResult("request_many_count", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/{d} term={s}",
            .{ got, responder_count, @tagName(iter.termination) },
        ) catch "e";
        reportResult("request_many_count", false, detail);
    }
}

pub fn testRequestManySentinel(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_sentinel",
            false,
            "responder connect failed",
        );
        return;
    };
    defer responder.deinit();

    const sub = responder.subscribeSync(
        "rm.sentinel.service",
    ) catch {
        reportResult(
            "request_many_sentinel",
            false,
            "responder sub failed",
        );
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn run(c: *nats.Client, s: *nats.Subscription) void {
            const req = s.nextMsgTimeout(3000) catch return;
            const m = req orelse return;
            defer m.deinit();
            const reply = m.reply_to orelse return;
            c.publish(reply, "part-1") catch return;
            c.publish(reply, "part-2") catch return;
            c.publish(reply, "") catch return;
        }
    };

    var fut = io_r.io().async(Handler.run, .{ responder, sub });
    defer _ = fut.cancel(io_r.io());

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_sentinel",
            false,
            "requester connect failed",
        );
        return;
    };
    defer requester.deinit();

    var iter = requester.requestMany(
        "rm.sentinel.service",
        "ping",
        .{
            .max_wait_ms = 2000,
            .sentinel = nats.emptyPayloadSentinel(),
        },
    ) catch {
        reportResult(
            "request_many_sentinel",
            false,
            "requestMany failed",
        );
        return;
    };
    defer iter.deinit();

    var got: u32 = 0;
    while (iter.next() catch null) |msg| {
        defer msg.deinit();
        if (msg.data.len > 0) got += 1;
    }

    if (got == 2 and iter.termination == .sentinel) {
        reportResult("request_many_sentinel", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d} term={s}",
            .{ got, @tagName(iter.termination) },
        ) catch "e";
        reportResult("request_many_sentinel", false, detail);
    }
}

pub fn testRequestManyMaxWait(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();
    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false, .no_responders = false },
    ) catch {
        reportResult(
            "request_many_max_wait",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    const start = std.Io.Timestamp.now(io.io(), .awake);

    var iter = client.requestMany(
        "rm.absent.subject",
        "ping",
        .{ .max_wait_ms = 80 },
    ) catch {
        reportResult(
            "request_many_max_wait",
            false,
            "requestMany failed",
        );
        return;
    };
    defer iter.deinit();

    var got: u32 = 0;
    while (iter.next() catch null) |msg| {
        defer msg.deinit();
        got += 1;
    }

    const end = std.Io.Timestamp.now(io.io(), .awake);
    const elapsed_ns: i96 = end.nanoseconds - start.nanoseconds;
    const elapsed_ms: i64 = @intCast(
        @divFloor(elapsed_ns, std.time.ns_per_ms),
    );

    const ok = got == 0 and
        iter.termination == .max_wait and
        elapsed_ms >= 70 and elapsed_ms < 500;
    if (ok) {
        reportResult("request_many_max_wait", true, "");
    } else {
        var buf: [80]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got={d} term={s} ms={d}",
            .{
                got,
                @tagName(iter.termination),
                elapsed_ms,
            },
        ) catch "e";
        reportResult("request_many_max_wait", false, detail);
    }
}

const DelayedResponder = struct {
    fn handle(
        c: *nats.Client,
        s: *nats.Subscription,
        io: std.Io,
        delay_ms: u32,
    ) void {
        const req = s.nextMsgTimeout(3000) catch return;
        const m = req orelse return;
        defer m.deinit();
        const reply = m.reply_to orelse return;
        io.sleep(.fromMilliseconds(delay_ms), .awake) catch return;
        c.publish(reply, "ok") catch return;
    }
};

// Verifies ADR-47 stall semantics: the first reply still gets the
// full max_wait_ms (stall_ms must not fire before any reply). The
// stall timer only applies between replies. Without the fix, this
// test would terminate at ~stall_ms with 0 messages.
pub fn testRequestManyStall(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io_r = utils.newIo(allocator);
    defer io_r.deinit();
    const responder = nats.Client.connect(
        allocator,
        io_r.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_stall",
            false,
            "responder connect failed",
        );
        return;
    };
    defer responder.deinit();

    const sub = responder.subscribeSync(
        "rm.stall.service",
    ) catch {
        reportResult(
            "request_many_stall",
            false,
            "responder sub failed",
        );
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    var fut = io_r.io().async(
        DelayedResponder.handle,
        .{ responder, sub, io_r.io(), @as(u32, 300) },
    );
    defer _ = fut.cancel(io_r.io());

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_stall",
            false,
            "requester connect failed",
        );
        return;
    };
    defer requester.deinit();

    const start = std.Io.Timestamp.now(io_req.io(), .awake);

    var iter = requester.requestMany(
        "rm.stall.service",
        "ping",
        .{
            .max_wait_ms = 2000,
            .stall_ms = 100,
        },
    ) catch {
        reportResult(
            "request_many_stall",
            false,
            "requestMany failed",
        );
        return;
    };
    defer iter.deinit();

    var got: u32 = 0;
    while (iter.next() catch null) |msg| {
        defer msg.deinit();
        got += 1;
    }

    const end = std.Io.Timestamp.now(io_req.io(), .awake);
    const elapsed_ns: i96 = end.nanoseconds - start.nanoseconds;
    const elapsed_ms: i64 = @intCast(
        @divFloor(elapsed_ns, std.time.ns_per_ms),
    );

    // First reply arrives ~300 ms in. Stall (100 ms) then fires
    // ~400 ms. Without the fix, the call would end ~100 ms with
    // got=0 and termination=.stall.
    const ok = got == 1 and
        iter.termination == .stall and
        elapsed_ms >= 300 and elapsed_ms < 1500;
    if (ok) {
        reportResult("request_many_stall", true, "");
    } else {
        var buf: [96]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got={d} term={s} ms={d}",
            .{
                got,
                @tagName(iter.termination),
                elapsed_ms,
            },
        ) catch "e";
        reportResult("request_many_stall", false, detail);
    }
}

// Verifies ADR-47 503/no-responders behavior: when the server has
// no_responders enabled, an unbound subject ends the iterator with
// termination=.no_responders rather than waiting out max_wait_ms.
pub fn testRequestManyNoResponders(allocator: std.mem.Allocator) void {
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
            "request_many_no_responders",
            false,
            "connect failed",
        );
        return;
    };
    defer client.deinit();

    const start = std.Io.Timestamp.now(io.io(), .awake);

    var iter = client.requestMany(
        "rm.absent.no_responders.subject",
        "ping",
        .{ .max_wait_ms = 2000 },
    ) catch {
        reportResult(
            "request_many_no_responders",
            false,
            "requestMany failed",
        );
        return;
    };
    defer iter.deinit();

    var got: u32 = 0;
    while (iter.next() catch null) |msg| {
        defer msg.deinit();
        got += 1;
    }

    const end = std.Io.Timestamp.now(io.io(), .awake);
    const elapsed_ns: i96 = end.nanoseconds - start.nanoseconds;
    const elapsed_ms: i64 = @intCast(
        @divFloor(elapsed_ns, std.time.ns_per_ms),
    );

    const ok = got == 0 and
        iter.termination == .no_responders and
        elapsed_ms < 1000;
    if (ok) {
        reportResult("request_many_no_responders", true, "");
    } else {
        var buf: [96]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got={d} term={s} ms={d}",
            .{
                got,
                @tagName(iter.termination),
                elapsed_ms,
            },
        ) catch "e";
        reportResult("request_many_no_responders", false, detail);
    }
}

const CollectingHandler = struct {
    count: *u32,
    bytes: *u32,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        self.count.* += 1;
        self.bytes.* += @intCast(msg.data.len);
    }
};

// Verifies the callback variant of requestMany delivers each reply
// to MsgHandler.onMessage and returns a RequestManyResult with the
// correct count and termination reason.
pub fn testRequestManyCallback(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const responder_count: u32 = 3;

    var io_ctxs: [3]*utils.TestIo = undefined;
    var responders: [3]*nats.Client = undefined;
    var subs: [3]*nats.Subscription = undefined;
    var futs: [3]std.Io.Future(void) = undefined;

    var i: usize = 0;
    while (i < responder_count) : (i += 1) {
        io_ctxs[i] = utils.newIo(allocator);
        responders[i] = nats.Client.connect(
            allocator,
            io_ctxs[i].io(),
            url,
            .{ .reconnect = false },
        ) catch {
            reportResult(
                "request_many_callback",
                false,
                "responder connect failed",
            );
            return;
        };
        subs[i] = responders[i].subscribeSync(
            "rm.cb.service",
        ) catch {
            reportResult(
                "request_many_callback",
                false,
                "responder sub failed",
            );
            return;
        };
    }
    defer {
        var j: usize = responder_count;
        while (j > 0) : (j -= 1) {
            _ = futs[j - 1].cancel(io_ctxs[j - 1].io());
            subs[j - 1].deinit();
            responders[j - 1].deinit();
            io_ctxs[j - 1].deinit();
        }
    }
    io_ctxs[0].io().sleep(.fromMilliseconds(50), .awake) catch {};

    i = 0;
    while (i < responder_count) : (i += 1) {
        futs[i] = io_ctxs[i].io().async(
            ScatterResponder.handle,
            .{ responders[i], subs[i], "abc" },
        );
    }

    const io_req = utils.newIo(allocator);
    defer io_req.deinit();
    const requester = nats.Client.connect(
        allocator,
        io_req.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult(
            "request_many_callback",
            false,
            "requester connect failed",
        );
        return;
    };
    defer requester.deinit();

    var count: u32 = 0;
    var bytes: u32 = 0;
    var handler = CollectingHandler{
        .count = &count,
        .bytes = &bytes,
    };

    const result = requester.requestManyCallback(
        "rm.cb.service",
        "ping",
        .{
            .max_wait_ms = 2000,
            .max_messages = responder_count,
        },
        nats.MsgHandler.init(CollectingHandler, &handler),
    ) catch {
        reportResult(
            "request_many_callback",
            false,
            "requestManyCallback failed",
        );
        return;
    };

    const ok = count == responder_count and
        bytes == responder_count * 3 and
        result.received == responder_count and
        result.termination == .max_messages;
    if (ok) {
        reportResult("request_many_callback", true, "");
    } else {
        var buf: [96]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "count={d} bytes={d} recv={d} term={s}",
            .{
                count,
                bytes,
                result.received,
                @tagName(result.termination),
            },
        ) catch "e";
        reportResult("request_many_callback", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testRequestMethod(allocator);
    testRequestReturns(allocator);
    testReplyToPreserved(allocator);
    testRequestReplySuccess(allocator);
    testCrossClientRequestReply(allocator);
    testRequestTimeout(allocator);
    testRequestWithLargePayload(allocator);
    testMultipleRequestsSequential(allocator);
    testMuxerLatencyFloor(allocator);
    testMuxerRapidSequential(allocator);
    testMuxerTimeoutCleanup(allocator);
    testRequestManyCount(allocator);
    testRequestManySentinel(allocator);
    testRequestManyMaxWait(allocator);
    testRequestManyStall(allocator);
    testRequestManyNoResponders(allocator);
    testRequestManyCallback(allocator);
}
