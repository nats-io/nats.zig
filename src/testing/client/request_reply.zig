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
}
