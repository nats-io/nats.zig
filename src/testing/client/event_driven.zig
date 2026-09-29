//! Event-driven I/O tests.
//!
//! Prove the client is event-driven, not busy-polling/spinning:
//!  1. publish-while-idle is delivered promptly (producer -> io_task wakeup,
//!     no lost wakeup);
//!  2. a blocked nextMsg() consumes ~no CPU and wakes when a message routes
//!     (io_task -> consumer condition);
//!  3. unsubscribe unblocks a waiting nextMsg() (cancellation);
//!  4. request/reply still round-trips (futex reply waiter);
//!  5. an idle connection barely advances the io_task loop counter.
//!
//! Requires a running nats-server (gated like the other integration tests;
//! wired into client/tests.zig runAll).

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

/// (1) + (5): after the io_task has gone idle (blocked in poll), a publish is
/// delivered promptly, and the io_task loop counter barely moved while idle.
pub fn testPublishWhileIdle(allocator: std.mem.Allocator) void {
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
        reportResult("event_driven_publish_idle", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("idle.subject") catch {
        reportResult("event_driven_publish_idle", false, "sub failed");
        return;
    };
    defer sub.deinit();

    // Let the SUB flush and the io_task settle into its blocking poll().
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    const loops_before = client.ioTaskLoops();

    // Stay idle for a second: an event-driven io_task blocks the whole time.
    io.io().sleep(.fromMilliseconds(1000), .awake) catch {};

    const loops_idle = client.ioTaskLoops() - loops_before;
    // Coarse keepalive is >= 15s, so ~0 wakeups in 1 idle second. A busy poll
    // (old 1ms) would be ~1000, a spin far more. Generous bound.
    if (loops_idle > 50) {
        var buf: [96]u8 = undefined;
        const m = std.fmt.bufPrint(
            &buf,
            "io_task not idle: {d} loops in 1s",
            .{loops_idle},
        ) catch "not idle";
        reportResult("event_driven_publish_idle", false, m);
        return;
    }

    // Publish while idle: the producer must wake the io_task promptly.
    client.publish("idle.subject", "wakeup") catch {
        reportResult("event_driven_publish_idle", false, "publish failed");
        return;
    };

    const got = sub.nextMsgTimeout(1000) catch {
        reportResult("event_driven_publish_idle", false, "next errored");
        return;
    };
    if (got) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "wakeup")) {
            reportResult("event_driven_publish_idle", true, "");
            return;
        }
        reportResult("event_driven_publish_idle", false, "wrong payload");
        return;
    }
    reportResult("event_driven_publish_idle", false, "message not delivered");
}

/// (2): a nextMsg() blocked on an empty subscription wakes when a message is
/// routed. The io_task loop counter shows it was not spinning meanwhile.
pub fn testNextBlocksThenWakes(allocator: std.mem.Allocator) void {
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
        reportResult("event_driven_next_wakes", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("block.subject") catch {
        reportResult("event_driven_next_wakes", false, "sub failed");
        return;
    };
    defer sub.deinit();
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Park a consumer on nextMsg with a bounded timeout. The awaited message
    // (or, on the bail-out paths, the still-pending one) is freed exactly once
    // by this deferred cancel -- do NOT also deinit it inside the await block
    // below, or the cached result is returned and its buffer freed twice.
    var future = io.io().async(nats.Client.Sub.nextMsg, .{sub});
    defer if (future.cancel(io.io())) |msg| msg.deinit() else |_| {};

    const loops_before = client.ioTaskLoops();
    // Consumer + io_task both blocked here; neither should spin.
    io.io().sleep(.fromMilliseconds(300), .awake) catch {};
    const loops_idle = client.ioTaskLoops() - loops_before;
    if (loops_idle > 50) {
        reportResult("event_driven_next_wakes", false, "io_task spun while blocked");
        return;
    }

    // Route a message: the parked consumer must wake and return it.
    client.publish("block.subject", "hi") catch {
        reportResult("event_driven_next_wakes", false, "publish failed");
        return;
    };

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "hi")) {
            reportResult("event_driven_next_wakes", true, "");
            return;
        }
        reportResult("event_driven_next_wakes", false, "wrong payload");
        return;
    } else |_| {
        reportResult("event_driven_next_wakes", false, "next errored");
        return;
    }
}

/// (3): unsubscribe() unblocks a waiting nextMsg() (returns error.Closed).
pub fn testUnsubscribeUnblocksNext(allocator: std.mem.Allocator) void {
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
        reportResult("event_driven_unsub_unblocks", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("cancel.subject") catch {
        reportResult("event_driven_unsub_unblocks", false, "sub failed");
        return;
    };
    defer sub.deinit();
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    var future = io.io().async(nats.Client.Sub.nextMsg, .{sub});
    // Let the consumer park.
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    // Unsubscribe must wake the parked consumer promptly.
    sub.unsubscribe() catch {};

    if (future.await(io.io())) |msg| {
        // A message is unexpected here; free it and fail.
        msg.deinit();
        reportResult("event_driven_unsub_unblocks", false, "unexpected message");
    } else |err| {
        if (err == error.Closed or err == error.Canceled) {
            reportResult("event_driven_unsub_unblocks", true, "");
        } else {
            reportResult("event_driven_unsub_unblocks", false, "wrong error");
        }
    }
}

/// (4): request/reply round-trips through the futex reply waiter.
pub fn testRequestReplyRoundtrip(allocator: std.mem.Allocator) void {
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
        reportResult("event_driven_request_reply", false, "responder connect failed");
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
        reportResult("event_driven_request_reply", false, "requester connect failed");
        return;
    };
    defer requester.deinit();

    const sub = responder.subscribeSync("ed.service") catch {
        reportResult("event_driven_request_reply", false, "responder sub failed");
        return;
    };
    defer sub.deinit();
    io_r.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const Handler = struct {
        fn handle(r: *nats.Client, s: *nats.Subscription) void {
            if (s.nextMsgTimeout(2000) catch null) |req| {
                defer req.deinit();
                if (req.reply_to) |reply_inbox| {
                    r.publish(reply_inbox, "pong") catch {};
                }
            }
        }
    };

    var handler = io_r.io().async(Handler.handle, .{ responder, sub });
    defer _ = handler.cancel(io_r.io());

    const reply = requester.request("ed.service", "ping", 2000) catch {
        reportResult("event_driven_request_reply", false, "request failed");
        return;
    };

    if (reply) |msg| {
        defer msg.deinit();
        if (std.mem.eql(u8, msg.data, "pong")) {
            reportResult("event_driven_request_reply", true, "");
            return;
        }
    }
    reportResult("event_driven_request_reply", false, "no reply or wrong data");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testPublishWhileIdle(allocator);
    testNextBlocksThenWakes(allocator);
    testUnsubscribeUnblocksNext(allocator);
    testRequestReplyRoundtrip(allocator);
}
