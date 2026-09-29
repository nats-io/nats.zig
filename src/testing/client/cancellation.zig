//! Cancellation contracts.
//!
//! Every blocking client call must be cancelable: cancelling the fiber running
//! it unblocks whatever it is parked in, the call reports cancellation, its
//! transient state is cleaned up, and the client stays usable. An earlier
//! capability gate proved the underlying primitives (futexWait/futexWaitTimeout,
//! io.sleep, connect, fillMore) are cancelable on the pin; these tests confirm
//! each *call* surfaces it with the defined semantics:
//!
//!   - nextMsg  -> error.Canceled   (subscription notify_seq futexWait)
//!   - flush    -> error.Canceled   (pong_seq futexWaitTimeout)
//!   - request  -> error.Canceled   (reply-waiter futexWaitTimeout; distinct
//!                                    from a timeout, which returns a null msg)
//!   - connect  -> error.Canceled   (INFO read; half-built client is rolled back)
//!
//! nextMsg/request park indefinitely against the real server, so they cancel
//! deterministically. flush/connect need a peer that withholds PONG/INFO, so
//! they run against a tiny in-process UDS mock that accepts and then goes silent.
//!
//! Wired into client/tests.zig runAll.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const net = std.Io.net;
const Io = std.Io;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

// A minimal server INFO with auth disabled, so the client's handshake completes
// without needing a PONG (checkAuthRejection only runs when auth_required).
const mock_info =
    "INFO {\"server_id\":\"mock\",\"server_name\":\"mock\",\"version\":\"2.10.0\"," ++
    "\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576," ++
    "\"headers\":true}\r\n";

/// Mock UDS peer: accept one connection, optionally send INFO, then hold the
/// socket open and silent until this fiber is cancelled (teardown). Never sends
/// a PONG, so a client's flush() parks forever until it too is cancelled.
fn mockServe(server: *net.Server, io: Io, send_info: bool) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);
    if (send_info) {
        var wbuf: [256]u8 = undefined;
        var w = stream.writer(io, &wbuf);
        w.interface.writeAll(mock_info) catch {};
        w.interface.flush() catch {};
    }
    // Hold the connection open. Teardown cancels this fiber, unblocking the
    // sleep; the deferred close then releases the accepted socket.
    io.sleep(.fromSeconds(3600), .awake) catch {};
}

/// nextMsg() on an empty subscription parks; cancelling it returns
/// error.Canceled and leaves the client usable.
fn testNextMsgCancel(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("cancel_nextmsg", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("cancel.nextmsg.silent") catch {
        reportResult("cancel_nextmsg", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Park a consumer on nextMsg (no publisher), then cancel it.
    var future = io.io().async(nats.Client.Sub.nextMsg, .{sub});
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    if (future.cancel(io.io())) |msg| {
        msg.deinit();
        reportResult("cancel_nextmsg", false, "returned a message, expected Canceled");
        return;
    } else |err| {
        if (err != error.Canceled) {
            reportResult("cancel_nextmsg", false, @errorName(err));
            return;
        }
    }

    // Client still usable: a fresh sub delivers a real message.
    const live = client.subscribeSync("cancel.nextmsg.live") catch {
        reportResult("cancel_nextmsg", false, "resubscribe failed");
        return;
    };
    defer live.deinit();
    client.publish("cancel.nextmsg.live", "ok") catch {
        reportResult("cancel_nextmsg", false, "publish failed");
        return;
    };
    if (live.nextMsgTimeout(1000) catch null) |m| {
        m.deinit();
        reportResult("cancel_nextmsg", true, "");
    } else {
        reportResult("cancel_nextmsg", false, "client unusable after cancel");
    }
}

/// request() to a subject with no responder parks on the reply waiter;
/// cancelling it returns error.Canceled (distinct from a timeout's null),
/// removes the reply token, and leaves the client usable.
fn testRequestCancel(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("cancel_request", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Subscribe (but never reply) so the server sees a responder and does NOT
    // send an immediate 503 No-Responders reply -- otherwise request() returns
    // that reply at once and there is nothing to cancel.
    const silent = client.subscribeSync("cancel.request.silent") catch {
        reportResult("cancel_request", false, "silent sub failed");
        return;
    };
    defer silent.deinit();

    // Long timeout so the call is parked (not timed out) when we cancel.
    var future = io.io().async(
        nats.Client.request,
        .{ client, "cancel.request.silent", "ping", @as(u32, 30_000) },
    );
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    if (future.cancel(io.io())) |maybe_msg| {
        if (maybe_msg) |m| {
            m.deinit();
            reportResult("cancel_request", false, "returned a message, expected Canceled");
        } else {
            reportResult("cancel_request", false, "returned null, expected Canceled");
        }
        return;
    } else |err| {
        if (err != error.Canceled) {
            reportResult("cancel_request", false, @errorName(err));
            return;
        }
    }

    // Client still usable: a real request/reply round-trips (proves the mux
    // survived the cancelled request). Use `concurrent` so the request actually
    // publishes and parks while we serve the responder from this thread; a lazy
    // `async` would not run until awaited, so the responder would see nothing.
    const responder = client.subscribeSync("cancel.request.echo") catch {
        reportResult("cancel_request", false, "responder sub failed");
        return;
    };
    defer responder.deinit();

    var rfuture = io.io().concurrent(
        nats.Client.request,
        .{ client, "cancel.request.echo", "hi", @as(u32, 2000) },
    ) catch {
        reportResult("cancel_request", false, "follow-up spawn failed");
        return;
    };
    if (responder.nextMsgTimeout(1500) catch null) |req| {
        defer req.deinit();
        if (req.reply_to) |r| client.publish(r, "pong") catch {};
    }
    if (rfuture.await(io.io())) |maybe_msg| {
        if (maybe_msg) |m| {
            m.deinit();
            reportResult("cancel_request", true, "");
        } else {
            reportResult("cancel_request", false, "no reply on follow-up");
        }
    } else |err| {
        reportResult("cancel_request", false, @errorName(err));
    }
}

/// flush() parks waiting for a PONG the mock never sends; cancelling it returns
/// error.Canceled.
fn testFlushCancel(allocator: std.mem.Allocator) void {
    const path = "/tmp/nats-zig-cancel-flush.sock";
    const io = utils.newIo(allocator);
    defer io.deinit();
    const the_io = io.io();

    Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    const ua = net.UnixAddress.init(path) catch {
        reportResult("cancel_flush", false, "addr init failed");
        return;
    };
    var server = ua.listen(the_io, .{}) catch {
        reportResult("cancel_flush", false, "listen failed");
        return;
    };
    defer {
        server.deinit(the_io);
        Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    }

    var mock = the_io.async(mockServe, .{ &server, the_io, true });
    defer _ = mock.cancel(the_io);

    // ping_interval large so the client's own keepalive never fires (which would
    // otherwise flag the silent peer stale and close before we cancel).
    var url_buf: [96]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats+uds://{s}", .{path}) catch {
        reportResult("cancel_flush", false, "url fmt failed");
        return;
    };
    const client = nats.Client.connect(allocator, the_io, url, .{
        .reconnect = false,
        .ping_interval_ms = 60_000,
    }) catch {
        reportResult("cancel_flush", false, "connect to mock failed");
        return;
    };
    defer client.deinit();

    // flush sends PING and parks; the mock never PONGs.
    var future = the_io.async(nats.Client.flush, .{ client, @as(u64, 5_000_000_000) });
    the_io.sleep(.fromMilliseconds(150), .awake) catch {};

    if (future.cancel(the_io)) |_| {
        reportResult("cancel_flush", false, "flush returned ok, expected Canceled");
    } else |err| {
        if (err == error.Canceled) {
            reportResult("cancel_flush", true, "");
        } else {
            reportResult("cancel_flush", false, @errorName(err));
        }
    }
}

/// connect() parks reading INFO the mock never sends; cancelling it returns
/// error.Canceled and rolls back the half-built client (no leak).
fn testConnectCancel(allocator: std.mem.Allocator) void {
    const path = "/tmp/nats-zig-cancel-connect.sock";
    const io = utils.newIo(allocator);
    defer io.deinit();
    const the_io = io.io();

    Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    const ua = net.UnixAddress.init(path) catch {
        reportResult("cancel_connect", false, "addr init failed");
        return;
    };
    var server = ua.listen(the_io, .{}) catch {
        reportResult("cancel_connect", false, "listen failed");
        return;
    };
    defer {
        server.deinit(the_io);
        Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    }

    // send_info = false: accept then stay silent, so connect parks reading INFO.
    var mock = the_io.async(mockServe, .{ &server, the_io, false });
    defer _ = mock.cancel(the_io);

    var url_buf: [96]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats+uds://{s}", .{path}) catch {
        reportResult("cancel_connect", false, "url fmt failed");
        return;
    };

    // Long connect timeout so the call is parked (not timed out) when cancelled.
    var future = the_io.async(nats.Client.connect, .{
        allocator, the_io, url,
        nats.Client.Options{
            .reconnect = false,
            .connect_timeout_ns = 10_000_000_000,
        },
    });
    the_io.sleep(.fromMilliseconds(150), .awake) catch {};

    if (future.cancel(the_io)) |client| {
        client.deinit();
        reportResult("cancel_connect", false, "connect succeeded, expected Canceled");
    } else |err| {
        if (err == error.Canceled) {
            reportResult("cancel_connect", true, "");
        } else {
            reportResult("cancel_connect", false, @errorName(err));
        }
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testNextMsgCancel(allocator);
    testRequestCancel(allocator);
    testFlushCancel(allocator);
    testConnectCancel(allocator);
}
