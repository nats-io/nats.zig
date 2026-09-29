//! Transient backpressure and half-open stale-trigger (write-error reconnect)
//! against an in-process UDS mock peer.
//!
//! These two cases need a peer that manipulates its *read* side, which a real
//! nats-server cannot be made to do on demand, so they run against a mock:
//!
//!  - backpressure: the mock stops reading while still *sending* to the client.
//!    The client's send buffer fills (writer backpressures), yet its reader must
//!    keep delivering the inbound MSGs and the client must NOT self-reconnect;
//!    when the mock reads again everything drains. Proves a backpressured writer
//!    never stalls inbound delivery and a *transient* wedge recovers.
//!  - half-open: the mock sends INFO then goes silent (never PONGs) — writes
//!    from the client are not read, reads never EOF. The writer's health check
//!    detects the stale peer (pings_outstanding exceeds max) and hands off to the
//!    reader, which closes; the trigger is not silently dropped.
//!
//! Wired into client/tests.zig runAll.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const net = std.Io.net;
const Io = std.Io;

const reportResult = utils.reportResult;

const mock_info =
    "INFO {\"server_id\":\"mock\",\"server_name\":\"mock\",\"version\":\"2.10.0\"," ++
    "\"proto\":1,\"host\":\"127.0.0.1\",\"port\":4222,\"max_payload\":1048576," ++
    "\"headers\":true}\r\n";

fn writeAll(stream: net.Stream, io: Io, bytes: []const u8) void {
    var wbuf: [512]u8 = undefined;
    var w = stream.writer(io, &wbuf);
    w.interface.writeAll(bytes) catch {};
    w.interface.flush() catch {};
}

// --- transient backpressure -----------------------------------------------

/// Mock for the backpressure test: accept, send INFO, wait for the client to
/// subscribe (sid 1), then WITHOUT reading push a few MSGs plus a PING at the
/// client. Hold the read
/// side idle for a window (the client's send buffer fills as it publishes), then
/// drain (peer reads again) so the backlog + queued PONG flow. Returns on EOF
/// when the test closes the client -- so the test *awaits* this fiber rather than
/// cancelling it (cancelling a fiber parked in an evented read against a
/// backpressured peer can deadlock the canceller).
fn b7Serve(server: *net.Server, io: Io) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);

    writeAll(stream, io, mock_info);

    // Give the client time to reach connected + subscribe to "bp.sub" (sid 1).
    io.sleep(.fromMilliseconds(300), .awake) catch {};

    // Deliver inbound MSGs (sid 1) while we are NOT reading -- the client's
    // reader must route these even though its writer is backpressured.
    var i: u8 = 0;
    while (i < 5) : (i += 1) {
        writeAll(stream, io, "MSG bp.sub 1 7\r\ninbound\r\n");
    }
    // Ask the client to PONG while it cannot write (queued, not dropped).
    writeAll(stream, io, "PING\r\n");

    // Hold the read side idle so the client's send buffer stays full (transient
    // wedge), then read again so the backlog + queued PONG drain.
    io.sleep(.fromMilliseconds(600), .awake) catch {};

    // Separate buffers: the stream reader owns `rbuf`; readSliceShort copies out
    // into `dst`. Reusing one for both aliases the @memcpy and panics.
    var rbuf: [16 * 1024]u8 = undefined;
    var dst: [16 * 1024]u8 = undefined;
    var r = stream.reader(io, &rbuf);
    while (true) {
        const n = r.interface.readSliceShort(&dst) catch break;
        if (n == 0) break; // client closed (test tore down) -> finish
    }
}

const PubState = struct {
    client: *nats.Client,
    io: Io,
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
};

/// Publish large messages to fill the client's send buffer (writer backpressure)
/// until told to stop. Yields each iteration -- a real publisher backs off rather
/// than busy-spinning, and on a single-threaded executor the yield is what lets
/// the reader fiber run (a spin here would starve it, which is a property of the
/// test harness, not the client).
fn backpressurePublisher(state: *PubState) void {
    var payload: [16 * 1024]u8 = undefined;
    @memset(&payload, 'x');
    while (!state.stop.load(.acquire)) {
        state.client.publish("bp.flood", &payload) catch {};
        state.io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
}

fn testTransientBackpressure(allocator: std.mem.Allocator) void {
    const path = "/tmp/nats-zig-bp-it.sock";
    const io = utils.newIo(allocator);
    defer io.deinit();
    const the_io = io.io();

    Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    const ua = net.UnixAddress.init(path) catch {
        reportResult("transient_backpressure", false, "addr init failed");
        return;
    };
    var server = ua.listen(the_io, .{}) catch {
        reportResult("transient_backpressure", false, "listen failed");
        return;
    };
    defer {
        server.deinit(the_io);
        Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    }

    var mock = the_io.async(b7Serve, .{ &server, the_io });
    // await, not cancel: declared before the client's defer so it runs AFTER
    // client.deinit -- the client's close is the EOF that lets b7Serve return.
    defer mock.await(the_io);

    var url_buf: [96]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats+uds://{s}", .{path}) catch {
        reportResult("transient_backpressure", false, "url fmt failed");
        return;
    };
    // Large ping interval so keepalive never flags the (deliberately silent-ish)
    // peer stale during the transient window; reconnect off so any teardown is
    // observable as a disconnect rather than a silent reconnect.
    const client = nats.Client.connect(allocator, the_io, url, .{
        .reconnect = false,
        .ping_interval_ms = 60_000,
    }) catch {
        reportResult("transient_backpressure", false, "connect to mock failed");
        return;
    };
    defer client.deinit();

    var sub = client.subscribeSync("bp.sub") catch {
        reportResult("transient_backpressure", false, "subscribe failed");
        return;
    };
    defer sub.deinit();
    // Ensure the SUB is on the wire before the mock starts pushing MSGs.
    client.flushBuffer() catch {};

    // Flood publishes so the writer backpressures against the non-reading peer.
    var pstate: PubState = .{ .client = client, .io = the_io };
    var pub_future = the_io.concurrent(backpressurePublisher, .{&pstate}) catch {
        reportResult("transient_backpressure", false, "publisher spawn failed");
        return;
    };

    // Core invariant: the reader keeps delivering inbound MSGs even while the
    // writer is backpressured. Collect the 5 the mock sent.
    var received: u8 = 0;
    var tries: u8 = 0;
    while (received < 5 and tries < 20) : (tries += 1) {
        if (sub.nextMsgTimeout(500) catch null) |msg| {
            msg.deinit();
            received += 1;
        }
    }

    pstate.stop.store(true, .release);
    pub_future.await(the_io);

    // No self-reconnect: the client stayed on the one (transiently wedged)
    // connection throughout.
    if (received >= 5 and client.isConnected()) {
        reportResult("transient_backpressure", true, "");
    } else if (received < 5) {
        reportResult("transient_backpressure", false, "reader stalled under backpressure");
    } else {
        reportResult("transient_backpressure", false, "connection dropped (unexpected reconnect/close)");
    }
}

// --- half-open stale trigger ----------------------------------------------

/// Mock for the half-open trigger: accept, send INFO, then go silent -- never
/// PONG, never read. The client's writer PINGs, gets nothing, and its health
/// check must mark the peer stale.
fn staleServe(server: *net.Server, io: Io) void {
    var stream = server.accept(io) catch return;
    defer stream.close(io);
    writeAll(stream, io, mock_info);
    io.sleep(.fromSeconds(3600), .awake) catch {};
}

fn testHalfOpenStaleTrigger(allocator: std.mem.Allocator) void {
    const path = "/tmp/nats-zig-stale-it.sock";
    const io = utils.newIo(allocator);
    defer io.deinit();
    const the_io = io.io();

    Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    const ua = net.UnixAddress.init(path) catch {
        reportResult("halfopen_stale_trigger", false, "addr init failed");
        return;
    };
    var server = ua.listen(the_io, .{}) catch {
        reportResult("halfopen_stale_trigger", false, "listen failed");
        return;
    };
    defer {
        server.deinit(the_io);
        Io.Dir.deleteFile(Io.Dir.cwd(), the_io, path) catch {};
    }

    var mock = the_io.async(staleServe, .{ &server, the_io });
    defer _ = mock.cancel(the_io);

    var url_buf: [96]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats+uds://{s}", .{path}) catch {
        reportResult("halfopen_stale_trigger", false, "url fmt failed");
        return;
    };
    // Fast keepalive with a 1-ping tolerance: the writer PINGs, no PONG comes,
    // the next health check sees pings_outstanding(1) >= max(1) -> stale ->
    // transport_failed -> reader closes. reconnect off so the close is terminal.
    const client = nats.Client.connect(allocator, the_io, url, .{
        .reconnect = false,
        .ping_interval_ms = 50,
        .max_pings_outstanding = 1,
    }) catch {
        reportResult("halfopen_stale_trigger", false, "connect to mock failed");
        return;
    };
    defer client.deinit();

    if (!client.isConnected()) {
        reportResult("halfopen_stale_trigger", false, "not connected initially");
        return;
    }

    // The stale trigger must fire well within a couple of ping intervals.
    var waited_ms: u32 = 0;
    while (client.isConnected() and waited_ms < 3000) {
        the_io.sleep(.fromMilliseconds(25), .awake) catch {};
        waited_ms += 25;
    }

    if (!client.isConnected()) {
        reportResult("halfopen_stale_trigger", true, "");
    } else {
        reportResult("halfopen_stale_trigger", false, "stale peer not detected (still connected)");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testTransientBackpressure(allocator);
    testHalfOpenStaleTrigger(allocator);
}
