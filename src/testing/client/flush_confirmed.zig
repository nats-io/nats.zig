//! FlushConfirmed Integration Tests
//!
//! Tests for flushConfirmed() which sends buffered data + PING and waits
//! for PONG confirmation from the server.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

/// Basic flushConfirmed - publish, confirm, verify receipt.
pub fn testFlushConfirmedBasic(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_basic", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.basic") catch {
        reportResult("flush_confirmed_basic", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    client.publish("fc.basic", "confirmed-message") catch {
        reportResult("flush_confirmed_basic", false, "publish failed");
        return;
    };

    // Use flushConfirmed with 5 second timeout
    client.flush(5_000_000_000) catch {
        reportResult("flush_confirmed_basic", false, "flushConfirmed failed");
        return;
    };

    var future = io.io().async(
        nats.Client.Sub.nextMsg,
        .{sub},
    );
    defer if (future.cancel(io.io())) |m| m.deinit() else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "confirmed-message")) {
            reportResult("flush_confirmed_basic", true, "");
            return;
        }
    } else |_| {}

    reportResult("flush_confirmed_basic", false, "message mismatch");
}

/// Test flushConfirmed with multiple buffered messages.
pub fn testFlushConfirmedMultipleMessages(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_multi", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.batch") catch {
        reportResult("flush_confirmed_multi", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // Publish 10 messages without flushing
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "msg-{d}", .{i}) catch "msg";
        client.publish("fc.batch", payload) catch {
            reportResult("flush_confirmed_multi", false, "publish failed");
            return;
        };
    }

    // Single flushConfirmed should send all
    client.flush(5_000_000_000) catch {
        reportResult("flush_confirmed_multi", false, "flushConfirmed failed");
        return;
    };

    // Verify all 10 messages received
    var received: u32 = 0;
    for (0..10) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("flush_confirmed_multi", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/10",
            .{received},
        ) catch "err";
        reportResult("flush_confirmed_multi", false, detail);
    }
}

/// Test that normal operations work after flushConfirmed.
pub fn testFlushConfirmedNoSideEffects(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_side_effects", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.side") catch {
        reportResult("flush_confirmed_side_effects", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // First: publish + flushConfirmed
    client.publish("fc.side", "first") catch {
        reportResult("flush_confirmed_side_effects", false, "pub1 failed");
        return;
    };
    client.flush(5_000_000_000) catch {
        reportResult("flush_confirmed_side_effects", false, "flushConfirmed failed");
        return;
    };

    // Second: publish + regular flush (should still work)
    client.publish("fc.side", "second") catch {
        reportResult("flush_confirmed_side_effects", false, "pub2 failed");
        return;
    };
    client.flushBuffer() catch {
        reportResult("flush_confirmed_side_effects", false, "flush failed");
        return;
    };

    // Verify both messages received
    var received: u32 = 0;
    for (0..2) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            received += 1;
        }
    }

    if (received == 2) {
        reportResult("flush_confirmed_side_effects", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/2",
            .{received},
        ) catch "err";
        reportResult("flush_confirmed_side_effects", false, detail);
    }
}

/// Compare flushConfirmed vs flush - both deliver messages.
pub fn testFlushConfirmedVsFlush(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_vs_flush", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.compare") catch {
        reportResult("flush_confirmed_vs_flush", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // Publish with regular flush
    client.publish("fc.compare", "via-flush") catch {
        reportResult("flush_confirmed_vs_flush", false, "pub1 failed");
        return;
    };
    client.flushBuffer() catch {};

    // Publish with flushConfirmed
    client.publish("fc.compare", "via-confirmed") catch {
        reportResult("flush_confirmed_vs_flush", false, "pub2 failed");
        return;
    };
    client.flush(5_000_000_000) catch {
        reportResult("flush_confirmed_vs_flush", false, "flushConfirmed failed");
        return;
    };

    // Verify both arrive in order
    const msg1 = sub.nextMsgTimeout(500) catch null;
    if (msg1 == null) {
        reportResult("flush_confirmed_vs_flush", false, "no msg1");
        return;
    }
    defer msg1.?.deinit();

    const msg2 = sub.nextMsgTimeout(500) catch null;
    if (msg2 == null) {
        reportResult("flush_confirmed_vs_flush", false, "no msg2");
        return;
    }
    defer msg2.?.deinit();

    const ok1 = std.mem.eql(u8, msg1.?.data, "via-flush");
    const ok2 = std.mem.eql(u8, msg2.?.data, "via-confirmed");

    if (ok1 and ok2) {
        reportResult("flush_confirmed_vs_flush", true, "");
    } else {
        reportResult("flush_confirmed_vs_flush", false, "wrong order/content");
    }
}

/// Test flushConfirmed returns error when not connected.
pub fn testFlushConfirmedNotConnected(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_not_connected", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Drain to close connection
    _ = client.drain() catch {
        reportResult("flush_confirmed_not_connected", false, "drain failed");
        return;
    };

    // Now try flushConfirmed - should fail
    const result = client.flush(1_000_000_000);

    if (result) |_| {
        reportResult("flush_confirmed_not_connected", false, "should have failed");
    } else |_| {
        reportResult("flush_confirmed_not_connected", true, "");
    }
}

/// Test flushConfirmed with large payload.
pub fn testFlushConfirmedLargePayload(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_large", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.large") catch {
        reportResult("flush_confirmed_large", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // Allocate 64KB payload
    const payload = allocator.alloc(u8, 64 * 1024) catch {
        reportResult("flush_confirmed_large", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("fc.large", payload) catch {
        reportResult("flush_confirmed_large", false, "publish failed");
        return;
    };

    client.flush(5_000_000_000) catch {
        reportResult("flush_confirmed_large", false, "flushConfirmed failed");
        return;
    };

    var future = io.io().async(
        nats.Client.Sub.nextMsg,
        .{sub},
    );
    defer if (future.cancel(io.io())) |m| m.deinit() else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 64 * 1024) {
            reportResult("flush_confirmed_large", true, "");
            return;
        }
    } else |_| {}

    reportResult("flush_confirmed_large", false, "wrong size");
}

/// Test multiple sequential flushConfirmed calls.
pub fn testFlushConfirmedRapidFire(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_rapid", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("fc.rapid") catch {
        reportResult("flush_confirmed_rapid", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.flushBuffer() catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // 20 cycles of publish + flushConfirmed
    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "rapid-{d}", .{i}) catch "msg";

        client.publish("fc.rapid", payload) catch {
            reportResult("flush_confirmed_rapid", false, "publish failed");
            return;
        };

        client.flush(5_000_000_000) catch {
            reportResult("flush_confirmed_rapid", false, "flushConfirmed failed");
            return;
        };
    }

    // Verify all 20 messages received
    var received: u32 = 0;
    for (0..20) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            received += 1;
        }
    }

    if (received == 20) {
        reportResult("flush_confirmed_rapid", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/20",
            .{received},
        ) catch "err";
        reportResult("flush_confirmed_rapid", false, detail);
    }
}

/// Test flushConfirmed timeout behavior.
pub fn testFlushConfirmedTimeout(allocator: std.mem.Allocator) void {
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
        reportResult("flush_confirmed_timeout", false, "connect failed");
        return;
    };

    // Drain to disconnect, then try flushConfirmed with short timeout
    _ = client.drain() catch {
        reportResult("flush_confirmed_timeout", false, "drain failed");
        client.deinit();
        return;
    };

    // Should fail quickly (NotConnected, not timeout in this case)
    const result = client.flush(100_000_000); // 100ms
    client.deinit();

    if (result) |_| {
        reportResult("flush_confirmed_timeout", false, "should have failed");
    } else |err| {
        // Accept NotConnected or Timeout as valid failures
        if (err == error.NotConnected or err == error.Timeout) {
            reportResult("flush_confirmed_timeout", true, "");
        } else {
            reportResult("flush_confirmed_timeout", false, "unexpected error");
        }
    }
}

/// Run all flushConfirmed tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testFlushConfirmedBasic(allocator);
    testFlushConfirmedMultipleMessages(allocator);
    testFlushConfirmedNoSideEffects(allocator);
    testFlushConfirmedVsFlush(allocator);
    testFlushConfirmedNotConnected(allocator);
    testFlushConfirmedLargePayload(allocator);
    testFlushConfirmedRapidFire(allocator);
    testFlushConfirmedTimeout(allocator);
}
