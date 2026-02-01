//! Stats Tests for NATS  Client

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

pub fn testClientStats(allocator: std.mem.Allocator) void {
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
        reportResult("client_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_stats = client.getStats();
    if (initial_stats.msgs_out != 0) {
        reportResult("client_stats", false, "initial msgs_out != 0");
        return;
    }

    client.publish("async.stats", "test") catch {};
    client.flush(allocator) catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1) {
        reportResult("client_stats", true, "");
    } else {
        reportResult("client_stats", false, "msgs_out not incremented");
    }
}

pub fn testStatsIncrement(allocator: std.mem.Allocator) void {
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
        reportResult("stats_increment", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    for (0..10) |_| {
        client.publish("async.stats.inc", "msg") catch {};
    }
    client.flush(allocator) catch {};

    const after = client.getStats();

    if (after.msgs_out >= before.msgs_out + 10) {
        reportResult("stats_increment", true, "");
    } else {
        reportResult("stats_increment", false, "stats not incremented");
    }
}

pub fn testStatsBytesAccuracy(allocator: std.mem.Allocator) void {
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
        reportResult("stats_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    const payload = "0123456789" ** 10;
    client.publish("async.stats.bytes", payload) catch {};
    client.flush(allocator) catch {};

    const after = client.getStats();

    if (after.bytes_out >= before.bytes_out + 100) {
        reportResult("stats_bytes", true, "");
    } else {
        reportResult("stats_bytes", false, "bytes not tracked");
    }
}

pub fn testStatsMsgsIn(allocator: std.mem.Allocator) void {
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
        reportResult("stats_msgs_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "msgsin.test") catch {
        reportResult("stats_msgs_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const before = client.getStats();

    for (0..25) |_| {
        client.publish("msgsin.test", "data") catch {};
    }
    client.flush(allocator) catch {};

    var received: u32 = 0;
    for (0..30) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    const after = client.getStats();
    const msgs_in = after.msgs_in - before.msgs_in;

    if (msgs_in == 25 and received == 25) {
        reportResult("stats_msgs_in", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "in={d} recv={d}",
            .{ msgs_in, received },
        ) catch "e";
        reportResult("stats_msgs_in", false, detail);
    }
}

pub fn testStatsBytesIn(allocator: std.mem.Allocator) void {
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
        reportResult("stats_bytes_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "bytesin.test") catch {
        reportResult("stats_bytes_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const before = client.getStats();

    const payload = "01234567890123456789012345678901234567890123456789";
    for (0..10) |_| {
        client.publish("bytesin.test", payload) catch {};
    }
    client.flush(allocator) catch {};

    for (0..15) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    const after = client.getStats();
    const bytes_in = after.bytes_in - before.bytes_in;

    if (bytes_in == 500) {
        reportResult("stats_bytes_in", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/500",
            .{bytes_in},
        ) catch "e";
        reportResult("stats_bytes_in", false, detail);
    }
}

pub fn testConnectsCounter(allocator: std.mem.Allocator) void {
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
        reportResult("stats_connects", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const stats = client.getStats();
    if (stats.connects >= 1) {
        reportResult("stats_connects", true, "");
    } else {
        reportResult("stats_connects", false, "connects not incremented");
    }
}

pub fn testSubStats(allocator: std.mem.Allocator) void {
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
        reportResult("sub_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "substats.test") catch {
        reportResult("sub_stats", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Initially should have 0 pending
    const initial = sub.getSubStats();
    if (initial.pending_msgs != 0) {
        reportResult("sub_stats", false, "initial pending != 0");
        return;
    }

    // Publish some messages
    for (0..5) |_| {
        client.publish("substats.test", "test data") catch {};
    }
    client.flush(allocator) catch {};

    // Wait for messages to arrive
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Check pending increased
    const after = sub.getSubStats();
    if (after.pending_msgs >= 5 or after.pending_bytes > 0) {
        reportResult("sub_stats", true, "");
    } else {
        reportResult("sub_stats", false, "pending not updated");
    }
}

pub fn testPendingBytes(allocator: std.mem.Allocator) void {
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
        reportResult("pending_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "pending.bytes") catch {
        reportResult("pending_bytes", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish messages
    const payload = "0123456789";
    for (0..10) |_| {
        client.publish("pending.bytes", payload) catch {};
    }
    client.flush(allocator) catch {};

    // Wait for messages
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Check pending bytes before receiving
    const before_recv = sub.pendingBytes();

    // Receive all messages
    for (0..10) |_| {
        const msg = sub.nextWithTimeout(allocator, 100) catch break;
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    // Check pending bytes after receiving
    const after_recv = sub.pendingBytes();

    if (before_recv > after_recv and after_recv == 0) {
        reportResult("pending_bytes", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "before={d} after={d}",
            .{ before_recv, after_recv },
        ) catch "e";
        reportResult("pending_bytes", false, detail);
    }
}

pub fn testMaxPending(allocator: std.mem.Allocator) void {
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
        reportResult("max_pending", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "max.pending") catch {
        reportResult("max_pending", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Publish messages to create high water mark
    for (0..20) |_| {
        client.publish("max.pending", "payload") catch {};
    }
    client.flush(allocator) catch {};

    // Wait for messages
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Get max pending before receiving
    const max = sub.maxPending();
    if (max.msgs == 0) {
        reportResult("max_pending", false, "max_msgs is 0");
        return;
    }

    // Receive all messages
    for (0..25) |_| {
        const msg = sub.nextWithTimeout(allocator, 100) catch break;
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    // Max should stay the same (high watermark)
    const max_after = sub.maxPending();
    if (max_after.msgs == max.msgs) {
        // Clear max pending
        sub.clearMaxPending();
        const max_cleared = sub.maxPending();
        if (max_cleared.msgs == 0) {
            reportResult("max_pending", true, "");
        } else {
            reportResult("max_pending", false, "clearMaxPending failed");
        }
    } else {
        reportResult("max_pending", false, "max decreased");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testClientStats(allocator);
    testStatsIncrement(allocator);
    testStatsBytesAccuracy(allocator);
    testStatsMsgsIn(allocator);
    testStatsBytesIn(allocator);
    testConnectsCounter(allocator);
    testSubStats(allocator);
    testPendingBytes(allocator);
    testMaxPending(allocator);
}
