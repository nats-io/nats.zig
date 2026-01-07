//! Statistics Tests for NATS Client
//!
//! Tests for client statistics tracking accuracy.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testStatisticsAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("statistics_accuracy", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Use unique subject to avoid cross-test contamination
    const inbox = nats.newInbox(allocator) catch {
        reportResult("statistics_accuracy", false, "inbox gen failed");
        return;
    };
    defer allocator.free(inbox);

    const sub = client.subscribe(allocator, inbox) catch {
        reportResult("statistics_accuracy", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Get stats before publish
    const stats_before = client.getStats();

    // Publish 10 messages of 100 bytes each
    const payload = "X" ** 100;
    for (0..10) |_| {
        client.publish(inbox, payload) catch {
            reportResult("statistics_accuracy", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Check output stats (relative to before)
    const stats_after = client.getStats();
    const msgs_published = stats_after.msgs_out - stats_before.msgs_out;
    const bytes_published = stats_after.bytes_out - stats_before.bytes_out;

    if (msgs_published != 10) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs_out={d} expected 10",
            .{msgs_published},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
        return;
    }
    if (bytes_published != 1000) {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "bytes_out={d} expected 1000",
            .{bytes_published},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
        return;
    }

    // Receive all messages
    var received: u32 = 0;
    for (0..10) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("statistics_accuracy", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "received={d} expected 10",
            .{received},
        ) catch "error";
        reportResult("statistics_accuracy", false, detail);
    }
}

pub fn testStatsIncrement(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_increment", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 5 messages of 10 bytes each
    for (0..5) |_| {
        client.publish("stats.test", "0123456789") catch {
            reportResult("stats_increment", false, "publish failed");
            return;
        };
    }

    const after = client.getStats();

    const msgs_diff = after.msgs_out - before.msgs_out;
    const bytes_diff = after.bytes_out - before.bytes_out;

    if (msgs_diff == 5 and bytes_diff == 50) {
        reportResult("stats_increment", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "msgs={d} bytes={d}",
            .{ msgs_diff, bytes_diff },
        ) catch "err";
        reportResult("stats_increment", false, detail);
    }
}

pub fn testStatsBytesAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "bytes.test") catch {
        reportResult("stats_bytes", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish exactly 100 bytes
    const payload = "0123456789" ** 10; // 100 bytes
    client.publish("bytes.test", payload) catch {};
    client.flush() catch {};

    // Receive
    if (sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
    }

    const after = client.getStats();
    const bytes_diff = after.bytes_out - before.bytes_out;

    // Should have published 100 bytes of payload
    if (bytes_diff == 100) {
        reportResult("stats_bytes", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{bytes_diff}) catch "e";
        reportResult("stats_bytes", false, detail);
    }
}

pub fn testStatsMsgsIn(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_msgs_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "msgsin.test") catch {
        reportResult("stats_msgs_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish 25 messages
    for (0..25) |_| {
        client.publish("msgsin.test", "data") catch {};
    }
    client.flush() catch {};

    // Receive all
    var received: u32 = 0;
    for (0..30) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stats_bytes_in", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "bytesin.test") catch {
        reportResult("stats_bytes_in", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    const before = client.getStats();

    // Publish 10 messages of 50 bytes each = 500 bytes total
    const payload = "01234567890123456789012345678901234567890123456789"; // 50 bytes
    for (0..10) |_| {
        client.publish("bytesin.test", payload) catch {};
    }
    client.flush() catch {};

    // Receive all
    for (0..15) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
        } else break;
    }

    const after = client.getStats();
    const bytes_in = after.bytes_in - before.bytes_in;

    // Should have received 500 bytes of payload
    if (bytes_in == 500) {
        reportResult("stats_bytes_in", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/500", .{bytes_in}) catch "e";
        reportResult("stats_bytes_in", false, detail);
    }
}

/// Runs all statistics tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testStatisticsAccuracy(allocator);
    testStatsIncrement(allocator);
    testStatsBytesAccuracy(allocator);
    testStatsMsgsIn(allocator);
    testStatsBytesIn(allocator);
}
