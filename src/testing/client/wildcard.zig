//! Wildcard Tests for NATS Client
//!
//! Tests for wildcard subscriptions (* and >) and pattern matching.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testWildcardSubscribe(allocator: std.mem.Allocator) void {
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
        reportResult("wildcard_subscribe", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Test * wildcard
    const sub1 = client.subscribeSync("wild.*") catch {
        reportResult("wildcard_subscribe", false, "* wildcard failed");
        return;
    };
    defer sub1.deinit();

    // Test > wildcard
    const sub2 = client.subscribeSync("wild.>") catch {
        reportResult("wildcard_subscribe", false, "> wildcard failed");
        return;
    };
    defer sub2.deinit();

    reportResult("wildcard_subscribe", true, "");
}

pub fn testWildcardMatching(allocator: std.mem.Allocator) void {
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
        reportResult("wildcard_matching", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Subscribe to foo.*
    const sub_star = client.subscribeSync("wtest.*") catch {
        reportResult("wildcard_matching", false, "star sub failed");
        return;
    };
    defer sub_star.deinit();

    // Subscribe to foo.>
    const sub_gt = client.subscribeSync("wtest.>") catch {
        reportResult("wildcard_matching", false, "gt sub failed");
        return;
    };
    defer sub_gt.deinit();

    // Publish to wtest.bar (matches both)
    client.publish("wtest.bar", "one") catch {
        reportResult("wildcard_matching", false, "pub1 failed");
        return;
    };

    // Publish to wtest.bar.baz (matches only >)
    client.publish("wtest.bar.baz", "two") catch {
        reportResult("wildcard_matching", false, "pub2 failed");
        return;
    };

    // star should get 1 message
    var star_count: u32 = 0;
    while (true) {
        const msg = sub_star.nextWithTimeout(200) catch {
            break;
        };
        if (msg) |m| {
            m.deinit();
            star_count += 1;
        } else {
            break;
        }
    }

    // gt should get 2 messages
    var gt_count: u32 = 0;
    while (true) {
        const msg = sub_gt.nextWithTimeout(200) catch {
            break;
        };
        if (msg) |m| {
            m.deinit();
            gt_count += 1;
        } else {
            break;
        }
    }

    if (star_count == 1 and gt_count == 2) {
        reportResult("wildcard_matching", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "star={d} gt={d}",
            .{ star_count, gt_count },
        ) catch "count error";
        reportResult("wildcard_matching", false, detail);
    }
}

pub fn testWildcardPositions(allocator: std.mem.Allocator) void {
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
        reportResult("wildcard_positions", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Wildcard at beginning: *.bar
    const sub1 = client.subscribeSync("*.middle.end") catch {
        reportResult("wildcard_positions", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    // Wildcard in middle: foo.*.baz
    const sub2 = client.subscribeSync("start.*.end") catch {
        reportResult("wildcard_positions", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    // Publish matching messages
    client.publish("foo.middle.end", "msg1") catch {};
    client.publish("start.bar.end", "msg2") catch {};

    var count: u32 = 0;
    if (sub1.nextWithTimeout(500) catch null) |m| {
        m.deinit();
        count += 1;
    }
    if (sub2.nextWithTimeout(500) catch null) |m| {
        m.deinit();
        count += 1;
    }

    if (count == 2) {
        reportResult("wildcard_positions", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail =
            std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("wildcard_positions", false, detail);
    }
}

pub fn testMultipleWildcards(allocator: std.mem.Allocator) void {
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
        reportResult("multi_wildcards", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Subscribe with multiple * wildcards
    const sub = client.subscribeSync("mw.*.middle.*") catch {
        reportResult("multi_wildcards", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Publish matching subjects
    client.publish("mw.foo.middle.bar", "hit1") catch {};
    client.publish("mw.a.middle.b", "hit2") catch {};
    client.publish("mw.xyz.other.abc", "miss") catch {}; // should not match

    var count: u32 = 0;
    for (0..4) |_| {
        const msg = sub.nextWithTimeout(200) catch break;
        if (msg) |m| {
            m.deinit();
            count += 1;
        } else break;
    }

    if (count == 2) {
        reportResult("multi_wildcards", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "e";
        reportResult("multi_wildcards", false, detail);
    }
}

pub fn testPublishSubscribe(allocator: std.mem.Allocator) void {
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
        reportResult("publish_subscribe", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("roundtrip.test") catch {
        reportResult("publish_subscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    client.publish("roundtrip.test", "hello from zig") catch {
        reportResult("publish_subscribe", false, "publish failed");
        return;
    };

    // Receive message
    const msg = sub.nextWithTimeout(1000) catch {
        reportResult("publish_subscribe", false, "nextWithTimeout failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.subject, "roundtrip.test") and
            std.mem.eql(u8, m.data, "hello from zig"))
        {
            reportResult("publish_subscribe", true, "");
            return;
        }
    }

    reportResult("publish_subscribe", false, "message not received");
}

/// Runs all wildcard tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testWildcardSubscribe(allocator);
    testWildcardMatching(allocator);
    testWildcardPositions(allocator);
    testMultipleWildcards(allocator);
    testPublishSubscribe(allocator);
}
