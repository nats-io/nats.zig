//! Subscribe Tests for NATS Client
//!
//! Tests for subscriptions, unsubscribing, and subscription state.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testSubscribeUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subscribe_unsubscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "test.>") catch {
        reportResult("subscribe_unsubscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    if (sub.sid == 0) {
        reportResult("subscribe_unsubscribe", false, "invalid sid");
        return;
    }

    sub.unsubscribe() catch {
        reportResult("subscribe_unsubscribe", false, "unsubscribe failed");
        return;
    };

    reportResult("subscribe_unsubscribe", true, "");
}

pub fn testMultipleSubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multiple_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "multi.one") catch {
        reportResult("multiple_subscriptions", false, "sub 1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "multi.two") catch {
        reportResult("multiple_subscriptions", false, "sub 2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "multi.three") catch {
        reportResult("multiple_subscriptions", false, "sub 3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    // SIDs should be unique and incrementing
    const valid = sub1.sid != sub2.sid and sub2.sid != sub3.sid and
        sub1.sid < sub2.sid and sub2.sid < sub3.sid;
    reportResult("multiple_subscriptions", valid, "invalid sids");
}

pub fn testSubscribeUnsubscribeReuse(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sub_unsub_reuse", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe, unsubscribe, subscribe again - 10 cycles
    for (0..10) |_| {
        const sub = client.subscribe(allocator, "cycle.test") catch {
            reportResult("sub_unsub_reuse", false, "subscribe failed");
            return;
        };

        sub.unsubscribe() catch {
            reportResult("sub_unsub_reuse", false, "unsubscribe failed");
            sub.deinit(allocator);
            return;
        };

        sub.deinit(allocator);
    }

    reportResult("sub_unsub_reuse", true, "");
}

pub fn testManySubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("many_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create and immediately cleanup 100 subscriptions in sequence
    var success_count: usize = 0;

    for (0..100) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "stress.sub.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("many_subscriptions", false, "sub failed");
            return;
        };
        sub.deinit(allocator);
        success_count += 1;
    }

    if (success_count == 100) {
        reportResult("many_subscriptions", true, "");
    } else {
        reportResult("many_subscriptions", false, "not all created");
    }
}

pub fn testDuplicateSubscription(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("duplicate_subscription", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two subscriptions to exact same subject
    const sub1 = client.subscribe(allocator, "dup.sub.test") catch {
        reportResult("duplicate_subscription", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "dup.sub.test") catch {
        reportResult("duplicate_subscription", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // Publish one message
    client.publish("dup.sub.test", "hello") catch {
        reportResult("duplicate_subscription", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Both should receive
    var count: u32 = 0;

    if (sub1.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 2) {
        reportResult("duplicate_subscription", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2", .{count}) catch "err";
        reportResult("duplicate_subscription", false, detail);
    }
}

pub fn testUnsubscribeStopsDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_stops_delivery", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "unsub.test") catch {
        reportResult("unsub_stops_delivery", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish first message
    client.publish("unsub.test", "before") catch {};
    client.flush() catch {};

    // Receive it
    const msg1 = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch null;
    if (msg1) |m| {
        m.deinit(allocator);
    } else {
        reportResult("unsub_stops_delivery", false, "no first msg");
        return;
    }

    // Unsubscribe
    sub.unsubscribe() catch {
        reportResult("unsub_stops_delivery", false, "unsub failed");
        return;
    };
    client.flush() catch {};

    // Calling nextMessage on closed subscription should return error
    const result = sub.nextMessage(allocator, .{ .timeout_ms = 200 });
    if (result) |msg_opt| {
        // Unexpected success
        if (msg_opt) |m| m.deinit(allocator);
        reportResult("unsub_stops_delivery", false, "should error on closed");
    } else |err| {
        // Expected: SubscriptionClosed error
        if (err == error.SubscriptionClosed) {
            reportResult("unsub_stops_delivery", true, "");
        } else {
            reportResult("unsub_stops_delivery", false, "wrong error type");
        }
    }
}

pub fn testUnsubscribeWithPending(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_with_pending", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "pending.test") catch {
        reportResult("unsub_with_pending", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish messages before reading
    for (0..5) |_| {
        client.publish("pending.test", "msg") catch {};
    }
    client.flush() catch {};

    // Small delay for messages to arrive
    std.posix.nanosleep(0, 50_000_000);

    // Unsubscribe while messages pending
    sub.unsubscribe() catch {
        reportResult("unsub_with_pending", false, "unsubscribe failed");
        return;
    };

    // Should complete without crash
    reportResult("unsub_with_pending", true, "");
}

pub fn testUnsubscribeBySid(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unsub_by_sid", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "sid.test") catch {
        reportResult("unsub_by_sid", false, "subscribe failed");
        return;
    };

    const sid = sub.sid;
    client.flush() catch {};

    // Publish before unsub
    client.publish("sid.test", "before") catch {};
    client.flush() catch {};

    // Unsubscribe by SID
    client.unsubscribeSid(sid) catch {
        sub.deinit(allocator);
        reportResult("unsub_by_sid", false, "unsub failed");
        return;
    };
    client.flush() catch {};

    // Publish after unsub
    client.publish("sid.test", "after") catch {};
    client.flush() catch {};

    // Should only get the "before" message
    var count: u32 = 0;
    for (0..3) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            count += 1;
        } else break;
    }
    sub.deinit(allocator);

    if (count <= 1) {
        reportResult("unsub_by_sid", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}", .{count}) catch "e";
        reportResult("unsub_by_sid", false, detail);
    }
}

pub fn testTwoSubsSameSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("two_subs_same", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "twosubs.same") catch {
        reportResult("two_subs_same", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "twosubs.same") catch {
        reportResult("two_subs_same", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // They should have different SIDs
    if (sub1.sid == sub2.sid) {
        reportResult("two_subs_same", false, "same SID");
        return;
    }

    // Publish one message
    client.publish("twosubs.same", "fanout") catch {};
    client.flush() catch {};

    // Both should receive it
    var count1: u32 = 0;
    var count2: u32 = 0;

    for (0..2) |_| {
        if (sub1.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            count1 += 1;
        }
    }
    for (0..2) |_| {
        if (sub2.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            count2 += 1;
        }
    }

    if (count1 == 1 and count2 == 1) {
        reportResult("two_subs_same", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}+{d}",
            .{ count1, count2 },
        ) catch "e";
        reportResult("two_subs_same", false, detail);
    }
}

pub fn testSubscribeAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subscribe_after_disconnect", false, "connect failed");
        return;
    };

    // Drain closes the connection
    client.drain(allocator) catch {
        reportResult("subscribe_after_disconnect", false, "drain failed");
        client.deinit(allocator);
        return;
    };

    // Subscribe after drain should fail
    const result = client.subscribe(allocator, "test.subject");
    client.deinit(allocator);

    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("subscribe_after_disconnect", false, "should have failed");
    } else |_| {
        reportResult("subscribe_after_disconnect", true, "");
    }
}

pub fn testRapidSubUnsubCycles(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("rapid_sub_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // 50 rapid cycles
    for (0..50) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "rapid.cycle.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("rapid_sub_unsub", false, "subscribe failed");
            return;
        };

        // Immediately unsubscribe
        sub.unsubscribe() catch {
            sub.deinit(allocator);
            reportResult("rapid_sub_unsub", false, "unsubscribe failed");
            return;
        };

        sub.deinit(allocator);
    }

    // If we got here without error, success
    reportResult("rapid_sub_unsub", true, "");
}

pub fn testSubscriptionQueueCapacity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sub_queue_cap", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "qcap.test") catch {
        reportResult("sub_queue_cap", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish many messages quickly
    for (0..100) |_| {
        client.publish("qcap.test", "qcap") catch {};
    }
    client.flush() catch {};

    // Receive what we can
    var received: u32 = 0;
    for (0..150) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 100 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    // Should receive all or most messages
    if (received >= 90) {
        reportResult("sub_queue_cap", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{received}) catch "e";
        reportResult("sub_queue_cap", false, detail);
    }
}

/// Runs all subscribe tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testSubscribeUnsubscribe(allocator);
    testMultipleSubscriptions(allocator);
    testSubscribeUnsubscribeReuse(allocator);
    testManySubscriptions(allocator);
    testDuplicateSubscription(allocator);
    testUnsubscribeStopsDelivery(allocator);
    testUnsubscribeWithPending(allocator);
    testUnsubscribeBySid(allocator);
    testTwoSubsSameSubject(allocator);
    testSubscribeAfterDisconnect(allocator);
    testRapidSubUnsubCycles(allocator);
    testSubscriptionQueueCapacity(allocator);
}
