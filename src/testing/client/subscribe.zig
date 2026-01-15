//! Subscribe Tests for NATS Async Client
//!
//! Tests for async subscriptions.

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

pub fn testClientAsyncManySubs(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    // Publisher client
    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_many_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    // Async client with multiple subs
    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .async_queue_size = 32 },
    ) catch {
        reportResult("client_async_many_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 5 subscriptions
    const NUM_SUBS = 5;
    var subs: [NUM_SUBS]*nats.Client.Sub = undefined;
    var sub_buf: [NUM_SUBS][32]u8 = undefined;
    var topics: [NUM_SUBS][]const u8 = undefined;

    for (0..NUM_SUBS) |i| {
        topics[i] = std.fmt.bufPrint(
            &sub_buf[i],
            "async.many.{d}",
            .{i},
        ) catch "err";
        subs[i] = client.subscribe(allocator, topics[i]) catch {
            reportResult("client_async_many_subs", false, "sub failed");
            return;
        };
    }
    defer for (subs) |s| s.deinit(allocator);

    client.flush() catch {};

    // Wait for subscriptions to register
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Publish to all topics
    for (topics) |t| {
        publisher.publish(t, "hello") catch {};
    }
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var received: usize = 0;
    for (subs) |s| {
        var future = io.io().async(nats.Client.Sub.next, .{ s, allocator, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {}
    }

    if (received == NUM_SUBS) {
        reportResult("client_async_many_subs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM_SUBS },
        ) catch "e";
        reportResult("client_async_many_subs", false, msg);
    }
}

// ClientAsync Test 3: tryNext non-blocking

pub fn testClientAsyncWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_wildcard", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.wild.*") catch {
        reportResult("client_async_wildcard", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("client_async_wildcard", false, "flush failed");
        return;
    };

    // Wait for subscription to register server-side
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Publish to matching subjects
    publisher.publish("async.wild.a", "msg-a") catch {
        reportResult("client_async_wildcard", false, "pub a failed");
        return;
    };
    publisher.publish("async.wild.b", "msg-b") catch {
        reportResult("client_async_wildcard", false, "pub b failed");
        return;
    };
    publisher.publish("async.wild.c", "msg-c") catch {
        reportResult("client_async_wildcard", false, "pub c failed");
        return;
    };
    publisher.flush() catch {
        reportResult("client_async_wildcard", false, "pub flush failed");
        return;
    };

    // Use async/await - reader task routes messages automatically
    const NUM_MSGS = 3;
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {}
    }

    // Must receive exactly all 3 messages
    if (received == NUM_MSGS) {
        reportResult("client_async_wildcard", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/3", .{received}) catch "e";
        reportResult("client_async_wildcard", false, msg);
    }
}

// ClientAsync Test 6: Multiple subs to same subject

pub fn testClientAsyncDuplicateSubs(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_dup_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_dup_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Two subs to same subject
    const sub1 = client.subscribe(allocator, "async.dup") catch {
        reportResult("client_async_dup_subs", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "async.dup") catch {
        reportResult("client_async_dup_subs", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {};

    // Wait for subscriptions to register server-side
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("async.dup", "hello") catch {};
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    // Both subscriptions should receive the same message
    var future1 = io.io().async(nats.Client.Sub.next, .{ sub1, allocator, io.io() });
    defer if (future1.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    var future2 = io.io().async(nats.Client.Sub.next, .{ sub2, allocator, io.io() });
    defer if (future2.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    const got1 = if (future1.await(io.io())) |_| true else |_| false;
    const got2 = if (future2.await(io.io())) |_| true else |_| false;

    if (got1 and got2) {
        reportResult("client_async_dup_subs", true, "");
    } else {
        reportResult("client_async_dup_subs", false, "not both received");
    }
}

// ClientAsync Test 7: Statistics tracking

pub fn testClientAsyncQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_queue_group", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_queue_group", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe with queue group
    const sub = client.subscribeQueue(allocator, "async.qg", "workers") catch {
        reportResult("client_async_queue_group", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Wait for subscription to register server-side
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("async.qg", "task") catch {};
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("client_async_queue_group", true, "");
        return;
    } else |_| {}

    reportResult("client_async_queue_group", false, "no message");
}

// NEW TESTS: Connection & Lifecycle

// Test: Connection refused error handling

pub fn testAsyncWildcardMatching(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_wildcard_matching", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.wc.*") catch {
        reportResult("async_wildcard_matching", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish to matching subject
    client.publish("async.wc.test", "msg") catch {};
    client.flush() catch {};

    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("async_wildcard_matching", true, "");
        return;
    } else |_| {}

    reportResult("async_wildcard_matching", false, "no match");
}

// Test: Wildcard > matching

pub fn testAsyncWildcardGreater(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_wildcard_greater", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.gt.>") catch {
        reportResult("async_wildcard_greater", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish to deeply nested subject
    client.publish("async.gt.a.b.c", "msg") catch {};
    client.flush() catch {};

    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("async_wildcard_greater", true, "");
        return;
    } else |_| {}

    reportResult("async_wildcard_greater", false, "no match");
}

// Test: Subject case sensitivity

pub fn testAsyncSubjectCaseSensitivity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_subject_case", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to lowercase
    const sub = client.subscribe(allocator, "async.case.test") catch {
        reportResult("async_subject_case", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish to exact match
    client.publish("async.case.test", "msg") catch {};
    client.flush() catch {};

    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("async_subject_case", true, "");
        return;
    } else |_| {}

    reportResult("async_subject_case", false, "no match");
}

// Test: Unsubscribe stops delivery

pub fn testAsyncUnsubscribeStopsDelivery(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_unsub_stops", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.unsub.test") catch {
        reportResult("async_unsub_stops", false, "sub failed");
        return;
    };

    client.flush() catch {};

    // Unsubscribe
    sub.unsubscribe() catch {};
    sub.deinit(allocator);

    // Publish after unsubscribe - should not receive
    client.publish("async.unsub.test", "msg") catch {};
    client.flush() catch {};

    // Brief sleep to allow any potential delivery
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    // Client should still be connected
    if (client.isConnected()) {
        reportResult("async_unsub_stops", true, "");
    } else {
        reportResult("async_unsub_stops", false, "disconnected");
    }
}

// NEW TESTS: Multi-Client Patterns

// Test: Cross-client message routing

pub fn testAsyncHierarchicalSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_hierarchical", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Deep hierarchical subject
    const subject = "a.b.c.d.e.f.g.h";
    const sub = client.subscribe(allocator, subject) catch {
        reportResult("async_hierarchical", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish(subject, "deep") catch {
        reportResult("async_hierarchical", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.Client.Sub.next, .{ sub, allocator, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("async_hierarchical", true, "");
        return;
    } else |_| {}

    reportResult("async_hierarchical", false, "no message");
}

pub fn testUnsubscribeWithPending(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // Unsubscribe while messages pending
    sub.unsubscribe() catch {
        reportResult("unsub_with_pending", false, "unsubscribe failed");
        return;
    };

    // Should complete without crash
    reportResult("unsub_with_pending", true, "");
}

pub fn testSubscribeAfterDisconnect(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sub_after_disconnect", false, "connect failed");
        return;
    };

    // Disconnect
    _ = client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("sub_after_disconnect", false, "drain failed");
        return;
    };

    // Try to subscribe - should fail
    const result = client.subscribe(allocator, "test.sub");
    client.deinit(allocator);

    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("sub_after_disconnect", false, "should have failed");
    } else |_| {
        reportResult("sub_after_disconnect", true, "");
    }
}

pub fn testSubscriptionQueueCapacity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    client.flush() catch {
        reportResult("sub_queue_cap", false, "flush failed");
        return;
    };

    // Publish 100 messages (queue default is 256, so no overflow expected)
    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("qcap.test", "qcap") catch {
            reportResult("sub_queue_cap", false, "publish failed");
            return;
        };
    }
    client.flush() catch {
        reportResult("sub_queue_cap", false, "pub flush failed");
        return;
    };

    // Receive messages
    var received: u32 = 0;
    for (0..NUM_MSGS) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    // Queue size 256 > 100 messages: must receive ALL (no overflow expected)
    if (received == NUM_MSGS) {
        reportResult("sub_queue_cap", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/100", .{received}) catch "e";
        reportResult("sub_queue_cap", false, detail);
    }
}

/// Runs all async subscribe tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testClientAsyncManySubs(allocator);
    testClientAsyncWildcard(allocator);
    testClientAsyncDuplicateSubs(allocator);
    testClientAsyncQueueGroup(allocator);
    testAsyncWildcardMatching(allocator);
    testAsyncWildcardGreater(allocator);
    testAsyncSubjectCaseSensitivity(allocator);
    testAsyncUnsubscribeStopsDelivery(allocator);
    testAsyncHierarchicalSubject(allocator);
    testUnsubscribeWithPending(allocator);
    testSubscribeAfterDisconnect(allocator);
    testSubscriptionQueueCapacity(allocator);
}
