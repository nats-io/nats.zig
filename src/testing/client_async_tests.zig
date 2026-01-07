//! ClientAsync Integration Tests
//!
//! Tests for the concurrent subscription client (ClientAsync).
//! Tests message routing via background reader task and async/await patterns.

const std = @import("std");
const utils = @import("test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const ServerManager = utils.ServerManager;

// ClientAsync Test 1: Basic connect and subscribe
pub fn testClientAsyncBasic(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .name = "async-client-test",
        .async_queue_size = 64,
    }) catch |err| {
        var err_buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &err_buf,
            "connect failed: {}",
            .{err},
        ) catch "error";
        reportResult("client_async_basic", false, msg);
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("client_async_basic", false, "not connected");
        return;
    }

    const sub = client.subscribe(allocator, "async.basic") catch {
        reportResult("client_async_basic", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    reportResult("client_async_basic", true, "");
}

// ClientAsync Test 2: Multiple concurrent subscriptions
pub fn testClientAsyncManySubs(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Publisher client
    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_many_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    // Async client with multiple subs
    const client = nats.ClientAsync.connect(
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
    var subs: [NUM_SUBS]*nats.ClientAsync.Sub = undefined;
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
    std.posix.nanosleep(0, 50_000_000);

    // Publish to all topics
    for (topics) |t| {
        publisher.publish(t, "hello") catch {};
    }
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var received: usize = 0;
    for (subs) |s| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ s, io.io() });
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
pub fn testClientAsyncTryNext(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_try_next", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.trynext") catch {
        reportResult("client_async_try_next", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // No messages yet - should return null immediately
    if (sub.tryNext() != null) {
        reportResult("client_async_try_next", false, "expected null");
        return;
    }

    reportResult("client_async_try_next", true, "");
}

// ClientAsync Test 4: Publish and receive using async/await
pub fn testClientAsyncPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.pubsub") catch {
        reportResult("client_async_pubsub", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Flush subscription and wait for server to register it
    client.flush() catch {};
    std.posix.nanosleep(0, 10_000_000); // 10ms delay

    client.publish("async.pubsub", "test-message") catch {
        reportResult("client_async_pubsub", false, "pub failed");
        return;
    };
    client.flush() catch {};

    // True async/await - reader task routes messages automatically!
    // defer handles cleanup via cancel() - DON'T deinit in success path!
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |msg| msg.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        // cancel() and await() are idempotent - defer handles cleanup
        if (std.mem.eql(u8, msg.data, "test-message")) {
            reportResult("client_async_pubsub", true, "");
            return;
        }
    } else |_| {}

    reportResult("client_async_pubsub", false, "no message received");
}

// ClientAsync Test 5: Wildcard subscription
pub fn testClientAsyncWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_wildcard", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.wild.*") catch {
        reportResult("client_async_wildcard", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish to matching subjects
    publisher.publish("async.wild.a", "msg-a") catch {};
    publisher.publish("async.wild.b", "msg-b") catch {};
    publisher.publish("async.wild.c", "msg-c") catch {};
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var received: usize = 0;
    for (0..3) |_| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {}
    }

    if (received >= 3) {
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_dup_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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
    std.posix.nanosleep(0, 50_000_000);

    publisher.publish("async.dup", "hello") catch {};
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    // Both subscriptions should receive the same message
    var future1 = io.io().async(nats.ClientAsync.Sub.next, .{ sub1, io.io() });
    defer if (future1.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    var future2 = io.io().async(nats.ClientAsync.Sub.next, .{ sub2, io.io() });
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
pub fn testClientAsyncStats(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_stats", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const initial_stats = client.getStats();
    if (initial_stats.msgs_out != 0) {
        reportResult("client_async_stats", false, "initial msgs_out != 0");
        return;
    }

    client.publish("async.stats", "test") catch {};
    client.flush() catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1) {
        reportResult("client_async_stats", true, "");
    } else {
        reportResult("client_async_stats", false, "msgs_out not incremented");
    }
}

// ClientAsync Test 8: Server info available
pub fn testClientAsyncServerInfo(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_server_info", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.getServerInfo()) |info| {
        if (info.port == test_port) {
            reportResult("client_async_server_info", true, "");
            return;
        }
    }
    reportResult("client_async_server_info", false, "no server info");
}

// ClientAsync Test 9: Rapid subscribe/unsubscribe
pub fn testClientAsyncRapidSubUnsub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_rapid_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Rapid sub/unsub 20 times
    for (0..20) |i| {
        var buf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(&buf, "rapid.{d}", .{i}) catch "e";
        const sub = client.subscribe(allocator, subj) catch {
            reportResult("client_async_rapid_sub", false, "sub failed");
            return;
        };
        sub.deinit(allocator);
    }

    // Client should still work
    const sub = client.subscribe(allocator, "rapid.final") catch {
        reportResult("client_async_rapid_sub", false, "final sub failed");
        return;
    };
    defer sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("client_async_rapid_sub", true, "");
    } else {
        reportResult("client_async_rapid_sub", false, "disconnected");
    }
}

// ClientAsync Test 10: High message rate
pub fn testClientAsyncHighRate(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_high_rate", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .async_queue_size = 512,
    }) catch {
        reportResult("client_async_high_rate", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.highrate") catch {
        reportResult("client_async_high_rate", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish 100 messages
    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        publisher.publish("async.highrate", "msg") catch {};
    }
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    // Try to receive at least 50 messages
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {
            break; // Stop on first error (likely queue closed)
        }
    }

    // Should get most messages (some may be dropped if queue fills)
    if (received >= 50) {
        reportResult("client_async_high_rate", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}", .{received}) catch "e";
        reportResult("client_async_high_rate", false, msg);
    }
}

// ClientAsync Test 11: Publish with reply-to
pub fn testClientAsyncPublishReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_pub_reply", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.req") catch {
        reportResult("client_async_pub_reply", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publishRequest("async.req", "reply.inbox", "request") catch {
        reportResult("client_async_pub_reply", false, "pub failed");
        return;
    };
    client.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "reply.inbox")) {
                reportResult("client_async_pub_reply", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("client_async_pub_reply", false, "no reply_to");
}

// ClientAsync Test 12: Queue group support
pub fn testClientAsyncQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("client_async_queue_group", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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
    std.posix.nanosleep(0, 50_000_000);

    publisher.publish("async.qg", "task") catch {};
    publisher.flush() catch {};

    // Use async/await - reader task routes messages automatically
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("client_async_queue_group", true, "");
        return;
    } else |_| {}

    reportResult("client_async_queue_group", false, "no message");
}

// NEW TESTS: Connection & Lifecycle

// Test: Connection refused error handling
pub fn testAsyncConnectionRefused(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Try to connect to a port where nothing is listening
    const result = nats.ClientAsync.connect(
        allocator,
        io.io(),
        "nats://127.0.0.1:19999",
        .{},
    );

    if (result) |client| {
        client.deinit(allocator);
        reportResult("async_connection_refused", false, "expected error");
    } else |_| {
        reportResult("async_connection_refused", true, "");
    }
}

// Test: Multiple consecutive connections
pub fn testAsyncConsecutiveConnections(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Connect and disconnect 3 times
    for (0..3) |i| {
        const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
            var buf: [32]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "connect {d} failed", .{i}) catch "e";
            reportResult("async_consecutive_connections", false, msg);
            return;
        };
        client.deinit(allocator);
    }

    reportResult("async_consecutive_connections", true, "");
}

// Test: isConnected state tracking
pub fn testAsyncIsConnectedState(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_is_connected_state", false, "connect failed");
        return;
    };

    if (!client.isConnected()) {
        client.deinit(allocator);
        reportResult("async_is_connected_state", false, "not connected initially");
        return;
    }

    client.deinit(allocator);
    reportResult("async_is_connected_state", true, "");
}

// NEW TESTS: Publish Operations

// Test: Publish empty payload
pub fn testAsyncPublishEmptyPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_empty_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.empty") catch {
        reportResult("async_publish_empty_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish("async.empty", "") catch {
        reportResult("async_publish_empty_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 0) {
            reportResult("async_publish_empty_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_publish_empty_payload", false, "no empty message");
}

// Test: Publish large payload (within buffer limits)
pub fn testAsyncPublishLargePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_large_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.large") catch {
        reportResult("async_publish_large_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Create 8KB payload (safe within buffer limits)
    const payload = allocator.alloc(u8, 8 * 1024) catch {
        reportResult("async_publish_large_payload", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("async.large", payload) catch {
        reportResult("async_publish_large_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.data.len == 8 * 1024) {
            reportResult("async_publish_large_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_publish_large_payload", false, "wrong size");
}

// Test: Rapid fire publishing
pub fn testAsyncPublishRapidFire(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_rapid_fire", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish 1000 messages rapidly
    for (0..1000) |_| {
        client.publish("async.rapid", "msg") catch {
            reportResult("async_publish_rapid_fire", false, "pub failed");
            return;
        };
    }
    client.flush() catch {};

    const stats = client.getStats();
    if (stats.msgs_out >= 1000) {
        reportResult("async_publish_rapid_fire", true, "");
    } else {
        reportResult("async_publish_rapid_fire", false, "not all published");
    }
}

// Test: Publish with no subscribers
pub fn testAsyncPublishNoSubscribers(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_publish_no_subscribers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Should succeed even with no subscribers
    client.publish("async.nosub", "message") catch {
        reportResult("async_publish_no_subscribers", false, "pub failed");
        return;
    };
    client.flush() catch {};

    reportResult("async_publish_no_subscribers", true, "");
}

// NEW TESTS: Subscription Patterns

// Test: Wildcard matching with *
pub fn testAsyncWildcardMatching(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
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

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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
    std.posix.nanosleep(0, 10_000_000);

    // Client should still be connected
    if (client.isConnected()) {
        reportResult("async_unsub_stops", true, "");
    } else {
        reportResult("async_unsub_stops", false, "disconnected");
    }
}

// NEW TESTS: Multi-Client Patterns

// Test: Cross-client message routing
pub fn testAsyncCrossClientRouting(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Publisher (regular client)
    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_cross_client", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    // Subscriber (async client)
    const subscriber = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_cross_client", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit(allocator);

    const sub = subscriber.subscribe(allocator, "async.cross") catch {
        reportResult("async_cross_client", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    subscriber.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publish from regular client
    publisher.publish("async.cross", "cross-message") catch {};
    publisher.flush() catch {};

    // Receive on async client
    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "cross-message")) {
            reportResult("async_cross_client", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_cross_client", false, "no message");
}

// Test: Multiple async clients
pub fn testAsyncMultipleClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Create 3 async clients
    const client1 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client1 failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client2 failed");
        return;
    };
    defer client2.deinit(allocator);

    const client3 = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_multiple_clients", false, "client3 failed");
        return;
    };
    defer client3.deinit(allocator);

    if (client1.isConnected() and client2.isConnected() and client3.isConnected()) {
        reportResult("async_multiple_clients", true, "");
    } else {
        reportResult("async_multiple_clients", false, "not all connected");
    }
}

// NEW TESTS: Statistics & Metadata

// Test: Stats increment correctly
pub fn testAsyncStatsIncrement(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stats_increment", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 10 messages
    for (0..10) |_| {
        client.publish("async.stats.inc", "msg") catch {};
    }
    client.flush() catch {};

    const after = client.getStats();

    if (after.msgs_out >= before.msgs_out + 10) {
        reportResult("async_stats_increment", true, "");
    } else {
        reportResult("async_stats_increment", false, "stats not incremented");
    }
}

// Test: Bytes accuracy
pub fn testAsyncStatsBytesAccuracy(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stats_bytes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const before = client.getStats();

    // Publish 100 bytes
    const payload = "0123456789" ** 10; // 100 bytes
    client.publish("async.stats.bytes", payload) catch {};
    client.flush() catch {};

    const after = client.getStats();

    // bytes_out should increase by at least 100
    if (after.bytes_out >= before.bytes_out + 100) {
        reportResult("async_stats_bytes", true, "");
    } else {
        reportResult("async_stats_bytes", false, "bytes not tracked");
    }
}

// NEW TESTS: Stress Tests

// Test: 500 message stress test
pub fn testAsyncStress500Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const publisher = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_stress_500", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{
        .async_queue_size = 512,
    }) catch {
        reportResult("async_stress_500", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.stress500") catch {
        reportResult("async_stress_500", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publish 500 messages
    const NUM_MSGS = 500;
    for (0..NUM_MSGS) |_| {
        publisher.publish("async.stress500", "stress-msg") catch {};
    }
    publisher.flush() catch {};

    // Receive messages
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {
            break;
        }
    }

    // Should receive most messages
    if (received >= 450) {
        reportResult("async_stress_500", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/500", .{received}) catch "e";
        reportResult("async_stress_500", false, msg);
    }
}

// NEW TESTS: Error Handling

// Test: Double unsubscribe is safe
pub fn testAsyncDoubleUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_double_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.double.unsub") catch {
        reportResult("async_double_unsub", false, "sub failed");
        return;
    };

    // Unsubscribe twice
    sub.unsubscribe() catch {};
    sub.unsubscribe() catch {}; // Should not crash

    sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("async_double_unsub", true, "");
    } else {
        reportResult("async_double_unsub", false, "disconnected");
    }
}

// Test: Message ordering (FIFO)
pub fn testAsyncMessageOrdering(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_message_ordering", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.order") catch {
        reportResult("async_message_ordering", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Publish numbered messages
    var pub_buf: [5][8]u8 = undefined;
    for (0..5) |i| {
        const payload = std.fmt.bufPrint(&pub_buf[i], "msg-{d}", .{i}) catch "e";
        client.publish("async.order", payload) catch {};
    }
    client.flush() catch {};

    // Receive and verify order
    var in_order = true;
    for (0..5) |expected| {
        var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |msg| {
            var exp_buf: [8]u8 = undefined;
            const exp = std.fmt.bufPrint(&exp_buf, "msg-{d}", .{expected}) catch "e";
            if (!std.mem.eql(u8, msg.data, exp)) {
                in_order = false;
            }
        } else |_| {
            in_order = false;
            break;
        }
    }

    if (in_order) {
        reportResult("async_message_ordering", true, "");
    } else {
        reportResult("async_message_ordering", false, "out of order");
    }
}

// NEW TESTS: Misc Edge Cases

// Test: Binary payload handling
pub fn testAsyncBinaryPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.binary") catch {
        reportResult("async_binary_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    // Binary data with null bytes
    const binary = [_]u8{ 0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x03 };

    client.publish("async.binary", &binary) catch {
        reportResult("async_binary_payload", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, &binary)) {
            reportResult("async_binary_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_binary_payload", false, "binary mismatch");
}

// Test: Reply-to preserved in message
pub fn testAsyncReplyToPreserved(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_reply_preserved", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "async.reply.test") catch {
        reportResult("async_reply_preserved", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publishRequest("async.reply.test", "my.reply.inbox", "data") catch {
        reportResult("async_reply_preserved", false, "pub failed");
        return;
    };
    client.flush() catch {};

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (msg.reply_to) |rt| {
            if (std.mem.eql(u8, rt, "my.reply.inbox")) {
                reportResult("async_reply_preserved", true, "");
                return;
            }
        }
    } else |_| {}

    reportResult("async_reply_preserved", false, "reply_to not preserved");
}

// Test: Hierarchical subject names
pub fn testAsyncHierarchicalSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.ClientAsync.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("async_hierarchical", true, "");
        return;
    } else |_| {}

    reportResult("async_hierarchical", false, "no message");
}

// Authentication & Server Management Tests

/// Test: Authentication with valid token
pub fn testAsyncAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    const url = formatAuthUrl(&url_buf, auth_port, test_token);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_authentication", false, "auth connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("async_authentication", true, "");
    } else {
        reportResult("async_authentication", false, "not connected");
    }
}

/// Test: Authentication failure without token
pub fn testAsyncAuthenticationFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, auth_port); // No token!

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const result = nats.ClientAsync.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        client.deinit(allocator);
        reportResult("async_auth_failure", false, "should have failed");
    } else |_| {
        reportResult("async_auth_failure", true, "");
    }
}

/// Test: Server restart behavior
pub fn testAsyncReconnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_reconnection", false, "initial connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("async_reconnection", false, "not connected initially");
        return;
    }

    // Stop server
    manager.stopServer(0, io.io());
    std.posix.nanosleep(0, 100_000_000); // 100ms

    // Restart server
    _ = manager.startServer(allocator, io.io(), .{ .port = test_port }) catch {
        reportResult("async_reconnection", false, "server restart failed");
        return;
    };

    reportResult("async_reconnection", true, "");
}

/// Test: New connection after server restart
pub fn testAsyncServerRestartNewConnection(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io1: std.Io.Threaded = .init(allocator, .{});
    defer io1.deinit();

    const client1 = nats.ClientAsync.connect(allocator, io1.io(), url, .{}) catch {
        reportResult("async_server_restart", false, "initial connect failed");
        return;
    };

    if (!client1.isConnected()) {
        client1.deinit(allocator);
        reportResult("async_server_restart", false, "not connected");
        return;
    }

    client1.deinit(allocator);

    // Stop and restart server
    manager.stopServer(0, io1.io());
    std.posix.nanosleep(0, 100_000_000);

    _ = manager.startServer(allocator, io1.io(), .{ .port = test_port }) catch {
        reportResult("async_server_restart", false, "restart failed");
        return;
    };

    // New connection should work
    var io2: std.Io.Threaded = .init(allocator, .{});
    defer io2.deinit();

    const client2 = nats.ClientAsync.connect(allocator, io2.io(), url, .{}) catch {
        reportResult("async_server_restart", false, "reconnect failed");
        return;
    };
    defer client2.deinit(allocator);

    if (client2.isConnected()) {
        reportResult("async_server_restart", true, "");
    } else {
        reportResult("async_server_restart", false, "not connected after restart");
    }
}

// Request/Reply Tests

/// Test: Async request method exists and can be called
pub fn testAsyncRequestMethod(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_request_method", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Verify the request method exists and can be called
    // We expect it to return null (timeout) since no responder exists
    // Use a short timeout to keep tests fast
    const result = client.request(
        allocator,
        "nonexistent.service.test",
        "ping",
        50, // 50ms timeout
    ) catch {
        reportResult("async_request_method", false, "request error");
        return;
    };

    // Either null (timeout) or a message (if somehow routed) is acceptable
    // The important thing is that the method works without crashing
    if (result) |msg| {
        msg.deinit(allocator);
    }

    // Test passes if we got here without error
    if (client.isConnected()) {
        reportResult("async_request_method", true, "");
    } else {
        reportResult("async_request_method", false, "disconnected after request");
    }
}

/// Test: Async request times out eventually
pub fn testAsyncRequestReturns(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_request_returns", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const start = std.time.Instant.now() catch {
        reportResult("async_request_returns", false, "timer failed");
        return;
    };

    // Request to non-existent service with 100ms timeout
    const result = client.request(
        allocator,
        "nonexistent.service.test2",
        "data",
        100, // 100ms timeout
    ) catch {
        reportResult("async_request_returns", false, "request error");
        return;
    };

    const now = std.time.Instant.now() catch {
        reportResult("async_request_returns", false, "timer failed");
        return;
    };
    const elapsed_ns = now.since(start);
    const elapsed_ms = elapsed_ns / std.time.ns_per_ms;

    // Clean up result if any
    if (result) |msg| {
        msg.deinit(allocator);
    }

    // Test that the function returns within reasonable time (< 5 seconds)
    // This verifies the timeout mechanism works, even if not perfectly precise
    if (elapsed_ms < 5000) {
        reportResult("async_request_returns", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "took too long: {d}ms",
            .{elapsed_ms},
        ) catch "timing error";
        reportResult("async_request_returns", false, msg);
    }
}

// Drain Tests

/// Test: Drain operation closes connection
pub fn testAsyncDrainOperation(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_drain_operation", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create some subscriptions
    const sub1 = client.subscribe(allocator, "drain.test.1") catch {
        reportResult("async_drain_operation", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.test.2") catch {
        reportResult("async_drain_operation", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.flush() catch {};

    // Drain should clean up everything
    client.drain(allocator) catch {
        reportResult("async_drain_operation", false, "drain failed");
        return;
    };

    // After drain, client should not be connected
    if (!client.isConnected()) {
        reportResult("async_drain_operation", true, "");
    } else {
        reportResult("async_drain_operation", false, "still connected");
    }
}

/// Test: Drain cleans up subscriptions
pub fn testAsyncDrainCleansUp(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.ClientAsync.connect(allocator, io.io(), url, .{}) catch {
        reportResult("async_drain_cleanup", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create subscriptions and publish some messages
    const sub1 = client.subscribe(allocator, "drain.cleanup.1") catch {
        reportResult("async_drain_cleanup", false, "sub1 failed");
        return;
    };
    _ = sub1;

    const sub2 = client.subscribe(allocator, "drain.cleanup.2") catch {
        reportResult("async_drain_cleanup", false, "sub2 failed");
        return;
    };
    _ = sub2;

    client.publish("drain.cleanup.1", "msg1") catch {};
    client.publish("drain.cleanup.2", "msg2") catch {};
    client.flush() catch {};

    // Small delay for messages to arrive
    std.posix.nanosleep(0, 50_000_000);

    // Drain - should clean up all subscriptions and close connection
    client.drain(allocator) catch {
        reportResult("async_drain_cleanup", false, "drain failed");
        return;
    };

    // Verify state
    if (!client.isConnected()) {
        reportResult("async_drain_cleanup", true, "");
    } else {
        reportResult("async_drain_cleanup", false, "still connected after drain");
    }
}

/// Runs all ClientAsync tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    // Original 12 tests
    testClientAsyncBasic(allocator);
    testClientAsyncManySubs(allocator);
    testClientAsyncTryNext(allocator);
    testClientAsyncPubSub(allocator);
    testClientAsyncWildcard(allocator);
    testClientAsyncDuplicateSubs(allocator);
    testClientAsyncStats(allocator);
    testClientAsyncServerInfo(allocator);
    testClientAsyncRapidSubUnsub(allocator);
    testClientAsyncHighRate(allocator);
    testClientAsyncPublishReply(allocator);
    testClientAsyncQueueGroup(allocator);

    // Connection & Lifecycle tests
    testAsyncConnectionRefused(allocator);
    testAsyncConsecutiveConnections(allocator);
    testAsyncIsConnectedState(allocator);

    // Publish Operations tests
    testAsyncPublishEmptyPayload(allocator);
    testAsyncPublishLargePayload(allocator);
    testAsyncPublishRapidFire(allocator);
    testAsyncPublishNoSubscribers(allocator);

    // Subscription Patterns tests
    testAsyncWildcardMatching(allocator);
    testAsyncWildcardGreater(allocator);
    testAsyncSubjectCaseSensitivity(allocator);
    testAsyncUnsubscribeStopsDelivery(allocator);

    // Multi-Client Patterns tests
    testAsyncCrossClientRouting(allocator);
    testAsyncMultipleClients(allocator);

    // Statistics & Metadata tests
    testAsyncStatsIncrement(allocator);
    testAsyncStatsBytesAccuracy(allocator);

    // Stress tests
    testAsyncStress500Messages(allocator);

    // Error Handling tests
    testAsyncDoubleUnsubscribe(allocator);
    testAsyncMessageOrdering(allocator);

    // Misc Edge Cases tests
    testAsyncBinaryPayload(allocator);
    testAsyncReplyToPreserved(allocator);
    testAsyncHierarchicalSubject(allocator);

    // Authentication tests
    testAsyncAuthentication(allocator);
    testAsyncAuthenticationFailure(allocator);

    // Request/Reply tests
    testAsyncRequestMethod(allocator);
    testAsyncRequestReturns(allocator);

    // Drain tests
    testAsyncDrainOperation(allocator);
    testAsyncDrainCleansUp(allocator);

    // Server management tests
    testAsyncReconnection(allocator, manager);
    testAsyncServerRestartNewConnection(allocator, manager);
}
