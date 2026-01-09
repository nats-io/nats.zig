//! Edge Cases Tests for NATS Async Client
//!
//! Tests for async edge cases.

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

pub fn testAsyncDoubleUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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
        var future = io.io().async(nats.Client.Sub.next, .{ sub, io.io() });
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

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
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

    var future = io.io().async(nats.Client.Sub.next, .{ sub, io.io() });
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, &binary)) {
            reportResult("async_binary_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("async_binary_payload", false, "binary mismatch");
}

pub fn testLongSubjectName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("long_subject_name", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Long but valid subject name (100 chars)
    const long_subject = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z" ++
        ".aa.bb.cc.dd.ee.ff.gg.hh.ii.jj.kk.ll.mm.nn";

    const sub = client.subscribe(allocator, long_subject) catch {
        reportResult("long_subject_name", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    client.publish(long_subject, "test") catch {
        reportResult("long_subject_name", false, "publish failed");
        return;
    };
    client.flush() catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("long_subject_name", true, "");
    } else {
        reportResult("long_subject_name", false, "no message");
    }
}

pub fn testSubjectWithNumbersHyphens(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_nums_hyphens", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subject with numbers, hyphens, underscores
    const subject = "test-123.foo_bar.baz-456";

    const sub = client.subscribe(allocator, subject) catch {
        reportResult("subject_nums_hyphens", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    client.publish(subject, "test") catch {
        reportResult("subject_nums_hyphens", false, "publish failed");
        return;
    };
    client.flush() catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("subject_nums_hyphens", true, "");
    } else {
        reportResult("subject_nums_hyphens", false, "no message");
    }
}

pub fn testDoubleFlush(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Double flush should be safe
    client.flush() catch {
        reportResult("double_flush", false, "flush1 failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "flush2 failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "flush3 failed");
        return;
    };

    reportResult("double_flush", true, "");
}

pub fn testDoubleDrain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_drain", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "double.drain.test") catch {
        reportResult("double_drain", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Double unsubscribe should be safe (drain equivalent for subscription)
    sub.unsubscribe() catch {};
    sub.unsubscribe() catch {};

    if (client.isConnected()) {
        reportResult("double_drain", true, "");
    } else {
        reportResult("double_drain", false, "disconnected");
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

    // Rapid subscribe/unsubscribe cycles
    for (0..20) |_| {
        const sub = client.subscribe(allocator, "rapid.cycle.test") catch {
            reportResult("rapid_sub_unsub", false, "subscribe failed");
            return;
        };
        sub.unsubscribe() catch {};
        sub.deinit(allocator);
    }

    if (client.isConnected()) {
        reportResult("rapid_sub_unsub", true, "");
    } else {
        reportResult("rapid_sub_unsub", false, "disconnected");
    }
}

pub fn testNewInboxUniqueness(allocator: std.mem.Allocator) void {
    // Generate 10 inboxes and verify uniqueness
    var inboxes: [10][]u8 = undefined;
    var created: usize = 0;

    defer for (inboxes[0..created]) |inbox| {
        allocator.free(inbox);
    };

    for (0..10) |i| {
        inboxes[i] = nats.newInbox(allocator) catch {
            reportResult("inbox_uniqueness", false, "newInbox failed");
            return;
        };
        created += 1;

        // Check this inbox is unique from all previous
        for (0..i) |j| {
            if (std.mem.eql(u8, inboxes[i], inboxes[j])) {
                reportResult("inbox_uniqueness", false, "duplicate inbox");
                return;
            }
        }
    }

    reportResult("inbox_uniqueness", true, "");
}

pub fn testEmptySubjectFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("empty_subject_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Empty subject should fail for subscribe
    const sub_result = client.subscribe(allocator, "");
    if (sub_result) |sub| {
        sub.deinit(allocator);
        reportResult("empty_subject_fails", false, "subscribe should fail");
        return;
    } else |_| {
        // Expected
    }

    reportResult("empty_subject_fails", true, "");
}

pub fn testSubjectWithSpacesFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_spaces_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subject with space should fail
    const result = client.publish("foo bar", "data");
    if (result) |_| {
        reportResult("subject_spaces_fails", false, "should have failed");
    } else |_| {
        reportResult("subject_spaces_fails", true, "");
    }
}

pub fn testInterleavedPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("interleaved_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "interleave.test") catch {
        reportResult("interleaved_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Interleave: publish, receive, publish, receive...
    var received: u32 = 0;
    for (0..10) |i| {
        var buf: [16]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "msg{d}", .{i}) catch continue;

        client.publish("interleave.test", payload) catch {
            reportResult("interleaved_pubsub", false, "publish failed");
            return;
        };
        client.flush() catch {};

        const msg = sub.nextWithTimeout(allocator, 500) catch {
            continue;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("interleaved_pubsub", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/10",
            .{received},
        ) catch "err";
        reportResult("interleaved_pubsub", false, detail);
    }
}

pub fn testReceiveOnlyAfterSubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("receive_after_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish BEFORE subscribing
    client.publish("timing.test", "before") catch {};
    client.flush() catch {};

    // Small delay
    std.posix.nanosleep(0, 50_000_000);

    // Now subscribe
    const sub = client.subscribe(allocator, "timing.test") catch {
        reportResult("receive_after_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish AFTER subscribing
    client.publish("timing.test", "after") catch {};
    client.flush() catch {};

    // Should only receive the "after" message
    const msg = sub.nextWithTimeout(allocator, 500) catch {
        reportResult("receive_after_sub", false, "receive error");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "after")) {
            reportResult("receive_after_sub", true, "");
        } else {
            reportResult("receive_after_sub", false, "got wrong message");
        }
    } else {
        reportResult("receive_after_sub", false, "no message");
    }
}

pub fn testDataIntegrityPattern(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("data_integrity", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "integrity.test") catch {
        reportResult("data_integrity", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Create pattern payload
    var payload: [256]u8 = undefined;
    for (&payload, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    client.publish("integrity.test", &payload) catch {
        reportResult("data_integrity", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextWithTimeout(allocator, 500) catch {
        reportResult("data_integrity", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len != 256) {
            reportResult("data_integrity", false, "wrong length");
            return;
        }
        for (m.data, 0..) |b, i| {
            if (b != @as(u8, @truncate(i))) {
                reportResult("data_integrity", false, "data corrupt");
                return;
            }
        }
        reportResult("data_integrity", true, "");
    } else {
        reportResult("data_integrity", false, "no message");
    }
}

pub fn testCompletePubSubRoundTrip(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("complete_roundtrip", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Verify connected
    if (!client.isConnected()) {
        reportResult("complete_roundtrip", false, "not connected");
        return;
    }

    // Subscribe
    const sub = client.subscribe(allocator, "roundtrip.100") catch {
        reportResult("complete_roundtrip", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Get stats before
    const before = client.getStats();

    // Publish with known data
    const test_data = "Test100-RoundTrip-Verification";
    client.publish("roundtrip.100", test_data) catch {
        reportResult("complete_roundtrip", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Receive
    const msg = sub.nextWithTimeout(allocator, 1000) catch {
        reportResult("complete_roundtrip", false, "receive failed");
        return;
    };

    if (msg == null) {
        reportResult("complete_roundtrip", false, "no message");
        return;
    }

    const m = msg.?;
    defer m.deinit(allocator);

    // Verify data
    if (!std.mem.eql(u8, m.data, test_data)) {
        reportResult("complete_roundtrip", false, "data mismatch");
        return;
    }

    // Verify subject
    if (!std.mem.eql(u8, m.subject, "roundtrip.100")) {
        reportResult("complete_roundtrip", false, "subject mismatch");
        return;
    }

    // Get stats after
    const after = client.getStats();

    // Verify stats updated
    if (after.msgs_out <= before.msgs_out) {
        reportResult("complete_roundtrip", false, "msgs_out not updated");
        return;
    }
    if (after.msgs_in <= before.msgs_in) {
        reportResult("complete_roundtrip", false, "msgs_in not updated");
        return;
    }

    reportResult("complete_roundtrip", true, "");
}

// Boundary Test: Queue at exact capacity
pub fn testQueueExactCapacity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Queue size exactly matches message count
    const QUEUE_SIZE = 64;
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .async_queue_size = QUEUE_SIZE,
    }) catch {
        reportResult("queue_exact_cap", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "boundary.exact") catch {
        reportResult("queue_exact_cap", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {
        reportResult("queue_exact_cap", false, "flush failed");
        return;
    };

    // Publish exactly QUEUE_SIZE messages
    for (0..QUEUE_SIZE) |_| {
        client.publish("boundary.exact", "x") catch {
            reportResult("queue_exact_cap", false, "publish failed");
            return;
        };
    }
    client.flush() catch {
        reportResult("queue_exact_cap", false, "pub flush failed");
        return;
    };

    // Must receive ALL messages (no overflow)
    var received: u32 = 0;
    for (0..QUEUE_SIZE) |_| {
        if (sub.nextWithTimeout(allocator, 200) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == QUEUE_SIZE) {
        reportResult("queue_exact_cap", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/64", .{received}) catch "e";
        reportResult("queue_exact_cap", false, detail);
    }
}

// Boundary Test: Queue overflow behavior
pub fn testQueueOverflow(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    // Small queue, publish more than capacity
    const QUEUE_SIZE = 32;
    const PUBLISH_COUNT = 64;
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .async_queue_size = QUEUE_SIZE,
    }) catch {
        reportResult("queue_overflow", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "boundary.overflow") catch {
        reportResult("queue_overflow", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {
        reportResult("queue_overflow", false, "flush failed");
        return;
    };

    // Small delay for subscription to register
    std.posix.nanosleep(0, 50_000_000);

    // Publish more than queue can hold (without consuming)
    for (0..PUBLISH_COUNT) |_| {
        client.publish("boundary.overflow", "x") catch {
            reportResult("queue_overflow", false, "publish failed");
            return;
        };
    }
    client.flush() catch {
        reportResult("queue_overflow", false, "pub flush failed");
        return;
    };

    // Now consume what we can
    var received: u32 = 0;
    for (0..PUBLISH_COUNT) |_| {
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    // With overflow, we should get at most QUEUE_SIZE messages
    // Some may be dropped due to overflow - this tests overflow handling
    if (received <= QUEUE_SIZE and received > 0) {
        reportResult("queue_overflow", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d} (expect <= {d})",
            .{ received, QUEUE_SIZE },
        ) catch "e";
        reportResult("queue_overflow", false, detail);
    }
}

// Boundary Test: Maximum subscriptions (256)
pub fn testMaxSubscriptions(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Try to create exactly 256 subscriptions (the documented limit)
    const MAX_SUBS = 256;
    var subs: [MAX_SUBS]?*nats.Subscription = undefined;
    @memset(&subs, null);

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    var created: usize = 0;
    for (0..MAX_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "maxsub.{d}",
            .{i},
        ) catch continue;
        subs[i] = client.subscribe(allocator, subject) catch {
            break;
        };
        created += 1;
    }

    // Must create exactly 256 subscriptions
    if (created == MAX_SUBS) {
        reportResult("max_subscriptions", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/256", .{created}) catch "e";
        reportResult("max_subscriptions", false, detail);
    }
}

// Boundary Test: Large payload handling
// Tests payloads near server max_payload limit.
pub fn testLargePayloadHandling(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("large_payload_handling", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large.payload") catch {
        reportResult("large_payload_handling", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("large_payload_handling", false, "flush failed");
        return;
    };

    // Test 64KB payload (well under typical 1MB limit)
    const payload_size = 64 * 1024;
    const payload = allocator.alloc(u8, payload_size) catch {
        reportResult("large_payload_handling", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'L');

    client.publish("large.payload", payload) catch {
        reportResult("large_payload_handling", false, "publish failed");
        return;
    };
    client.flush() catch {
        reportResult("large_payload_handling", false, "pub flush failed");
        return;
    };

    if (sub.nextWithTimeout(allocator, 5000) catch null) |m| {
        defer m.deinit(allocator);
        if (m.data.len == payload_size) {
            reportResult("large_payload_handling", true, "");
        } else {
            var buf: [48]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "got {d} bytes",
                .{m.data.len},
            ) catch "e";
            reportResult("large_payload_handling", false, detail);
        }
    } else {
        reportResult("large_payload_handling", false, "no message");
    }
}

// Boundary Test: Subject length limits
// Tests subjects of various lengths.
pub fn testSubjectLengthBoundary(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_len_boundary", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Test 200 char subject (should work)
    var long_subject_buf: [200]u8 = undefined;
    for (&long_subject_buf, 0..) |*c, i| {
        // Alternate between 'a'-'z' and '.' for valid subject
        if (i % 10 == 9) {
            c.* = '.';
        } else {
            c.* = 'a' + @as(u8, @intCast(i % 26));
        }
    }

    const sub = client.subscribe(allocator, &long_subject_buf) catch {
        reportResult("subject_len_boundary", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {
        reportResult("subject_len_boundary", false, "flush failed");
        return;
    };

    client.publish(&long_subject_buf, "test") catch {
        reportResult("subject_len_boundary", false, "publish failed");
        return;
    };
    client.flush() catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("subject_len_boundary", true, "");
    } else {
        reportResult("subject_len_boundary", false, "no message");
    }
}

// Boundary Test: Zero-length payload
// Tests publishing empty payloads.
pub fn testZeroLengthPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("zero_len_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "zero.payload") catch {
        reportResult("zero_len_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Empty payload
    client.publish("zero.payload", "") catch {
        reportResult("zero_len_payload", false, "publish failed");
        return;
    };
    client.flush() catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        defer m.deinit(allocator);
        if (m.data.len == 0) {
            reportResult("zero_len_payload", true, "");
        } else {
            reportResult("zero_len_payload", false, "non-empty data");
        }
    } else {
        reportResult("zero_len_payload", false, "no message");
    }
}

// Boundary Test: Single byte payload
// Tests publishing single byte payloads.
pub fn testSingleBytePayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("single_byte_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "single.byte") catch {
        reportResult("single_byte_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush() catch {};

    // Single byte
    client.publish("single.byte", "X") catch {
        reportResult("single_byte_payload", false, "publish failed");
        return;
    };
    client.flush() catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        defer m.deinit(allocator);
        if (m.data.len == 1 and m.data[0] == 'X') {
            reportResult("single_byte_payload", true, "");
        } else {
            reportResult("single_byte_payload", false, "wrong data");
        }
    } else {
        reportResult("single_byte_payload", false, "no message");
    }
}

// Boundary Test: SID boundaries
// Tests SID allocation doesn't overflow.
pub fn testSidBoundaries(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("sid_boundaries", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create and destroy 100 subscriptions, verify SIDs increase
    var last_sid: u64 = 0;
    for (0..100) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "sid.test.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribe(allocator, subject) catch {
            reportResult("sid_boundaries", false, "subscribe failed");
            return;
        };

        // SID should be strictly increasing
        if (sub.sid <= last_sid and i > 0) {
            sub.deinit(allocator);
            reportResult("sid_boundaries", false, "SID not increasing");
            return;
        }
        last_sid = sub.sid;
        sub.deinit(allocator);
    }

    if (client.isConnected()) {
        reportResult("sid_boundaries", true, "");
    } else {
        reportResult("sid_boundaries", false, "disconnected");
    }
}

// Boundary Test: Exceeding max subscriptions should fail
pub fn testMaxSubscriptionsExceeded(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_subs_exceeded", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 256 subscriptions first
    const MAX_SUBS = 256;
    var subs: [MAX_SUBS]?*nats.Subscription = undefined;
    @memset(&subs, null);

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    var created: usize = 0;
    for (0..MAX_SUBS) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(
            &subject_buf,
            "exceedsub.{d}",
            .{i},
        ) catch continue;
        subs[i] = client.subscribe(allocator, subject) catch {
            break;
        };
        created += 1;
    }

    if (created != MAX_SUBS) {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "only created {d}/256",
            .{created},
        ) catch "e";
        reportResult("max_subs_exceeded", false, detail);
        return;
    }

    // 257th subscription should fail with TooManySubscriptions
    const result = client.subscribe(allocator, "exceedsub.257");
    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("max_subs_exceeded", false, "257th should fail");
    } else |err| {
        if (err == error.TooManySubscriptions) {
            reportResult("max_subs_exceeded", true, "");
        } else {
            reportResult("max_subs_exceeded", false, "wrong error type");
        }
    }
}

/// Runs all async edge case tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testAsyncDoubleUnsubscribe(allocator);
    testAsyncMessageOrdering(allocator);
    testAsyncBinaryPayload(allocator);
    testLongSubjectName(allocator);
    testSubjectWithNumbersHyphens(allocator);
    testDoubleFlush(allocator);
    testDoubleDrain(allocator);
    testRapidSubUnsubCycles(allocator);
    testNewInboxUniqueness(allocator);
    testEmptySubjectFails(allocator);
    testSubjectWithSpacesFails(allocator);
    testInterleavedPubSub(allocator);
    testReceiveOnlyAfterSubscribe(allocator);
    testDataIntegrityPattern(allocator);
    testCompletePubSubRoundTrip(allocator);
    // Boundary tests
    testQueueExactCapacity(allocator);
    testQueueOverflow(allocator);
    testMaxSubscriptions(allocator);
    testLargePayloadHandling(allocator);
    testSubjectLengthBoundary(allocator);
    testZeroLengthPayload(allocator);
    testSingleBytePayload(allocator);
    testSidBoundaries(allocator);
    testMaxSubscriptionsExceeded(allocator);
}
