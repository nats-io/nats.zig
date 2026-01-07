//! Edge Cases Tests for NATS Client
//!
//! Tests for edge cases and validation.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

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

    // Multiple flushes should all succeed
    client.flush() catch {
        reportResult("double_flush", false, "first flush failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "second flush failed");
        return;
    };
    client.flush() catch {
        reportResult("double_flush", false, "third flush failed");
        return;
    };

    reportResult("double_flush", true, "");
}

// Test 38: Double unsubscribe should be safe (error, not panic)

pub fn testDoubleUnsubscribe(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_unsubscribe", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "double.unsub") catch {
        reportResult("double_unsubscribe", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // First unsubscribe
    sub.unsubscribe() catch {
        reportResult("double_unsubscribe", false, "first unsub failed");
        return;
    };

    // Second unsubscribe - should error or be idempotent, not panic
    const result = sub.unsubscribe();
    if (result) |_| {
        // Idempotent is acceptable
        reportResult("double_unsubscribe", true, "");
    } else |_| {
        // Error is also acceptable (subscription already closed)
        reportResult("double_unsubscribe", true, "");
    }
}

// Test 39: Binary payload (non-UTF8 data)

pub fn testDoubleDrain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("double_drain", false, "connect failed");
        return;
    };

    // First drain
    client.drain(allocator) catch {
        reportResult("double_drain", false, "first drain failed");
        client.deinit(allocator);
        return;
    };

    // Second drain - should error, not panic
    const result = client.drain(allocator);
    client.deinit(allocator);

    if (result) |_| {
        // Idempotent is fine
        reportResult("double_drain", true, "");
    } else |_| {
        // Error is also fine (already drained)
        reportResult("double_drain", true, "");
    }
}

// Test 47: Verify isConnected() reflects state correctly

pub fn testBinaryPayload(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "binary.test") catch {
        reportResult("binary_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Binary payload with null bytes and high bytes
    const binary_data = [_]u8{ 0x00, 0x01, 0xFF, 0xFE, 0x80, 0x7F, 0x00, 0xFF };

    client.publish("binary.test", &binary_data) catch {
        reportResult("binary_payload", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("binary_payload", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == binary_data.len and
            std.mem.eql(u8, m.data, &binary_data))
        {
            reportResult("binary_payload", true, "");
        } else {
            reportResult("binary_payload", false, "data mismatch");
        }
    } else {
        reportResult("binary_payload", false, "no message");
    }
}

// Test 40: Many subscriptions (stress test)

pub fn testMessageOrdering(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("message_ordering", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "order.test") catch {
        reportResult("message_ordering", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 100 numbered messages
    var buf: [16]u8 = undefined;
    for (0..100) |i| {
        const payload = std.fmt.bufPrint(&buf, "{d}", .{i}) catch continue;
        client.publish("order.test", payload) catch {
            reportResult("message_ordering", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Verify received in order
    for (0..100) |expected| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
            reportResult("message_ordering", false, "receive failed");
            return;
        };
        if (msg) |m| {
            defer m.deinit(allocator);
            const received = std.fmt.parseInt(usize, m.data, 10) catch {
                reportResult("message_ordering", false, "parse failed");
                return;
            };
            if (received != expected) {
                reportResult("message_ordering", false, "out of order");
                return;
            }
        } else {
            reportResult("message_ordering", false, "missing message");
            return;
        }
    }

    reportResult("message_ordering", true, "");
}

// Test 30: Unsubscribe stops delivery

pub fn testPingPong(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("ping_pong", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // flushWithTimeout does PING/PONG roundtrip
    client.flushWithTimeout(5000) catch {
        reportResult("ping_pong", false, "flush timeout");
        return;
    };

    reportResult("ping_pong", true, "");
}

// Test 32: Cross-client message routing (client A publishes, client B receives)

pub fn testExplicitPing(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("explicit_ping", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Send explicit ping
    client.ping() catch {
        reportResult("explicit_ping", false, "ping failed");
        return;
    };

    // Multiple pings should work
    for (0..5) |_| {
        client.ping() catch {
            reportResult("explicit_ping", false, "multi ping failed");
            return;
        };
    }

    reportResult("explicit_ping", true, "");
}

// Test 80: Stats bytes accuracy

pub fn testMultipleFlushes(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_flushes", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Multiple flushes in sequence should all succeed
    for (0..10) |_| {
        client.publish("flush.seq", "data") catch {
            reportResult("multi_flushes", false, "publish failed");
            return;
        };
        client.flush() catch {
            reportResult("multi_flushes", false, "flush failed");
            return;
        };
    }

    reportResult("multi_flushes", true, "");
}

// Test 95: newInbox generates unique inboxes

pub fn testFlushTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("flush_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Publish something
    client.publish("flush.timeout.test", "data") catch {
        reportResult("flush_timeout", false, "publish failed");
        return;
    };

    // Flush with explicit timeout
    client.flushWithTimeout(5000) catch {
        reportResult("flush_timeout", false, "flush timeout failed");
        return;
    };

    reportResult("flush_timeout", true, "");
}

// Test 76: State after various operations

pub fn testVeryShortTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("very_short_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "short.timeout") catch {
        reportResult("very_short_timeout", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // 1ms timeout - should return null quickly, not hang
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1 }) catch {
        reportResult("very_short_timeout", false, "error on short timeout");
        return;
    };

    if (msg == null) {
        reportResult("very_short_timeout", true, "");
    } else {
        msg.?.deinit(allocator);
        reportResult("very_short_timeout", false, "unexpected message");
    }
}

// Test 54: Reply-to is preserved in message

pub fn testZeroTimeout(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("zero_timeout", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "zero.timeout") catch {
        reportResult("zero_timeout", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Zero timeout - should return immediately with null
    var timer = std.time.Timer.start() catch {
        reportResult("zero_timeout", false, "timer failed");
        return;
    };

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 0 }) catch {
        reportResult("zero_timeout", false, "error on zero timeout");
        return;
    };

    const elapsed_ms = timer.read() / std.time.ns_per_ms;

    if (msg == null and elapsed_ms < 100) {
        reportResult("zero_timeout", true, "");
    } else if (msg != null) {
        msg.?.deinit(allocator);
        reportResult("zero_timeout", false, "unexpected message");
    } else {
        reportResult("zero_timeout", false, "took too long");
    }
}

// Test 64: Multiple queue groups on same subject

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

// Test 62: Subject with spaces should fail

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

// Test 63: Zero timeout returns immediately

pub fn testHierarchicalSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("hierarchical_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Deep hierarchical subject
    const deep_subject = "level1.level2.level3.level4.level5.data";

    const sub = client.subscribe(allocator, deep_subject) catch {
        reportResult("hierarchical_subject", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish(deep_subject, "deep message") catch {
        reportResult("hierarchical_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("hierarchical_subject", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.subject, deep_subject)) {
            reportResult("hierarchical_subject", true, "");
        } else {
            reportResult("hierarchical_subject", false, "subject mismatch");
        }
    } else {
        reportResult("hierarchical_subject", false, "no message");
    }
}

// Test 42: Flush after every publish (correctness check)

pub fn testLongSubjectName(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("long_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Long but valid subject (100 chars)
    const long_subject = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t" ++
        ".u.v.w.x.y.z.aa.bb.cc.dd.ee.ff.gg.hh.ii.jj.kk.ll.mm.nn";

    const sub = client.subscribe(allocator, long_subject) catch {
        reportResult("long_subject", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish(long_subject, "test") catch {
        reportResult("long_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("long_subject", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        reportResult("long_subject", true, "");
    } else {
        reportResult("long_subject", false, "no message");
    }
}

// Test 49: Consecutive connections (connect, disconnect, connect again)

pub fn testComplexSubjectNames(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("complex_subjects", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Test various complex but valid subject names
    const subjects = [_][]const u8{
        "a-b-c.d-e-f",
        "123.456.789",
        "user-123.order-456",
        "v1.api.users.get",
        "event_stream.user_created",
    };

    var all_ok = true;
    for (subjects) |subj| {
        const sub = client.subscribe(allocator, subj) catch {
            all_ok = false;
            break;
        };
        defer sub.deinit(allocator);
        client.flush() catch {};

        client.publish(subj, "test") catch {
            all_ok = false;
            break;
        };
        client.flush() catch {};

        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 300 }) catch {
            all_ok = false;
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
        } else {
            all_ok = false;
            break;
        }
    }

    if (all_ok) {
        reportResult("complex_subjects", true, "");
    } else {
        reportResult("complex_subjects", false, "failed");
    }
}

// Test 94: Multiple sequential flushes

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

    const subject = "user-123.order-456.item-789";

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

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
        reportResult("subject_nums_hyphens", false, "receive failed");
        return;
    };

    if (msg) |m| {
        m.deinit(allocator);
        reportResult("subject_nums_hyphens", true, "");
    } else {
        reportResult("subject_nums_hyphens", false, "no message");
    }
}

// Test 56: Wildcard * at different positions

pub fn testSubjectTokens(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_tokens", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Valid multi-token subject
    const sub = client.subscribe(allocator, "a.b.c.d.e.f") catch {
        reportResult("subject_tokens", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    client.publish("a.b.c.d.e.f", "deep") catch {
        reportResult("subject_tokens", false, "publish failed");
        return;
    };
    client.flush() catch {};

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
        reportResult("subject_tokens", false, "receive failed");
        return;
    };

    if (msg) |m| {
        m.deinit(allocator);
        reportResult("subject_tokens", true, "");
    } else {
        reportResult("subject_tokens", false, "no message");
    }
}

// Test 84: Stress test - 1000 messages

pub fn testSubjectCaseSensitivity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("subject_case", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe to lowercase
    const sub_lower = client.subscribe(allocator, "case.test") catch {
        reportResult("subject_case", false, "sub lower failed");
        return;
    };
    defer sub_lower.deinit(allocator);

    // Subscribe to uppercase
    const sub_upper = client.subscribe(allocator, "CASE.TEST") catch {
        reportResult("subject_case", false, "sub upper failed");
        return;
    };
    defer sub_upper.deinit(allocator);

    client.flush() catch {};

    // Publish to lowercase
    client.publish("case.test", "lower") catch {};
    // Publish to uppercase
    client.publish("CASE.TEST", "upper") catch {};
    client.flush() catch {};

    // Each should only receive their own
    var lower_count: u32 = 0;
    var upper_count: u32 = 0;

    for (0..2) |_| {
        if (sub_lower.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            lower_count += 1;
        }
    }
    for (0..2) |_| {
        if (sub_upper.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            upper_count += 1;
        }
    }

    // NATS is case sensitive - each should get exactly 1
    if (lower_count == 1 and upper_count == 1) {
        reportResult("subject_case", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "lower={d} upper={d}",
            .{ lower_count, upper_count },
        ) catch "err";
        reportResult("subject_case", false, detail);
    }
}

// Test 69: Subscriber receives only after subscribe

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

        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
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

// Test 58: Publish to wildcard subject should fail

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
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
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

// Test 70: Stress test - 500 messages

pub fn testReceiveMessageWithHeaders(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("receive_headers", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "headers.test") catch {
        reportResult("receive_headers", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Use nats CLI to publish message with headers
    var child = std.process.Child.init(
        &[_][]const u8{
            "nats",
            "pub",
            "-s",
            url,
            "--header",
            "X-Custom:test-value",
            "--header",
            "X-Another:second",
            "headers.test",
            "payload-data",
        },
        allocator,
    );
    child.spawn(io.io()) catch {
        reportResult("receive_headers", false, "nats cli spawn");
        return;
    };
    _ = child.wait(io.io()) catch {
        reportResult("receive_headers", false, "nats cli wait");
        return;
    };

    // Receive message
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("receive_headers", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);

        // Verify data
        if (!std.mem.eql(u8, m.data, "payload-data")) {
            reportResult("receive_headers", false, "wrong data");
            return;
        }

        // Verify headers exist
        if (m.headers) |h| {
            // Headers should contain our custom header
            if (std.mem.indexOf(u8, h, "X-Custom")) |_| {
                reportResult("receive_headers", true, "");
            } else {
                reportResult("receive_headers", false, "header not found");
            }
        } else {
            reportResult("receive_headers", false, "no headers");
        }
    } else {
        reportResult("receive_headers", false, "no message");
    }
}

// Test 75: Flush timeout behavior

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

    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 500 }) catch {
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

// Test 99: Protocol version in server info

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
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 1000 }) catch {
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

// Async Tests

// Test: Basic async receive

/// Runs all edge case tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testDoubleFlush(allocator);
    testDoubleUnsubscribe(allocator);
    testDoubleDrain(allocator);
    testBinaryPayload(allocator);
    testMessageOrdering(allocator);
    testPingPong(allocator);
    testExplicitPing(allocator);
    testMultipleFlushes(allocator);
    testFlushTimeout(allocator);
    testVeryShortTimeout(allocator);
    testZeroTimeout(allocator);
    testEmptySubjectFails(allocator);
    testSubjectWithSpacesFails(allocator);
    testHierarchicalSubject(allocator);
    testLongSubjectName(allocator);
    testComplexSubjectNames(allocator);
    testSubjectWithNumbersHyphens(allocator);
    testSubjectTokens(allocator);
    testSubjectCaseSensitivity(allocator);
    testInterleavedPubSub(allocator);
    testReceiveOnlyAfterSubscribe(allocator);
    testReceiveMessageWithHeaders(allocator);
    testDataIntegrityPattern(allocator);
    testCompletePubSubRoundTrip(allocator);
}
