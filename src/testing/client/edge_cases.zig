//! Edge Cases Tests for NATS  Client

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

pub fn testDoubleUnsubscribe(allocator: std.mem.Allocator) void {
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
        reportResult("double_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "double.unsub") catch {
        reportResult("double_unsub", false, "sub failed");
        return;
    };

    sub.unsubscribe() catch {};
    sub.unsubscribe() catch {};

    sub.deinit(allocator);

    if (client.isConnected()) {
        reportResult("double_unsub", true, "");
    } else {
        reportResult("double_unsub", false, "disconnected");
    }
}

pub fn testMessageOrdering(allocator: std.mem.Allocator) void {
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
        reportResult("message_ordering", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "order") catch {
        reportResult("message_ordering", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    var pub_buf: [5][8]u8 = undefined;
    for (0..5) |i| {
        const payload = std.fmt.bufPrint(
            &pub_buf[i],
            "msg-{d}",
            .{i},
        ) catch "e";
        client.publish("order", payload) catch {};
    }
    client.flush(allocator) catch {};

    var in_order = true;
    for (0..5) |expected| {
        var future = io.io().async(
            nats.Client.Sub.next,
            .{ sub, allocator, io.io() },
        );
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |msg| {
            var exp_buf: [8]u8 = undefined;
            const exp = std.fmt.bufPrint(
                &exp_buf,
                "msg-{d}",
                .{expected},
            ) catch "e";
            if (!std.mem.eql(u8, msg.data, exp)) {
                in_order = false;
            }
        } else |_| {
            in_order = false;
            break;
        }
    }

    if (in_order) {
        reportResult("message_ordering", true, "");
    } else {
        reportResult("message_ordering", false, "out of order");
    }
}

pub fn testBinaryPayload(allocator: std.mem.Allocator) void {
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
        reportResult("binary_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "binary") catch {
        reportResult("binary_payload", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    const binary = [_]u8{ 0x00, 0x01, 0x02, 0xFF, 0xFE, 0x00, 0x03 };

    client.publish("binary", &binary) catch {
        reportResult("binary_payload", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, &binary)) {
            reportResult("binary_payload", true, "");
            return;
        }
    } else |_| {}

    reportResult("binary_payload", false, "binary mismatch");
}

pub fn testLongSubjectName(allocator: std.mem.Allocator) void {
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
        reportResult("long_subject_name", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const long_subject = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z" ++
        ".aa.bb.cc.dd.ee.ff.gg.hh.ii.jj.kk.ll.mm.nn";

    const sub = client.subscribe(allocator, long_subject) catch {
        reportResult("long_subject_name", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish(long_subject, "test") catch {
        reportResult("long_subject_name", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("subject_nums_hyphens", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const subject = "test-123.foo_bar.baz-456";

    const sub = client.subscribe(allocator, subject) catch {
        reportResult("subject_nums_hyphens", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish(subject, "test") catch {
        reportResult("subject_nums_hyphens", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("double_flush", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("double_flush", false, "flush1 failed");
        return;
    };
    client.flush(allocator) catch {
        reportResult("double_flush", false, "flush2 failed");
        return;
    };
    client.flush(allocator) catch {
        reportResult("double_flush", false, "flush3 failed");
        return;
    };

    reportResult("double_flush", true, "");
}

pub fn testDoubleDrain(allocator: std.mem.Allocator) void {
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
        reportResult("double_drain", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "double.drain.test") catch {
        reportResult("double_drain", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("rapid_sub_unsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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
    var inboxes: [10][]u8 = undefined;
    var created: usize = 0;

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    defer for (inboxes[0..created]) |inbox| {
        allocator.free(inbox);
    };

    for (0..10) |i| {
        inboxes[i] = nats.newInbox(allocator, io.io()) catch {
            reportResult("inbox_uniqueness", false, "newInbox failed");
            return;
        };
        created += 1;

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("empty_subject_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("subject_spaces_fails", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("interleaved_pubsub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "interleave.test") catch {
        reportResult("interleaved_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    var received: u32 = 0;
    for (0..10) |i| {
        var buf: [16]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "msg{d}", .{i}) catch continue;

        client.publish("interleave.test", payload) catch {
            reportResult("interleaved_pubsub", false, "publish failed");
            return;
        };
        client.flush(allocator) catch {};

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("receive_after_sub", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.publish("timing.test", "before") catch {};
    client.flush(allocator) catch {};

    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const sub = client.subscribe(allocator, "timing.test") catch {
        reportResult("receive_after_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    client.publish("timing.test", "after") catch {};
    client.flush(allocator) catch {};

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("data_integrity", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "integrity.test") catch {
        reportResult("data_integrity", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    // Create pattern payload
    var payload: [256]u8 = undefined;
    for (&payload, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    client.publish("integrity.test", &payload) catch {
        reportResult("data_integrity", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("complete_roundtrip", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    if (!client.isConnected()) {
        reportResult("complete_roundtrip", false, "not connected");
        return;
    }

    const sub = client.subscribe(allocator, "roundtrip.100") catch {
        reportResult("complete_roundtrip", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const before = client.getStats();

    const test_data = "Test100-RoundTrip-Verification";
    client.publish("roundtrip.100", test_data) catch {
        reportResult("complete_roundtrip", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

    if (!std.mem.eql(u8, m.data, test_data)) {
        reportResult("complete_roundtrip", false, "data mismatch");
        return;
    }

    if (!std.mem.eql(u8, m.subject, "roundtrip.100")) {
        reportResult("complete_roundtrip", false, "subject mismatch");
        return;
    }

    const after = client.getStats();

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

pub fn testQueueExactCapacity(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const QUEUE_SIZE = 64;
    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = QUEUE_SIZE,
        .reconnect = false,
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
    client.flush(allocator) catch {
        reportResult("queue_exact_cap", false, "flush failed");
        return;
    };

    for (0..QUEUE_SIZE) |_| {
        client.publish("boundary.exact", "x") catch {
            reportResult("queue_exact_cap", false, "publish failed");
            return;
        };
    }
    client.flush(allocator) catch {
        reportResult("queue_exact_cap", false, "pub flush failed");
        return;
    };

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
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/64",
            .{received},
        ) catch "e";
        reportResult("queue_exact_cap", false, detail);
    }
}

pub fn testQueueOverflow(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const QUEUE_SIZE = 32;
    const PUBLISH_COUNT = 64;

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = QUEUE_SIZE,
        .reconnect = false,
    }) catch {
        reportResult("queue_overflow", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub_reader = client.subscribe(allocator, "overflow.reader") catch {
        reportResult("queue_overflow", false, "sub_reader failed");
        return;
    };
    defer sub_reader.deinit(allocator);

    const sub_target = client.subscribe(allocator, "overflow.target") catch {
        reportResult("queue_overflow", false, "sub_target failed");
        return;
    };
    defer sub_target.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("queue_overflow", false, "flush failed");
        return;
    };
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    for (0..PUBLISH_COUNT) |_| {
        client.publish("overflow.target", "x") catch {
            reportResult("queue_overflow", false, "publish target failed");
            return;
        };
    }
    client.publish("overflow.reader", "trigger") catch {
        reportResult("queue_overflow", false, "publish reader failed");
        return;
    };
    client.flush(allocator) catch {
        reportResult("queue_overflow", false, "pub flush failed");
        return;
    };

    if (sub_reader.nextWithTimeout(allocator, 2000) catch null) |m| {
        m.deinit(allocator);
    } else {
        reportResult("queue_overflow", false, "reader timeout");
        return;
    }

    var received: u32 = 0;
    while (sub_target.tryNext()) |m| {
        m.deinit(allocator);
        received += 1;
    }

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

pub fn testMaxSubscriptions(allocator: std.mem.Allocator) void {
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
        reportResult("max_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

    if (created == MAX_SUBS) {
        reportResult("max_subscriptions", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/256",
            .{created},
        ) catch "e";
        reportResult("max_subscriptions", false, detail);
    }
}

pub fn testLargePayloadHandling(allocator: std.mem.Allocator) void {
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
        reportResult("large_payload_handling", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large.payload") catch {
        reportResult("large_payload_handling", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("large_payload_handling", false, "flush failed");
        return;
    };

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
    client.flush(allocator) catch {
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

pub fn testSubjectLengthBoundary(allocator: std.mem.Allocator) void {
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
        reportResult("subject_len_boundary", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var long_subject_buf: [200]u8 = undefined;
    for (&long_subject_buf, 0..) |*c, i| {
        if (i % 10 == 9 and i < 199) {
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

    client.flush(allocator) catch {
        reportResult("subject_len_boundary", false, "flush failed");
        return;
    };

    client.publish(&long_subject_buf, "test") catch {
        reportResult("subject_len_boundary", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("subject_len_boundary", true, "");
    } else {
        reportResult("subject_len_boundary", false, "no message");
    }
}

pub fn testZeroLengthPayload(allocator: std.mem.Allocator) void {
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
        reportResult("zero_len_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "zero.payload") catch {
        reportResult("zero_len_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("zero.payload", "") catch {
        reportResult("zero_len_payload", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

pub fn testSingleBytePayload(allocator: std.mem.Allocator) void {
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
        reportResult("single_byte_payload", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "single.byte") catch {
        reportResult("single_byte_payload", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("single.byte", "X") catch {
        reportResult("single_byte_payload", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

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

pub fn testSidBoundaries(allocator: std.mem.Allocator) void {
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
        reportResult("sid_boundaries", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

pub fn testMaxSubscriptionsExceeded(allocator: std.mem.Allocator) void {
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
        reportResult("max_subs_exceeded", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

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

pub fn runAll(allocator: std.mem.Allocator) void {
    testDoubleUnsubscribe(allocator);
    testMessageOrdering(allocator);
    testBinaryPayload(allocator);
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
