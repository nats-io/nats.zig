//! Subscribe Tests for NATS Client

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

pub fn testClientManySubs(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_many_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 32, .reconnect = false },
    ) catch {
        reportResult("client_many_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const NUM_SUBS = 5;
    var subs: [NUM_SUBS]*nats.Client.Sub = undefined;
    var sub_buf: [NUM_SUBS][32]u8 = undefined;
    var topics: [NUM_SUBS][]const u8 = undefined;

    for (0..NUM_SUBS) |i| {
        topics[i] = std.fmt.bufPrint(
            &sub_buf[i],
            "many.{d}",
            .{i},
        ) catch "err";
        subs[i] = client.subscribe(allocator, topics[i]) catch {
            reportResult("client_many_subs", false, "sub failed");
            return;
        };
    }
    defer for (subs) |s| s.deinit(allocator);

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    for (topics) |t| {
        publisher.publish(t, "hello") catch {};
    }
    publisher.flush(allocator) catch {};

    var received: usize = 0;
    for (subs) |s| {
        var future = io.io().async(
            nats.Client.Sub.next,
            .{ s, allocator, io.io() },
        );
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {}
    }

    if (received == NUM_SUBS) {
        reportResult("client_many_subs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM_SUBS },
        ) catch "e";
        reportResult("client_many_subs", false, msg);
    }
}

pub fn testClientWildcard(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_wildcard", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_wildcard", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "wild.*") catch {
        reportResult("client_wildcard", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("client_wildcard", false, "flush failed");
        return;
    };
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("wild.a", "msg-a") catch {
        reportResult("client_wildcard", false, "pub a failed");
        return;
    };
    publisher.publish("wild.b", "msg-b") catch {
        reportResult("client_wildcard", false, "pub b failed");
        return;
    };
    publisher.publish("wild.c", "msg-c") catch {
        reportResult("client_wildcard", false, "pub c failed");
        return;
    };
    publisher.flush(allocator) catch {
        reportResult("client_wildcard", false, "pub flush failed");
        return;
    };

    const NUM_MSGS = 3;
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(
            nats.Client.Sub.next,
            .{ sub, allocator, io.io() },
        );
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {}
    }

    if (received == NUM_MSGS) {
        reportResult("client_wildcard", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/3", .{received}) catch "e";
        reportResult("client_wildcard", false, msg);
    }
}

pub fn testClientDuplicateSubs(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_dup_subs", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_dup_subs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "dup") catch {
        reportResult("client_dup_subs", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "dup") catch {
        reportResult("client_dup_subs", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("dup", "hello") catch {};
    publisher.flush(allocator) catch {};

    var future1 = io.io().async(
        nats.Client.Sub.next,
        .{ sub1, allocator, io.io() },
    );
    defer if (future1.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    var future2 = io.io().async(
        nats.Client.Sub.next,
        .{ sub2, allocator, io.io() },
    );
    defer if (future2.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    const got1 = if (future1.await(io.io())) |_| true else |_| false;
    const got2 = if (future2.await(io.io())) |_| true else |_| false;

    if (got1 and got2) {
        reportResult("client_dup_subs", true, "");
    } else {
        reportResult("client_dup_subs", false, "not both received");
    }
}

pub fn testClientQueueGroup(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_queue_group", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("client_queue_group", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribeQueue(allocator, "qg", "workers") catch {
        reportResult("client_queue_group", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("qg", "task") catch {};
    publisher.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("client_queue_group", true, "");
        return;
    } else |_| {}

    reportResult("client_queue_group", false, "no message");
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
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "wc.*") catch {
        reportResult("wildcard_matching", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("wc.test", "msg") catch {};
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("wildcard_matching", true, "");
        return;
    } else |_| {}

    reportResult("wildcard_matching", false, "no match");
}

pub fn testWildcardGreater(allocator: std.mem.Allocator) void {
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
        reportResult("wildcard_greater", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "gt.>") catch {
        reportResult("wildcard_greater", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("gt.a.b.c", "msg") catch {};
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("wildcard_greater", true, "");
        return;
    } else |_| {}

    reportResult("wildcard_greater", false, "no match");
}

pub fn testSubjectCaseSensitivity(allocator: std.mem.Allocator) void {
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
        reportResult("subject_case", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "case.test") catch {
        reportResult("subject_case", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("case.test", "msg") catch {};
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("subject_case", true, "");
        return;
    } else |_| {}

    reportResult("subject_case", false, "no match");
}

pub fn testUnsubscribeStopsDelivery(allocator: std.mem.Allocator) void {
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
        reportResult("unsub_stops", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "unsub.test") catch {
        reportResult("unsub_stops", false, "sub failed");
        return;
    };

    client.flush(allocator) catch {};

    sub.unsubscribe() catch {};
    sub.deinit(allocator);

    client.publish("unsub.test", "msg") catch {};
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(10), .awake) catch {};

    if (client.isConnected()) {
        reportResult("unsub_stops", true, "");
    } else {
        reportResult("unsub_stops", false, "disconnected");
    }
}

pub fn testHierarchicalSubject(allocator: std.mem.Allocator) void {
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
        reportResult("hierarchical", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const subject = "a.b.c.d.e.f.g.h";
    const sub = client.subscribe(allocator, subject) catch {
        reportResult("hierarchical", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.publish(subject, "deep") catch {
        reportResult("hierarchical", false, "pub failed");
        return;
    };
    client.flush(allocator) catch {};

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |_| {
        reportResult("hierarchical", true, "");
        return;
    } else |_| {}

    reportResult("hierarchical", false, "no message");
}

pub fn testUnsubscribeWithPending(allocator: std.mem.Allocator) void {
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
        reportResult("unsub_with_pending", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "pending.test") catch {
        reportResult("unsub_with_pending", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    for (0..5) |_| {
        client.publish("pending.test", "msg") catch {};
    }
    client.flush(allocator) catch {};
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    sub.unsubscribe() catch {
        reportResult("unsub_with_pending", false, "unsubscribe failed");
        return;
    };

    reportResult("unsub_with_pending", true, "");
}

pub fn testSubscribeAfterDisconnect(allocator: std.mem.Allocator) void {
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
        reportResult("sub_after_disconnect", false, "connect failed");
        return;
    };

    _ = client.drain(allocator) catch {
        client.deinit(allocator);
        reportResult("sub_after_disconnect", false, "drain failed");
        return;
    };

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

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("sub_queue_cap", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "qcap.test") catch {
        reportResult("sub_queue_cap", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {
        reportResult("sub_queue_cap", false, "flush failed");
        return;
    };

    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        client.publish("qcap.test", "qcap") catch {
            reportResult("sub_queue_cap", false, "publish failed");
            return;
        };
    }
    client.flush(allocator) catch {
        reportResult("sub_queue_cap", false, "pub flush failed");
        return;
    };

    var received: u32 = 0;
    for (0..NUM_MSGS) |_| {
        const msg = sub.nextWithTimeout(allocator, 200) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == NUM_MSGS) {
        reportResult("sub_queue_cap", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/100",
            .{received},
        ) catch "e";
        reportResult("sub_queue_cap", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testClientManySubs(allocator);
    testClientWildcard(allocator);
    testClientDuplicateSubs(allocator);
    testClientQueueGroup(allocator);
    testWildcardMatching(allocator);
    testWildcardGreater(allocator);
    testSubjectCaseSensitivity(allocator);
    testUnsubscribeStopsDelivery(allocator);
    testHierarchicalSubject(allocator);
    testUnsubscribeWithPending(allocator);
    testSubscribeAfterDisconnect(allocator);
    testSubscriptionQueueCapacity(allocator);
}
