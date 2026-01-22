//! Multi-Client Tests for NATS Client

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

pub fn testCrossClientRouting(allocator: std.mem.Allocator) void {
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
        reportResult("cross_client", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const subscriber = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_client", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit(allocator);

    const sub = subscriber.subscribe(allocator, "cross") catch {
        reportResult("cross_client", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    subscriber.flush(allocator) catch {
        reportResult("cross_client", false, "sub flush failed");
        return;
    };
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("cross", "cross-message") catch {
        reportResult("cross_client", false, "publish failed");
        return;
    };
    publisher.flush(allocator) catch {
        reportResult("cross_client", false, "pub flush failed");
        return;
    };

    var future = io.io().async(
        nats.Client.Sub.next,
        .{ sub, allocator, io.io() },
    );
    defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

    if (future.await(io.io())) |msg| {
        if (std.mem.eql(u8, msg.data, "cross-message")) {
            reportResult("cross_client", true, "");
            return;
        }
    } else |_| {}

    reportResult("cross_client", false, "no message");
}

pub fn testMultipleClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client1 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multiple_clients", false, "client1 failed");
        return;
    };
    defer client1.deinit(allocator);

    const client2 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multiple_clients", false, "client2 failed");
        return;
    };
    defer client2.deinit(allocator);

    const client3 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multiple_clients", false, "client3 failed");
        return;
    };
    defer client3.deinit(allocator);

    if (client1.isConnected() and client2.isConnected() and
        client3.isConnected())
    {
        reportResult("multiple_clients", true, "");
    } else {
        reportResult("multiple_clients", false, "not all connected");
    }
}

pub fn testClientHighRate(allocator: std.mem.Allocator) void {
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
        reportResult("client_high_rate", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 512,
        .reconnect = false,
    }) catch {
        reportResult("client_high_rate", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "highrate") catch {
        reportResult("client_high_rate", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("client_high_rate", false, "flush failed");
        return;
    };

    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        publisher.publish("highrate", "msg") catch {
            reportResult("client_high_rate", false, "publish failed");
            return;
        };
    }
    publisher.flush(allocator) catch {
        reportResult("client_high_rate", false, "pub flush failed");
        return;
    };

    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        var future = io.io().async(
            nats.Client.Sub.next,
            .{ sub, allocator, io.io() },
        );
        defer if (future.cancel(io.io())) |m| m.deinit(allocator) else |_| {};

        if (future.await(io.io())) |_| {
            received += 1;
        } else |_| {
            break;
        }
    }

    if (received == NUM_MSGS) {
        reportResult("client_high_rate", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "got {d}/100", .{received}) catch "e";
        reportResult("client_high_rate", false, msg);
    }
}

pub fn testThreeClientChain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - initial publisher
    var io_a: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_a.deinit();
    const client_a = nats.Client.connect(
        allocator,
        io_a.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("three_client_chain", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - middleware (receives from A, forwards to C)
    var io_b: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_b.deinit();
    const client_b = nats.Client.connect(
        allocator,
        io_b.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("three_client_chain", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C - final receiver
    var io_c: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_c.deinit();
    const client_c = nats.Client.connect(
        allocator,
        io_c.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("three_client_chain", false, "C connect failed");
        return;
    };
    defer client_c.deinit(allocator);

    // B subscribes to "step1"
    const sub_b = client_b.subscribe(allocator, "chain.step1") catch {
        reportResult("three_client_chain", false, "B sub failed");
        return;
    };
    defer sub_b.deinit(allocator);

    // C subscribes to "step2"
    const sub_c = client_c.subscribe(allocator, "chain.step2") catch {
        reportResult("three_client_chain", false, "C sub failed");
        return;
    };
    defer sub_c.deinit(allocator);

    client_b.flush(allocator) catch {};
    client_c.flush(allocator) catch {};
    io_a.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // A publishes to step1
    client_a.publish("chain.step1", "start") catch {
        reportResult("three_client_chain", false, "A publish failed");
        return;
    };
    client_a.flush(allocator) catch {};

    // B receives and forwards to step2
    const msg_b = sub_b.nextWithTimeout(allocator, 2000) catch {
        reportResult("three_client_chain", false, "B receive failed");
        return;
    };
    if (msg_b) |m| {
        defer m.deinit(allocator);
        client_b.publish("chain.step2", "forwarded") catch {
            reportResult("three_client_chain", false, "B forward failed");
            return;
        };
        client_b.flush(allocator) catch {};
    } else {
        reportResult("three_client_chain", false, "B no message");
        return;
    }

    // C receives final message
    const msg_c = sub_c.nextWithTimeout(allocator, 2000) catch {
        reportResult("three_client_chain", false, "C receive failed");
        return;
    };
    if (msg_c) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "forwarded")) {
            reportResult("three_client_chain", true, "");
        } else {
            reportResult("three_client_chain", false, "wrong data");
        }
    } else {
        reportResult("three_client_chain", false, "C no message");
    }
}

pub fn testMultipleSubscribersSameSubject(allocator: std.mem.Allocator) void {
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
        reportResult("multi_sub_same_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub1 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush(allocator) catch {};

    client.publish("broadcast.test", "hello all") catch {
        reportResult("multi_sub_same_subject", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    var count: u32 = 0;

    if (sub1.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub3.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 3) {
        reportResult("multi_sub_same_subject", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/3", .{count}) catch "err";
        reportResult("multi_sub_same_subject", false, detail);
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testCrossClientRouting(allocator);
    testMultipleClients(allocator);
    testClientHighRate(allocator);
    testThreeClientChain(allocator);
    testMultipleSubscribersSameSubject(allocator);
}
