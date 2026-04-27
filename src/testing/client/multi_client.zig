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

    const io = utils.newIo(allocator);
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
    defer publisher.deinit();

    const subscriber = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("cross_client", false, "sub connect failed");
        return;
    };
    defer subscriber.deinit();

    const sub = subscriber.subscribeSync("cross") catch {
        reportResult("cross_client", false, "sub failed");
        return;
    };
    defer sub.deinit();

    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    publisher.publish("cross", "cross-message") catch {
        reportResult("cross_client", false, "publish failed");
        return;
    };

    var future = io.io().async(
        nats.Client.Sub.nextMsg,
        .{sub},
    );
    defer if (future.cancel(io.io())) |m| m.deinit() else |_| {};

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

    const io = utils.newIo(allocator);
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
    defer client1.deinit();

    const client2 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multiple_clients", false, "client2 failed");
        return;
    };
    defer client2.deinit();

    const client3 = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("multiple_clients", false, "client3 failed");
        return;
    };
    defer client3.deinit();

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

    const io = utils.newIo(allocator);
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
    defer publisher.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 512,
        .reconnect = false,
    }) catch {
        reportResult("client_high_rate", false, "connect failed");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("highrate") catch {
        reportResult("client_high_rate", false, "sub failed");
        return;
    };
    defer sub.deinit();

    // Wait for subscription to be registered on server
    // io.io().sleep(.fromMilliseconds(50), .awake) catch {};
    client.flush(50_000_000) catch {};

    const NUM_MSGS = 100;
    for (0..NUM_MSGS) |_| {
        publisher.publish("highrate", "msg") catch {
            reportResult("client_high_rate", false, "publish failed");
            return;
        };
    }

    publisher.flush(500_000_000) catch {};

    std.debug.print("[TEST] flush done, starting receive loop\n", .{});

    var received: usize = 0;
    for (0..NUM_MSGS) |i| {
        std.debug.print("[TEST] recv {d}: calling io.async()\n", .{i});
        var future = io.io().async(
            nats.Client.Sub.nextMsg,
            .{sub},
        );
        defer if (future.cancel(io.io())) |m| m.deinit() else |_| {};

        std.debug.print("[TEST] recv {d}: calling future.await()\n", .{i});
        if (future.await(io.io())) |_| {
            std.debug.print("[TEST] recv {d}: got message\n", .{i});
            received += 1;
        } else |_| {
            std.debug.print("[TEST] recv {d}: await failed\n", .{i});
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
    const io_a = utils.newIo(allocator);
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
    defer client_a.deinit();

    // Client B - middleware (receives from A, forwards to C)
    const io_b = utils.newIo(allocator);
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
    defer client_b.deinit();

    // Client C - final receiver
    const io_c = utils.newIo(allocator);
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
    defer client_c.deinit();

    // B subscribes to "step1"
    const sub_b = client_b.subscribeSync("chain.step1") catch {
        reportResult("three_client_chain", false, "B sub failed");
        return;
    };
    defer sub_b.deinit();

    // C subscribes to "step2"
    const sub_c = client_c.subscribeSync("chain.step2") catch {
        reportResult("three_client_chain", false, "C sub failed");
        return;
    };
    defer sub_c.deinit();

    io_a.io().sleep(.fromMilliseconds(50), .awake) catch {};

    // A publishes to step1
    client_a.publish("chain.step1", "start") catch {
        reportResult("three_client_chain", false, "A publish failed");
        return;
    };

    // B receives and forwards to step2
    const msg_b = sub_b.nextMsgTimeout(2000) catch {
        reportResult("three_client_chain", false, "B receive failed");
        return;
    };
    if (msg_b) |m| {
        defer m.deinit();
        client_b.publish("chain.step2", "forwarded") catch {
            reportResult("three_client_chain", false, "B forward failed");
            return;
        };
    } else {
        reportResult("three_client_chain", false, "B no message");
        return;
    }

    // C receives final message
    const msg_c = sub_c.nextMsgTimeout(2000) catch {
        reportResult("three_client_chain", false, "C receive failed");
        return;
    };
    if (msg_c) |m| {
        defer m.deinit();
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

    const io = utils.newIo(allocator);
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
    defer client.deinit();

    const sub1 = client.subscribeSync("broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub1 failed");
        return;
    };
    defer sub1.deinit();

    const sub2 = client.subscribeSync("broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub2 failed");
        return;
    };
    defer sub2.deinit();

    const sub3 = client.subscribeSync("broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub3 failed");
        return;
    };
    defer sub3.deinit();

    client.publish("broadcast.test", "hello all") catch {
        reportResult("multi_sub_same_subject", false, "publish failed");
        return;
    };

    client.flush(500_000_000) catch {};

    var count: u32 = 0;

    if (sub1.nextMsgTimeout(500) catch null) |m| {
        m.deinit();
        count += 1;
    }
    if (sub2.nextMsgTimeout(500) catch null) |m| {
        m.deinit();
        count += 1;
    }
    if (sub3.nextMsgTimeout(500) catch null) |m| {
        m.deinit();
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
