//! Stress Tests for NATS Client

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

pub fn testStress500Messages(allocator: std.mem.Allocator) void {
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
        reportResult("stress_500", false, "pub connect failed");
        return;
    };
    defer publisher.deinit(allocator);

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 512,
        .reconnect = false,
    }) catch {
        reportResult("stress_500", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress500") catch {
        reportResult("stress_500", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch {
        reportResult("stress_500", false, "flush failed");
        return;
    };
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    const NUM_MSGS = 500;
    for (0..NUM_MSGS) |_| {
        publisher.publish("stress500", "stress-msg") catch {
            reportResult("stress_500", false, "publish failed");
            return;
        };
    }
    publisher.flush(allocator) catch {
        reportResult("stress_500", false, "pub flush failed");
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
        reportResult("stress_500", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg =
            std.fmt.bufPrint(&buf, "got {d}/500", .{received}) catch "e";
        reportResult("stress_500", false, msg);
    }
}

pub fn testStress1000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 1024,
        .reconnect = false,
    }) catch {
        reportResult("stress_1000", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress1k") catch {
        reportResult("stress_1000", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {
        reportResult("stress_1000", false, "flush failed");
        return;
    };

    const NUM_MSGS = 1000;
    for (0..NUM_MSGS) |_| {
        client.publish("stress1k", "stress-msg") catch {
            reportResult("stress_1000", false, "publish failed");
            return;
        };
    }
    client.flush(allocator) catch {
        reportResult("stress_1000", false, "pub flush failed");
        return;
    };

    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == NUM_MSGS) {
        reportResult("stress_1000", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg =
            std.fmt.bufPrint(&buf, "got {d}/1000", .{received}) catch "e";
        reportResult("stress_1000", false, msg);
    }
}

pub fn testStress2000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .sub_queue_size = 2048,
        .reconnect = false,
    }) catch {
        reportResult("stress_2000", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress2k") catch {
        reportResult("stress_2000", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {
        reportResult("stress_2000", false, "flush failed");
        return;
    };

    const NUM_MSGS = 2000;
    for (0..20) |_| {
        for (0..100) |_| {
            client.publish("stress2k", "stress-msg") catch {
                reportResult("stress_2000", false, "publish failed");
                return;
            };
        }
        client.flush(allocator) catch {
            reportResult("stress_2000", false, "batch flush failed");
            return;
        };
    }

    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (sub.nextWithTimeout(allocator, 100) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        } else break;
    }

    if (received == NUM_MSGS) {
        reportResult("stress_2000", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg =
            std.fmt.bufPrint(&buf, "got {d}/2000", .{received}) catch "e";
        reportResult("stress_2000", false, msg);
    }
}

pub fn testPayload30KB(allocator: std.mem.Allocator) void {
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
        reportResult("payload_30kb", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.30kb") catch {
        reportResult("payload_30kb", false, "sub failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const payload = allocator.alloc(u8, 30 * 1024) catch {
        reportResult("payload_30kb", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    client.publish("stress.30kb", payload) catch {
        reportResult("payload_30kb", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 3000) catch null) |m| {
        defer m.deinit(allocator);
        if (m.data.len == 30 * 1024) {
            reportResult("payload_30kb", true, "");
        } else {
            reportResult("payload_30kb", false, "wrong size");
        }
    } else {
        reportResult("payload_30kb", false, "no message");
    }
}

pub fn testManySubscriptions(allocator: std.mem.Allocator) void {
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
        reportResult("many_subscriptions", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    var subs: [50]?*nats.Subscription = undefined;
    @memset(&subs, null);

    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    var created: usize = 0;
    for (0..50) |i| {
        var subject_buf: [32]u8 = undefined;
        const subject =
            std.fmt.bufPrint(&subject_buf, "manysub.{d}", .{i}) catch {
                continue;
            };
        subs[i] = client.subscribe(allocator, subject) catch {
            break;
        };
        created += 1;
    }

    if (created == 50) {
        reportResult("many_subscriptions", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const msg =
            std.fmt.bufPrint(&buf, "created {d}/50", .{created}) catch "e";
        reportResult("many_subscriptions", false, msg);
    }
}

pub fn testPayloadBoundary(allocator: std.mem.Allocator) void {
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
        reportResult("payload_boundary", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "boundary.test") catch {
        reportResult("payload_boundary", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    const sizes = [_]usize{ 1024, 4096, 8192, 15360 };
    var all_passed = true;

    for (sizes) |size| {
        const payload = allocator.alloc(u8, size) catch {
            all_passed = false;
            break;
        };
        defer allocator.free(payload);
        @memset(payload, 'B');

        client.publish("boundary.test", payload) catch {
            all_passed = false;
            break;
        };
        client.flush(allocator) catch {};

        const msg = sub.nextWithTimeout(allocator, 2000) catch {
            all_passed = false;
            break;
        };

        if (msg) |m| {
            if (m.data.len != size) all_passed = false;
            m.deinit(allocator);
        } else {
            all_passed = false;
            break;
        }
    }

    if (all_passed) {
        reportResult("payload_boundary", true, "");
    } else {
        reportResult("payload_boundary", false, "size mismatch");
    }
}

pub fn testFiveConcurrentClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var ios: [5]std.Io.Threaded = undefined;
    var clients: [5]?*nats.Client = [_]?*nats.Client{null} ** 5;
    var count: usize = 0;

    defer {
        for (0..count) |i| {
            if (clients[i]) |c| {
                c.deinit(allocator);
            }
            ios[i].deinit();
        }
    }

    for (0..5) |i| {
        ios[i] = .init(allocator, .{ .environ = .empty });
        clients[i] = nats.Client.connect(
            allocator,
            ios[i].io(),
            url,
            .{ .reconnect = false },
        ) catch {
            reportResult("five_concurrent", false, "connect failed");
            return;
        };
        count += 1;
    }

    var all_connected = true;
    for (0..5) |i| {
        if (clients[i]) |c| {
            if (!c.isConnected()) all_connected = false;
        }
    }

    if (all_connected) {
        reportResult("five_concurrent", true, "");
    } else {
        reportResult("five_concurrent", false, "not all connected");
    }
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testStress500Messages(allocator);
    testStress1000Messages(allocator);
    testStress2000Messages(allocator);
    testPayload30KB(allocator);
    testManySubscriptions(allocator);
    testPayloadBoundary(allocator);
    testFiveConcurrentClients(allocator);
}
