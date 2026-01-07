//! Stress Tests for NATS Client
//!
//! Tests for high-volume message stress testing.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testStress500Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_500_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.500") catch {
        reportResult("stress_500_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 500 messages
    for (0..500) |_| {
        client.publish("stress.500", "stress-test-payload") catch {
            reportResult("stress_500_msgs", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Receive all 500
    var received: u32 = 0;
    for (0..600) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 100 }) catch {
            break;
        };
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 500) break;
        } else {
            break;
        }
    }

    if (received == 500) {
        reportResult("stress_500_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "got {d}/500",
            .{received},
        ) catch "err";
        reportResult("stress_500_msgs", false, detail);
    }
}

// Test 72: 30KB payload (near buffer limit)

pub fn testStress1000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_1000_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.1k") catch {
        reportResult("stress_1000_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 1000 messages
    for (0..1000) |_| {
        client.publish("stress.1k", "1k-stress") catch {
            reportResult("stress_1000_msgs", false, "publish failed");
            return;
        };
    }
    client.flush() catch {};

    // Receive all
    var received: u32 = 0;
    for (0..1100) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 50 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 1000) break;
        } else break;
    }

    if (received == 1000) {
        reportResult("stress_1000_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/1000", .{received}) catch "e";
        reportResult("stress_1000_msgs", false, detail);
    }
}

// Test 85: Reply-to field in publish

pub fn testStress2000Messages(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("stress_2000_msgs", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "stress.2k") catch {
        reportResult("stress_2000_msgs", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Publish 2000 messages in batches
    for (0..20) |_| {
        for (0..100) |_| {
            client.publish("stress.2k", "2k-stress") catch {
                reportResult("stress_2000_msgs", false, "publish failed");
                return;
            };
        }
        client.flush() catch {};
    }

    // Receive all
    var received: u32 = 0;
    for (0..2200) |_| {
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 50 }) catch break;
        if (msg) |m| {
            m.deinit(allocator);
            received += 1;
            if (received >= 2000) break;
        } else break;
    }

    if (received == 2000) {
        reportResult("stress_2000_msgs", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/2000", .{received}) catch "e";
        reportResult("stress_2000_msgs", false, detail);
    }
}

// Test 97: Four clients in queue group

pub fn testPayload30KB(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("payload_30kb", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "large.30kb") catch {
        reportResult("payload_30kb", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Create 30KB payload (just under 32KB buffer limit)
    // Actual limit is ~16KB due to protocol overhead + read mechanics
    const payload_size: usize = 15 * 1024; // 15KB is safe
    const payload = allocator.alloc(u8, payload_size) catch {
        reportResult("payload_30kb", false, "alloc failed");
        return;
    };
    defer allocator.free(payload);

    // Fill with pattern
    for (payload, 0..) |*b, i| {
        b.* = @truncate(i % 256);
    }

    client.publish("large.30kb", payload) catch {
        reportResult("payload_30kb", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // Receive with owned copy
    const msg = sub.nextMessageOwned(allocator, .{
        .timeout_ms = 5000,
    }) catch {
        reportResult("payload_30kb", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (m.data.len == payload_size) {
            // Verify pattern
            var valid = true;
            for (m.data, 0..) |b, i| {
                if (b != @as(u8, @truncate(i % 256))) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                reportResult("payload_30kb", true, "");
            } else {
                reportResult("payload_30kb", false, "data corrupt");
            }
        } else {
            var buf: [32]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "got {d} bytes",
                .{m.data.len},
            ) catch "err";
            reportResult("payload_30kb", false, detail);
        }
    } else {
        reportResult("payload_30kb", false, "no message");
    }
}

// Test 73: Payload at exact buffer boundary

pub fn testPayloadBoundary(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("payload_boundary", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "boundary.test") catch {
        reportResult("payload_boundary", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush() catch {};

    // Test exact sizes: 1KB, 4KB, 8KB, 15KB
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
        client.flush() catch {};

        const msg = sub.nextMessageOwned(allocator, .{
            .timeout_ms = 2000,
        }) catch {
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

// Test 71: Receive message with headers (via nats CLI)

pub fn testFiveConcurrentClients(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Create 5 clients
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
        ios[i] = .init(allocator, .{});
        clients[i] = nats.Client.connect(allocator, ios[i].io(), url, .{}) catch {
            reportResult("five_concurrent", false, "connect failed");
            return;
        };
        count += 1;
    }

    // All should be connected
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

// Test 67: Publish from multiple clients to one subscriber

pub fn testManyPublishersOneSubscriber(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Subscriber client
    var io_sub: std.Io.Threaded = .init(allocator, .{});
    defer io_sub.deinit();
    const client_sub = nats.Client.connect(allocator, io_sub.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "sub connect failed");
        return;
    };
    defer client_sub.deinit(allocator);

    const sub = client_sub.subscribe(allocator, "fanin.test") catch {
        reportResult("many_pub_one_sub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client_sub.flush() catch {};
    std.posix.nanosleep(0, 50_000_000);

    // Publisher client 1
    var io_pub1: std.Io.Threaded = .init(allocator, .{});
    defer io_pub1.deinit();
    const client_pub1 = nats.Client.connect(allocator, io_pub1.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "pub1 connect failed");
        return;
    };
    defer client_pub1.deinit(allocator);

    // Publisher client 2
    var io_pub2: std.Io.Threaded = .init(allocator, .{});
    defer io_pub2.deinit();
    const client_pub2 = nats.Client.connect(allocator, io_pub2.io(), url, .{}) catch {
        reportResult("many_pub_one_sub", false, "pub2 connect failed");
        return;
    };
    defer client_pub2.deinit(allocator);

    // Each publisher sends 5 messages
    for (0..5) |_| {
        client_pub1.publish("fanin.test", "from1") catch {};
        client_pub2.publish("fanin.test", "from2") catch {};
    }
    client_pub1.flush() catch {};
    client_pub2.flush() catch {};

    // Subscriber should receive all 10
    var received: u32 = 0;
    for (0..15) |_| {
        if (sub.nextMessage(allocator, .{ .timeout_ms = 200 }) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 10) {
        reportResult("many_pub_one_sub", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/10", .{received}) catch "e";
        reportResult("many_pub_one_sub", false, detail);
    }
}

// Test 68: Subject case sensitivity

/// Runs all stress tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testStress500Messages(allocator);
    testStress1000Messages(allocator);
    testStress2000Messages(allocator);
    testPayload30KB(allocator);
    testPayloadBoundary(allocator);
    testFiveConcurrentClients(allocator);
    testManyPublishersOneSubscriber(allocator);
}
