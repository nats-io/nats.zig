//! Server Pool Tests
//!
//! Comprehensive unit tests for ServerPool including edge cases,
//! failure tracking, cooldown behavior, and URL parsing.

const std = @import("std");
const ServerPool = @import("server_pool.zig").ServerPool;
const Server = @import("server_pool.zig").Server;
const MAX_SERVERS = @import("server_pool.zig").MAX_SERVERS;
const MAX_URL_LEN = @import("server_pool.zig").MAX_URL_LEN;

// URL Parsing Tests

test "parse URL with IPv4 address" {
    const pool = try ServerPool.init("nats://192.168.1.100:4222");
    try std.testing.expectEqualStrings("192.168.1.100", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "parse URL with different port" {
    const pool = try ServerPool.init("nats://localhost:5222");
    try std.testing.expectEqual(@as(u16, 5222), pool.servers[0].port);
}

test "parse URL with max port" {
    const pool = try ServerPool.init("nats://localhost:65535");
    try std.testing.expectEqual(@as(u16, 65535), pool.servers[0].port);
}

test "parse URL with port 1" {
    const pool = try ServerPool.init("nats://localhost:1");
    try std.testing.expectEqual(@as(u16, 1), pool.servers[0].port);
}

test "parse URL with invalid port uses default" {
    const pool = try ServerPool.init("nats://localhost:invalid");
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "parse URL with port overflow uses default" {
    const pool = try ServerPool.init("nats://localhost:99999");
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "parse URL with user only" {
    const pool = try ServerPool.init("nats://user@localhost:4222");
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
}

test "parse URL with complex auth" {
    const pool = try ServerPool.init("nats://user:p@ss:word@localhost:4222");
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "parse URL with just host no scheme no port" {
    const pool = try ServerPool.init("myserver");
    try std.testing.expectEqualStrings("myserver", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "parse URL preserves original" {
    const original = "nats://demo.nats.io:4222";
    const pool = try ServerPool.init(original);
    try std.testing.expectEqualStrings(original, pool.servers[0].getUrl());
}

test "pool starts empty after primary" {
    const pool = try ServerPool.init("nats://localhost:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.serverCount());
}

test "pool can hold MAX_SERVERS" {
    var pool = try ServerPool.init("nats://server0:4222");

    var i: u8 = 1;
    while (i < MAX_SERVERS) : (i += 1) {
        var buf: [32]u8 = undefined;
        const url = std.fmt.bufPrint(&buf, "nats://server{d}:4222", .{i}) catch
            unreachable;
        try pool.addServer(url);
    }

    try std.testing.expectEqual(MAX_SERVERS, pool.serverCount());
}

test "pool full returns error" {
    var pool = try ServerPool.init("nats://server0:4222");

    var i: u8 = 1;
    while (i < MAX_SERVERS) : (i += 1) {
        var buf: [32]u8 = undefined;
        const url = std.fmt.bufPrint(&buf, "nats://server{d}:4222", .{i}) catch
            unreachable;
        try pool.addServer(url);
    }

    const result = pool.addServer("nats://overflow:4222");
    try std.testing.expectError(error.PoolFull, result);
}

test "URL too long returns error" {
    var long_url: [MAX_URL_LEN + 10]u8 = undefined;
    @memset(&long_url, 'a');
    const result = ServerPool.init(&long_url);
    try std.testing.expectError(error.InvalidUrl, result);
}

test "URL exactly max length succeeds" {
    var url: [MAX_URL_LEN]u8 = undefined;
    @memset(&url, 'a');
    @memcpy(url[0..7], "server:");
    const pool = try ServerPool.init(&url);
    try std.testing.expectEqual(@as(u8, 1), pool.serverCount());
}

test "exact duplicate rejected" {
    var pool = try ServerPool.init("nats://localhost:4222");
    try pool.addServer("nats://localhost:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.serverCount());
}

test "different port not duplicate" {
    var pool = try ServerPool.init("nats://localhost:4222");
    try pool.addServer("nats://localhost:4223");
    try std.testing.expectEqual(@as(u8, 2), pool.serverCount());
}

test "different host not duplicate" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");
    try std.testing.expectEqual(@as(u8, 2), pool.serverCount());
}

test "case sensitive URLs" {
    var pool = try ServerPool.init("nats://Server1:4222");
    try pool.addServer("nats://server1:4222");
    try std.testing.expectEqual(@as(u8, 2), pool.serverCount());
}

test "rotation starts from second server" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");
    try pool.addServer("nats://server3:4222");

    const now: u64 = 1_000_000_000_000;

    const s1 = pool.nextServer(now).?;
    try std.testing.expectEqualStrings("nats://server2:4222", s1.getUrl());
}

test "rotation wraps around" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");

    const now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now); // server2
    _ = pool.nextServer(now); // server1
    const s3 = pool.nextServer(now).?; // server2 again
    try std.testing.expectEqualStrings("nats://server2:4222", s3.getUrl());
}

test "single server rotation returns same" {
    var pool = try ServerPool.init("nats://only:4222");

    const now: u64 = 1_000_000_000_000;

    const s1 = pool.nextServer(now).?;
    const s2 = pool.nextServer(now).?;
    const s3 = pool.nextServer(now).?;

    try std.testing.expectEqualStrings("nats://only:4222", s1.getUrl());
    try std.testing.expectEqualStrings("nats://only:4222", s2.getUrl());
    try std.testing.expectEqualStrings("nats://only:4222", s3.getUrl());
}

// Failure Tracking Tests

test "failure count increments" {
    var pool = try ServerPool.init("nats://server:4222");
    const now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    try std.testing.expectEqual(@as(u8, 0), pool.servers[0].consecutive_failures);

    pool.markCurrentFailed();
    try std.testing.expectEqual(@as(u8, 1), pool.servers[0].consecutive_failures);

    pool.markCurrentFailed();
    try std.testing.expectEqual(@as(u8, 2), pool.servers[0].consecutive_failures);
}

test "failure count saturates at 255" {
    var pool = try ServerPool.init("nats://server:4222");
    const now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);

    var i: u16 = 0;
    while (i < 300) : (i += 1) {
        pool.markCurrentFailed();
    }

    try std.testing.expectEqual(@as(u8, 255), pool.servers[0].consecutive_failures);
}

test "reset failures clears all" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");

    var now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    pool.markCurrentFailed();
    pool.markCurrentFailed();

    now += 100_000_000_000;
    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    pool.resetFailures();

    try std.testing.expectEqual(@as(u8, 0), pool.servers[0].consecutive_failures);
    try std.testing.expectEqual(@as(u8, 0), pool.servers[1].consecutive_failures);
}

test "cooldown increases with failures" {
    var pool = try ServerPool.init("nats://server:4222");
    var now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    now += 4_000_000_000;
    try std.testing.expect(pool.nextServer(now) == null);

    now += 2_000_000_000;
    try std.testing.expect(pool.nextServer(now) != null);
    pool.markCurrentFailed();

    now += 8_000_000_000;
    try std.testing.expect(pool.nextServer(now) == null);

    now += 4_000_000_000;
    try std.testing.expect(pool.nextServer(now) != null);
}

test "all servers on cooldown returns null" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");

    var now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    pool.markCurrentFailed();
    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    now += 1_000_000_000;
    try std.testing.expect(pool.nextServer(now) == null);
}

test "cooldown expires allows retry" {
    var pool = try ServerPool.init("nats://server:4222");
    var now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    now += 6_000_000_000;
    try std.testing.expect(pool.nextServer(now) != null);
}

test "healthy server chosen over failed" {
    var pool = try ServerPool.init("nats://failed:4222");
    try pool.addServer("nats://healthy:4222");

    const now: u64 = 1_000_000_000_000;

    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    const server = pool.nextServer(now).?;
    try std.testing.expectEqualStrings("nats://healthy:4222", server.getUrl());
}

test "add from connect_urls" {
    var pool = try ServerPool.init("nats://primary:4222");

    var urls: [16][256]u8 = undefined;
    var lens: [16]u8 = [_]u8{0} ** 16;

    const url1 = "nats://cluster1:4222";
    const url2 = "nats://cluster2:4222";

    @memcpy(urls[0][0..url1.len], url1);
    lens[0] = url1.len;

    @memcpy(urls[1][0..url2.len], url2);
    lens[1] = url2.len;

    pool.addFromConnectUrls(&urls, &lens, 2);

    try std.testing.expectEqual(@as(u8, 3), pool.serverCount());
}

test "add from connect_urls skips empty" {
    var pool = try ServerPool.init("nats://primary:4222");

    var urls: [16][256]u8 = undefined;
    var lens: [16]u8 = [_]u8{0} ** 16;

    const url1 = "nats://cluster1:4222";
    @memcpy(urls[0][0..url1.len], url1);
    lens[0] = url1.len;
    lens[2] = 0;

    pool.addFromConnectUrls(&urls, &lens, 3);

    try std.testing.expectEqual(@as(u8, 2), pool.serverCount());
}

test "add from connect_urls deduplicates" {
    var pool = try ServerPool.init("nats://primary:4222");

    var urls: [16][256]u8 = undefined;
    var lens: [16]u8 = [_]u8{0} ** 16;

    const url1 = "nats://primary:4222";
    @memcpy(urls[0][0..url1.len], url1);
    lens[0] = url1.len;

    pool.addFromConnectUrls(&urls, &lens, 1);

    try std.testing.expectEqual(@as(u8, 1), pool.serverCount());
}

// Current Server Access Tests

test "currentUrl on empty pool returns none" {
    // Can't create empty pool directly, but test the behavior
    var pool = try ServerPool.init("nats://server:4222");
    // Manually clear for testing (don't do this in production!)
    pool.count = 0;
    try std.testing.expectEqualStrings("none", pool.currentUrl());
}

test "current returns server reference" {
    var pool = try ServerPool.init("nats://server:4222");
    const server = pool.current().?;
    try std.testing.expectEqualStrings("nats://server:4222", server.getUrl());
}

test "current allows modification" {
    var pool = try ServerPool.init("nats://server:4222");
    const server = pool.current().?;
    server.consecutive_failures = 5;
    try std.testing.expectEqual(@as(u8, 5), pool.servers[0].consecutive_failures);
}

// Server Struct Tests

test "server default values" {
    const server: Server = .{};
    try std.testing.expectEqual(@as(u8, 0), server.url_len);
    try std.testing.expectEqual(@as(u16, 4222), server.port);
    try std.testing.expectEqual(@as(u8, 0), server.consecutive_failures);
    try std.testing.expectEqual(@as(u64, 0), server.last_attempt_ns);
}

test "server getUrl returns correct slice" {
    var server: Server = .{};
    const url = "nats://test:1234";
    @memcpy(server.url[0..url.len], url);
    server.url_len = url.len;

    try std.testing.expectEqualStrings(url, server.getUrl());
}

test "server getHost returns correct slice" {
    var server: Server = .{};
    const url = "nats://myhost:4222";
    @memcpy(server.url[0..url.len], url);
    server.url_len = url.len;
    server.host_start = 7;
    server.host_len = 6;

    try std.testing.expectEqualStrings("myhost", server.getHost());
}

test "primary index preserved" {
    var pool = try ServerPool.init("nats://primary:4222");
    try pool.addServer("nats://secondary:4222");

    try std.testing.expectEqual(@as(u8, 0), pool.primary_idx);
}

test "timestamps updated on next_server" {
    var pool = try ServerPool.init("nats://server:4222");

    const time1: u64 = 1_000_000_000_000;
    _ = pool.nextServer(time1);
    try std.testing.expectEqual(time1, pool.servers[0].last_attempt_ns);

    const time2: u64 = 2_000_000_000_000;
    _ = pool.nextServer(time2);
    try std.testing.expectEqual(time2, pool.servers[0].last_attempt_ns);
}

test "zero time works" {
    var pool = try ServerPool.init("nats://server:4222");
    const server = pool.nextServer(0);
    try std.testing.expect(server != null);
}

test "max time works" {
    var pool = try ServerPool.init("nats://server:4222");
    const server = pool.nextServer(std.math.maxInt(u64));
    try std.testing.expect(server != null);
}
