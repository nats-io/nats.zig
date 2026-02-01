//! Server Pool for Reconnection
//!
//! Manages multiple servers for reconnection with round-robin rotation.
//! Servers are discovered from initial URL and INFO connect_urls.

const std = @import("std");
const assert = std.debug.assert;
const defaults = @import("../defaults.zig");

/// Maximum number of servers in the pool.
pub const MAX_SERVERS: u8 = defaults.Server.max_pool_size;

/// Maximum URL length.
pub const MAX_URL_LEN: u16 = defaults.Server.max_url_len;

/// Cooldown period after failure before retry (ns).
const FAILURE_COOLDOWN_NS: u64 = defaults.Server.failure_cooldown_ns;

/// Server entry in the pool.
pub const Server = struct {
    url: [MAX_URL_LEN]u8 = undefined,
    url_len: u8 = 0,
    host_start: u8 = 0,
    host_len: u8 = 0,
    port: u16 = defaults.Protocol.port,
    consecutive_failures: u8 = 0,
    last_attempt_ns: u64 = 0,
    /// Whether this server uses TLS (from tls:// scheme).
    use_tls: bool = false,

    /// Get the URL as a slice.
    pub fn getUrl(self: *const Server) []const u8 {
        return self.url[0..self.url_len];
    }

    /// Get the host as a slice.
    pub fn getHost(self: *const Server) []const u8 {
        return self.url[self.host_start..][0..self.host_len];
    }
};

/// Server pool for reconnection rotation.
pub const ServerPool = struct {
    servers: [MAX_SERVERS]Server = undefined,
    count: u8 = 0,
    current_idx: u8 = 0,
    primary_idx: u8 = 0,

    /// Initialize server pool with primary server URL.
    pub fn init(primary_url: []const u8) error{InvalidUrl}!ServerPool {
        var pool: ServerPool = .{};

        if (primary_url.len == 0 or primary_url.len > MAX_URL_LEN) {
            return error.InvalidUrl;
        }

        pool.addServer(primary_url) catch return error.InvalidUrl;
        pool.primary_idx = 0;

        assert(pool.count > 0);
        return pool;
    }

    /// Add a server to the pool. Returns false if pool is full or URL invalid.
    pub fn addServer(self: *ServerPool, url: []const u8) !void {
        if (self.count >= MAX_SERVERS) return error.PoolFull;
        if (url.len == 0 or url.len > MAX_URL_LEN) return error.InvalidUrl;

        for (self.servers[0..self.count]) |*existing| {
            if (std.mem.eql(u8, existing.getUrl(), url)) {
                return;
            }
        }

        var server: Server = .{};

        const url_len: u8 = @intCast(url.len);
        @memcpy(server.url[0..url_len], url);
        server.url_len = url_len;

        var remaining = url;

        if (std.mem.startsWith(u8, remaining, "tls://")) {
            remaining = remaining[6..];
            server.host_start = 6;
            server.use_tls = true;
        } else if (std.mem.startsWith(u8, remaining, "nats://")) {
            remaining = remaining[7..];
            server.host_start = 7;
        }

        if (std.mem.indexOf(u8, remaining, "@")) |at_pos| {
            remaining = remaining[at_pos + 1 ..];
            server.host_start += @intCast(at_pos + 1);
        }

        if (std.mem.indexOf(u8, remaining, ":")) |colon_pos| {
            server.host_len = @intCast(colon_pos);
            server.port = std.fmt.parseInt(
                u16,
                remaining[colon_pos + 1 ..],
                10,
            ) catch 4222;
        } else {
            server.host_len = @intCast(remaining.len);
            server.port = 4222;
        }

        assert(server.host_len > 0);
        assert(server.port > 0);

        self.servers[self.count] = server;
        self.count += 1;
    }

    /// Add servers from ServerInfo connect_urls.
    /// Returns the number of new servers that were added (not duplicates).
    pub fn addFromConnectUrls(
        self: *ServerPool,
        urls: []const [256]u8,
        lens: []const u8,
        count: u8,
    ) u8 {
        assert(urls.len >= count);
        assert(lens.len >= count);

        const before = self.count;
        for (0..count) |i| {
            const len = lens[i];
            if (len == 0) continue;
            const url = urls[i][0..len];
            self.addServer(url) catch continue;
        }
        return self.count - before;
    }

    /// Get next server for connection attempt (round-robin).
    /// Skips servers that failed recently (cooldown).
    /// Returns null if all servers are on cooldown.
    pub fn nextServer(self: *ServerPool, now_ns: u64) ?*Server {
        if (self.count == 0) return null;

        assert(self.count > 0);
        assert(self.current_idx < self.count);

        var attempts: u8 = 0;
        while (attempts < self.count) : (attempts += 1) {
            self.current_idx = (self.current_idx + 1) % self.count;
            var server = &self.servers[self.current_idx];

            if (server.consecutive_failures > 0) {
                const cooldown = FAILURE_COOLDOWN_NS *
                    @as(u64, server.consecutive_failures);
                if (now_ns - server.last_attempt_ns < cooldown) {
                    continue;
                }
            }

            server.last_attempt_ns = now_ns;
            return server;
        }

        return null;
    }

    /// Mark current server as failed.
    pub fn markCurrentFailed(self: *ServerPool) void {
        if (self.count == 0) return;
        assert(self.current_idx < self.count);

        var server = &self.servers[self.current_idx];
        if (server.consecutive_failures < 255) {
            server.consecutive_failures += 1;
        }
    }

    /// Reset all failure counts (called on successful connect).
    pub fn resetFailures(self: *ServerPool) void {
        for (self.servers[0..self.count]) |*server| {
            server.consecutive_failures = 0;
        }
    }

    /// Get current server URL as slice.
    pub fn currentUrl(self: *const ServerPool) []const u8 {
        if (self.count == 0) return "none";
        assert(self.current_idx < self.count);
        return self.servers[self.current_idx].getUrl();
    }

    /// Get current server.
    pub fn current(self: *ServerPool) ?*Server {
        if (self.count == 0) return null;
        assert(self.current_idx < self.count);
        return &self.servers[self.current_idx];
    }

    /// Get server count.
    pub fn serverCount(self: *const ServerPool) u8 {
        return self.count;
    }
};

test "server pool init" {
    const pool = try ServerPool.init("nats://localhost:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.count);
    try std.testing.expectEqualStrings(
        "nats://localhost:4222",
        pool.servers[0].getUrl(),
    );
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "server pool init with auth" {
    const pool = try ServerPool.init("nats://user:pass@localhost:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.count);
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "server pool init without port" {
    const pool = try ServerPool.init("nats://localhost");
    try std.testing.expectEqual(@as(u8, 1), pool.count);
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "server pool init without scheme" {
    const pool = try ServerPool.init("localhost:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.count);
    try std.testing.expectEqualStrings("localhost", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
}

test "server pool add servers" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");
    try pool.addServer("nats://server3:4222");

    try std.testing.expectEqual(@as(u8, 3), pool.count);
}

test "server pool deduplication" {
    var pool = try ServerPool.init("nats://localhost:4222");
    try pool.addServer("nats://localhost:4222"); // Duplicate
    try pool.addServer("nats://localhost:4222"); // Duplicate

    try std.testing.expectEqual(@as(u8, 1), pool.count);
}

test "server pool rotation" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("nats://server2:4222");
    try pool.addServer("nats://server3:4222");

    const now: u64 = 1000000000000;

    // Should rotate through servers
    const s1 = pool.nextServer(now).?;
    try std.testing.expectEqualStrings("nats://server2:4222", s1.getUrl());

    const s2 = pool.nextServer(now).?;
    try std.testing.expectEqualStrings("nats://server3:4222", s2.getUrl());

    const s3 = pool.nextServer(now).?;
    try std.testing.expectEqualStrings("nats://server1:4222", s3.getUrl());
}

test "server pool failure tracking" {
    var pool = try ServerPool.init("nats://server1:4222");

    var now: u64 = 1000000000000;

    // Get server and mark as failed
    _ = pool.nextServer(now);
    pool.markCurrentFailed();

    try std.testing.expectEqual(@as(u8, 1), pool.servers[0].consecutive_failures);

    // Should be on cooldown
    now += 1000000000; // +1 second (cooldown is 5 seconds)
    try std.testing.expect(pool.nextServer(now) == null);

    // After cooldown, should be available
    now += 10000000000; // +10 seconds
    try std.testing.expect(pool.nextServer(now) != null);
}

test "server pool reset failures" {
    var pool = try ServerPool.init("nats://server1:4222");

    _ = pool.nextServer(0);
    pool.markCurrentFailed();
    pool.markCurrentFailed();

    try std.testing.expectEqual(@as(u8, 2), pool.servers[0].consecutive_failures);

    pool.resetFailures();

    try std.testing.expectEqual(@as(u8, 0), pool.servers[0].consecutive_failures);
}

test "server pool empty url" {
    const result = ServerPool.init("");
    try std.testing.expectError(error.InvalidUrl, result);
}

test "server pool tls scheme" {
    const pool = try ServerPool.init("tls://secure.example.com:4222");
    try std.testing.expectEqual(@as(u8, 1), pool.count);
    try std.testing.expectEqualStrings("secure.example.com", pool.servers[0].getHost());
    try std.testing.expectEqual(@as(u16, 4222), pool.servers[0].port);
    try std.testing.expect(pool.servers[0].use_tls);
}

test "server pool nats scheme not tls" {
    const pool = try ServerPool.init("nats://localhost:4222");
    try std.testing.expect(!pool.servers[0].use_tls);
}

test "server pool mixed schemes" {
    var pool = try ServerPool.init("nats://server1:4222");
    try pool.addServer("tls://server2:4222");

    try std.testing.expectEqual(@as(u8, 2), pool.count);
    try std.testing.expect(!pool.servers[0].use_tls);
    try std.testing.expect(pool.servers[1].use_tls);
}
