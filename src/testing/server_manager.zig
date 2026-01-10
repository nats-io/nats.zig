//! NATS Server Manager
//!
//! Manages nats-server process lifecycle for integration testing.
//! Handles spawning, readiness detection, and graceful shutdown.

const std = @import("std");
const assert = std.debug.assert;
const posix = std.posix;
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Configuration for a NATS server instance.
pub const ServerConfig = struct {
    /// Port to listen on.
    port: u16 = 4222,
    /// Optional authentication token.
    auth_token: ?[]const u8 = null,
    /// Enable debug/verbose output.
    debug: bool = false,
};

/// A managed NATS server instance.
pub const ServerInstance = struct {
    process: ?std.process.Child = null,
    config: ServerConfig,
    port_buf: [8]u8 = undefined,

    /// Creates a new server instance with the given config.
    pub fn init(config: ServerConfig) ServerInstance {
        assert(config.port > 0);
        return .{ .config = config };
    }

    /// Starts the nats-server process.
    pub fn start(self: *ServerInstance, allocator: Allocator, io: Io) !void {
        assert(self.process == null);

        const port_str = std.fmt.bufPrint(
            &self.port_buf,
            "{d}",
            .{self.config.port},
        ) catch unreachable;

        var args: std.ArrayList([]const u8) = .{};
        defer args.deinit(allocator);

        try args.append(allocator, "nats-server");
        try args.append(allocator, "-p");
        try args.append(allocator, port_str);

        if (self.config.auth_token) |token| {
            try args.append(allocator, "--auth");
            try args.append(allocator, token);
        }

        if (self.config.debug) {
            try args.append(allocator, "-DV");
        }

        self.process = std.process.Child.init(args.items, allocator);
        self.process.?.stdout_behavior = .Ignore;
        self.process.?.stderr_behavior = .Ignore;

        try self.process.?.spawn(io);
        assert(self.process != null);
    }

    /// Waits for the server to become ready by probing the TCP port.
    pub fn waitReady(self: *ServerInstance, timeout_ms: u32) !void {
        assert(self.process != null);
        const max_attempts = timeout_ms / 50;
        var attempts: u32 = 0;

        while (attempts < max_attempts) : (attempts += 1) {
            if (self.probePort()) {
                return;
            }
            posix.nanosleep(0, 50_000_000); // 50ms
        }

        return error.ServerStartTimeout;
    }

    /// Probes if the server port is accepting connections.
    fn probePort(self: *ServerInstance) bool {
        const sock = posix.socket(
            posix.AF.INET,
            posix.SOCK.STREAM,
            0,
        ) catch return false;
        defer posix.close(sock);

        var addr: posix.sockaddr.in = .{
            .family = posix.AF.INET,
            .port = @byteSwap(self.config.port),
            .addr = @byteSwap(@as(u32, 0x7f000001)), // 127.0.0.1
        };

        posix.connect(
            sock,
            @ptrCast(&addr),
            @sizeOf(posix.sockaddr.in),
        ) catch return false;

        return true;
    }

    /// Stops the server process.
    pub fn stop(self: *ServerInstance, io: Io) void {
        if (self.process) |*proc| {
            _ = proc.kill(io) catch {};
            _ = proc.wait(io) catch {};
            self.process = null;
        }
        assert(self.process == null);
    }

    /// Returns true if the server process is running.
    pub fn isRunning(self: *const ServerInstance) bool {
        return self.process != null;
    }
};

/// Manages multiple server instances.
pub const ServerManager = struct {
    servers: std.ArrayList(ServerInstance) = .{},

    /// Creates a new server manager.
    pub fn init(allocator: Allocator) ServerManager {
        _ = allocator;
        return .{};
    }

    /// Frees resources and stops all servers.
    pub fn deinit(self: *ServerManager, allocator: Allocator, io: Io) void {
        self.stopAll(io);
        self.servers.deinit(allocator);
    }

    /// Starts a new server with the given config.
    pub fn startServer(
        self: *ServerManager,
        allocator: Allocator,
        io: Io,
        config: ServerConfig,
    ) !*ServerInstance {
        var instance: ServerInstance = .init(config);
        try instance.start(allocator, io);
        try instance.waitReady(5000);

        try self.servers.append(allocator, instance);
        return &self.servers.items[self.servers.items.len - 1];
    }

    /// Stops all managed servers.
    pub fn stopAll(self: *ServerManager, io: Io) void {
        for (self.servers.items) |*server| {
            server.stop(io);
        }
    }

    /// Stops a specific server by index.
    pub fn stopServer(self: *ServerManager, index: usize, io: Io) void {
        if (index < self.servers.items.len) {
            self.servers.items[index].stop(io);
        }
    }

    /// Returns the number of managed servers.
    pub fn count(self: *const ServerManager) usize {
        return self.servers.items.len;
    }
};

test "server config defaults" {
    const config: ServerConfig = .{};
    try std.testing.expectEqual(@as(u16, 4222), config.port);
    try std.testing.expect(config.auth_token == null);
    try std.testing.expect(!config.debug);
}

test "server instance init" {
    var instance: ServerInstance = .init(.{ .port = 14222 });
    try std.testing.expectEqual(@as(u16, 14222), instance.config.port);
    try std.testing.expect(!instance.isRunning());
}
