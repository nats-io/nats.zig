//! NATS Test Server
//!
//! Self-contained nats-server for integration testing.
//! Returns by value - each test owns its servers.
//! Use `defer server.deinit(io)` for automatic cleanup.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Configuration for a NATS server instance.
pub const ServerConfig = struct {
    /// Port to listen on.
    port: u16 = 4222,
    /// Optional authentication token.
    auth_token: ?[]const u8 = null,
    /// Optional path to config file (-c option).
    config_file: ?[]const u8 = null,
    /// Enable debug/verbose output.
    debug: bool = false,
    /// Enable JetStream.
    jetstream: bool = false,
};

/// A self-contained NATS server for testing.
/// Returns by value - no pointer stability issues.
/// Call deinit() when done (typically via defer).
pub const TestServer = struct {
    process: ?std.process.Child = null,
    config: ServerConfig,
    port_buf: [8]u8 = undefined,

    /// Starts a test server. Returns owned instance.
    /// Usage: `var server = TestServer.start(...) catch return;`
    ///        `defer server.deinit(io);`
    pub fn start(allocator: Allocator, io: Io, config: ServerConfig) !TestServer {
        assert(config.port > 0);

        var server: TestServer = .{ .config = config };

        const port_str = std.fmt.bufPrint(
            &server.port_buf,
            "{d}",
            .{config.port},
        ) catch unreachable;

        var args: std.ArrayList([]const u8) = .{};
        defer args.deinit(allocator);

        try args.append(allocator, "nats-server");
        try args.append(allocator, "-p");
        try args.append(allocator, port_str);

        if (config.auth_token) |token| {
            try args.append(allocator, "--auth");
            try args.append(allocator, token);
        }

        if (config.config_file) |config_file| {
            try args.append(allocator, "-c");
            try args.append(allocator, config_file);
        }

        if (config.jetstream) {
            try args.append(allocator, "-js");
        }

        if (config.debug) {
            try args.append(allocator, "-DV");
        }

        server.process = try std.process.spawn(io, .{
            .argv = args.items,
            .stdout = .ignore,
            .stderr = .ignore,
        });
        assert(server.process != null);

        // Wait for server to become ready
        try server.waitReady(io, 5000);

        // Give server extra time to fully initialize after port is open
        io.sleep(.fromMilliseconds(500), .awake) catch {};

        return server;
    }

    /// Stops and cleans up. Safe to call multiple times (idempotent).
    /// Typically called via defer: `defer server.deinit(io);`
    pub fn deinit(self: *TestServer, io: Io) void {
        self.stop(io);
    }

    /// Stops the server. Idempotent - safe if already stopped or process died.
    pub fn stop(self: *TestServer, io: Io) void {
        if (self.process) |*proc| {
            std.debug.print(
                "[SERVER] Killing server on port {d}...\n",
                .{self.config.port},
            );
            // Check if process is still alive before killing (avoid ESRCH panic)
            // Signal 0 checks if process exists without sending a signal
            if (proc.id) |pid| {
                // Use linux kill syscall with signal 0 to check if process exists
                const sig_zero: std.os.linux.SIG = @enumFromInt(0);
                const rc = std.os.linux.kill(pid, sig_zero);
                // rc == 0 means process exists, negative means error (e.g., ESRCH)
                const alive = (rc == 0);
                if (alive) {
                    proc.kill(io);
                }
            }
            self.process = null;
            // Give OS time to fully terminate the process and close sockets
            io.sleep(.fromMilliseconds(100), .awake) catch {};
            std.debug.print("[SERVER] Server killed, waited 100ms\n", .{});
        }
    }

    /// Returns true if the server process is running.
    pub fn isRunning(self: *const TestServer) bool {
        return self.process != null;
    }

    /// Waits for the server to become ready by probing the TCP port.
    fn waitReady(self: *TestServer, io: Io, timeout_ms: u32) !void {
        assert(self.process != null);
        const max_attempts = timeout_ms / 50;
        var attempts: u32 = 0;

        while (attempts < max_attempts) : (attempts += 1) {
            if (self.probePort(io)) {
                return;
            }
            io.sleep(.fromMilliseconds(50), .awake) catch {};
        }

        return error.ServerStartTimeout;
    }

    /// Probes if the server port is accepting connections.
    fn probePort(self: *TestServer, io: Io) bool {
        const address = Io.net.IpAddress.parse(
            "127.0.0.1",
            self.config.port,
        ) catch return false;

        const stream = Io.net.IpAddress.connect(address, io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch return false;
        stream.close(io);

        return true;
    }
};

// Legacy aliases for backward compatibility during migration
pub const ServerInstance = TestServer;
pub const ServerManager = struct {
    servers: std.ArrayList(TestServer) = .{},

    /// Max servers expected in any test - pre-allocate to avoid reallocation
    const MAX_SERVERS: usize = 16;

    pub fn init(allocator: Allocator) ServerManager {
        var mgr = ServerManager{};
        // Pre-allocate to prevent reallocation (which invalidates pointers)
        mgr.servers.ensureTotalCapacity(allocator, MAX_SERVERS) catch {};
        return mgr;
    }

    pub fn deinit(self: *ServerManager, allocator: Allocator, io: Io) void {
        self.stopAll(io);
        self.servers.deinit(allocator);
    }

    pub fn startServer(
        self: *ServerManager,
        allocator: Allocator,
        io: Io,
        config: ServerConfig,
    ) !*TestServer {
        const server = try TestServer.start(allocator, io, config);
        try self.servers.ensureTotalCapacity(allocator, self.servers.items.len + 1);
        try self.servers.append(allocator, server);
        return &self.servers.items[self.servers.items.len - 1];
    }

    pub fn stopAll(self: *ServerManager, io: Io) void {
        for (self.servers.items) |*server| {
            server.stop(io);
        }
        io.sleep(.fromMilliseconds(500), .awake) catch {};
    }

    pub fn stopServer(self: *ServerManager, index: usize, io: Io) void {
        if (index < self.servers.items.len) {
            self.servers.items[index].stop(io);
        }
        io.sleep(.fromMilliseconds(500), .awake) catch {};
    }

    pub fn count(self: *const ServerManager) usize {
        return self.servers.items.len;
    }
};

test "server config defaults" {
    const config: ServerConfig = .{};
    try std.testing.expectEqual(@as(u16, 4222), config.port);
    try std.testing.expect(config.auth_token == null);
    try std.testing.expect(config.config_file == null);
    try std.testing.expect(!config.debug);
}

test "test server init" {
    var server: TestServer = .{ .config = .{ .port = 14222 } };
    try std.testing.expectEqual(@as(u16, 14222), server.config.port);
    try std.testing.expect(!server.isRunning());
}
