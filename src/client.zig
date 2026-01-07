//! NATS Client
//!
//! High-level client API for connecting to NATS servers.
//! Provides publish, subscribe, and request/reply functionality.
//!
//! Connection-scoped: Io, Reader, Writer stored for lifetime of connection.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

const protocol = @import("protocol.zig");
const Parser = protocol.Parser;
const ServerInfo = protocol.ServerInfo;
const OwnedServerInfo = protocol.OwnedServerInfo;

const connection = @import("connection.zig");
const State = connection.State;
const Event = connection.Event;
const EventQueue = connection.EventQueue;

const pubsub = @import("pubsub.zig");
const subscription_mod = @import("pubsub/subscription.zig");
const sync = @import("sync.zig");
const memory = @import("memory.zig");
const SidMap = memory.SidMap;

/// Client connection options.
pub const Options = struct {
    /// Client name for identification.
    name: ?[]const u8 = null,
    /// Enable verbose mode.
    verbose: bool = false,
    /// Enable pedantic mode.
    pedantic: bool = false,
    /// Username for auth.
    user: ?[]const u8 = null,
    /// Password for auth.
    pass: ?[]const u8 = null,
    /// Auth token.
    auth_token: ?[]const u8 = null,
    /// Connection timeout in nanoseconds.
    connect_timeout_ns: u64 = 5_000_000_000,
    /// Per-subscription async queue size (for ClientAsync).
    async_queue_size: u16 = 256,
};

/// Connection statistics (Go client parity).
pub const Stats = struct {
    /// Total messages received.
    msgs_in: u64 = 0,
    /// Total messages sent.
    msgs_out: u64 = 0,
    /// Total bytes received.
    bytes_in: u64 = 0,
    /// Total bytes sent.
    bytes_out: u64 = 0,
    /// Number of reconnects.
    reconnects: u32 = 0,
};

/// Parse result for NATS URL.
pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8,
    pass: ?[]const u8,
};

/// Parses a NATS URL like nats://user:pass@host:port
pub fn parseUrl(url: []const u8) error{InvalidUrl}!ParsedUrl {
    assert(url.len > 0);
    var remaining = url;

    // Strip nats:// prefix
    if (std.mem.startsWith(u8, remaining, "nats://")) {
        remaining = remaining[7..];
    }

    var user: ?[]const u8 = null;
    var pass: ?[]const u8 = null;

    // Check for user:pass@
    if (std.mem.indexOf(u8, remaining, "@")) |at_pos| {
        const auth = remaining[0..at_pos];
        remaining = remaining[at_pos + 1 ..];

        if (std.mem.indexOf(u8, auth, ":")) |colon_pos| {
            user = auth[0..colon_pos];
            pass = auth[colon_pos + 1 ..];
        } else {
            user = auth;
        }
    }

    // Parse host:port
    var host: []const u8 = undefined;
    var port: u16 = 4222;

    if (std.mem.indexOf(u8, remaining, ":")) |colon_pos| {
        host = remaining[0..colon_pos];
        port = std.fmt.parseInt(u16, remaining[colon_pos + 1 ..], 10) catch {
            return error.InvalidUrl;
        };
    } else {
        host = remaining;
    }

    if (host.len == 0) return error.InvalidUrl;

    assert(host.len > 0);
    assert(port > 0);
    return .{
        .host = host,
        .port = port,
        .user = user,
        .pass = pass,
    };
}

/// Fixed subscription limits.
pub const MAX_SUBSCRIPTIONS: u16 = 256;
pub const SIDMAP_CAPACITY: u32 = 512; // 2x for load factor

/// NATS Client for pub/sub messaging.
///
/// Connection-scoped: Io, Reader, Writer stored for connection lifetime.
pub const Client = struct {
    /// Subscription type instantiated with Client.
    pub const Sub = subscription_mod.Subscription(Client);

    /// Direct message result - zero-copy slices into read buffer.
    pub const DirectMsg = struct {
        subject: []const u8,
        sid: u64,
        reply_to: ?[]const u8,
        data: []const u8,
        headers: ?[]const u8,
        consumed: usize,
    };

    // Connection-scoped: stored for lifetime
    io: Io,
    stream: net.Stream,
    reader: net.Stream.Reader,
    writer: net.Stream.Writer,

    parser: Parser,
    server_info: ?OwnedServerInfo,
    events: EventQueue,
    next_sid: u64,
    state: State,
    read_buffer: [32768]u8,
    write_buffer: [32768]u8,
    pending_toss: usize,

    // Pre-allocated subscription routing
    sidmap: SidMap,
    sidmap_keys: [SIDMAP_CAPACITY]u64,
    sidmap_vals: [SIDMAP_CAPACITY]u16,
    sub_ptrs: [MAX_SUBSCRIPTIONS]?*Sub,
    free_slots: [MAX_SUBSCRIPTIONS]u16,
    free_count: u16,

    // Connection statistics
    stats: Stats,

    /// Connects to a NATS server.
    /// URL format: nats://[user:pass@]host[:port]
    /// Io stored for connection lifetime.
    pub fn connect(
        allocator: Allocator,
        io: Io,
        url: []const u8,
        opts: Options,
    ) !*Client {
        assert(url.len > 0);
        const parsed = try parseUrl(url);

        // Allocate client
        const client = try allocator.create(Client);
        // Initialize server_info early so errdefer can check it safely
        client.server_info = null;
        errdefer {
            // Free server_info if allocated during failed handshake
            if (client.server_info) |*info| {
                info.deinit(allocator);
            }
            allocator.destroy(client);
        }

        // Parse address (handle localhost specially)
        const host = if (std.mem.eql(u8, parsed.host, "localhost"))
            "127.0.0.1"
        else
            parsed.host;

        const address = net.IpAddress.parse(host, parsed.port) catch {
            return error.InvalidAddress;
        };

        // Connect
        client.stream = net.IpAddress.connect(address, io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch {
            return error.ConnectionFailed;
        };

        // Set TCP_NODELAY to disable Nagle's algorithm for low latency
        const enable: u32 = 1;
        std.posix.setsockopt(
            client.stream.socket.handle,
            std.posix.IPPROTO.TCP,
            std.os.linux.TCP.NODELAY,
            std.mem.asBytes(&enable),
        ) catch {};

        // Initialize buffers
        client.read_buffer = undefined;
        client.write_buffer = undefined;

        // Store Io and create Reader/Writer once for connection lifetime
        client.io = io;
        client.reader = client.stream.reader(io, &client.read_buffer);
        client.writer = client.stream.writer(io, &client.write_buffer);

        // Initialize state
        client.parser = .{};
        client.events = .{};
        client.next_sid = 1;
        client.state = .connecting;
        client.pending_toss = 0;

        // Initialize SidMap and free slot stack
        client.sidmap_keys = undefined;
        client.sidmap_vals = undefined;
        client.sidmap = SidMap.init(&client.sidmap_keys, &client.sidmap_vals);
        client.sub_ptrs = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS;
        // Initialize free slot stack (all slots available)
        for (0..MAX_SUBSCRIPTIONS) |i| {
            client.free_slots[i] = @intCast(MAX_SUBSCRIPTIONS - 1 - i);
        }
        client.free_count = MAX_SUBSCRIPTIONS;
        client.stats = .{};

        // Perform handshake
        try client.handshake(allocator, opts, parsed);

        assert(client.next_sid >= 1);
        assert(client.state == .connected);
        return client;
    }

    fn handshake(
        self: *Client,
        allocator: Allocator,
        opts: Options,
        parsed: ParsedUrl,
    ) !void {
        assert(self.state == .connecting);
        assert(parsed.host.len > 0);

        // Read INFO from server (use stored reader)
        const info_data = self.reader.interface.peekGreedy(1) catch {
            return error.ConnectionFailed;
        };

        // Parse INFO command
        var consumed: usize = 0;
        const cmd = self.parser.parse(allocator, info_data, &consumed) catch {
            return error.ProtocolError;
        };

        // Consume parsed data from buffer
        assert(consumed <= info_data.len);
        self.reader.interface.toss(consumed);

        if (cmd) |c| {
            switch (c) {
                .info => |parsed_info| {
                    self.server_info = parsed_info;
                    self.state = .connected;
                },
                else => return error.UnexpectedCommand,
            }
        } else {
            return error.NoInfoReceived;
        }

        // Send CONNECT
        const pass = opts.pass orelse parsed.pass;
        var user = opts.user orelse parsed.user;
        var auth_token = opts.auth_token;

        // If URL has user but no pass, treat as token auth
        if (parsed.user != null and parsed.pass == null and opts.user == null) {
            auth_token = parsed.user;
            user = null;
        }

        const connect_opts = protocol.ConnectOptions{
            .verbose = opts.verbose,
            .pedantic = opts.pedantic,
            .name = opts.name,
            .user = user,
            .pass = pass,
            .auth_token = auth_token,
            .lang = "zig",
            .version = "0.1.0",
            .protocol = 1,
        };

        protocol.Encoder.encodeConnect(
            &self.writer.interface,
            connect_opts,
        ) catch {
            return error.EncodingFailed;
        };

        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };

        // Check for auth rejection from server
        if (self.server_info.?.auth_required) {
            try self.checkAuthRejection();
        }
    }

    /// Checks for -ERR auth rejection after CONNECT.
    fn checkAuthRejection(self: *Client) !void {
        assert(self.state == .connected);

        // Poll for incoming data with 250ms timeout
        var poll_fds = [_]std.posix.pollfd{.{
            .fd = self.stream.socket.handle,
            .events = std.posix.POLL.IN,
            .revents = 0,
        }};

        const poll_result = std.posix.poll(&poll_fds, 250) catch {
            return;
        };

        if (poll_result == 0) return;

        if (poll_fds[0].revents & std.posix.POLL.IN != 0) {
            const response = self.reader.interface.peekGreedy(1) catch {
                self.state = .closed;
                return error.AuthorizationViolation;
            };

            if (std.mem.startsWith(u8, response, "-ERR")) {
                self.state = .closed;
                return error.AuthorizationViolation;
            }
        }
    }

    /// Closes the connection and frees resources.
    pub fn deinit(self: *Client, allocator: Allocator) void {
        assert(self.next_sid >= 1);

        // Skip close if already closed by drain()
        if (self.state != .closed) {
            self.stream.close(self.io);
        }

        // Clean up subscriptions via slot array
        for (self.sub_ptrs) |maybe_sub| {
            if (maybe_sub) |sub| {
                sub.messages.close();
                sub.messages.deinit(allocator);
                allocator.free(sub.subject);
                if (sub.queue_group) |qg| {
                    allocator.free(qg);
                }
                allocator.destroy(sub);
            }
        }

        if (self.server_info) |*info| {
            info.deinit(allocator);
        }
        allocator.destroy(self);
    }

    /// Publishes a message to a subject.
    pub fn publish(
        self: *Client,
        subject: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = null,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };

        self.stats.msgs_out += 1;
        self.stats.bytes_out += payload.len;
    }

    /// Publishes with a reply-to subject.
    pub fn publishRequest(
        self: *Client,
        subject: []const u8,
        reply_to: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        assert(reply_to.len > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = reply_to,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };

        self.stats.msgs_out += 1;
        self.stats.bytes_out += payload.len;
    }

    /// Sends request and waits for reply (Go client parity).
    pub fn request(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
        payload: []const u8,
        timeout_ms: u32,
    ) !?DirectMsg {
        assert(subject.len > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }

        // Generate unique inbox for reply
        const inbox = try pubsub.newInbox(allocator);
        defer allocator.free(inbox);

        // Subscribe to inbox
        const sub = try self.subscribe(allocator, inbox);
        defer sub.deinit(allocator);

        // Publish with reply-to
        try self.publishRequest(subject, inbox, payload);
        try self.flush();

        // Wait for reply with timeout using pollDirect
        return self.pollDirect(allocator, timeout_ms);
    }

    /// Flushes pending writes with timeout (Go client parity).
    pub fn flushWithTimeout(self: *Client, timeout_ms: u32) !void {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        assert(timeout_ms > 0);

        const start = std.time.Instant.now() catch {
            return error.TimerUnavailable;
        };
        const timeout_ns: u64 = @as(u64, timeout_ms) * std.time.ns_per_ms;

        // Try to flush
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };

        // Verify with PING/PONG roundtrip
        try self.ping();

        // Flush after ping
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };

        // Wait for PONG response
        while (true) {
            const now = std.time.Instant.now() catch {
                return error.TimerUnavailable;
            };
            const elapsed = now.since(start);
            if (elapsed >= timeout_ns) return error.Timeout;

            const remaining_ns = timeout_ns - elapsed;
            const remaining_ms: u32 = @intCast(remaining_ns / std.time.ns_per_ms);

            // Poll for response
            var poll_fds = [_]std.posix.pollfd{.{
                .fd = self.stream.socket.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            }};

            const ready = std.posix.poll(&poll_fds, @intCast(remaining_ms)) catch {
                return error.PollFailed;
            };

            if (ready == 0) return error.Timeout;

            // Read and check for PONG
            self.reader.interface.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream, error.ReadFailed => {
                        self.state = .disconnected;
                        return error.Disconnected;
                    },
                }
            };

            const data = self.reader.interface.buffered();
            if (std.mem.indexOf(u8, data, "PONG\r\n")) |pos| {
                self.reader.interface.toss(pos + 6);
                return;
            }
        }
    }

    /// Async flush - returns Future for true async/await.
    /// Usage:
    ///   var future = client.flushAsync();
    ///   defer future.cancel(io) catch {};
    ///   try future.await(io);
    pub fn flushAsync(self: *Client) std.Io.Future(anyerror!void) {
        return self.io.async(asyncFlushImpl, .{ self.io, self });
    }

    /// Internal async flush implementation.
    fn asyncFlushImpl(io: std.Io, self: *Client) anyerror!void {
        _ = io; // Captured for cancellation
        try self.flush();
    }

    /// Async request/reply - returns Future for true async/await.
    /// Usage:
    ///   var future = client.requestAsync(allocator, "service", "data", 5000);
    ///   defer if (future.cancel(io)) |m| {
    ///       if (m) |msg| msg.deinit(allocator);
    ///   } else |_| {};
    ///   if (try future.await(io)) |reply| { ... }
    pub fn requestAsync(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
        payload: []const u8,
        timeout_ms: u32,
    ) std.Io.Future(anyerror!?DirectMsg) {
        return self.io.async(asyncRequestImpl, .{
            self.io,
            self,
            allocator,
            subject,
            payload,
            timeout_ms,
        });
    }

    /// Internal async request implementation.
    fn asyncRequestImpl(
        io: std.Io,
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
        payload: []const u8,
        timeout_ms: u32,
    ) anyerror!?DirectMsg {
        _ = io; // Captured for cancellation
        return self.request(allocator, subject, payload, timeout_ms);
    }

    /// Gracefully drains subscriptions and closes (Go client parity).
    pub fn drain(self: *Client, allocator: Allocator) !void {
        if (self.state != .connected) {
            return error.NotConnected;
        }

        // Unsubscribe all active subscriptions
        for (self.sub_ptrs, 0..) |maybe_sub, slot_idx| {
            if (maybe_sub) |sub| {
                // Send UNSUB
                protocol.Encoder.encodeUnsub(&self.writer.interface, .{
                    .sid = sub.sid,
                    .max_msgs = null,
                }) catch {};

                // Remove from SidMap
                _ = self.sidmap.remove(sub.sid);
                self.sub_ptrs[slot_idx] = null;
                self.free_slots[self.free_count] = @intCast(slot_idx);
                self.free_count += 1;

                // Clean up subscription
                sub.messages.close();
                sub.messages.deinit(allocator);
                allocator.free(sub.subject);
                if (sub.queue_group) |qg| allocator.free(qg);
                allocator.destroy(sub);
            }
        }

        // Flush remaining writes
        self.writer.interface.flush() catch {};

        // Update state
        self.state = .draining;

        // Close connection
        self.stream.close(self.io);
        self.state = .closed;

        // Free server_info to prevent leak
        if (self.server_info) |*info| {
            info.deinit(allocator);
            self.server_info = null;
        }
    }

    /// Returns connection statistics (Go client parity).
    pub fn getStats(self: *const Client) Stats {
        assert(self.next_sid >= 1);
        return self.stats;
    }

    /// Subscribes to a subject. Returns subscription for Go-style polling.
    pub fn subscribe(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
    ) !*Sub {
        return self.subscribeQueue(allocator, subject, null);
    }

    /// Subscribes to a subject with queue group.
    pub fn subscribeQueue(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
        queue_group: ?[]const u8,
    ) !*Sub {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validateSubscribe(subject);
        assert(self.next_sid >= 1);

        // Allocate slot from free stack
        if (self.free_count == 0) {
            return error.TooManySubscriptions;
        }
        self.free_count -= 1;
        const slot_idx = self.free_slots[self.free_count];

        const sid = self.next_sid;
        self.next_sid += 1;

        // Create subscription with its own message queue
        const sub = try allocator.create(Sub);
        errdefer {
            allocator.destroy(sub);
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
        }

        const owned_subject = try allocator.dupe(u8, subject);
        errdefer allocator.free(owned_subject);

        const owned_queue = if (queue_group) |qg|
            try allocator.dupe(u8, qg)
        else
            null;
        errdefer if (owned_queue) |qg| allocator.free(qg);

        // Message queue (4096 capacity)
        const msg_queue = try sync.ThreadSafeQueue(
            subscription_mod.Message,
        ).init(allocator, 4096);

        sub.* = .{
            .client = self,
            .sid = sid,
            .subject = owned_subject,
            .queue_group = owned_queue,
            .messages = msg_queue,
            .state = .active,
            .max_msgs = 0,
            .received_msgs = 0,
        };

        // Store in SidMap and slot array
        self.sidmap.put(sid, slot_idx) catch {
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
            return error.TooManySubscriptions;
        };
        self.sub_ptrs[slot_idx] = sub;

        // Send SUB command
        protocol.Encoder.encodeSub(&self.writer.interface, .{
            .subject = subject,
            .queue_group = queue_group,
            .sid = sid,
        }) catch {
            return error.EncodingFailed;
        };

        return sub;
    }

    /// Unsubscribes by SID (called by Subscription.unsubscribe).
    pub fn unsubscribeSid(self: *Client, sid: u64) !void {
        assert(sid > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }

        protocol.Encoder.encodeUnsub(&self.writer.interface, .{
            .sid = sid,
            .max_msgs = null,
        }) catch {
            return error.EncodingFailed;
        };

        // Remove from SidMap and release slot
        if (self.sidmap.get(sid)) |slot_idx| {
            self.sub_ptrs[slot_idx] = null;
            _ = self.sidmap.remove(sid);
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
        }
    }

    /// Get subscription by SID using O(1) SidMap lookup.
    pub fn getSubscriptionBySid(self: *Client, sid: u64) ?*Sub {
        assert(sid > 0);
        if (self.sidmap.get(sid)) |slot_idx| {
            return self.sub_ptrs[slot_idx];
        }
        return null;
    }

    /// Unsubscribes and cleans up a subscription.
    pub fn unsubscribe(self: *Client, allocator: Allocator, sub: *Sub) !void {
        assert(sub.sid > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }

        try self.unsubscribeSid(sub.sid);
        sub.deinit(allocator);
    }

    /// Flushes pending writes to the server.
    pub fn flush(self: *Client) !void {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }

    /// Sends PING to server.
    pub fn ping(self: *Client) !void {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        self.writer.interface.writeAll("PING\r\n") catch {
            return error.WriteFailed;
        };
    }

    /// Returns true if connected.
    pub fn isConnected(self: *const Client) bool {
        assert(self.next_sid >= 1);
        return self.state == .connected;
    }

    /// Returns server info received during handshake.
    pub fn getServerInfo(self: *const Client) ?*const OwnedServerInfo {
        assert(self.next_sid >= 1);
        if (self.server_info) |*info| {
            return info;
        }
        return null;
    }

    /// Polls for incoming data and processes commands.
    pub fn poll(
        self: *Client,
        allocator: Allocator,
        timeout_ms: ?u32,
    ) !bool {
        assert(self.state.canReceive());

        // First check what's already buffered (non-blocking)
        var data = self.reader.interface.buffered();

        // If buffer empty, wait for socket to be readable
        if (data.len == 0) {
            var poll_fds = [_]std.posix.pollfd{.{
                .fd = self.stream.socket.handle,
                .events = std.posix.POLL.IN,
                .revents = 0,
            }};

            const timeout: i32 = if (timeout_ms) |ms|
                @intCast(ms)
            else
                -1;

            const ready = std.posix.poll(&poll_fds, timeout) catch {
                return error.PollFailed;
            };

            if (ready == 0) return false; // Timeout

            // Read more data into buffer
            self.reader.interface.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream => {
                        self.state = .disconnected;
                        return false;
                    },
                    error.ReadFailed => {
                        self.state = .disconnected;
                        return false;
                    },
                }
            };

            data = self.reader.interface.buffered();
            if (data.len == 0) return false;
        }

        // Parse and handle all complete commands
        var total_consumed: usize = 0;
        while (total_consumed < data.len) {
            var consumed: usize = 0;
            const remaining = data[total_consumed..];

            const cmd = self.parser.parse(
                allocator,
                remaining,
                &consumed,
            ) catch {
                return error.ProtocolError;
            };

            total_consumed += consumed;

            if (cmd) |c| {
                try self.handleCommand(allocator, c);
            } else {
                // Parser needs more data - try to read more
                if (total_consumed > 0) {
                    self.reader.interface.toss(total_consumed);
                    total_consumed = 0;
                }
                self.reader.interface.fillMore() catch |err| {
                    switch (err) {
                        error.EndOfStream => {
                            self.state = .disconnected;
                            break;
                        },
                        error.ReadFailed => {
                            self.state = .disconnected;
                            break;
                        },
                    }
                };
                data = self.reader.interface.buffered();
                if (data.len == 0) break;
            }
        }

        // Consume processed data from buffer
        if (total_consumed > 0) {
            self.reader.interface.toss(total_consumed);
        }

        return total_consumed > 0;
    }

    /// Toss bytes that were consumed in previous pollDirect() call.
    pub fn tossPending(self: *Client) void {
        assert(self.pending_toss <= self.read_buffer.len);
        if (self.pending_toss > 0) {
            self.reader.interface.toss(self.pending_toss);
            self.pending_toss = 0;
        }
    }

    /// Polls for a single message (zero-copy).
    pub fn pollDirect(
        self: *Client,
        allocator: Allocator,
        timeout_ms: ?u32,
    ) !?DirectMsg {
        assert(self.state.canReceive());

        // Toss previously consumed bytes
        self.tossPending();

        // Setup timeout tracking
        const has_timeout = timeout_ms != null;
        const start: std.time.Instant = if (has_timeout)
            std.time.Instant.now() catch return error.TimerUnavailable
        else
            undefined;
        const timeout_ns: u64 = if (timeout_ms) |ms|
            @as(u64, ms) * std.time.ns_per_ms
        else
            0;

        while (true) {
            // Get buffered data
            var data = self.reader.interface.buffered();

            // If buffer empty, wait for socket
            if (data.len == 0) {
                const remaining_ms: ?u32 = if (has_timeout) blk: {
                    const now = std.time.Instant.now() catch {
                        return error.TimerUnavailable;
                    };
                    const elapsed = now.since(start);
                    if (elapsed >= timeout_ns) return null;
                    const ns_per_ms = std.time.ns_per_ms;
                    const remaining = (timeout_ns - elapsed) / ns_per_ms;
                    break :blk @intCast(remaining);
                } else null;

                var poll_fds = [_]std.posix.pollfd{.{
                    .fd = self.stream.socket.handle,
                    .events = std.posix.POLL.IN,
                    .revents = 0,
                }};

                const timeout_i32: i32 = if (remaining_ms) |ms|
                    @intCast(ms)
                else
                    -1;

                const ready = std.posix.poll(&poll_fds, timeout_i32) catch {
                    return error.PollFailed;
                };

                if (ready == 0) return null;

                self.reader.interface.fillMore() catch |err| {
                    switch (err) {
                        error.EndOfStream => {
                            self.state = .disconnected;
                            return null;
                        },
                        error.ReadFailed => {
                            self.state = .disconnected;
                            return null;
                        },
                    }
                };

                data = self.reader.interface.buffered();
                if (data.len == 0) return null;
            }

            // Parse one command
            var consumed: usize = 0;
            const cmd = self.parser.parse(
                allocator,
                data,
                &consumed,
            ) catch {
                return error.ProtocolError;
            };

            if (cmd) |c| {
                switch (c) {
                    .ping => {
                        self.reader.interface.toss(consumed);
                        try self.sendPong();
                    },
                    .pong, .ok => {
                        self.reader.interface.toss(consumed);
                    },
                    .err => |msg| {
                        self.reader.interface.toss(consumed);
                        _ = self.events.push(.{ .server_error = msg });
                    },
                    .info => |new_info| {
                        self.reader.interface.toss(consumed);
                        if (self.server_info) |*info| {
                            info.deinit(std.heap.page_allocator);
                        }
                        self.server_info = new_info;
                    },
                    .msg => |args| {
                        self.stats.msgs_in += 1;
                        self.stats.bytes_in += args.payload.len;

                        self.pending_toss = consumed;
                        return .{
                            .subject = args.subject,
                            .sid = args.sid,
                            .reply_to = args.reply_to,
                            .data = args.payload,
                            .headers = null,
                            .consumed = consumed,
                        };
                    },
                    .hmsg => |args| {
                        self.stats.msgs_in += 1;
                        self.stats.bytes_in += args.payload.len + args.headers.len;

                        self.pending_toss = consumed;
                        return .{
                            .subject = args.subject,
                            .sid = args.sid,
                            .reply_to = args.reply_to,
                            .data = args.payload,
                            .headers = if (args.headers.len > 0)
                                args.headers
                            else
                                null,
                            .consumed = consumed,
                        };
                    },
                }
            } else {
                // Need more data - read more
                self.reader.interface.fillMore() catch |err| {
                    switch (err) {
                        error.EndOfStream => {
                            self.state = .disconnected;
                            return null;
                        },
                        error.ReadFailed => {
                            self.state = .disconnected;
                            return null;
                        },
                    }
                };
            }
        }
    }

    /// Returns the next event from the event queue.
    pub fn nextEvent(self: *Client) ?Event {
        assert(self.next_sid >= 1);
        return self.events.pop();
    }

    /// Handles a parsed server command.
    fn handleCommand(
        self: *Client,
        allocator: Allocator,
        cmd: protocol.ServerCommand,
    ) !void {
        assert(self.state.canReceive());
        switch (cmd) {
            .ping => try self.sendPong(),
            .pong => {},
            .ok => {},
            .err => |msg| {
                _ = self.events.push(.{ .server_error = msg });
            },
            .msg => |args| try self.handleMessage(allocator, args),
            .hmsg => |args| try self.handleHMessage(allocator, args),
            .info => |new_info| {
                if (self.server_info) |*info| {
                    info.deinit(std.heap.page_allocator);
                }
                self.server_info = new_info;
            },
        }
    }

    /// Handles MSG command - routes message to subscription queue.
    fn handleMessage(
        self: *Client,
        allocator: Allocator,
        args: protocol.MsgArgs,
    ) !void {
        assert(args.subject.len > 0);
        assert(args.sid > 0);

        self.stats.msgs_in += 1;
        self.stats.bytes_in += args.payload.len;

        if (self.getSubscriptionBySid(args.sid)) |sub| {
            const alloc = allocator;

            const subject = try alloc.dupe(u8, args.subject);
            errdefer alloc.free(subject);

            const data = try alloc.dupe(u8, args.payload);
            errdefer alloc.free(data);

            const reply_to = if (args.reply_to) |rt|
                try alloc.dupe(u8, rt)
            else
                null;
            sub.messages.push(.{
                .subject = subject,
                .sid = args.sid,
                .reply_to = reply_to,
                .data = data,
                .headers = null,
                .owned = true,
            }) catch {
                alloc.free(subject);
                alloc.free(data);
                if (reply_to) |rt| alloc.free(rt);
            };
        }
    }

    /// Handles HMSG command - routes message with headers to subscription.
    fn handleHMessage(
        self: *Client,
        allocator: Allocator,
        args: protocol.HMsgArgs,
    ) !void {
        assert(args.subject.len > 0);
        assert(args.sid > 0);

        self.stats.msgs_in += 1;
        self.stats.bytes_in += args.payload.len + args.headers.len;

        if (self.getSubscriptionBySid(args.sid)) |sub| {
            const alloc = allocator;

            const subject = try alloc.dupe(u8, args.subject);
            errdefer alloc.free(subject);

            const data = try alloc.dupe(u8, args.payload);
            errdefer alloc.free(data);

            const reply_to = if (args.reply_to) |rt|
                try alloc.dupe(u8, rt)
            else
                null;
            errdefer if (reply_to) |rt| alloc.free(rt);

            const headers = if (args.headers.len > 0)
                try alloc.dupe(u8, args.headers)
            else
                null;

            sub.messages.push(.{
                .subject = subject,
                .sid = args.sid,
                .reply_to = reply_to,
                .data = data,
                .headers = headers,
                .owned = true,
            }) catch {
                alloc.free(subject);
                alloc.free(data);
                if (reply_to) |rt| alloc.free(rt);
                if (headers) |h| alloc.free(h);
            };
        }
    }

    /// Sends PONG response to server PING.
    fn sendPong(self: *Client) !void {
        assert(self.state.canSend());
        self.writer.interface.writeAll("PONG\r\n") catch {
            return error.WriteFailed;
        };
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }
};

test "parse url" {
    {
        const parsed = try parseUrl("nats://localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(parsed.user == null);
    }

    {
        const parsed = try parseUrl("nats://user:pass@localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expectEqualSlices(u8, "user", parsed.user.?);
        try std.testing.expectEqualSlices(u8, "pass", parsed.pass.?);
    }

    {
        const parsed = try parseUrl("localhost");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
    }

    {
        const parsed = try parseUrl("127.0.0.1:4223");
        try std.testing.expectEqualSlices(u8, "127.0.0.1", parsed.host);
        try std.testing.expectEqual(@as(u16, 4223), parsed.port);
    }
}

test "parse url with user only" {
    const parsed = try parseUrl("nats://admin@localhost:4222");
    try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
    try std.testing.expectEqualSlices(u8, "admin", parsed.user.?);
    try std.testing.expect(parsed.pass == null);
}

test "parse url invalid" {
    try std.testing.expectError(error.InvalidUrl, parseUrl("nats://"));
    try std.testing.expectError(error.InvalidUrl, parseUrl("nats://:4222"));
}

test "parse url default port" {
    const parsed = try parseUrl("nats://myserver");
    try std.testing.expectEqualSlices(u8, "myserver", parsed.host);
    try std.testing.expectEqual(@as(u16, 4222), parsed.port);
}

test "options defaults" {
    const opts: Options = .{};
    try std.testing.expect(opts.name == null);
    try std.testing.expect(!opts.verbose);
    try std.testing.expect(!opts.pedantic);
    try std.testing.expect(opts.user == null);
    try std.testing.expect(opts.pass == null);
    const expected_timeout: u64 = 5_000_000_000;
    try std.testing.expectEqual(expected_timeout, opts.connect_timeout_ns);
}

test "stats defaults" {
    const stats: Stats = .{};
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_in);
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_out);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_in);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_out);
    try std.testing.expectEqual(@as(u32, 0), stats.reconnects);
}
