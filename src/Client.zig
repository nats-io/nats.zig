//! NATS Client
//!
//! High-level client API for connecting to NATS servers.
//! Uses std.Io for native async I/O with concurrent subscription support.
//!
//! Key features:
//! - Dedicated reader task routes messages to per-subscription Io.Queue
//! - Multiple subscriptions can call next() concurrently
//! - Reader task starts automatically on connect
//! - Colorblind async: works blocking or async based on Io implementation
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
const connection = @import("connection.zig");
const State = connection.State;
const pubsub = @import("pubsub.zig");
const subscription_mod = @import("pubsub/subscription.zig");
const memory = @import("memory.zig");
const SidMap = memory.SidMap;

const Client = @This();

/// Message received on a subscription.
///
/// All slices point into a single backing buffer for cache efficiency.
/// Messages are owned and must be freed via deinit(allocator).
///
/// For zero-copy access without allocation overhead, see MessageRef.
pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,
    owned: bool = true,
    /// Single backing buffer (all slices point into this).
    backing_buf: ?[]u8 = null,

    /// Frees message data.
    pub fn deinit(self: *const Message, allocator: Allocator) void {
        if (!self.owned) return;
        if (self.backing_buf) |buf| {
            allocator.free(buf);
            return;
        }
        // Separate allocations (legacy path)
        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply_to) |rt| allocator.free(rt);
        if (self.headers) |h| allocator.free(h);
    }
};

/// Zero-copy message reference (slices borrow from read buffer).
///
/// MUST be consumed before next read operation or converted via toOwned().
/// Slices become invalid after any call that reads from the socket.
/// Use for high-throughput scenarios where allocation overhead matters.
pub const MessageRef = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,

    /// Converts borrowed MessageRef to owned Message by copying all data.
    /// Use when you need to keep the message beyond the current iteration.
    pub fn toOwned(self: MessageRef, allocator: Allocator) !Message {
        const reply_len = if (self.reply_to) |rt| rt.len else 0;
        const hdr_len = if (self.headers) |h| h.len else 0;
        const total = self.subject.len + self.data.len + reply_len + hdr_len;

        assert(self.subject.len > 0);
        assert(self.sid > 0);

        const buf = try allocator.alloc(u8, total);
        errdefer allocator.free(buf);

        var offset: usize = 0;

        @memcpy(buf[offset..][0..self.subject.len], self.subject);
        const subj = buf[offset..][0..self.subject.len];
        offset += self.subject.len;

        @memcpy(buf[offset..][0..self.data.len], self.data);
        const payload = buf[offset..][0..self.data.len];
        offset += self.data.len;

        const reply = if (self.reply_to) |rt| blk: {
            @memcpy(buf[offset..][0..rt.len], rt);
            const slice = buf[offset..][0..rt.len];
            offset += rt.len;
            break :blk slice;
        } else null;

        const hdrs = if (self.headers) |h| blk: {
            @memcpy(buf[offset..][0..h.len], h);
            break :blk buf[offset..][0..h.len];
        } else null;

        return Message{
            .subject = subj,
            .sid = self.sid,
            .reply_to = reply,
            .data = payload,
            .headers = hdrs,
            .owned = true,
            .backing_buf = buf,
        };
    }
};

/// Client connection options.
///
/// All fields have sensible defaults. Common customizations:
/// - name: Client identifier visible in server logs
/// - user/pass or auth_token: Authentication credentials
/// - buffer_size: Increase for large messages (default 256KB)
/// - async_queue_size: Messages buffered per subscription (default 256)
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
    /// Per-subscription queue size.
    async_queue_size: u16 = 256,
    /// Echo messages back to sender (default true).
    echo: bool = true,
    /// Enable message headers support.
    headers: bool = true,
    /// Request no_responders notification for requests.
    no_responders: bool = true,
    /// Require TLS connection.
    tls_required: bool = false,
    /// NKey seed for authentication.
    nkey_seed: ?[]const u8 = null,
    /// JWT for authentication.
    jwt: ?[]const u8 = null,
    /// Read/write buffer size. Must be >= max message size you expect.
    /// Default 256KB is suitable for most workloads. Increase if sending
    /// large messages (NATS max_payload default is 1MB).
    buffer_size: usize = 256 * 1024,
    /// TCP receive buffer size hint. Larger values allow more messages to
    /// queue in the kernel before backpressure kicks in. Default 256KB.
    /// Set to 0 to use system default.
    tcp_rcvbuf: u32 = 256 * 1024,
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

/// Fixed subscription limits.
pub const MAX_SUBSCRIPTIONS: u16 = 256;
pub const SIDMAP_CAPACITY: u32 = 512;

/// Default queue size per subscription.
pub const DEFAULT_QUEUE_SIZE: u16 = 256;

/// Parses a NATS URL like nats://user:pass@host:port
pub fn parseUrl(url: []const u8) error{InvalidUrl}!ParsedUrl {
    if (url.len == 0) return error.InvalidUrl;
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

/// Subscription type alias.
pub const Sub = Subscription;

// Connection state (set at connect time)
io: Io,
stream: net.Stream,
reader: net.Stream.Reader,
writer: net.Stream.Writer,
options: Options,

// Buffers (allocated based on options.buffer_size)
read_buffer: []u8,
write_buffer: []u8,

// Subscription routing (O(1) via SidMap)
sidmap: SidMap,
sidmap_keys: [SIDMAP_CAPACITY]u64,
sidmap_vals: [SIDMAP_CAPACITY]u16,
free_slots: [MAX_SUBSCRIPTIONS]u16,

// Fields with defaults
parser: Parser = .{},
server_info: ?ServerInfo = null,
state: State = .connecting,
pending_toss: usize = 0,
sub_ptrs: [MAX_SUBSCRIPTIONS]?*Sub = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS,
free_count: u16 = MAX_SUBSCRIPTIONS,
next_sid: u64 = 1,
read_mutex: Io.Mutex = .init,
stats: Stats = .{},

/// Connects to a NATS server.
///
/// Arguments:
///     allocator: Allocator for client and buffer memory
///     io: Io interface for async I/O operations
///     url: NATS server URL (e.g., "nats://localhost:4222")
///     opts: Connection options (timeouts, auth, buffer sizes)
///
/// Returns pointer to connected Client. Caller owns and must call deinit().
pub fn connect(
    allocator: Allocator,
    io: Io,
    url: []const u8,
    opts: Options,
) !*Client {
    const parsed = try parseUrl(url);

    const client = try allocator.create(Client);
    // Initialize fields with defaults (allocator.create returns undefined)
    client.server_info = null;
    client.parser = .{};
    client.state = .connecting;
    client.pending_toss = 0;
    client.sub_ptrs = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS;
    client.free_count = MAX_SUBSCRIPTIONS;
    client.next_sid = 1;
    client.read_mutex = .init;
    client.stats = .{};
    errdefer {
        if (client.server_info) |*info| {
            info.deinit(allocator);
        }
        allocator.destroy(client);
    }

    // Parse address
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

    // Set TCP_NODELAY
    const enable: u32 = 1;
    std.posix.setsockopt(
        client.stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.os.linux.TCP.NODELAY,
        std.mem.asBytes(&enable),
    ) catch {};

    // Set TCP receive buffer size for better backpressure handling
    if (opts.tcp_rcvbuf > 0) {
        std.posix.setsockopt(
            client.stream.socket.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.RCVBUF,
            std.mem.asBytes(&opts.tcp_rcvbuf),
        ) catch {};
    }

    // Allocate buffers based on options
    client.read_buffer = allocator.alloc(u8, opts.buffer_size) catch {
        client.stream.close(io);
        allocator.destroy(client);
        return error.OutOfMemory;
    };
    errdefer allocator.free(client.read_buffer);

    client.write_buffer = allocator.alloc(u8, opts.buffer_size) catch {
        allocator.free(client.read_buffer);
        client.stream.close(io);
        allocator.destroy(client);
        return error.OutOfMemory;
    };
    errdefer allocator.free(client.write_buffer);

    // Initialize I/O and state (other fields use struct defaults)
    client.io = io;
    client.reader = client.stream.reader(io, client.read_buffer);
    client.writer = client.stream.writer(io, client.write_buffer);
    client.options = opts;

    // Initialize SidMap and free slot stack
    client.sidmap_keys = undefined;
    client.sidmap_vals = undefined;
    client.sidmap = .init(&client.sidmap_keys, &client.sidmap_vals);
    for (0..MAX_SUBSCRIPTIONS) |i| {
        client.free_slots[i] = @intCast(MAX_SUBSCRIPTIONS - 1 - i);
    }

    // Perform handshake
    try client.handshake(allocator, opts, parsed);

    assert(client.next_sid >= 1);
    assert(client.state == .connected);
    return client;
}

/// Performs NATS handshake (INFO/CONNECT exchange).
fn handshake(
    self: *Client,
    allocator: Allocator,
    opts: Options,
    parsed: ParsedUrl,
) !void {
    assert(self.state == .connecting);
    assert(parsed.host.len > 0);

    const reader = &self.reader.interface;
    const writer = &self.writer.interface;

    // Read INFO from server
    const info_data = reader.peekGreedy(1) catch {
        return error.ConnectionFailed;
    };

    var consumed: usize = 0;
    const cmd = self.parser.parse(allocator, info_data, &consumed) catch {
        return error.ProtocolError;
    };

    assert(consumed <= info_data.len);
    reader.toss(consumed);

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
        .echo = opts.echo,
        .headers = opts.headers,
        .no_responders = opts.no_responders,
        .tls_required = opts.tls_required,
        .jwt = opts.jwt,
        .nkey = opts.nkey_seed,
    };

    protocol.Encoder.encodeConnect(writer, connect_opts) catch {
        return error.EncodingFailed;
    };

    writer.flush() catch {
        return error.WriteFailed;
    };

    // Check for auth rejection
    if (self.server_info.?.auth_required) {
        try self.checkAuthRejection();
    }
}

/// Checks for -ERR auth rejection after CONNECT.
fn checkAuthRejection(self: *Client) !void {
    assert(self.state == .connected);

    const reader = &self.reader.interface;

    // Brief sleep to allow server to respond with -ERR if auth fails
    self.io.sleep(.fromMilliseconds(100), .awake) catch {};

    // Check if any data is buffered (non-blocking peek)
    const buffered = reader.buffered();
    if (buffered.len > 0) {
        if (std.mem.startsWith(u8, buffered, "-ERR")) {
            self.state = .closed;
            return error.AuthorizationViolation;
        }
    }

    // Try a non-blocking peek to see if more data arrived
    const response = reader.peekGreedy(1) catch {
        return;
    };

    if (std.mem.startsWith(u8, response, "-ERR")) {
        self.state = .closed;
        return error.AuthorizationViolation;
    }
}

/// Subscribes to a subject.
///
/// Arguments:
///     allocator: Allocator for subscription resources
///     subject: Subject pattern to subscribe to (wildcards allowed: *, >)
///
/// Returns subscription pointer. Caller must call sub.deinit() when done.
pub fn subscribe(
    self: *Client,
    allocator: Allocator,
    subject: []const u8,
) !*Sub {
    return self.subscribeQueue(allocator, subject, null);
}

/// Subscribes with queue group for load balancing.
///
/// Arguments:
///     allocator: Allocator for subscription resources
///     subject: Subject pattern to subscribe to
///     queue_group: Queue group name (messages distributed among members)
///
/// Queue groups allow multiple subscribers to share the message load.
/// Only one subscriber in the group receives each message.
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
    if (queue_group) |qg| try pubsub.validateQueueGroup(qg);
    assert(self.next_sid >= 1);

    // Allocate slot
    if (self.free_count == 0) {
        return error.TooManySubscriptions;
    }
    self.free_count -= 1;
    const slot_idx = self.free_slots[self.free_count];

    const sid = self.next_sid;
    self.next_sid += 1;

    // Create subscription
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

    // Allocate Io.Queue buffer
    const queue_size = self.options.async_queue_size;
    const queue_buf = try allocator.alloc(Message, queue_size);
    errdefer allocator.free(queue_buf);

    sub.* = .{
        .client = self,
        .sid = sid,
        .subject = owned_subject,
        .queue_group = owned_queue,
        .queue_buf = queue_buf,
        .queue = .init(queue_buf),
        .state = .active,
        .received_msgs = 0,
    };

    // Store in SidMap
    self.sidmap.put(sid, slot_idx) catch {
        // Cleanup - errdefers inactive after sub.* initialization
        allocator.free(queue_buf);
        if (owned_queue) |qg| allocator.free(qg);
        allocator.free(owned_subject);
        allocator.destroy(sub);
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
        // Rollback sidmap and sub_ptrs registration
        _ = self.sidmap.remove(sid);
        self.sub_ptrs[slot_idx] = null;
        self.free_slots[self.free_count] = slot_idx;
        self.free_count += 1;
        // Cleanup allocations
        allocator.free(queue_buf);
        if (owned_queue) |qg| allocator.free(qg);
        allocator.free(owned_subject);
        allocator.destroy(sub);
        return error.EncodingFailed;
    };

    return sub;
}

/// Publishes a message to a subject.
///
/// Arguments:
///     subject: Destination subject (no wildcards allowed)
///     payload: Message data
///
/// Messages are buffered. Call flush() to ensure delivery.
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
    if (self.server_info) |info| {
        if (payload.len > info.max_payload) return error.PayloadTooLarge;
    }

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
    try pubsub.validateReplyTo(reply_to);
    if (self.server_info) |info| {
        if (payload.len > info.max_payload) return error.PayloadTooLarge;
    }

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

/// Flushes pending writes to the server.
pub fn flush(self: *Client) !void {
    if (!self.state.canSend()) {
        return error.NotConnected;
    }
    self.writer.interface.flush() catch {
        return error.WriteFailed;
    };
}

/// Sends a request and waits for a reply with timeout.
///
/// Arguments:
///     allocator: Allocator for response message
///     subject: Request destination subject
///     payload: Request data
///     timeout_ms: Maximum time to wait for reply in milliseconds
///
/// Creates a temporary inbox subscription, sends request with reply-to,
/// and waits for response using io.select(). Returns null on timeout.
pub fn request(
    self: *Client,
    allocator: Allocator,
    subject: []const u8,
    payload: []const u8,
    timeout_ms: u32,
) !?Message {
    assert(subject.len > 0);
    assert(timeout_ms > 0);
    if (!self.state.canSend()) {
        return error.NotConnected;
    }

    // Generate unique inbox for reply
    const inbox = try pubsub.newInbox(allocator, self.io);
    defer allocator.free(inbox);

    // Subscribe to inbox (temporary subscription)
    const sub = try self.subscribe(allocator, inbox);
    defer sub.deinit(allocator);

    // Flush subscription registration before publishing
    try self.flush();

    // Brief delay to ensure server has registered subscription
    self.io.sleep(.fromMilliseconds(5), .awake) catch {};

    // Publish request with reply-to
    try self.publishRequest(subject, inbox, payload);
    try self.flush();

    // Wait for reply using io.select()
    var response_future = self.io.async(
        Subscription.next,
        .{ sub, allocator, self.io },
    );

    var timeout_future = self.io.async(
        sleepForRequest,
        .{ self.io, timeout_ms },
    );

    const select_result = self.io.select(.{
        .response = &response_future,
        .timeout = &timeout_future,
    }) catch |err| {
        timeout_future.cancel(self.io);
        if (response_future.cancel(self.io)) |msg| {
            msg.deinit(allocator);
        } else |_| {}
        if (err == error.Canceled) return null;
        return err;
    };

    switch (select_result) {
        .response => |msg_result| {
            timeout_future.cancel(self.io);
            return msg_result catch |err| {
                if (err == error.Canceled or err == error.Closed) {
                    return null;
                }
                return err;
            };
        },
        .timeout => {
            if (response_future.cancel(self.io)) |msg| {
                msg.deinit(allocator);
            } else |_| {}
            return null;
        },
    }
}

/// Helper for request timeout.
fn sleepForRequest(io: Io, timeout_ms: u32) void {
    io.sleep(.fromMilliseconds(timeout_ms), .awake) catch {};
}

/// Gracefully drains subscriptions and closes the connection.
///
/// Arguments:
///     alloc: Allocator used for subscription cleanup
///
/// Unsubscribes all active subscriptions, drains remaining messages,
/// and closes the connection. Use for graceful shutdown.
pub fn drain(self: *Client, alloc: Allocator) !void {
    if (self.state != .connected) {
        return error.NotConnected;
    }
    assert(self.next_sid >= 1);

    // Acquire mutex for subscription cleanup (prevents races with next())
    self.read_mutex.lockUncancelable(self.io);

    // Unsubscribe all active subscriptions
    for (self.sub_ptrs, 0..) |maybe_sub, slot_idx| {
        if (maybe_sub) |sub| {
            // Buffer UNSUB command (no I/O yet)
            protocol.Encoder.encodeUnsub(&self.writer.interface, .{
                .sid = sub.sid,
                .max_msgs = null,
            }) catch {};

            // Close queue and clear from data structures
            sub.queue.close(self.io);
            _ = self.sidmap.remove(sub.sid);
            self.sub_ptrs[slot_idx] = null;
            self.free_slots[self.free_count] = @intCast(slot_idx);
            self.free_count += 1;

            // Drain remaining messages from queue (in-memory, no socket I/O)
            var drain_buf: [1]Message = undefined;
            while (true) {
                const n = sub.queue.get(self.io, &drain_buf, 0) catch break;
                if (n == 0) break;
                drain_buf[0].deinit(alloc);
            }

            // Mark as drained - sub.deinit() frees resources
            sub.client_destroyed = true;
        }
    }

    self.read_mutex.unlock(self.io);

    // I/O operations after mutex released
    self.writer.interface.flush() catch {};
    self.state = .draining;

    self.stream.close(self.io);
    self.state = .closed;

    if (self.server_info) |*info| {
        info.deinit(alloc);
        self.server_info = null;
    }
}

/// Returns true if connected.
pub fn isConnected(self: *const Client) bool {
    assert(self.next_sid >= 1);
    return self.state == .connected;
}

/// Returns connection statistics.
pub fn getStats(self: *const Client) Stats {
    assert(self.next_sid >= 1);
    return self.stats;
}

/// Returns server info.
pub fn getServerInfo(self: *const Client) ?*const ServerInfo {
    assert(self.next_sid >= 1);
    if (self.server_info) |*info| {
        return info;
    }
    return null;
}

/// Get subscription by SID.
pub fn getSubscriptionBySid(self: *Client, sid: u64) ?*Sub {
    assert(sid > 0);
    if (self.sidmap.get(sid)) |slot_idx| {
        return self.sub_ptrs[slot_idx];
    }
    return null;
}

/// Unsubscribes by SID.
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

    if (self.sidmap.get(sid)) |slot_idx| {
        self.sub_ptrs[slot_idx] = null;
        _ = self.sidmap.remove(sid);
        self.free_slots[self.free_count] = slot_idx;
        self.free_count += 1;
    }
}

/// Toss bytes consumed from previous read.
pub fn tossPending(self: *Client) void {
    assert(self.pending_toss <= self.read_buffer.len);
    if (self.pending_toss > 0) {
        self.reader.interface.toss(self.pending_toss);
        self.pending_toss = 0;
    }
}

/// Routes a single message from socket to subscription queues.
pub fn routeNextMessage(
    self: *Client,
    allocator: Allocator,
) !bool {
    // Check state instead of asserting - allows graceful disconnect handling
    if (!self.state.canReceive()) {
        return error.NotConnected;
    }

    self.tossPending();

    const reader = &self.reader.interface;

    while (true) {
        var data = reader.buffered();

        if (data.len == 0) {
            reader.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream, error.ReadFailed => {
                        self.state = .disconnected;
                        return false;
                    },
                }
            };

            data = reader.buffered();
            if (data.len == 0) return false;
        }

        var consumed: usize = 0;
        const cmd = self.parser.parse(allocator, data, &consumed) catch {
            return error.ProtocolError;
        };

        if (cmd) |c| {
            switch (c) {
                .ping => {
                    reader.toss(consumed);
                    try self.sendPong();
                },
                .pong, .ok => {
                    reader.toss(consumed);
                },
                .err => {
                    reader.toss(consumed);
                },
                .info => |new_info| {
                    reader.toss(consumed);
                    if (self.server_info) |*info| {
                        info.deinit(allocator);
                    }
                    self.server_info = new_info;
                },
                .msg => |args| {
                    // Validate payload size against server max_payload
                    if (self.server_info) |info| {
                        if (args.payload_len > info.max_payload) {
                            reader.toss(consumed);
                            continue;
                        }
                    }

                    self.stats.msgs_in += 1;
                    self.stats.bytes_in += args.payload.len;

                    if (self.getSubscriptionBySid(args.sid)) |sub| {
                        // Allocate-and-queue delivery
                        const reply_len = if (args.reply_to) |rt|
                            rt.len
                        else
                            0;
                        const total = args.subject.len +
                            args.payload.len + reply_len;

                        const buf = allocator.alloc(u8, total) catch {
                            reader.toss(consumed);
                            continue;
                        };

                        @memcpy(buf[0..args.subject.len], args.subject);
                        const subj = buf[0..args.subject.len];

                        const data_start = args.subject.len;
                        const data_end = data_start + args.payload.len;
                        @memcpy(buf[data_start..data_end], args.payload);
                        const payload = buf[data_start..data_end];

                        const reply = if (args.reply_to) |rt| blk: {
                            @memcpy(buf[data_end..], rt);
                            break :blk buf[data_end..][0..rt.len];
                        } else null;

                        const msg = Message{
                            .subject = subj,
                            .sid = args.sid,
                            .reply_to = reply,
                            .data = payload,
                            .headers = null,
                            .owned = true,
                            .backing_buf = buf,
                        };

                        sub.pushMessage(msg) catch {
                            allocator.free(buf);
                            sub.dropped_msgs += 1;
                        };

                        sub.received_msgs += 1;
                    }
                    reader.toss(consumed);
                    return true;
                },
                .hmsg => |args| {
                    // Validate total content size against server max_payload
                    if (self.server_info) |info| {
                        if (args.total_len > info.max_payload) {
                            reader.toss(consumed);
                            continue;
                        }
                    }

                    self.stats.msgs_in += 1;
                    self.stats.bytes_in +=
                        args.payload.len + args.headers.len;

                    if (self.getSubscriptionBySid(args.sid)) |sub| {
                        // Allocate-and-queue delivery
                        const reply_len = if (args.reply_to) |rt|
                            rt.len
                        else
                            0;
                        const hdrs_len = args.headers.len;
                        const total = args.subject.len +
                            args.payload.len + reply_len + hdrs_len;

                        const buf = allocator.alloc(u8, total) catch {
                            reader.toss(consumed);
                            continue;
                        };

                        var offset: usize = 0;

                        @memcpy(
                            buf[offset..][0..args.subject.len],
                            args.subject,
                        );
                        const subj = buf[offset..][0..args.subject.len];
                        offset += args.subject.len;

                        @memcpy(
                            buf[offset..][0..args.payload.len],
                            args.payload,
                        );
                        const payload = buf[offset..][0..args.payload.len];
                        offset += args.payload.len;

                        const reply = if (args.reply_to) |rt| blk: {
                            @memcpy(buf[offset..][0..rt.len], rt);
                            const slice = buf[offset..][0..rt.len];
                            offset += rt.len;
                            break :blk slice;
                        } else null;

                        const hdrs = if (hdrs_len > 0) blk: {
                            @memcpy(
                                buf[offset..][0..hdrs_len],
                                args.headers,
                            );
                            break :blk buf[offset..][0..hdrs_len];
                        } else null;

                        const msg = Message{
                            .subject = subj,
                            .sid = args.sid,
                            .reply_to = reply,
                            .data = payload,
                            .headers = hdrs,
                            .owned = true,
                            .backing_buf = buf,
                        };

                        sub.pushMessage(msg) catch {
                            allocator.free(buf);
                            sub.dropped_msgs += 1;
                        };

                        sub.received_msgs += 1;
                    }
                    reader.toss(consumed);
                    return true;
                },
            }
        } else {
            // Check if buffer is full before trying to read more.
            // Full buffer with unparseable data = protocol error.
            const buffered_len = reader.buffered().len;
            if (buffered_len >= self.read_buffer.len) {
                return error.ProtocolError;
            }
            reader.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream, error.ReadFailed => {
                        self.state = .disconnected;
                        return false;
                    },
                }
            };
        }
    }
}

/// Routes next message as zero-copy MessageRef (no allocation).
/// Returns null if no message available or connection closed.
/// Caller must consume MessageRef before next call (slices borrow buffer).
pub fn routeNextMessageRef(self: *Client, allocator: Allocator) !?MessageRef {
    if (!self.state.canReceive()) {
        return error.NotConnected;
    }

    self.tossPending();

    const reader = &self.reader.interface;

    while (true) {
        var data = reader.buffered();

        if (data.len == 0) {
            reader.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream, error.ReadFailed => {
                        self.state = .disconnected;
                        return null;
                    },
                }
            };

            data = reader.buffered();
            if (data.len == 0) return null;
        }

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
                    self.pending_toss = consumed;
                    try self.sendPong();
                    self.tossPending();
                },
                .pong, .ok => {
                    self.pending_toss = consumed;
                    self.tossPending();
                },
                .err => {
                    self.pending_toss = consumed;
                    self.tossPending();
                },
                .info => |new_info| {
                    self.pending_toss = consumed;
                    if (self.server_info) |*info| {
                        info.deinit(allocator);
                    }
                    self.server_info = new_info;
                    self.tossPending();
                },
                .msg => |args| {
                    // Validate payload size against server max_payload
                    if (self.server_info) |info| {
                        if (args.payload_len > info.max_payload) {
                            reader.toss(consumed);
                            continue;
                        }
                    }

                    self.stats.msgs_in += 1;
                    self.stats.bytes_in += args.payload.len;
                    self.pending_toss = consumed;

                    assert(args.subject.len > 0);
                    assert(args.sid > 0);

                    return MessageRef{
                        .subject = args.subject,
                        .sid = args.sid,
                        .reply_to = args.reply_to,
                        .data = args.payload,
                        .headers = null,
                    };
                },
                .hmsg => |args| {
                    // Validate total content size against server max_payload
                    if (self.server_info) |info| {
                        if (args.total_len > info.max_payload) {
                            reader.toss(consumed);
                            continue;
                        }
                    }

                    self.stats.msgs_in += 1;
                    self.stats.bytes_in +=
                        args.payload.len + args.headers.len;
                    self.pending_toss = consumed;

                    assert(args.subject.len > 0);
                    assert(args.sid > 0);

                    return MessageRef{
                        .subject = args.subject,
                        .sid = args.sid,
                        .reply_to = args.reply_to,
                        .data = args.payload,
                        .headers = args.headers,
                    };
                },
            }
        } else {
            const buffered_len = reader.buffered().len;
            if (buffered_len >= self.read_buffer.len) {
                return error.ProtocolError;
            }
            reader.fillMore() catch |err| {
                switch (err) {
                    error.EndOfStream, error.ReadFailed => {
                        self.state = .disconnected;
                        return null;
                    },
                }
            };
        }
    }
}

/// Sends PONG response.
fn sendPong(self: *Client) !void {
    assert(self.state.canSend());
    const writer = &self.writer.interface;
    writer.writeAll("PONG\r\n") catch {
        return error.WriteFailed;
    };
    writer.flush() catch {
        return error.WriteFailed;
    };
}

/// Closes all subscription queues (wakes waiters with error).
pub fn closeAllQueues(self: *Client) void {
    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            sub.queue.close(self.io);
        }
    }
}

/// Closes the connection and frees all resources.
///
/// Arguments:
///     alloc: Allocator used at connect() time
///
/// Drains if connected, closes all subscription queues, frees buffers.
/// Safe to call multiple times.
pub fn deinit(self: *Client, alloc: Allocator) void {
    assert(self.next_sid >= 1);

    // Drain if connected (drain handles its own mutex)
    if (self.state == .connected) {
        self.drain(alloc) catch {};
    } else {
        // Not connected - need mutex for queue/sub cleanup
        self.read_mutex.lockUncancelable(self.io);
        self.closeAllQueues();
        for (self.sub_ptrs) |maybe_sub| {
            if (maybe_sub) |sub| {
                sub.client_destroyed = true;
            }
        }
        self.read_mutex.unlock(self.io);
    }

    // Close connection if not already closed by drain
    if (self.state != .closed) {
        self.stream.close(self.io);
        self.state = .closed;
    }

    // Free client resources
    if (self.server_info) |*info| {
        info.deinit(alloc);
    }
    alloc.free(self.read_buffer);
    alloc.free(self.write_buffer);
    alloc.destroy(self);
}

/// Subscription state.
pub const SubscriptionState = enum {
    active,
    draining,
    unsubscribed,
};

/// Subscription with Io.Queue for async message delivery.
///
/// Supports multiple concurrent consumers via inline routing:
/// - First subscriber to call next() reads from socket
/// - Messages for other subscriptions are routed to their queues
/// - Io.Mutex ensures only one reader at a time
///
/// Use next() for blocking receive, nextWithTimeout() for bounded waits,
/// nextRef() for zero-copy access, or tryNext() for non-blocking poll.
pub const Subscription = struct {
    client: *Client,
    sid: u64,
    subject: []const u8,
    queue_group: ?[]const u8,
    queue_buf: []Message,
    queue: Io.Queue(Message),
    state: SubscriptionState,
    received_msgs: u64,
    dropped_msgs: u64 = 0,
    client_destroyed: bool = false,

    /// Blocks until a message is available or connection is closed.
    ///
    /// Arguments:
    ///     allocator: Allocator for owned message copy
    ///     io: Io interface for blocking operations
    ///
    /// Uses inline routing: acquires read mutex, reads from socket, routes
    /// other subscriptions' messages to their queues. Returns owned Message
    /// that caller must free via msg.deinit(allocator).
    pub fn next(self: *Subscription, allocator: Allocator, io: Io) !Message {
        assert(self.state == .active or self.state == .draining);

        // 1. Check our queue first (routed by other subscriptions)
        if (self.tryNext()) |msg| {
            return msg;
        }

        // 2. Acquire read mutex (only one reader at a time)
        try self.client.read_mutex.lock(io);
        defer self.client.read_mutex.unlock(io);

        // 3. Double-check queue (might have been filled while waiting)
        if (self.tryNext()) |msg| {
            return msg;
        }

        // 4. Read and route until we get our message
        while (true) {
            const ref = try self.client.routeNextMessageRef(allocator) orelse {
                return error.Closed;
            };

            if (ref.sid == self.sid) {
                // Convert to owned and return
                const owned = try ref.toOwned(allocator);
                self.client.tossPending();
                self.received_msgs += 1;
                return owned;
            }

            // Route to other subscription's queue
            if (self.client.getSubscriptionBySid(ref.sid)) |other_sub| {
                const owned = ref.toOwned(allocator) catch {
                    self.client.tossPending();
                    continue;
                };
                other_sub.pushMessage(owned) catch {
                    // Queue full - message dropped
                    other_sub.dropped_msgs += 1;
                    owned.deinit(allocator);
                };
                other_sub.received_msgs += 1;
            }
            self.client.tossPending();
        }
    }

    /// Try receive without blocking. Returns null if no message available.
    pub fn tryNext(self: *Subscription) ?Message {
        var buf: [1]Message = undefined;
        const n = self.queue.get(self.client.io, &buf, 0) catch return null;
        if (n == 0) return null;
        return buf[0];
    }

    /// Returns next message as zero-copy MessageRef.
    /// Slices borrow from read buffer - valid until next nextRef() call.
    /// Uses inline routing with mutex protection.
    /// Use with: while (try sub.nextRef(allocator, io)) |msg| { ... }
    pub fn nextRef(
        self: *Subscription,
        allocator: Allocator,
        io: Io,
    ) !?MessageRef {
        assert(self.state == .active or self.state == .draining);

        // Acquire read mutex (only one reader at a time)
        try self.client.read_mutex.lock(io);
        defer self.client.read_mutex.unlock(io);

        while (true) {
            const ref = try self.client.routeNextMessageRef(allocator) orelse {
                return null;
            };

            if (ref.sid == self.sid) {
                // Zero-copy return! Slices borrow from read buffer.
                self.received_msgs += 1;
                return ref;
            }

            // Message for different subscription - route to its queue
            if (self.client.getSubscriptionBySid(ref.sid)) |other_sub| {
                const owned = ref.toOwned(allocator) catch {
                    self.client.tossPending();
                    continue;
                };
                other_sub.pushMessage(owned) catch {
                    owned.deinit(allocator);
                };
                other_sub.received_msgs += 1;
            }
            self.client.tossPending();
        }
    }

    /// Returns next message as zero-copy MessageRef, blocking up to timeout_ms.
    /// Returns null on timeout or connection close.
    pub fn nextRefBlock(
        self: *Subscription,
        allocator: Allocator,
        io: Io,
        timeout_ms: u32,
    ) !?MessageRef {
        assert(self.state == .active or self.state == .draining);
        assert(timeout_ms > 0);

        const start = std.time.Instant.now() catch {
            return try self.nextRef(allocator, io);
        };
        const timeout_ns = @as(u64, timeout_ms) * 1_000_000;

        while (true) {
            const now = std.time.Instant.now() catch return null;
            if (now.since(start) >= timeout_ns) {
                return null;
            }

            if (try self.nextRef(allocator, io)) |ref| {
                return ref;
            }
        }
    }

    /// Batch receive - waits for at least 1, returns up to buf.len.
    pub fn nextBatch(self: *Subscription, io: Io, buf: []Message) !usize {
        assert(self.state == .active or self.state == .draining);
        assert(buf.len > 0);
        return self.queue.get(io, buf, 1);
    }

    /// Non-blocking batch receive.
    pub fn tryNextBatch(self: *Subscription, buf: []Message) usize {
        return self.queue.get(self.client.io, buf, 0) catch 0;
    }

    /// Receive with timeout using io.select().
    ///
    /// Arguments:
    ///     allocator: Allocator for message
    ///     timeout_ms: Maximum wait time in milliseconds
    ///
    /// Returns null on timeout. Uses async select for efficient waiting.
    pub fn nextWithTimeout(
        self: *Subscription,
        allocator: Allocator,
        timeout_ms: u32,
    ) !?Message {
        assert(self.state == .active or self.state == .draining);
        assert(timeout_ms > 0);

        const io = self.client.io;

        var response_future = io.async(next, .{ self, allocator, io });
        var timeout_future = io.async(
            Client.sleepForRequest,
            .{ io, timeout_ms },
        );

        const select_result = io.select(.{
            .response = &response_future,
            .timeout = &timeout_future,
        }) catch |err| {
            timeout_future.cancel(io);
            if (response_future.cancel(io)) |msg| {
                msg.deinit(allocator);
            } else |_| {}
            if (err == error.Canceled) return null;
            return err;
        };

        switch (select_result) {
            .response => |msg_result| {
                timeout_future.cancel(io);
                return msg_result catch |err| {
                    if (err == error.Canceled or err == error.Closed) {
                        return null;
                    }
                    return err;
                };
            },
            .timeout => {
                if (response_future.cancel(io)) |msg| {
                    msg.deinit(allocator);
                } else |_| {}
                return null;
            },
        }
    }

    /// Returns queue capacity.
    pub fn capacity(self: *Subscription) usize {
        return self.queue.capacity();
    }

    /// Returns count of messages dropped due to queue overflow.
    /// Only incremented when other subscriptions route messages to this one
    /// and the queue is full. The reading subscription bypasses its queue.
    pub fn getDroppedCount(self: *Subscription) u64 {
        return self.dropped_msgs;
    }

    /// Push message to queue (called by reader task).
    pub fn pushMessage(self: *Subscription, msg: Message) !void {
        const n = self.queue.put(
            self.client.io,
            &.{msg},
            0,
        ) catch return error.QueueFull;
        if (n == 0) return error.QueueFull;
    }

    /// Unsubscribe from the subject.
    pub fn unsubscribe(self: *Subscription) !void {
        if (self.state == .unsubscribed) return;
        self.state = .unsubscribed;
        self.queue.close(self.client.io);
        try self.client.unsubscribeSid(self.sid);
    }

    /// Closes the subscription and frees resources.
    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        // Skip client access if client already destroyed/drained
        if (!self.client_destroyed) {
            // Close queue first (wakes any waiters)
            self.queue.close(self.client.io);

            // Acquire mutex for shared state modification
            self.client.read_mutex.lockUncancelable(self.client.io);

            if (self.state != .unsubscribed) {
                self.client.unsubscribeSid(self.sid) catch {};
            }

            if (self.client.sidmap.get(self.sid)) |slot_idx| {
                self.client.sub_ptrs[slot_idx] = null;
                _ = self.client.sidmap.remove(self.sid);
                self.client.free_slots[self.client.free_count] = slot_idx;
                self.client.free_count += 1;
            }

            self.client.read_mutex.unlock(self.client.io);

            // Drain remaining messages (no mutex - queue is closed)
            var drain_buf: [1]Message = undefined;
            while (true) {
                const n = self.queue.get(
                    self.client.io,
                    &drain_buf,
                    0,
                ) catch break;
                if (n == 0) break;
                drain_buf[0].deinit(allocator);
            }
        }

        // Always free local resources
        allocator.free(self.queue_buf);
        allocator.free(self.subject);
        if (self.queue_group) |qg| {
            allocator.free(qg);
        }
        allocator.destroy(self);
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
    try std.testing.expectEqual(DEFAULT_QUEUE_SIZE, opts.async_queue_size);
}

test "stats defaults" {
    const stats: Stats = .{};
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_in);
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_out);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_in);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_out);
    try std.testing.expectEqual(@as(u32, 0), stats.reconnects);
}
