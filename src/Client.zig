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
const TieredSlab = memory.TieredSlab;
const SpscQueue = @import("sync/spsc_queue.zig").SpscQueue;
const dbg = @import("dbg.zig");
const defaults = @import("defaults.zig");

const Client = @This();

/// Gets current time in nanoseconds (Zig 0.16 compatible).
fn getNowNs() error{TimerUnavailable}!u64 {
    const instant = std.time.Instant.now() catch return error.TimerUnavailable;
    const secs: u64 = @intCast(instant.timestamp.sec);
    const nsecs: u64 = @intCast(instant.timestamp.nsec);
    return secs * std.time.ns_per_s + nsecs;
}

/// Message received on a subscription.
///
/// All slices point into a single backing buffer for cache efficiency.
/// Call deinit() to free - allocator param kept for API compat but ignored.
pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,
    owned: bool = true,
    /// Single backing buffer (all slices point into this).
    backing_buf: ?[]u8 = null,
    /// Return queue for thread-safe deallocation (reader thread frees).
    return_queue: ?*SpscQueue([]u8) = null,

    /// Frees message data. Pushes to return queue for slab-allocated msgs.
    pub fn deinit(self: *const Message, allocator: Allocator) void {
        if (!self.owned) return;
        if (self.backing_buf) |buf| {
            if (self.return_queue) |rq| {
                // Push to return queue - reader thread will free to slab
                _ = rq.push(buf);
            } else {
                allocator.free(buf);
            }
            return;
        }
        // Separate allocations (legacy path)
        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply_to) |rt| allocator.free(rt);
        if (self.headers) |h| allocator.free(h);
    }
};

/// Client connection options.
///
/// All fields have sensible defaults. Common customizations:
/// - name: Client identifier visible in server logs
/// - user/pass or auth_token: Authentication credentials
/// - buffer_size: Increase for large messages (default 256KB)
/// - sub_queue_size: Messages buffered per subscription (default 1024)
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
    connect_timeout_ns: u64 = defaults.Connection.timeout_ns,
    /// Per-subscription queue size (messages buffered before dropping).
    sub_queue_size: u32 = defaults.Memory.queue_size.value(),
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
    buffer_size: usize = defaults.Connection.buffer_size,
    /// TCP receive buffer size hint. Larger values allow more messages to
    /// queue in the kernel before backpressure kicks in. Default 256KB.
    /// Set to 0 to use system default.
    tcp_rcvbuf: u32 = defaults.Connection.tcp_rcvbuf,

    // === RECONNECTION OPTIONS ===

    /// Enable automatic reconnection on disconnect.
    reconnect: bool = defaults.Reconnection.enabled,
    /// Maximum reconnection attempts (0 = infinite).
    max_reconnect_attempts: u32 = defaults.Reconnection.max_attempts,
    /// Initial wait between reconnect attempts (ms).
    reconnect_wait_ms: u32 = defaults.Reconnection.wait_ms,
    /// Maximum wait with exponential backoff (ms).
    reconnect_wait_max_ms: u32 = defaults.Reconnection.wait_max_ms,
    /// Jitter percentage for backoff (0-50).
    reconnect_jitter_percent: u8 = defaults.Reconnection.jitter_percent,
    /// Discover servers from INFO connect_urls.
    discover_servers: bool = defaults.Reconnection.discover_servers,
    /// Size of pending buffer for publishes during reconnect.
    /// Set to 0 to disable buffering (publish returns error during reconnect).
    pending_buffer_size: usize = defaults.Reconnection.pending_buffer_size,

    // === PING/PONG HEALTH CHECK ===

    /// Interval between client-initiated PINGs (ms). 0 = disable.
    ping_interval_ms: u32 = defaults.Connection.ping_interval_ms,
    /// Max outstanding PINGs before connection is considered stale.
    max_pings_outstanding: u8 = defaults.Connection.max_pings_outstanding,
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

/// Subscription backup for restoration after reconnect.
/// Stores essential subscription state with inline buffers (no allocation).
pub const SubBackup = struct {
    sid: u64 = 0,
    subject_buf: [256]u8 = undefined,
    subject_len: u8 = 0,
    queue_group_buf: [64]u8 = undefined,
    queue_group_len: u8 = 0,

    /// Get subject as slice.
    pub fn getSubject(self: *const SubBackup) []const u8 {
        return self.subject_buf[0..self.subject_len];
    }

    /// Get queue group as optional slice.
    pub fn getQueueGroup(self: *const SubBackup) ?[]const u8 {
        if (self.queue_group_len == 0) return null;
        return self.queue_group_buf[0..self.queue_group_len];
    }
};

/// Result of drain operation for visibility into cleanup quality.
pub const DrainResult = struct {
    /// Count of UNSUB commands that failed to encode.
    unsub_failures: u16 = 0,
    /// True if final flush failed (data may not have reached server).
    flush_failed: bool = false,
};

/// Subscribe command data (used by restoreSubscriptions).
pub const SubscribeCmd = struct {
    sid: u64,
    subject: []const u8,
    queue_group: ?[]const u8,
};

/// Parse result for NATS URL.
pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8,
    pass: ?[]const u8,
};

/// Fixed subscription limits (from defaults.zig).
pub const MAX_SUBSCRIPTIONS: u16 = defaults.Client.max_subscriptions;
pub const SIDMAP_CAPACITY: u32 = defaults.Client.sidmap_capacity;

/// Default queue size per subscription (messages buffered before dropping).
pub const DEFAULT_QUEUE_SIZE: u32 = defaults.Memory.queue_size.value();

// Compile-time validation of capacity constraints
comptime {
    assert(SIDMAP_CAPACITY >= MAX_SUBSCRIPTIONS);
}

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
sub_ptrs: [MAX_SUBSCRIPTIONS]?*Sub = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS,
free_count: u16 = MAX_SUBSCRIPTIONS,
next_sid: u64 = 1,
read_mutex: Io.Mutex = .init,
stats: Stats = .{},

// Connection diagnostics
tcp_nodelay_set: bool = false,
tcp_rcvbuf_set: bool = false,

// Fast path cache for single-subscription case
cached_sub: ?*Sub = null,

// Cached max_payload from server_info (avoids optional unwrap in hot path)
max_payload: usize = 1024 * 1024,

// Slab allocator for message buffers (~26 MB pre-allocated)
tiered_slab: TieredSlab = undefined,

// Return queue for cross-thread buffer deallocation (main -> reader thread)
// Main thread pushes used buffers here, io_task drains and frees to slab
return_queue: SpscQueue([]u8) = undefined,
return_queue_buf: [][]u8 = undefined,

// Reconnection state
server_pool: connection.ServerPool = undefined,
server_pool_initialized: bool = false,
sub_backups: [MAX_SUBSCRIPTIONS]SubBackup = [_]SubBackup{.{}} ** MAX_SUBSCRIPTIONS,
sub_backup_count: u16 = 0,
reconnect_attempt: u32 = 0,
original_url: [256]u8 = undefined,
original_url_len: u8 = 0,

// Pending buffer for publishes during reconnect
pending_buffer: ?[]u8 = null,
pending_buffer_pos: usize = 0,
pending_buffer_capacity: usize = 0,

// PING/PONG health check state
last_ping_sent_ns: u64 = 0,
last_pong_received_ns: u64 = 0,
pings_outstanding: u8 = 0,

// Background I/O task infrastructure
write_mutex: Io.Mutex = .init,
/// Future for background I/O task (for proper cancellation in deinit).
io_task_future: ?Io.Future(void) = null,

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
    client.sub_ptrs = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS;
    client.free_count = MAX_SUBSCRIPTIONS;
    client.next_sid = 1;
    client.read_mutex = .init;
    client.stats = .{};
    client.cached_sub = null;
    client.max_payload = 1024 * 1024;

    // Initialize reconnection state
    client.server_pool = undefined;
    client.server_pool_initialized = false;
    client.sub_backups = [_]SubBackup{.{}} ** MAX_SUBSCRIPTIONS;
    client.sub_backup_count = 0;
    client.reconnect_attempt = 0;
    client.original_url = undefined;
    client.original_url_len = 0;

    // Initialize pending buffer state
    client.pending_buffer = null;
    client.pending_buffer_pos = 0;
    client.pending_buffer_capacity = 0;

    // Initialize health check state
    client.last_ping_sent_ns = 0;
    client.last_pong_received_ns = 0;
    client.pings_outstanding = 0;

    // Initialize background I/O task infrastructure
    client.write_mutex = .init;

    // Initialize slab allocator (critical for O(1) message allocation)
    client.tiered_slab = TieredSlab.init(allocator) catch |err| {
        allocator.destroy(client);
        return err;
    };

    // Initialize return queue for cross-thread buffer deallocation
    // Size matches sub_queue_size to handle max in-flight messages
    const rq_size = opts.sub_queue_size;
    client.return_queue_buf = allocator.alloc([]u8, rq_size) catch |err| {
        client.tiered_slab.deinit();
        allocator.destroy(client);
        return err;
    };
    client.return_queue = SpscQueue([]u8).init(client.return_queue_buf);

    errdefer {
        allocator.free(client.return_queue_buf);
        client.tiered_slab.deinit();
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
    errdefer client.stream.close(io);

    // Set TCP_NODELAY (track success for diagnostics)
    const enable: u32 = 1;
    client.tcp_nodelay_set = true;
    std.posix.setsockopt(
        client.stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.os.linux.TCP.NODELAY,
        std.mem.asBytes(&enable),
    ) catch {
        client.tcp_nodelay_set = false;
    };

    // Set TCP receive buffer size for better backpressure handling
    client.tcp_rcvbuf_set = opts.tcp_rcvbuf > 0;
    if (opts.tcp_rcvbuf > 0) {
        std.posix.setsockopt(
            client.stream.socket.handle,
            std.posix.SOL.SOCKET,
            std.posix.SO.RCVBUF,
            std.mem.asBytes(&opts.tcp_rcvbuf),
        ) catch {
            client.tcp_rcvbuf_set = false;
        };
    }

    // Allocate buffers based on options
    client.read_buffer = allocator.alloc(u8, opts.buffer_size) catch {
        return error.OutOfMemory;
    };
    errdefer allocator.free(client.read_buffer);

    client.write_buffer = allocator.alloc(u8, opts.buffer_size) catch {
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

    // Store original URL for reconnection
    const url_len: u8 = @intCast(@min(url.len, 256));
    @memcpy(client.original_url[0..url_len], url[0..url_len]);
    client.original_url_len = url_len;

    // Initialize server pool with primary URL
    client.server_pool = connection.ServerPool.init(url) catch {
        return error.InvalidUrl;
    };
    client.server_pool_initialized = true;

    // Add discovered servers from INFO connect_urls
    if (opts.discover_servers) {
        if (client.server_info) |info| {
            client.server_pool.addFromConnectUrls(
                &info.connect_urls,
                &info.connect_urls_lens,
                info.connect_urls_count,
            );
        }
    }

    // Initialize pending buffer for reconnect
    try client.initPendingBuffer(allocator);

    // Initialize health check timestamps
    const now_ns = getNowNs() catch 0;
    client.last_ping_sent_ns = now_ns;
    client.last_pong_received_ns = now_ns;

    // Start background I/O task for message routing and keepalive
    // MUST use concurrent() for true parallelism - async() may not schedule
    // the task until the main thread yields, causing deadlock on flush()
    client.io_task_future = io.concurrent(
        connection.io_task.run,
        .{ client, allocator },
    ) catch blk: {
        dbg.print("WARNING: concurrent() failed, using async()", .{});
        break :blk io.async(connection.io_task.run, .{ client, allocator });
    };

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
                self.max_payload = parsed_info.max_payload;
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

/// Cleanup subscription resources after failed registration.
/// Inline to avoid function call overhead in error path.
inline fn cleanupFailedSub(
    self: *Client,
    sub: *Sub,
    allocator: Allocator,
    slot_idx: u16,
    queue_buf: []Message,
    owned_queue: ?[]const u8,
    owned_subject: []const u8,
    remove_from_sidmap: bool,
) void {
    if (remove_from_sidmap) {
        _ = self.sidmap.remove(sub.sid);
        self.sub_ptrs[slot_idx] = null;
    }
    if (self.cached_sub == sub) self.cached_sub = null;
    self.free_slots[self.free_count] = slot_idx;
    self.free_count += 1;
    allocator.free(queue_buf);
    if (owned_queue) |qg| allocator.free(qg);
    allocator.free(owned_subject);
    allocator.destroy(sub);
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
    const queue_size = self.options.sub_queue_size;
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
        self.cleanupFailedSub(
            sub,
            allocator,
            slot_idx,
            queue_buf,
            owned_queue,
            owned_subject,
            false,
        );
        return error.TooManySubscriptions;
    };
    self.sub_ptrs[slot_idx] = sub;
    self.cached_sub = sub;

    // Send SUB command
    const writer = &self.writer.interface;
    protocol.Encoder.encodeSub(writer, .{
        .subject = subject,
        .queue_group = queue_group,
        .sid = sid,
    }) catch {
        self.cleanupFailedSub(
            sub,
            allocator,
            slot_idx,
            queue_buf,
            owned_queue,
            owned_subject,
            true,
        );
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
/// Thread-safe: protected by write_mutex for concurrent publish.
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
    if (payload.len > self.max_payload) return error.PayloadTooLarge;

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = &self.writer.interface;
    protocol.Encoder.encodePub(writer, .{
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
/// Thread-safe: protected by write_mutex for concurrent publish.
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
    if (payload.len > self.max_payload) return error.PayloadTooLarge;

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = &self.writer.interface;
    protocol.Encoder.encodePub(writer, .{
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
///
/// Sends all buffered data to the TCP socket. This is a simple TCP flush
/// without PING/PONG verification - for maximum performance.
pub fn flush(self: *Client, allocator: Allocator) !void {
    _ = allocator;
    if (!self.state.canSend()) {
        return error.NotConnected;
    }

    // Flush write buffer under mutex to sync with other writers
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);
    self.writer.interface.flush() catch return error.WriteFailed;
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
    try self.flush(allocator);

    // Brief delay to ensure server has registered subscription
    self.io.sleep(.fromMilliseconds(5), .awake) catch {};

    // Publish request with reply-to
    try self.publishRequest(subject, inbox, payload);
    try self.flush(allocator);

    // Wait for reply using io.select()
    var response_future = self.io.async(
        Subscription.next,
        .{ sub, allocator, self.io },
    );
    var timeout_future = self.io.async(
        sleepForRequest,
        .{ self.io, timeout_ms },
    );

    // Winner-tracking pattern: defer cleanup for non-winners
    var winner: enum { none, response, timeout } = .none;

    defer if (winner != .response) {
        if (response_future.cancel(self.io)) |msg| {
            msg.deinit(allocator);
        } else |_| {}
    };
    defer if (winner != .timeout) {
        timeout_future.cancel(self.io);
    };

    const select_result = self.io.select(.{
        .response = &response_future,
        .timeout = &timeout_future,
    }) catch |err| {
        if (err == error.Canceled) return null;
        return err;
    };

    switch (select_result) {
        .response => |msg_result| {
            winner = .response;
            return msg_result catch |err| {
                if (err == error.Canceled or err == error.Closed) {
                    return null;
                }
                return err;
            };
        },
        .timeout => {
            winner = .timeout;
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
/// Returns DrainResult indicating any failures during cleanup.
/// Unsubscribes all active subscriptions, drains remaining messages,
/// and closes the connection. Use for graceful shutdown.
pub fn drain(self: *Client, alloc: Allocator) !DrainResult {
    if (self.state != .connected) {
        return error.NotConnected;
    }
    assert(self.next_sid >= 1);

    var result: DrainResult = .{};
    const writer = &self.writer.interface;

    // Acquire mutex for subscription cleanup (prevents races with next())
    self.read_mutex.lockUncancelable(self.io);

    // Unsubscribe all active subscriptions
    for (self.sub_ptrs, 0..) |maybe_sub, slot_idx| {
        if (maybe_sub) |sub| {
            // Buffer UNSUB command (no I/O yet)
            protocol.Encoder.encodeUnsub(writer, .{
                .sid = sub.sid,
                .max_msgs = null,
            }) catch {
                result.unsub_failures += 1;
            };

            // Close queue and clear from data structures
            sub.queue.close(self.io);
            _ = self.sidmap.remove(sub.sid);
            self.sub_ptrs[slot_idx] = null;
            if (self.cached_sub == sub) self.cached_sub = null;
            self.free_slots[self.free_count] = @intCast(slot_idx);
            self.free_count += 1;

            // Drain remaining messages from queue (in-memory, no socket I/O)
            var drain_buf: [1]Message = undefined;
            while (true) {
                const n = sub.queue.popBatch(&drain_buf);
                if (n == 0) break;
                drain_buf[0].deinit(alloc);
            }

            // Mark as drained - sub.deinit() frees resources
            sub.client_destroyed = true;
        }
    }

    self.read_mutex.unlock(self.io);

    // I/O operations after mutex released
    writer.flush() catch {
        result.flush_failed = true;
    };
    self.state = .draining;

    self.stream.close(self.io);
    self.state = .closed;

    if (self.server_info) |*info| {
        info.deinit(alloc);
        self.server_info = null;
    }

    return result;
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

/// Returns true if TCP_NODELAY was successfully set.
pub fn isTcpNoDelaySet(self: *const Client) bool {
    return self.tcp_nodelay_set;
}

/// Returns true if TCP receive buffer was successfully set.
pub fn isTcpRcvBufSet(self: *const Client) bool {
    return self.tcp_rcvbuf_set;
}

/// Get subscription by SID.
/// Uses cached pointer for fast path when single subscription matches.
pub inline fn getSubscriptionBySid(self: *Client, sid: u64) ?*Sub {
    assert(sid > 0);
    // Fast path: cached subscription (common in benchmarks)
    if (self.cached_sub) |sub| {
        if (sub.sid == sid) return sub;
    }
    // Normal hash lookup
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

    const writer = &self.writer.interface;
    protocol.Encoder.encodeUnsub(writer, .{
        .sid = sid,
        .max_msgs = null,
    }) catch {
        return error.EncodingFailed;
    };

    if (self.sidmap.get(sid)) |slot_idx| {
        if (self.cached_sub) |cached| {
            if (cached.sid == sid) self.cached_sub = null;
        }
        self.sub_ptrs[slot_idx] = null;
        _ = self.sidmap.remove(sid);
        self.free_slots[self.free_count] = slot_idx;
        self.free_count += 1;
    }
}

/// Destroys a subscription safely while client is alive.
/// Called by Subscription.deinit() when client_destroyed == false.
pub fn destroySubscription(
    self: *Client,
    sub: *Sub,
    allocator: Allocator,
) void {
    sub.queue.close(self.io);

    self.read_mutex.lockUncancelable(self.io);
    defer self.read_mutex.unlock(self.io);

    if (sub.state != .unsubscribed) {
        const writer = &self.writer.interface;
        protocol.Encoder.encodeUnsub(writer, .{
            .sid = sub.sid,
            .max_msgs = null,
        }) catch {};
    }

    if (self.sidmap.get(sub.sid)) |slot_idx| {
        self.sub_ptrs[slot_idx] = null;
        if (self.cached_sub == sub) self.cached_sub = null;
        _ = self.sidmap.remove(sub.sid);
        self.free_slots[self.free_count] = slot_idx;
        self.free_count += 1;
    }

    // Drain remaining messages
    var drain_buf: [1]Message = undefined;
    while (true) {
        const n = sub.queue.popBatch(&drain_buf);
        if (n == 0) break;
        drain_buf[0].deinit(allocator);
    }

    // Free subscription resources
    allocator.free(sub.queue_buf);
    allocator.free(sub.subject);
    if (sub.queue_group) |qg| allocator.free(qg);
    allocator.destroy(sub);
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

/// Sends PING for health check.
fn sendPing(self: *Client) !void {
    assert(self.state == .connected);
    const writer = &self.writer.interface;
    writer.writeAll("PING\r\n") catch {
        return error.WriteFailed;
    };
    writer.flush() catch {
        return error.WriteFailed;
    };
    self.last_ping_sent_ns = getNowNs() catch self.last_ping_sent_ns;
    self.pings_outstanding += 1;
    dbg.pingPong("PING_SENT", self.pings_outstanding);
}

/// Handles PONG response from server.
fn handlePong(self: *Client) void {
    self.last_pong_received_ns = getNowNs() catch self.last_pong_received_ns;
    self.pings_outstanding = 0;
    dbg.pingPong("PONG_RECEIVED", 0);
}

/// Checks connection health and triggers reconnect if stale.
/// Called from io_task loop.
fn maybeHealthCheck(self: *Client, allocator: Allocator) !void {
    _ = allocator;

    if (self.options.ping_interval_ms == 0) return;
    if (self.state != .connected) return;

    const now_ns = getNowNs() catch return;
    const interval_ns: u64 = @as(u64, self.options.ping_interval_ms) * 1_000_000;

    // Check if too many PINGs outstanding (connection stale)
    if (self.pings_outstanding >= self.options.max_pings_outstanding) {
        dbg.print(
            "Connection stale: {d} PINGs outstanding",
            .{self.pings_outstanding},
        );
        // TODO: Trigger reconnect when Phase 6 is implemented
        // For now, just reset state to prevent repeated warnings
        self.pings_outstanding = 0;
        return;
    }

    // Check if it's time to send PING
    if (now_ns - self.last_ping_sent_ns >= interval_ns) {
        self.sendPing() catch |err| {
            dbg.print("Failed to send PING: {s}", .{@errorName(err)});
        };
    }
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
/// Closes connection, stops io_task, frees buffers.
/// Uses close-then-cancel pattern for reliable shutdown.
/// Safe to call multiple times.
pub fn deinit(self: *Client, alloc: Allocator) void {
    assert(self.next_sid >= 1);

    // CLOSE-THEN-CANCEL PATTERN (see zig-0.16 skill ASYNC.md)
    // Signal-based cancel has race condition with network I/O.
    // Close socket first to unblock fillMore(), then cancel completes quickly.

    // 1. Save state and mark as closed (prevents reconnection attempts)
    const was_open = self.state != .closed;
    self.state = .closed;

    // 2. Close stream if still open - unblocks any pending fillMore()
    //    io_task will see error, check state == .closed, exit cleanly
    if (was_open) {
        self.stream.close(self.io);
    }

    // 3. Now cancel completes quickly (io_task already exiting or exited)
    if (self.io_task_future) |*future| {
        _ = future.cancel(self.io);
        self.io_task_future = null;
    }

    // 4. Cleanup subscriptions (io_task is now gone)
    self.closeAllQueues();
    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            sub.client_destroyed = true;
        }
    }

    // Free client resources
    if (self.server_info) |*info| {
        info.deinit(alloc);
    }

    // Drain return queue before destroying slab (free any pending buffers)
    while (self.return_queue.pop()) |buf| {
        self.tiered_slab.free(buf);
    }
    alloc.free(self.return_queue_buf);
    self.tiered_slab.deinit();

    // Free pending buffer
    self.deinitPendingBuffer(alloc);

    alloc.free(self.read_buffer);
    alloc.free(self.write_buffer);
    alloc.destroy(self);
}

// =============================================================================
// Reconnection Support
// =============================================================================

/// Backup all active subscriptions for restoration after reconnect.
/// Stores SID, subject, and queue_group in inline buffers (no allocation).
pub fn backupSubscriptions(self: *Client) void {
    self.sub_backup_count = 0;

    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            if (sub.state != .active) continue;
            if (self.sub_backup_count >= MAX_SUBSCRIPTIONS) break;

            var backup = &self.sub_backups[self.sub_backup_count];
            backup.sid = sub.sid;

            // Copy subject
            const subj_len: u8 = @intCast(@min(sub.subject.len, 256));
            @memcpy(backup.subject_buf[0..subj_len], sub.subject[0..subj_len]);
            backup.subject_len = subj_len;

            // Copy queue_group if present
            if (sub.queue_group) |qg| {
                const qg_len: u8 = @intCast(@min(qg.len, 64));
                @memcpy(backup.queue_group_buf[0..qg_len], qg[0..qg_len]);
                backup.queue_group_len = qg_len;
            } else {
                backup.queue_group_len = 0;
            }

            self.sub_backup_count += 1;
        }
    }
}

/// Restore subscriptions after reconnect (preserves original SIDs).
/// Re-sends SUB commands to server with the same SIDs so existing
/// subscription pointers continue to work.
pub fn restoreSubscriptions(self: *Client) !void {
    if (self.sub_backup_count == 0) return;

    const writer = &self.writer.interface;

    for (self.sub_backups[0..self.sub_backup_count]) |*backup| {
        if (backup.sid == 0) continue;

        const subject = backup.getSubject();
        const queue_group = backup.getQueueGroup();

        // Send SUB with SAME SID
        protocol.Encoder.encodeSub(writer, .{
            .subject = subject,
            .queue_group = queue_group,
            .sid = backup.sid,
        }) catch return error.RestoreSubscriptionsFailed;
    }

    writer.flush() catch return error.RestoreSubscriptionsFailed;
}

/// Initialize pending buffer for publishes during reconnect.
fn initPendingBuffer(self: *Client, allocator: Allocator) !void {
    if (self.options.pending_buffer_size == 0) return;
    if (self.pending_buffer != null) return; // Already initialized

    self.pending_buffer = try allocator.alloc(u8, self.options.pending_buffer_size);
    self.pending_buffer_capacity = self.options.pending_buffer_size;
    self.pending_buffer_pos = 0;
}

/// Free pending buffer.
fn deinitPendingBuffer(self: *Client, allocator: Allocator) void {
    if (self.pending_buffer) |buf| {
        allocator.free(buf);
        self.pending_buffer = null;
        self.pending_buffer_pos = 0;
        self.pending_buffer_capacity = 0;
    }
}

/// Buffer a publish during reconnect.
/// Returns error if buffer is full or not initialized.
fn bufferPendingPublish(
    self: *Client,
    subject: []const u8,
    payload: []const u8,
) !void {
    const buf = self.pending_buffer orelse return error.NotConnected;
    const remaining = self.pending_buffer_capacity - self.pending_buffer_pos;

    // Estimate encoded size: "PUB subject len\r\npayload\r\n"
    // PUB + space + subject + space + len(max 10 digits) + \r\n + payload + \r\n
    const encoded_size = 4 + subject.len + 1 + 10 + 2 + payload.len + 2;
    if (encoded_size > remaining) {
        return error.PendingBufferFull;
    }

    // Encode directly into pending buffer using fixed buffer stream
    var fbs = std.io.fixedBufferStream(buf[self.pending_buffer_pos..]);
    const writer = fbs.writer();

    // Write PUB command manually (simpler than using Encoder)
    try writer.writeAll("PUB ");
    try writer.writeAll(subject);
    try writer.writeByte(' ');

    // Write payload length
    var len_buf: [10]u8 = undefined;
    const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{payload.len}) catch {
        return error.EncodingFailed;
    };
    try writer.writeAll(len_str);
    try writer.writeAll("\r\n");
    try writer.writeAll(payload);
    try writer.writeAll("\r\n");

    self.pending_buffer_pos += fbs.pos;
}

/// Flush pending buffer after reconnect.
fn flushPendingBuffer(self: *Client) !void {
    if (self.pending_buffer_pos == 0) return;

    const buf = self.pending_buffer orelse return;
    self.writer.interface.writeAll(buf[0..self.pending_buffer_pos]) catch {
        return error.WriteFailed;
    };
    self.writer.interface.flush() catch return error.WriteFailed;
    self.pending_buffer_pos = 0;
    dbg.pendingBuffer("FLUSHED", 0, self.pending_buffer_capacity);
}

/// Cleanup client state for reconnection.
/// Closes old stream but preserves subscriptions and pending buffer.
fn cleanupForReconnect(self: *Client) void {
    dbg.stateChange("connected", "reconnecting");

    // Close old stream
    self.stream.close(self.io);

    // Clear server info (will be refreshed on reconnect)
    // Note: Don't free - we'll get new info from server
}

/// Attempt connection to a single server.
/// Returns true on success, error on failure.
pub fn tryConnect(self: *Client, allocator: Allocator, server: *connection.server_pool.Server) !void {
    const host = server.getHost();
    const port = server.port;

    dbg.reconnectEvent("CONNECTING", self.reconnect_attempt + 1, server.getUrl());

    // Parse address
    const address = net.IpAddress.parse(host, port) catch {
        return error.InvalidAddress;
    };

    // Connect
    self.stream = net.IpAddress.connect(address, self.io, .{
        .mode = .stream,
        .protocol = .tcp,
    }) catch {
        return error.ConnectionFailed;
    };
    errdefer self.stream.close(self.io);

    // Set TCP_NODELAY
    const enable: u32 = 1;
    std.posix.setsockopt(
        self.stream.socket.handle,
        std.posix.IPPROTO.TCP,
        std.posix.TCP.NODELAY,
        std.mem.asBytes(&enable),
    ) catch {};

    // Reinitialize reader/writer with existing buffers
    self.reader = self.stream.reader(self.io, self.read_buffer);
    self.writer = self.stream.writer(self.io, self.write_buffer);

    // Parse URL to get auth info
    const parsed = parseUrl(server.getUrl()) catch ParsedUrl{
        .host = host,
        .port = port,
        .user = null,
        .pass = null,
    };

    // Perform handshake
    try self.handshake(allocator, self.options, parsed);

    // Initialize health check timestamps
    const now_ns = getNowNs() catch 0;
    self.last_ping_sent_ns = now_ns;
    self.last_pong_received_ns = now_ns;
    self.pings_outstanding = 0;

    dbg.reconnectEvent("CONNECTED", self.reconnect_attempt + 1, server.getUrl());
}

/// Wait with exponential backoff + jitter.
pub fn waitBackoff(self: *Client) void {
    const opts = self.options;
    const attempt = @min(self.reconnect_attempt, 10);

    const base: u64 = opts.reconnect_wait_ms;
    const exp_wait = base << @as(u6, @intCast(attempt));
    const capped = @min(exp_wait, opts.reconnect_wait_max_ms);

    // Add jitter using Io.random()
    var rand_buf: [4]u8 = undefined;
    self.io.random(&rand_buf);
    const rand = std.mem.readInt(u32, &rand_buf, .little);
    const jitter_range = capped * opts.reconnect_jitter_percent / 100;

    // Calculate final wait with jitter
    var final_wait = capped;
    if (jitter_range > 0) {
        const jitter_val = rand % (jitter_range * 2 + 1);
        if (jitter_val > jitter_range) {
            final_wait = capped + (jitter_val - jitter_range);
        } else {
            final_wait = capped -| jitter_range + jitter_val;
        }
    }
    final_wait = @max(100, final_wait);

    dbg.print("Backoff wait: {d}ms (attempt {d})", .{ final_wait, attempt + 1 });
    self.io.sleep(.fromMilliseconds(final_wait), .awake) catch {};
}

/// Attempt reconnection with exponential backoff.
/// Can be called automatically (from io_task) or manually by user.
pub fn reconnect(self: *Client, allocator: Allocator) !void {
    // Validate state
    if (self.state != .disconnected and self.state != .reconnecting) {
        if (self.state == .connected) return; // Already connected
        return error.InvalidState;
    }

    if (!self.options.reconnect) {
        return error.ReconnectDisabled;
    }

    // Initialize server pool if not already done
    if (!self.server_pool_initialized) {
        const url = self.original_url[0..self.original_url_len];
        self.server_pool = connection.ServerPool.init(url) catch {
            return error.InvalidUrl;
        };
        self.server_pool_initialized = true;
    }

    // Backup subscriptions before cleanup
    self.backupSubscriptions();
    self.cleanupForReconnect();
    self.state = .reconnecting;

    const max = self.options.max_reconnect_attempts;
    const infinite = max == 0;

    dbg.print(
        "Starting reconnect (max_attempts={d}, infinite={any})",
        .{ max, infinite },
    );

    while (infinite or self.reconnect_attempt < max) {
        // Get next server
        const now_ns = getNowNs() catch 0;
        const server = self.server_pool.nextServer(now_ns) orelse {
            dbg.print("All servers on cooldown, waiting...", .{});
            self.waitBackoff();
            continue;
        };

        dbg.reconnectEvent(
            "ATTEMPT",
            self.reconnect_attempt + 1,
            server.getUrl(),
        );

        // Attempt connection
        if (self.tryConnect(allocator, server)) {
            // SUCCESS!
            self.restoreSubscriptions() catch |err| {
                dbg.print("Failed to restore subscriptions: {s}", .{@errorName(err)});
            };
            self.flushPendingBuffer() catch |err| {
                dbg.print("Failed to flush pending buffer: {s}", .{@errorName(err)});
            };

            self.state = .connected;
            self.reconnect_attempt = 0;
            self.stats.reconnects += 1;
            self.server_pool.resetFailures();

            dbg.stateChange("reconnecting", "connected");
            dbg.print(
                "Reconnect successful (total reconnects: {d})",
                .{self.stats.reconnects},
            );
            return;
        } else |err| {
            dbg.reconnectEvent("FAILED", self.reconnect_attempt + 1, server.getUrl());
            dbg.print("Connection attempt failed: {s}", .{@errorName(err)});

            self.server_pool.markCurrentFailed();
            self.reconnect_attempt += 1;
            self.waitBackoff();
        }
    }

    // All attempts exhausted
    self.state = .closed;
    dbg.stateChange("reconnecting", "closed");
    dbg.print("Reconnect failed: max attempts ({d}) exhausted", .{max});
    return error.ReconnectFailed;
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
/// or tryNext() for non-blocking poll.
pub const Subscription = struct {
    client: *Client,
    sid: u64,
    subject: []const u8,
    queue_group: ?[]const u8,
    queue_buf: []Message,
    queue: SpscQueue(Message),
    state: SubscriptionState,
    received_msgs: u64,
    dropped_msgs: u64 = 0,
    alloc_failed_msgs: u64 = 0,
    client_destroyed: bool = false,

    /// Blocks until a message is available or connection is closed.
    ///
    /// Arguments:
    ///     allocator: Allocator for owned message copy
    ///     io: Io interface for blocking operations
    /// Blocks until a message arrives on this subscription.
    ///
    /// The background io_task handles all socket I/O and routes messages
    /// to subscription queues. This function just blocks on the queue.
    /// Lock-free spin-wait for message.
    ///
    /// Returns owned Message that caller must free via msg.deinit(allocator).
    pub fn next(self: *Subscription, allocator: Allocator, io: Io) !Message {
        _ = allocator;
        _ = io;
        assert(self.state == .active or self.state == .draining);

        // Spin-wait on lock-free queue
        while (true) {
            if (self.queue.pop()) |msg| return msg;
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }
            std.atomic.spinLoopHint();
        }
    }

    /// Try receive without blocking. Returns null if no message available.
    pub fn tryNext(self: *Subscription) ?Message {
        return self.queue.pop();
    }

    /// Batch receive - waits for at least 1, returns up to buf.len.
    pub fn nextBatch(self: *Subscription, io: Io, buf: []Message) !usize {
        _ = io;
        assert(self.state == .active or self.state == .draining);
        assert(buf.len > 0);

        // Spin-wait for at least one message
        while (true) {
            const count = self.queue.popBatch(buf);
            if (count > 0) return count;
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }
            std.atomic.spinLoopHint();
        }
    }

    /// Non-blocking batch receive.
    pub fn tryNextBatch(self: *Subscription, buf: []Message) usize {
        return self.queue.popBatch(buf);
    }

    /// Receive with timeout using timer-based polling.
    ///
    /// Arguments:
    ///     allocator: Allocator for message (unused, kept for API compat)
    ///     timeout_ms: Maximum wait time in milliseconds
    ///
    /// Returns null on timeout. Uses lock-free polling with timer.
    pub fn nextWithTimeout(
        self: *Subscription,
        allocator: Allocator,
        timeout_ms: u32,
    ) !?Message {
        _ = allocator;
        assert(self.state == .active or self.state == .draining);
        assert(timeout_ms > 0);

        // Use Instant for timeout check - but only check every N iterations
        // to avoid syscall overhead
        const start = std.time.Instant.now() catch {
            // Fallback to spin-only if timer unavailable
            return self.queue.pop();
        };
        const timeout_ns: u64 = @as(u64, timeout_ms) * std.time.ns_per_ms;
        var check_counter: u32 = 0;

        while (true) {
            if (self.queue.pop()) |msg| return msg;
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }

            // Check timeout only every 10000 iterations to reduce syscalls
            check_counter +%= 1;
            if (check_counter >= 10000) {
                check_counter = 0;
                const now = std.time.Instant.now() catch return null;
                if (now.since(start) >= timeout_ns) return null;
            }

            std.atomic.spinLoopHint();
        }
    }

    /// Returns queue capacity.
    pub fn getCapacity(self: *const Subscription) usize {
        return self.queue.capacity;
    }

    /// Returns count of messages dropped due to queue overflow.
    /// Only incremented when other subscriptions route messages to this one
    /// and the queue is full. The reading subscription bypasses its queue.
    pub fn getDroppedCount(self: *const Subscription) u64 {
        return self.dropped_msgs;
    }

    /// Returns count of messages dropped due to allocation failure.
    pub fn getAllocFailedCount(self: *const Subscription) u64 {
        return self.alloc_failed_msgs;
    }

    /// Push message to queue (called by io_task).
    /// Lock-free, never blocks.
    pub fn pushMessage(self: *Subscription, msg: Message) !void {
        if (!self.queue.push(msg)) return error.QueueFull;
    }

    /// Unsubscribe from the subject.
    pub fn unsubscribe(self: *Subscription) !void {
        if (self.state == .unsubscribed) return;
        self.state = .unsubscribed;
        // Note: SpscQueue doesn't need close signaling
        try self.client.unsubscribeSid(self.sid);
    }

    /// Closes the subscription and frees resources.
    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        if (self.client_destroyed) {
            // Client already handled cleanup - just free local resources
            allocator.free(self.queue_buf);
            allocator.free(self.subject);
            if (self.queue_group) |qg| allocator.free(qg);
            allocator.destroy(self);
        } else {
            // Client is alive - delegate for safe cleanup
            self.client.destroySubscription(self, allocator);
        }
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
    try std.testing.expectEqual(defaults.Connection.timeout_ns, opts.connect_timeout_ns);
    try std.testing.expectEqual(defaults.Memory.queue_size.value(), opts.sub_queue_size);
}

test "stats defaults" {
    const stats: Stats = .{};
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_in);
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_out);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_in);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_out);
    try std.testing.expectEqual(@as(u32, 0), stats.reconnects);
}
