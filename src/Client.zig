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
const tls = std.crypto.tls;
const Certificate = std.crypto.Certificate;

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
const events_mod = @import("events.zig");
pub const Event = events_mod.Event;
pub const EventHandler = events_mod.EventHandler;

const headers = @import("protocol/headers.zig");
pub const HeaderEntry = headers.Entry;
pub const HeaderMap = protocol.HeaderMap;

const nkey_auth = @import("auth.zig");
const creds_auth = nkey_auth.creds;

const Client = @This();

/// Gets current time in nanoseconds (Zig 0.16 compatible).
fn getNowNs() error{TimerUnavailable}!u64 {
    const instant = std.time.Instant.now() catch return error.TimerUnavailable;
    const secs: u64 = @intCast(instant.timestamp.sec);
    const nsecs: u64 = @intCast(instant.timestamp.nsec);
    return secs * std.time.ns_per_s + nsecs;
}

/// Message received on a subscription. Call deinit() to free.
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
            assert(self.return_queue != null);
            const rq = self.return_queue.?;
            // Yield allows io_task to drain before retry
            while (!rq.push(buf)) {
                std.Thread.yield() catch {};
            }
            return;
        }
        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply_to) |rt| allocator.free(rt);
        if (self.headers) |h| allocator.free(h);
    }

    /// Sends a reply to this message using the reply_to subject.
    /// Convenience method for request/reply pattern.
    /// Returns error.NoReplyTo if message has no reply_to subject.
    pub fn respond(self: *const Message, client: *Client, payload: []const u8) !void {
        const reply_to = self.reply_to orelse return error.NoReplyTo;
        assert(reply_to.len > 0);
        try client.publish(reply_to, payload);
    }

    /// Returns the total size of the message in bytes.
    /// Includes subject, data, reply_to, and headers.
    pub fn size(self: *const Message) usize {
        var total: usize = self.subject.len + self.data.len;
        if (self.reply_to) |rt| total += rt.len;
        if (self.headers) |h| total += h.len;
        return total;
    }

    /// Extracts HTTP-like status code from headers (on-demand parsing).
    /// Returns null if no headers or no status code present.
    /// Common codes: 503 (no responders), 408 (timeout), 404 (not found).
    pub fn getStatus(self: *const Message) ?u16 {
        const hdrs = self.headers orelse return null;
        return headers.extractStatus(hdrs);
    }

    /// Returns true if this is a no-responders message (status 503).
    /// Used to detect when a request has no available responders.
    pub fn isNoResponders(self: *const Message) bool {
        return self.getStatus() == 503;
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

    // TLS OPTIONS

    /// Path to CA certificate file (PEM). Null = use system CAs.
    tls_ca_file: ?[]const u8 = null,
    /// Path to client certificate file for mTLS (PEM).
    tls_cert_file: ?[]const u8 = null,
    /// Path to client private key file for mTLS (PEM).
    tls_key_file: ?[]const u8 = null,
    /// Skip server certificate verification (INSECURE - testing only).
    tls_insecure_skip_verify: bool = false,
    /// Perform TLS handshake before NATS protocol (required by some proxies).
    tls_handshake_first: bool = false,

    /// NKey seed for authentication.
    nkey_seed: ?[]const u8 = null,
    /// NKey seed file path (alternative to nkey_seed).
    nkey_seed_file: ?[]const u8 = null,
    /// NKey public key for callback-based signing.
    nkey_pubkey: ?[]const u8 = null,
    /// NKey signing callback (returns true on success).
    nkey_sign_fn: ?*const fn (nonce: []const u8, sig: *[64]u8) bool = null,
    /// JWT for authentication.
    jwt: ?[]const u8 = null,
    /// Credentials file path (.creds file with JWT + NKey seed).
    /// Mutually exclusive with jwt/nkey_seed options.
    creds_file: ?[]const u8 = null,
    /// Credentials content (alternative to file path).
    /// Use when credentials are loaded from environment/memory.
    creds: ?[]const u8 = null,
    /// Read/write buffer size. Must be >= max message size you expect (1MB).
    buffer_size: usize = defaults.Connection.buffer_size,
    /// TCP receive buffer size hint. Larger values allow more messages to
    /// queue in the kernel before backpressure kicks in. Default 256KB.
    /// Set to 0 to use system default.
    tcp_rcvbuf: u32 = defaults.Connection.tcp_rcvbuf,

    // RECONNECTION OPTIONS

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
    /// Custom reconnect delay callback. If set, overrides default exponential
    /// backoff. Called with attempt number (1-based), returns delay in ms.
    /// Example: `fn(attempt: u32) u32 { return attempt * 1000; }`
    custom_reconnect_delay: ?*const fn (attempt: u32) u32 = null,
    /// Discover servers from INFO connect_urls.
    discover_servers: bool = defaults.Reconnection.discover_servers,
    /// Size of pending buffer for publishes during reconnect.
    /// Set to 0 to disable buffering (publish returns error during reconnect).
    pending_buffer_size: usize = defaults.Reconnection.pending_buffer_size,

    // PING/PONG HEALTH CHECK

    /// Interval between client-initiated PINGs (ms). 0 = disable.
    ping_interval_ms: u32 = defaults.Connection.ping_interval_ms,
    /// Max outstanding PINGs before connection is considered stale.
    max_pings_outstanding: u8 = defaults.Connection.max_pings_outstanding,

    // ERROR REPORTING

    /// Messages between rate-limited error notifications.
    /// After first error (alloc_failed, protocol_error), subsequent errors
    /// only notify every N messages. Prevents event queue flooding.
    error_notify_interval_msgs: u64 = defaults.ErrorReporting.notify_interval_msgs,

    // EVENT CALLBACKS

    /// Event handler for connection lifecycle callbacks (optional).
    /// Use EventHandler.init(T, &handler) to create from a handler struct.
    event_handler: ?EventHandler = null,

    // INBOX/REQUEST OPTIONS

    /// Custom prefix for inbox subjects. Default is "_INBOX".
    /// Used for request/reply pattern inbox generation.
    inbox_prefix: []const u8 = "_INBOX",

    // CONNECTION BEHAVIOR

    /// Retry connection on initial connect failure (before returning error).
    /// When true, connect() will retry using reconnect settings.
    retry_on_failed_connect: bool = false,
    /// Don't randomize server order for connection attempts.
    /// When true, servers are tried in the order provided.
    no_randomize: bool = false,
    /// Ignore servers discovered via cluster INFO.
    /// Only use explicitly configured servers.
    ignore_discovered_servers: bool = false,
    /// Default timeout for drain operations (ms).
    drain_timeout_ms: u32 = 30_000,
    /// Default timeout for flush operations (ms).
    flush_timeout_ms: u32 = 10_000,
};

/// Connection statistics
/// Thread ownership: io_task exclusively writes msgs_in/bytes_in,
/// main thread exclusively writes msgs_out/bytes_out. No concurrent
/// modifications to same counter, so atomics are not needed.
pub const Stats = struct {
    /// Total messages received (written by io_task only).
    msgs_in: u64 = 0,
    /// Total messages sent (written by main thread only).
    msgs_out: u64 = 0,
    /// Total bytes received (written by io_task only).
    bytes_in: u64 = 0,
    /// Total bytes sent (written by main thread only).
    bytes_out: u64 = 0,
    /// Number of reconnects (written by io_task only).
    reconnects: u32 = 0,
    /// Total successful connections (initial + reconnects).
    connects: u32 = 0,
};

/// Debug counters for io_task buffer operations.
/// Only incremented when dbg.enabled.
/// Written exclusively by io_task thread, safe to read after deinit.
pub const IoTaskStats = struct {
    /// Number of tryFillBuffer() calls.
    fill_calls: u64 = 0,
    /// Cumulative bytes already buffered (before read).
    fill_buffered_hits: u64 = 0,
    /// Poll timeouts (no data available).
    fill_poll_timeouts: u64 = 0,
    /// Successful socket reads.
    fill_read_success: u64 = 0,
};

/// Subscription backup for restoration after reconnect.
/// Stores essential subscription state with inline buffers.
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

/// Result of drain operation.
pub const DrainResult = struct {
    /// Count of UNSUB commands that failed to encode.
    unsub_failures: u16 = 0,
    /// True if final flush failed (data may not have reached server).
    flush_failed: bool = false,

    /// Returns true if drain completed without any failures.
    pub fn isClean(self: DrainResult) bool {
        return self.unsub_failures == 0 and !self.flush_failed;
    }
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
    use_tls: bool,
};

/// Fixed subscription limits (from defaults.zig).
pub const MAX_SUBSCRIPTIONS: u16 = defaults.Client.max_subscriptions;
pub const SIDMAP_CAPACITY: u32 = defaults.Client.sidmap_capacity;

/// Default queue size per subscription (messages buffered before dropping).
pub const DEFAULT_QUEUE_SIZE: u32 = defaults.Memory.queue_size.value();

comptime {
    assert(SIDMAP_CAPACITY >= MAX_SUBSCRIPTIONS);
}

/// Parses a NATS URL like nats://user:pass@host:port or tls://host:port
pub fn parseUrl(url: []const u8) error{InvalidUrl}!ParsedUrl {
    if (url.len == 0) return error.InvalidUrl;
    var remaining = url;
    var use_tls = false;

    if (std.mem.startsWith(u8, remaining, "tls://")) {
        remaining = remaining[6..];
        use_tls = true;
    } else if (std.mem.startsWith(u8, remaining, "nats://")) {
        remaining = remaining[7..];
    }

    var user: ?[]const u8 = null;
    var pass: ?[]const u8 = null;

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
        .use_tls = use_tls,
    };
}

/// Subscription type alias.
pub const Sub = Subscription;

io: Io,
stream: net.Stream,
reader: net.Stream.Reader,
writer: net.Stream.Writer,
/// Active reader interface (TCP or TLS). Set once at connection, used by io_task.
active_reader: *Io.Reader = undefined,
/// Active writer interface (TCP or TLS). Set once at connection, used by io_task.
active_writer: *Io.Writer = undefined,
options: Options,

read_buffer: []u8,
write_buffer: []u8,

sidmap: SidMap,
sidmap_keys: [SIDMAP_CAPACITY]u64,
sidmap_vals: [SIDMAP_CAPACITY]u16,
free_slots: [MAX_SUBSCRIPTIONS]u16,

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

// Cached max_payload from server_info
max_payload: usize = 1024 * 1024,

// Slab allocator for message buffers
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

// PING/PONG health check state (atomics for cross-thread access)
// Main thread reads during health check (~100ms), io_task writes on PONG.
// Uses monotonic ordering - exact timing not critical, eventual visibility suffices.
last_ping_sent_ns: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
last_pong_received_ns: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
pings_outstanding: std.atomic.Value(u8) = std.atomic.Value(u8).init(0),

// Debug counters for io_task (only used when dbg.enabled)
io_task_stats: IoTaskStats = .{},

// Error rate-limiting state (written by io_task only)
/// Count of protocol parse errors encountered.
protocol_errors: u64 = 0,
/// msgs_in value when protocol_error event was last pushed (rate-limit).
last_parse_error_notified_at: u64 = 0,

// Last async error tracking (written by io_task only)
/// Last async error that occurred on the connection.
last_error: ?anyerror = null,
/// Message associated with last error (inline buffer, no allocation).
last_error_msg: [256]u8 = undefined,
/// Length of last_error_msg content.
last_error_msg_len: u8 = 0,

// Background I/O task infrastructure
write_mutex: Io.Mutex = .init,
/// Future for background I/O task (for proper cancellation in deinit).
io_task_future: ?Io.Future(void) = null,

// Event callback infrastructure
/// Event queue for io_task -> callback_task communication.
/// SpscQueue for non-blocking push from io_task hot path.
event_queue: ?*SpscQueue(Event) = null,
/// Buffer backing the event queue.
event_queue_buf: ?[]Event = null,
/// Future for callback task (dispatches events to user handler).
callback_task_future: ?Io.Future(void) = null,
/// Event handler (copied from options for callback_task access).
event_handler: ?EventHandler = null,
/// Flag to track if lame duck event has been fired.
lame_duck_notified: bool = false,

// TLS state
/// TLS client instance (owns decryption state).
tls_client: ?tls.Client = null,
/// TLS read buffer (must be at least tls.Client.min_buffer_len).
tls_read_buffer: ?[]u8 = null,
/// TLS write buffer (must be at least tls.Client.min_buffer_len).
tls_write_buffer: ?[]u8 = null,
/// CA certificate bundle for verification.
ca_bundle: ?Certificate.Bundle = null,
/// Whether TLS is enabled for this connection.
use_tls: bool = false,
/// Host for TLS SNI and certificate verification.
tls_host: [256]u8 = undefined,
/// Length of tls_host.
tls_host_len: u8 = 0,

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
    // Validate URL length - reject rather than truncate
    if (url.len > defaults.Server.max_url_len) return error.UrlTooLong;

    const parsed = try parseUrl(url);

    const client = try allocator.create(Client);
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

    // Initialize health check state (atomics)
    client.last_ping_sent_ns.raw = 0;
    client.last_pong_received_ns.raw = 0;
    client.pings_outstanding.raw = 0;
    client.io_task_stats = .{};

    // Initialize error tracking state
    client.protocol_errors = 0;
    client.last_parse_error_notified_at = 0;
    client.last_error = null;
    client.last_error_msg_len = 0;

    // Initialize background I/O task infrastructure
    client.write_mutex = .init;

    // Initialize event callback infrastructure
    client.event_queue = null;
    client.event_queue_buf = null;
    client.callback_task_future = null;
    client.event_handler = opts.event_handler;
    client.lame_duck_notified = false;

    // Initialize TLS state
    client.tls_client = null;
    client.tls_read_buffer = null;
    client.tls_write_buffer = null;
    client.ca_bundle = null;
    // Determine if TLS should be used: URL scheme, explicit option, or CA file set
    client.use_tls = parsed.use_tls or opts.tls_required or
        opts.tls_ca_file != null or opts.tls_handshake_first;
    client.tls_host = undefined;
    client.tls_host_len = 0;

    // Store host for TLS SNI and certificate verification
    if (client.use_tls) {
        if (parsed.host.len > 255) return error.HostTooLong;
        const host_len: u8 = @intCast(parsed.host.len);
        @memcpy(client.tls_host[0..host_len], parsed.host);
        client.tls_host_len = host_len;
    }

    // Initialize slab allocator (critical for O(1) message allocation)
    client.tiered_slab = TieredSlab.init(allocator) catch |err| {
        allocator.destroy(client);
        return err;
    };

    // Initialize return queue for cross-thread buffer deallocation
    // Size must exceed slab tier capacity to avoid blocking when buffers
    // are split between sub_queue, processing, and return_queue
    const rq_size = opts.sub_queue_size * 2;
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
        // TLS cleanup
        if (client.tls_read_buffer) |buf| allocator.free(buf);
        if (client.tls_write_buffer) |buf| allocator.free(buf);
        if (client.ca_bundle) |*bundle| bundle.deinit(allocator);
        allocator.destroy(client);
    }

    const host = if (std.mem.eql(u8, parsed.host, "localhost"))
        "127.0.0.1"
    else
        parsed.host;

    const address = net.IpAddress.parse(host, parsed.port) catch {
        return error.InvalidAddress;
    };

    client.stream = net.IpAddress.connect(address, io, .{
        .mode = .stream,
        .protocol = .tcp,
    }) catch {
        return error.ConnectionFailed;
    };
    errdefer client.stream.close(io);

    // TCP_NODELAY
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

    client.read_buffer = allocator.alloc(u8, opts.buffer_size) catch {
        return error.OutOfMemory;
    };
    errdefer allocator.free(client.read_buffer);

    client.write_buffer = allocator.alloc(u8, opts.buffer_size) catch {
        return error.OutOfMemory;
    };
    errdefer allocator.free(client.write_buffer);

    client.io = io;
    client.reader = client.stream.reader(io, client.read_buffer);
    client.writer = client.stream.writer(io, client.write_buffer);
    // Default to TCP reader/writer (updated by upgradeTls if TLS is used)
    client.active_reader = &client.reader.interface;
    client.active_writer = &client.writer.interface;
    client.options = opts;

    client.sidmap_keys = undefined;
    client.sidmap_vals = undefined;
    client.sidmap = .init(&client.sidmap_keys, &client.sidmap_vals);
    for (0..MAX_SUBSCRIPTIONS) |i| {
        client.free_slots[i] = @intCast(MAX_SUBSCRIPTIONS - 1 - i);
    }

    // TLS-first mode: upgrade to TLS before NATS protocol
    if (client.use_tls and opts.tls_handshake_first) {
        try client.upgradeTls(allocator, opts);
    }

    try client.handshake(allocator, opts, parsed);
    // Note: TLS upgrade (if needed) now happens inside handshake(),
    // between receiving INFO and sending CONNECT per NATS protocol.

    assert(url.len <= defaults.Server.max_url_len);
    const url_len: u8 = @intCast(url.len);
    @memcpy(client.original_url[0..url_len], url);
    client.original_url_len = url_len;

    client.server_pool = connection.ServerPool.init(url) catch {
        return error.InvalidUrl;
    };
    client.server_pool_initialized = true;

    if (opts.discover_servers) {
        if (client.server_info) |info| {
            const new_servers = client.server_pool.addFromConnectUrls(
                &info.connect_urls,
                &info.connect_urls_lens,
                info.connect_urls_count,
            );
            if (new_servers > 0) {
                client.pushEvent(.{ .discovered_servers = .{ .count = new_servers } });
            }
        }
    }

    try client.initPendingBuffer(allocator);

    const now_ns = getNowNs() catch 0;
    client.last_ping_sent_ns.store(now_ns, .monotonic);
    client.last_pong_received_ns.store(now_ns, .monotonic);

    // concurrent() required - async() may deadlock on flush()
    client.io_task_future = io.concurrent(
        connection.io_task.run,
        .{ client, allocator },
    ) catch blk: {
        dbg.print("WARNING: concurrent() failed, using async()", .{});
        break :blk io.async(connection.io_task.run, .{ client, allocator });
    };

    // Spawn callback task if event handler provided
    if (opts.event_handler != null) {
        // Allocate event queue buffer (256 events is plenty for lifecycle)
        const eq_buf = try allocator.alloc(Event, 256);
        client.event_queue_buf = eq_buf;
        errdefer {
            allocator.free(eq_buf);
            client.event_queue_buf = null;
        }

        // Create event queue (SpscQueue for non-blocking push from io_task)
        const eq = try allocator.create(SpscQueue(Event));
        eq.* = SpscQueue(Event).init(eq_buf);
        client.event_queue = eq;
        errdefer {
            allocator.destroy(eq);
            client.event_queue = null;
        }

        // Spawn callback task
        client.callback_task_future = io.concurrent(
            callbackTaskFn,
            .{client},
        ) catch blk: {
            dbg.print(
                "WARNING: callback concurrent() failed, using async()",
                .{},
            );
            break :blk io.async(callbackTaskFn, .{client});
        };

        // Push initial connected event
        _ = eq.push(.{ .connected = {} });

        // Push socket option warnings (non-fatal, performance impact)
        if (!client.tcp_nodelay_set) {
            _ = eq.push(.{
                .err = .{
                    .err = events_mod.Error.TcpNoDelayFailed,
                    .msg = null,
                },
            });
        }
        if (!client.tcp_rcvbuf_set and opts.tcp_rcvbuf > 0) {
            _ = eq.push(.{
                .err = .{
                    .err = events_mod.Error.TcpRcvBufFailed,
                    .msg = null,
                },
            });
        }
    }

    assert(client.next_sid >= 1);
    assert(client.state == .connected);
    return client;
}

/// Push event to callback queue (called by io_task).
/// Non-blocking, drops event if queue is full.
pub fn pushEvent(self: *Client, event: Event) void {
    if (self.event_queue) |q| {
        _ = q.push(event);
    }
}

/// Callback task: drains event queue and dispatches to user handler.
/// Runs concurrently, uses io.sleep(0) for async-aware yield with cancellation.
/// Exits on .closed event, null queue (deinit), or when canceled during shutdown.
fn callbackTaskFn(client: *Client) void {
    dbg.print("callback_task: STARTED", .{});

    const handler = client.event_handler orelse return;

    while (State.atomicLoad(&client.state) != .closed) {
        // Check if queue was nulled by deinit() - must exit immediately
        const queue = client.event_queue orelse break;

        // Drain all pending events
        while (queue.pop()) |event| {
            switch (event) {
                .connected => handler.dispatchConnect(),
                .disconnected => |e| handler.dispatchDisconnect(e.err),
                .reconnected => handler.dispatchReconnect(),
                .closed => {
                    handler.dispatchClose();
                    dbg.print("callback_task: EXITED (closed event)", .{});
                    return;
                },
                .slow_consumer => {
                    handler.dispatchError(events_mod.Error.SlowConsumer);
                },
                .err => |e| handler.dispatchError(e.err),
                .lame_duck => handler.dispatchLameDuck(),
                .alloc_failed => {
                    handler.dispatchError(events_mod.Error.AllocationFailed);
                },
                .protocol_error => {
                    handler.dispatchError(events_mod.Error.ProtocolParseError);
                },
                .discovered_servers => |e| {
                    handler.dispatchDiscoveredServers(e.count);
                },
                .draining => handler.dispatchDraining(),
                .subscription_complete => |e| {
                    handler.dispatchSubscriptionComplete(e.sid);
                },
            }
        }
        // Async-aware yield with cancellation support (replaces std.Thread.yield)
        client.io.sleep(.fromNanoseconds(0), .awake) catch |err| {
            if (err == error.Canceled) break;
        };
    }

    // Drain any remaining events queued during shutdown
    if (client.event_queue) |queue| {
        while (queue.pop()) |event| {
            switch (event) {
                .connected => handler.dispatchConnect(),
                .disconnected => |e| handler.dispatchDisconnect(e.err),
                .reconnected => handler.dispatchReconnect(),
                .closed => {}, // Will dispatch below
                .slow_consumer => {
                    handler.dispatchError(events_mod.Error.SlowConsumer);
                },
                .err => |e| handler.dispatchError(e.err),
                .lame_duck => handler.dispatchLameDuck(),
                .alloc_failed => {
                    handler.dispatchError(events_mod.Error.AllocationFailed);
                },
                .protocol_error => {
                    handler.dispatchError(events_mod.Error.ProtocolParseError);
                },
                .discovered_servers => |e| {
                    handler.dispatchDiscoveredServers(e.count);
                },
                .draining => handler.dispatchDraining(),
                .subscription_complete => |e| {
                    handler.dispatchSubscriptionComplete(e.sid);
                },
            }
        }
    }

    // Dispatch final close event if not already done
    handler.dispatchClose();
    dbg.print("callback_task: EXITED (state closed)", .{});
}

/// Upgrades the connection to TLS.
/// Allocates TLS buffers, loads CA certificates, and performs handshake.
fn upgradeTls(
    self: *Client,
    allocator: Allocator,
    opts: Options,
) !void {
    assert(self.use_tls);
    assert(self.tls_client == null);
    assert(self.tls_host_len > 0);

    // Allocate TLS buffers if not already done
    if (self.tls_read_buffer == null) {
        self.tls_read_buffer =
            try allocator.alloc(u8, defaults.Tls.buffer_size);
    }
    errdefer if (self.tls_read_buffer) |buf| {
        allocator.free(buf);
        self.tls_read_buffer = null;
    };

    if (self.tls_write_buffer == null) {
        self.tls_write_buffer =
            try allocator.alloc(u8, defaults.Tls.buffer_size);
    }
    errdefer if (self.tls_write_buffer) |buf| {
        allocator.free(buf);
        self.tls_write_buffer = null;
    };

    // Load CA bundle (unless insecure mode)
    if (!opts.tls_insecure_skip_verify) {
        if (self.ca_bundle == null) {
            self.ca_bundle = .{};
        }
        const now = try Io.Clock.real.now(self.io);
        if (opts.tls_ca_file) |ca_path| {
            // Load custom CA bundle from file (propagates file system errors)
            try self.ca_bundle.?.addCertsFromFilePathAbsolute(
                allocator,
                self.io,
                now,
                ca_path,
            );
        } else {
            // Use system CAs
            try self.ca_bundle.?.rescan(allocator, self.io, now);
        }
    }

    // Generate entropy for TLS handshake
    var entropy: [tls.Client.Options.entropy_len]u8 = undefined;
    self.io.randomSecure(&entropy) catch {
        self.io.random(&entropy);
    };

    // Get current timestamp for certificate validation
    const now = try Io.Clock.real.now(self.io);

    // Build TLS options with inline unions
    const tls_opts: tls.Client.Options = .{
        .host = if (opts.tls_insecure_skip_verify)
            .no_verification
        else
            .{ .explicit = self.tls_host[0..self.tls_host_len] },
        .ca = if (opts.tls_insecure_skip_verify)
            .no_verification
        else
            .{ .bundle = self.ca_bundle.? },
        .read_buffer = self.tls_read_buffer.?,
        .write_buffer = self.tls_write_buffer.?,
        .entropy = &entropy,
        .realtime_now_seconds = now.toSeconds(),
    };

    // Perform TLS handshake (propagates TLS errors)
    self.tls_client = try tls.Client.init(
        &self.reader.interface,
        &self.writer.interface,
        tls_opts,
    );

    // Update active reader/writer to TLS (no branching in io_task hot path)
    self.active_reader = &self.tls_client.?.reader;
    self.active_writer = &self.tls_client.?.writer;

    dbg.print("TLS handshake completed", .{});
}

/// Performs NATS handshake (INFO/CONNECT exchange).
fn handshake(
    self: *Client,
    allocator: Allocator,
    opts: Options,
    parsed: ParsedUrl,
) !void {
    // Allow both initial connect and reconnection states
    assert(self.state == .connecting or self.state == .reconnecting);
    assert(parsed.host.len > 0);

    // Use active reader (TLS or TCP depending on connection state)
    // Note: writer is fetched later after potential TLS upgrade
    const reader = self.active_reader;

    // Read INFO from server with connection timeout
    const info_data =
        try self.peekWithTimeout(reader, opts.connect_timeout_ns);

    var consumed: usize = 0;
    const cmd = self.parser.parse(allocator, info_data, &consumed) catch {
        return error.ProtocolError;
    };

    assert(consumed <= info_data.len);
    reader.toss(consumed);

    if (cmd) |c| {
        switch (c) {
            .info => |parsed_info| {
                // Free old server_info if reconnecting
                if (self.server_info) |*old| {
                    old.deinit(allocator);
                }
                self.server_info = parsed_info;
                self.max_payload = parsed_info.max_payload;
                self.state = .connected;
                self.stats.connects += 1;
            },
            else => return error.UnexpectedCommand,
        }
    } else {
        return error.NoInfoReceived;
    }

    // TLS upgrade: after INFO, before CONNECT (per NATS protocol)
    // Server sends INFO in plain text, then expects TLS handshake if required
    if (self.use_tls and self.tls_client == null) {
        const server_tls =
            if (self.server_info) |info| info.tls_required else false;
        if (server_tls or opts.tls_required or opts.tls_ca_file != null) {
            try self.upgradeTls(allocator, opts);
        }
    }

    // Send CONNECT (now over TLS if upgraded)
    // Re-fetch writer since TLS upgrade may have changed active_writer
    const writer_for_connect = self.active_writer;

    const pass = opts.pass orelse parsed.pass;
    var user = opts.user orelse parsed.user;
    var auth_token = opts.auth_token;

    if (parsed.user != null and parsed.pass == null and opts.user == null) {
        auth_token = parsed.user;
        user = null;
    }

    // Authentication: sign nonce if credentials provided
    // Priority: creds_file > creds > nkey_seed > nkey_seed_file > nkey_sign_fn
    var sig_buf: [86]u8 = undefined;
    var pubkey_buf: [56]u8 = undefined;
    var sig_slice: ?[]const u8 = null;
    var pubkey_slice: ?[]const u8 = null;
    // Buffer for credentials file (must outlive signing operation)
    var creds_buf: [8192]u8 = undefined;
    // Buffer for seed from file (must outlive signing operation)
    var file_seed_buf: [128]u8 = undefined;
    // JWT to send (may come from opts.jwt or parsed credentials)
    var jwt_to_send: ?[]const u8 = opts.jwt;

    if (opts.creds_file) |path| {
        // Load credentials from file (propagates file system errors)
        const creds = try creds_auth.loadFile(self.io, path, &creds_buf);
        jwt_to_send = creds.jwt;

        if (self.server_info.?.nonce) |nonce| {
            var kp = nkey_auth.KeyPair.fromSeed(creds.seed) catch {
                return error.InvalidNKeySeed;
            };
            defer kp.wipe();

            sig_slice = kp.signEncoded(nonce, &sig_buf);
            pubkey_slice = kp.publicKey(&pubkey_buf);
        }
        // Note: creds_buf contains JWT (not secret) and seed.
        // Seed is wiped via kp.wipe(). Buffer on stack gets overwritten.
    } else if (opts.creds) |content| {
        // Parse credentials from provided content
        const creds = try creds_auth.parse(content);
        jwt_to_send = creds.jwt;

        if (self.server_info.?.nonce) |nonce| {
            var kp = nkey_auth.KeyPair.fromSeed(creds.seed) catch {
                return error.InvalidNKeySeed;
            };
            defer kp.wipe();

            sig_slice = kp.signEncoded(nonce, &sig_buf);
            pubkey_slice = kp.publicKey(&pubkey_buf);
        }
    } else if (opts.nkey_seed) |seed| {
        if (self.server_info.?.nonce) |nonce| {
            var kp = nkey_auth.KeyPair.fromSeed(seed) catch {
                return error.InvalidNKeySeed;
            };
            defer kp.wipe();

            sig_slice = kp.signEncoded(nonce, &sig_buf);
            pubkey_slice = kp.publicKey(&pubkey_buf);
        }
    } else if (opts.nkey_seed_file) |path| {
        if (self.server_info.?.nonce) |nonce| {
            const seed = try readSeedFile(self.io, path, &file_seed_buf);
            defer std.crypto.secureZero(u8, file_seed_buf[0..seed.len]);

            var kp = nkey_auth.KeyPair.fromSeed(seed) catch {
                return error.InvalidNKeySeed;
            };
            defer kp.wipe();

            sig_slice = kp.signEncoded(nonce, &sig_buf);
            pubkey_slice = kp.publicKey(&pubkey_buf);
        }
    } else if (opts.nkey_sign_fn) |sign_fn| {
        if (self.server_info.?.nonce) |nonce| {
            var raw_sig: [64]u8 = undefined;
            if (!sign_fn(nonce, &raw_sig)) {
                return error.NKeySigningFailed;
            }
            sig_slice = std.base64.url_safe_no_pad.Encoder.encode(
                &sig_buf,
                &raw_sig,
            );
            pubkey_slice = opts.nkey_pubkey;
        }
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
        .jwt = jwt_to_send,
        .nkey = pubkey_slice,
        .sig = sig_slice,
    };

    protocol.Encoder.encodeConnect(writer_for_connect, connect_opts) catch {
        return error.EncodingFailed;
    };

    writer_for_connect.flush() catch {
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

    const reader = self.active_reader;

    // Brief sleep to allow server to respond with -ERR if auth fails
    self.io.sleep(.fromMilliseconds(100), .awake) catch {};

    // Check if any data is buffered (non-blocking peek)
    const buffered_data = reader.buffered();
    if (buffered_data.len > 0) {
        if (std.mem.startsWith(u8, buffered_data, "-ERR")) {
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

/// Reads from socket with connection timeout using io.select().
/// Returns data or error.ConnectionTimeout if timeout expires.
fn peekWithTimeout(
    self: *Client,
    reader: *Io.Reader,
    timeout_ns: u64,
) ![]u8 {
    assert(timeout_ns > 0);

    // Race read against timeout using io.select()
    var read_future = self.io.async(peekGreedyAsync, .{ reader, self.io });
    var timeout_future = self.io.async(sleepNs, .{ self.io, timeout_ns });

    // Winner-tracking pattern to avoid double-free
    var winner: enum { none, read, timeout } = .none;

    defer if (winner != .read) {
        if (read_future.cancel(self.io)) |_| {} else |_| {}
    };
    defer if (winner != .timeout) {
        timeout_future.cancel(self.io);
    };

    const result = self.io.select(.{
        .read = &read_future,
        .timeout = &timeout_future,
    }) catch {
        return error.ConnectionFailed;
    };

    switch (result) {
        .read => |read_result| {
            winner = .read;
            return read_result catch error.ConnectionFailed;
        },
        .timeout => {
            winner = .timeout;
            return error.ConnectionTimeout;
        },
    }
}

/// Async wrapper for peekGreedy (used with io.async).
fn peekGreedyAsync(reader: *Io.Reader, io: Io) ![]u8 {
    _ = io;
    return reader.peekGreedy(1);
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

    // Validate lengths for backup buffer compatibility
    if (subject.len > defaults.Limits.max_subject_len)
        return error.SubjectTooLong;

    if (queue_group) |qg| {
        if (qg.len > defaults.Limits.max_queue_group_len) {
            return error.QueueGroupTooLong;
        }
    }
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

    const writer = self.active_writer;
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

    const writer = self.active_writer;
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

    const writer = self.active_writer;
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

/// Publishes a message with headers.
///
/// Arguments:
///     subject: Destination subject (no wildcards allowed)
///     hdrs: Header entries to include
///     payload: Message data
///
/// Messages are buffered. Call flush() to ensure delivery.
/// Thread-safe: protected by write_mutex for concurrent publish.
pub fn publishWithHeaders(
    self: *Client,
    subject: []const u8,
    hdrs: []const headers.Entry,
    payload: []const u8,
) !void {
    assert(subject.len > 0);
    assert(hdrs.len > 0);
    if (!self.state.canSend()) {
        return error.NotConnected;
    }
    try pubsub.validatePublish(subject);

    const hdr_size = headers.encodedSize(hdrs);
    if (hdr_size + payload.len > self.max_payload) return error.PayloadTooLarge;

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = self.active_writer;
    protocol.Encoder.encodeHPubWithEntries(writer, .{
        .subject = subject,
        .reply_to = null,
        .headers = hdrs,
        .payload = payload,
    }) catch {
        return error.EncodingFailed;
    };

    self.stats.msgs_out += 1;
    self.stats.bytes_out += payload.len;
}

/// Publishes with headers and reply-to subject.
///
/// Arguments:
///     subject: Destination subject (no wildcards allowed)
///     reply_to: Subject for reply
///     hdrs: Header entries to include
///     payload: Message data
///
/// Thread-safe: protected by write_mutex for concurrent publish.
pub fn publishRequestWithHeaders(
    self: *Client,
    subject: []const u8,
    reply_to: []const u8,
    hdrs: []const headers.Entry,
    payload: []const u8,
) !void {
    assert(subject.len > 0);
    assert(reply_to.len > 0);
    assert(hdrs.len > 0);
    if (!self.state.canSend()) {
        return error.NotConnected;
    }
    try pubsub.validatePublish(subject);
    try pubsub.validateReplyTo(reply_to);

    const hdr_size = headers.encodedSize(hdrs);
    if (hdr_size + payload.len > self.max_payload) return error.PayloadTooLarge;

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = self.active_writer;
    protocol.Encoder.encodeHPubWithEntries(writer, .{
        .subject = subject,
        .reply_to = reply_to,
        .headers = hdrs,
        .payload = payload,
    }) catch {
        return error.EncodingFailed;
    };

    self.stats.msgs_out += 1;
    self.stats.bytes_out += payload.len;
}

/// Publishes with a HeaderMap builder.
///
/// Arguments:
///     allocator: Allocator for temporary header encoding
///     subject: Destination subject (no wildcards allowed)
///     header_map: HeaderMap containing headers to include
///     payload: Message data
///
/// Messages are buffered. Call flush() to ensure delivery.
/// Thread-safe: protected by write_mutex for concurrent publish.
pub fn publishWithHeaderMap(
    self: *Client,
    allocator: Allocator,
    subject: []const u8,
    header_map: *const protocol.HeaderMap,
    payload: []const u8,
) !void {
    assert(subject.len > 0);
    if (header_map.isEmpty()) return error.EmptyHeaders;
    if (!self.state.canSend()) return error.NotConnected;
    try pubsub.validatePublish(subject);

    // Encode headers to NATS format
    const hdr_bytes = try header_map.encode(allocator);
    defer allocator.free(hdr_bytes);

    if (hdr_bytes.len + payload.len > self.max_payload) {
        return error.PayloadTooLarge;
    }

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = self.active_writer;
    protocol.Encoder.encodeHPub(writer, .{
        .subject = subject,
        .reply_to = null,
        .headers = hdr_bytes,
        .payload = payload,
    }) catch {
        return error.EncodingFailed;
    };

    self.stats.msgs_out += 1;
    self.stats.bytes_out += payload.len;
}

/// Publishes a Message object (convenience for republishing/forwarding).
///
/// Arguments:
///     msg: Message to publish (uses subject, data, and headers if present)
///
/// Useful for forwarding received messages or republishing with same content.
/// Messages are buffered. Call flush() to ensure delivery.
/// Thread-safe: protected by write_mutex for concurrent publish.
pub fn publishMsg(self: *Client, msg: *const Message) !void {
    assert(msg.subject.len > 0);
    if (!self.state.canSend()) return error.NotConnected;
    try pubsub.validatePublish(msg.subject);

    // Check payload size (including headers if present)
    const total_size = if (msg.headers) |h|
        h.len + msg.data.len
    else
        msg.data.len;
    if (total_size > self.max_payload) return error.PayloadTooLarge;

    // Acquire write mutex for thread-safe buffer access
    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);

    const writer = self.active_writer;

    if (msg.headers) |hdrs| {
        // Publish with headers using raw header bytes
        protocol.Encoder.encodeHPub(writer, .{
            .subject = msg.subject,
            .reply_to = null,
            .headers = hdrs,
            .payload = msg.data,
        }) catch return error.EncodingFailed;
    } else {
        // Publish without headers
        protocol.Encoder.encodePub(writer, .{
            .subject = msg.subject,
            .reply_to = null,
            .payload = msg.data,
        }) catch return error.EncodingFailed;
    }

    self.stats.msgs_out += 1;
    self.stats.bytes_out += msg.data.len;
}

/// Sends a request with headers and waits for a reply with timeout.
///
/// Arguments:
///     allocator: Allocator for response message
///     subject: Request destination subject
///     hdrs: Header entries to include in request
///     payload: Request data
///     timeout_ms: Maximum time to wait for reply in milliseconds
///
/// Creates a temporary inbox subscription, sends request with reply-to
/// and headers, and waits for response using io.select().
/// Returns null on timeout.
pub fn requestWithHeaders(
    self: *Client,
    allocator: Allocator,
    subject: []const u8,
    hdrs: []const headers.Entry,
    payload: []const u8,
    timeout_ms: u32,
) !?Message {
    assert(subject.len > 0);
    assert(hdrs.len > 0);
    assert(timeout_ms > 0);
    if (!self.state.canSend()) {
        return error.NotConnected;
    }

    // Generate unique inbox for reply (uses configured inbox_prefix)
    const inbox = try self.newInbox(allocator);
    defer allocator.free(inbox);

    // Subscribe to inbox (temporary subscription)
    const sub = try self.subscribe(allocator, inbox);
    defer sub.deinit(allocator);

    // Flush subscription registration before publishing
    try self.flush(allocator);

    // Brief delay to ensure server has registered subscription
    self.io.sleep(.fromMilliseconds(5), .awake) catch {};

    // Publish request with reply-to and headers
    try self.publishRequestWithHeaders(subject, inbox, hdrs, payload);
    try self.flush(allocator);

    // Wait for reply using io.select()
    var response_future = self.io.async(
        Subscription.next,
        .{ sub, allocator, self.io },
    );
    var timeout_future = self.io.async(
        sleepMs,
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

/// Flushes pending writes to the server.
///
/// Sends all buffered data to the TCP socket. This is a simple TCP flush
/// without PING/PONG verification - for maximum performance.
pub fn flush(self: *Client, allocator: Allocator) !void {
    _ = allocator;
    if (!self.state.canSend()) {
        return error.NotConnected;
    }

    try self.write_mutex.lock(self.io);
    defer self.write_mutex.unlock(self.io);
    self.active_writer.flush() catch return error.WriteFailed;

    // TLS: active_writer.flush() only encrypts to TCP buffer.
    // Must also flush the underlying TCP writer to send to network.
    if (self.use_tls) {
        self.writer.interface.flush() catch return error.WriteFailed;
    }
}

/// Sends all buffered data with a timeout.
/// Returns error.Timeout if the flush doesn't complete in time.
pub fn flushTimeout(
    self: *Client,
    allocator: Allocator,
    timeout_ns: u64,
) !void {
    assert(timeout_ns > 0);
    if (!self.state.canSend()) {
        return error.NotConnected;
    }

    var flush_future = self.io.async(flushHelper, .{ self, allocator });
    var timeout_future = self.io.async(sleepNs, .{ self.io, timeout_ns });

    var winner: enum { none, flush, timeout } = .none;

    defer if (winner != .flush) {
        _ = flush_future.cancel(self.io);
    };
    defer if (winner != .timeout) {
        timeout_future.cancel(self.io);
    };

    const select_result = self.io.select(.{
        .flush = &flush_future,
        .timeout = &timeout_future,
    }) catch |err| {
        if (err == error.Canceled) return error.Canceled;
        return err;
    };

    switch (select_result) {
        .flush => |result| {
            winner = .flush;
            return result;
        },
        .timeout => {
            winner = .timeout;
            return error.Timeout;
        },
    }
}

/// Helper for async flush.
fn flushHelper(self: *Client, allocator: Allocator) !void {
    return self.flush(allocator);
}

/// Forces an immediate reconnection attempt.
/// Closes the current connection and triggers reconnection logic.
/// Subscriptions will be restored automatically.
pub fn forceReconnect(self: *Client) !void {
    const state = State.atomicLoad(&self.state);
    if (state == .closed) return error.ConnectionClosed;
    if (state == .reconnecting) return; // Already reconnecting
    if (state != .connected) return error.NotConnected;

    assert(self.next_sid >= 1);

    // Close the socket - io_task will detect and start reconnection
    self.stream.close(self.io);
    State.atomicStore(&self.state, .reconnecting);
}

/// Gracefully drains with a timeout.
/// Returns error.Timeout if drain doesn't complete in time.
pub fn drainTimeout(
    self: *Client,
    allocator: Allocator,
    timeout_ns: u64,
) !DrainResult {
    assert(timeout_ns > 0);
    if (self.state != .connected) {
        return error.NotConnected;
    }

    var drain_future = self.io.async(drainHelper, .{ self, allocator });
    var timeout_future = self.io.async(sleepNs, .{ self.io, timeout_ns });

    var winner: enum { none, drain, timeout } = .none;

    defer if (winner != .drain) {
        _ = drain_future.cancel(self.io);
    };
    defer if (winner != .timeout) {
        timeout_future.cancel(self.io);
    };

    const select_result = self.io.select(.{
        .drain = &drain_future,
        .timeout = &timeout_future,
    }) catch |err| {
        if (err == error.Canceled) return error.Canceled;
        return err;
    };

    switch (select_result) {
        .drain => |result| {
            winner = .drain;
            return result;
        },
        .timeout => {
            winner = .timeout;
            return error.Timeout;
        },
    }
}

/// Helper for async drain.
fn drainHelper(self: *Client, allocator: Allocator) !DrainResult {
    return self.drain(allocator);
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

    // Generate unique inbox for reply (uses configured inbox_prefix)
    const inbox = try self.newInbox(allocator);
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
        sleepMs,
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

/// Sends a request using a Message object and waits for a reply.
///
/// Arguments:
///     allocator: Allocator for response message
///     msg: Message to send (uses subject, data, and headers if present)
///     timeout_ms: Maximum time to wait for reply in milliseconds
///
/// Useful for forwarding request messages or republishing with same content.
/// Creates a temporary inbox subscription, sends request with reply-to,
/// and waits for response. Returns null on timeout.
pub fn requestMsg(
    self: *Client,
    allocator: Allocator,
    msg: *const Message,
    timeout_ms: u32,
) !?Message {
    assert(msg.subject.len > 0);
    assert(timeout_ms > 0);
    if (!self.state.canSend()) return error.NotConnected;

    // Generate unique inbox for reply
    const inbox = try self.newInbox(allocator);
    defer allocator.free(inbox);

    // Subscribe to inbox (temporary subscription)
    const sub = try self.subscribe(allocator, inbox);
    defer sub.deinit(allocator);

    // Flush subscription registration before publishing
    try self.flush(allocator);

    // Brief delay to ensure server has registered subscription
    self.io.sleep(.fromMilliseconds(5), .awake) catch {};

    // Publish request with reply-to (with or without headers)
    try self.write_mutex.lock(self.io);
    {
        defer self.write_mutex.unlock(self.io);
        const writer = self.active_writer;

        if (msg.headers) |hdrs| {
            protocol.Encoder.encodeHPub(writer, .{
                .subject = msg.subject,
                .reply_to = inbox,
                .headers = hdrs,
                .payload = msg.data,
            }) catch return error.EncodingFailed;
        } else {
            protocol.Encoder.encodePub(writer, .{
                .subject = msg.subject,
                .reply_to = inbox,
                .payload = msg.data,
            }) catch return error.EncodingFailed;
        }

        self.stats.msgs_out += 1;
        self.stats.bytes_out += msg.data.len;
    }
    try self.flush(allocator);

    // Wait for reply using io.select()
    var response_future = self.io.async(
        Subscription.next,
        .{ sub, allocator, self.io },
    );
    var timeout_future = self.io.async(
        sleepMs,
        .{ self.io, timeout_ms },
    );

    var winner: enum { none, response, timeout } = .none;

    defer if (winner != .response) {
        if (response_future.cancel(self.io)) |reply| {
            reply.deinit(allocator);
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
                if (err == error.Canceled or err == error.Closed) return null;
                return err;
            };
        },
        .timeout => {
            winner = .timeout;
            return null;
        },
    }
}

/// Sleep helper for timeouts (milliseconds).
fn sleepMs(io: Io, timeout_ms: u32) void {
    io.sleep(.fromMilliseconds(timeout_ms), .awake) catch {};
}

/// Helper for connection timeout (nanoseconds).
fn sleepNs(io: Io, timeout_ns: u64) void {
    io.sleep(.fromNanoseconds(timeout_ns), .awake) catch {};
}

/// Reads NKey seed from file, trimming whitespace.
/// Returns slice into buf containing the seed.
/// File system errors (FileNotFound, AccessDenied, etc.) propagate directly.
/// Returns InvalidNKeySeedFile only for content issues (empty/whitespace-only).
fn readSeedFile(io: Io, path: []const u8, buf: *[128]u8) ![]const u8 {
    assert(path.len > 0);

    const data = try Io.Dir.readFile(.cwd(), io, path, buf);

    if (data.len == 0) return error.InvalidNKeySeedFile;
    assert(data.len > 0);

    // Trim leading/trailing whitespace
    var start: usize = 0;
    var end: usize = data.len;

    while (start < end and std.ascii.isWhitespace(buf[start])) {
        start += 1;
    }
    while (end > start and std.ascii.isWhitespace(buf[end - 1])) {
        end -= 1;
    }

    if (start >= end) return error.InvalidNKeySeedFile;
    assert(start < end);
    return buf[start..end];
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
    const writer = self.active_writer;

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

            // Mark subscription state - sub.deinit() frees resources
            sub.state = .unsubscribed;
            sub.client_destroyed = true;
        }
    }

    self.read_mutex.unlock(self.io);

    // I/O operations after mutex released
    writer.flush() catch {
        result.flush_failed = true;
    };
    self.state = .draining;
    self.pushEvent(.{ .draining = {} });

    self.stream.close(self.io);
    self.state = .closed;

    if (self.server_info) |*info| {
        info.deinit(alloc);
        self.server_info = null;
    }

    // Push err event if drain had failures
    if (!result.isClean()) {
        self.pushEvent(.{
            .err = .{
                .err = events_mod.Error.DrainIncomplete,
                .msg = null,
            },
        });
    }

    return result;
}

/// Returns true if connected.
pub fn isConnected(self: *const Client) bool {
    assert(self.next_sid >= 1);
    // Use atomic load for cross-thread visibility (io_task may update state)
    return @atomicLoad(State, &self.state, .acquire) == .connected;
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

/// Returns true if connection is using TLS.
pub fn isTls(self: *const Client) bool {
    assert(self.next_sid >= 1);
    return self.use_tls;
}

/// Returns true if TCP_NODELAY was successfully set.
pub fn isTcpNoDelaySet(self: *const Client) bool {
    return self.tcp_nodelay_set;
}

/// Returns true if TCP receive buffer was successfully set.
pub fn isTcpRcvBufSet(self: *const Client) bool {
    return self.tcp_rcvbuf_set;
}

// =========================================================================
// Connection Info Getters
// =========================================================================

/// Returns the currently connected server URL.
/// Returns the original URL used to connect, or null if not connected.
pub fn getConnectedUrl(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.original_url_len == 0) return null;
    return self.original_url[0..self.original_url_len];
}

/// Returns the connected server's unique ID.
/// This is the `server_id` from the INFO response.
pub fn getConnectedServerId(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        if (info.server_id.len > 0) return info.server_id;
    }
    return null;
}

/// Returns the connected server's name.
/// This is the `server_name` from the INFO response.
pub fn getConnectedServerName(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        if (info.server_name.len > 0) return info.server_name;
    }
    return null;
}

/// Returns the connected server's version string.
/// This is the `version` from the INFO response (e.g., "2.10.0").
pub fn getConnectedServerVersion(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        if (info.version.len > 0) return info.version;
    }
    return null;
}

/// Checks if the server version meets minimum requirements.
///
/// Arguments:
///     min_major: Minimum major version required
///     min_minor: Minimum minor version required
///     min_patch: Minimum patch version required
///
/// Returns true if server version >= min_major.min_minor.min_patch.
/// Returns false if not connected or version cannot be parsed.
///
/// Example: `client.checkCompatibility(2, 10, 0)` checks for NATS 2.10.0+.
pub fn checkCompatibility(
    self: *const Client,
    min_major: u16,
    min_minor: u16,
    min_patch: u16,
) bool {
    assert(self.next_sid >= 1);
    const version = self.getConnectedServerVersion() orelse return false;

    // Parse version string (e.g., "2.10.0" or "2.10.0-beta")
    var parts = std.mem.splitScalar(u8, version, '.');
    const major_str = parts.next() orelse return false;
    const minor_str = parts.next() orelse return false;
    const patch_str = parts.next() orelse "0";

    // Parse major
    const major = std.fmt.parseInt(u16, major_str, 10) catch return false;

    // Parse minor
    const minor = std.fmt.parseInt(u16, minor_str, 10) catch return false;

    // Parse patch (strip suffix like "-beta" if present)
    var patch_clean = patch_str;
    if (std.mem.indexOfScalar(u8, patch_str, '-')) |idx| {
        patch_clean = patch_str[0..idx];
    }
    const patch = std.fmt.parseInt(u16, patch_clean, 10) catch 0;

    // Compare: major > min OR (major == min AND minor > min) OR ...
    if (major > min_major) return true;
    if (major < min_major) return false;
    if (minor > min_minor) return true;
    if (minor < min_minor) return false;
    return patch >= min_patch;
}

/// Returns the maximum payload size allowed by the server.
/// Defaults to 1MB if not yet connected.
pub fn getMaxPayload(self: *const Client) usize {
    assert(self.next_sid >= 1);
    return self.max_payload;
}

/// Returns true if the server supports message headers (NATS 2.2+).
pub fn headersSupported(self: *const Client) bool {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.headers;
    }
    return false;
}

/// Returns the number of known servers in the connection pool.
/// This includes the original server and any discovered via cluster INFO.
pub fn getServerCount(self: *const Client) u8 {
    assert(self.next_sid >= 1);
    if (self.server_pool_initialized) {
        return self.server_pool.serverCount();
    }
    return 0;
}

/// Returns a server URL from the pool at the given index.
/// Use with getServerCount() to iterate all known servers.
pub fn getServerUrl(self: *const Client, index: u8) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (!self.server_pool_initialized) return null;
    if (index >= self.server_pool.count) return null;
    return self.server_pool.servers[index].getUrl();
}

/// Returns the count of discovered servers from cluster INFO.
/// These are additional servers beyond the original connection URL.
pub fn getDiscoveredServerCount(self: *const Client) u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.connect_urls_count;
    }
    return 0;
}

/// Returns a discovered server URL at the given index.
/// Use with getDiscoveredServerCount() to iterate discovered servers.
pub fn getDiscoveredServerUrl(self: *const Client, index: u8) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.getConnectUrl(index);
    }
    return null;
}

/// Returns the connected server's cluster name.
/// This is the `cluster` from the INFO response.
pub fn getConnectedClusterName(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.cluster;
    }
    return null;
}

/// Returns true if the server requires authentication.
/// Derived from `auth_required` in the INFO response.
pub fn authRequired(self: *const Client) bool {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.auth_required;
    }
    return false;
}

/// Returns true if the server requires TLS.
/// Derived from `tls_required` in the INFO response.
pub fn tlsRequired(self: *const Client) bool {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.tls_required;
    }
    return false;
}

/// Returns the client name from options.
/// This is the name used in the CONNECT command.
pub fn getName(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    return self.options.name;
}

/// Returns the client ID assigned by the server.
/// This is the `client_id` from the INFO response.
pub fn getClientID(self: *const Client) ?u64 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.client_id;
    }
    return null;
}

/// Returns the client IP as seen by the server.
/// This is the `client_ip` from the INFO response.
pub fn getClientIP(self: *const Client) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        return info.client_ip;
    }
    return null;
}

/// Returns the connected server address as "host:port" string.
/// Writes to the provided buffer and returns the slice.
/// Returns null if not connected or buffer too small.
pub fn getConnectedAddr(
    self: *const Client,
    buf: []u8,
) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.server_info) |info| {
        if (info.host.len == 0) return null;
        assert(info.port > 0);

        // Format "host:port" into buffer
        const result = std.fmt.bufPrint(buf, "{s}:{d}", .{
            info.host,
            info.port,
        }) catch return null;
        return result;
    }
    return null;
}

/// Returns the connected URL with password redacted.
/// Replaces password with "***" for safe logging.
/// Writes to the provided buffer and returns the slice.
pub fn getConnectedUrlRedacted(
    self: *const Client,
    buf: []u8,
) ?[]const u8 {
    assert(self.next_sid >= 1);
    if (self.original_url_len == 0) return null;

    const url = self.original_url[0..self.original_url_len];

    // Check if URL has credentials (look for @ in URL)
    const at_pos = std.mem.indexOf(u8, url, "@") orelse {
        // No credentials, return as-is
        if (buf.len < url.len) return null;
        @memcpy(buf[0..url.len], url);
        return buf[0..url.len];
    };

    // Find protocol prefix (nats:// or tls://)
    var prefix_len: usize = 0;
    if (std.mem.startsWith(u8, url, "nats://")) {
        prefix_len = 7;
    } else if (std.mem.startsWith(u8, url, "tls://")) {
        prefix_len = 6;
    }

    const auth_part = url[prefix_len..at_pos];
    const colon_pos = std.mem.indexOf(u8, auth_part, ":") orelse {
        // No password (just token or user), return as-is
        if (buf.len < url.len) return null;
        @memcpy(buf[0..url.len], url);
        return buf[0..url.len];
    };

    // Redact password: "user:pass@host" -> "user:***@host"
    const user = auth_part[0..colon_pos];
    const host_part = url[at_pos..];
    const redacted_pass = "***";

    const new_len = prefix_len + user.len + 1 + redacted_pass.len + host_part.len;
    if (buf.len < new_len) return null;

    var pos: usize = 0;
    @memcpy(buf[pos..][0..prefix_len], url[0..prefix_len]);
    pos += prefix_len;
    @memcpy(buf[pos..][0..user.len], user);
    pos += user.len;
    buf[pos] = ':';
    pos += 1;
    @memcpy(buf[pos..][0..redacted_pass.len], redacted_pass);
    pos += redacted_pass.len;
    @memcpy(buf[pos..][0..host_part.len], host_part);
    pos += host_part.len;

    assert(pos == new_len);
    return buf[0..new_len];
}

/// Last error info returned by getLastError().
pub const LastErrorInfo = struct {
    err: anyerror,
    msg: ?[]const u8,
};

/// Returns the last async error that occurred on the connection.
/// This includes server -ERR messages and other async errors.
/// The error message is from the server (e.g., permission violation).
/// Returns null if no error has occurred since last clear.
pub fn getLastError(self: *const Client) ?LastErrorInfo {
    assert(self.next_sid >= 1);
    if (self.last_error) |err| {
        const msg: ?[]const u8 = if (self.last_error_msg_len > 0)
            self.last_error_msg[0..self.last_error_msg_len]
        else
            null;
        return .{ .err = err, .msg = msg };
    }
    return null;
}

/// Clears the last error.
/// Call after handling the error to reset state.
pub fn clearLastError(self: *Client) void {
    assert(self.next_sid >= 1);
    self.last_error = null;
    self.last_error_msg_len = 0;
}

// =========================================================================
// Connection State Methods
// =========================================================================

/// Returns the current connection state.
/// Thread-safe: uses atomic load for cross-thread visibility.
pub fn getStatus(self: *const Client) State {
    assert(self.next_sid >= 1);
    return State.atomicLoad(&self.state);
}

/// Returns true if the connection is permanently closed.
/// Once closed, the client cannot be reconnected.
pub fn isClosed(self: *const Client) bool {
    assert(self.next_sid >= 1);
    return State.atomicLoad(&self.state) == .closed;
}

/// Returns true if the connection is draining.
/// During drain, no new subscriptions allowed but existing messages are delivered.
pub fn isDraining(self: *const Client) bool {
    assert(self.next_sid >= 1);
    return State.atomicLoad(&self.state) == .draining;
}

/// Returns true if the connection is attempting to reconnect.
pub fn isReconnecting(self: *const Client) bool {
    assert(self.next_sid >= 1);
    return State.atomicLoad(&self.state) == .reconnecting;
}

/// Returns the number of active subscriptions.
pub fn numSubscriptions(self: *const Client) usize {
    assert(self.next_sid >= 1);
    // free_count tracks available slots; total - free = active
    return MAX_SUBSCRIPTIONS - self.free_count;
}

/// Measures round-trip time to the server by sending PING and waiting for PONG.
/// Returns RTT in nanoseconds.
/// This is a blocking operation that waits for the server to respond.
pub fn getRtt(self: *Client) !u64 {
    if (!self.state.canSend()) return error.NotConnected;
    assert(self.next_sid >= 1);

    // Record start time
    const start_ns = getNowNs() catch return error.TimerUnavailable;

    // Send PING
    try self.write_mutex.lock(self.io);
    self.active_writer.writeAll("PING\r\n") catch {
        self.write_mutex.unlock(self.io);
        return error.WriteFailed;
    };
    self.active_writer.flush() catch {
        self.write_mutex.unlock(self.io);
        return error.WriteFailed;
    };
    if (self.use_tls) {
        self.writer.interface.flush() catch {
            self.write_mutex.unlock(self.io);
            return error.WriteFailed;
        };
    }
    self.write_mutex.unlock(self.io);

    // Wait for PONG - poll last_pong_received_ns
    // io_task handles PONG and updates the timestamp
    const old_pong_ns = self.last_pong_received_ns.load(.acquire);
    const timeout_ns: u64 = 5_000_000_000; // 5 second timeout
    var check_counter: u32 = 0;

    while (true) {
        const current_pong_ns = self.last_pong_received_ns.load(.acquire);
        if (current_pong_ns > old_pong_ns) {
            // PONG received - calculate RTT
            const end_ns = getNowNs() catch return error.TimerUnavailable;
            return end_ns - start_ns;
        }

        // Check for timeout periodically
        check_counter +%= 1;
        if (check_counter >= defaults.Spin.timeout_check_iterations) {
            check_counter = 0;
            const now_ns = getNowNs() catch return error.TimerUnavailable;
            if (now_ns - start_ns >= timeout_ns) {
                return error.Timeout;
            }
        }

        std.atomic.spinLoopHint();
    }
}

/// Generates a new unique inbox subject using the configured prefix.
/// Caller owns returned memory.
pub fn newInbox(self: *Client, allocator: Allocator) ![]u8 {
    assert(self.next_sid >= 1);
    const prefix = self.options.inbox_prefix;
    const random_len = 22;
    const total_len = prefix.len + 1 + random_len; // prefix.random

    const result = try allocator.alloc(u8, total_len);
    @memcpy(result[0..prefix.len], prefix);
    result[prefix.len] = '.';

    // Fill random portion with base62 characters
    const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" ++
        "abcdefghijklmnopqrstuvwxyz";
    self.io.random(result[prefix.len + 1 ..]);
    for (result[prefix.len + 1 ..]) |*b| {
        b.* = alphabet[@mod(b.*, alphabet.len)];
    }

    return result;
}

/// Reset rate-limit counters, allowing errors to re-trigger events.
/// Call this if you want immediate re-notification of ongoing errors.
/// Resets both subscription alloc_failed and client protocol_error thresholds.
pub fn resetErrorNotifications(self: *Client) void {
    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            sub.last_alloc_notified_at = 0;
        }
    }
    self.last_parse_error_notified_at = 0;
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

/// Sends PONG response.
fn sendPong(self: *Client) !void {
    assert(self.state.canSend());
    const writer = self.active_writer;
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
    const writer = self.active_writer;
    writer.writeAll("PING\r\n") catch {
        return error.WriteFailed;
    };
    writer.flush() catch {
        return error.WriteFailed;
    };
    const now = getNowNs() catch self.last_ping_sent_ns.load(.monotonic);
    self.last_ping_sent_ns.store(now, .monotonic);
    const new_outstanding = self.pings_outstanding.fetchAdd(1, .monotonic) + 1;
    dbg.pingPong("PING_SENT", new_outstanding);
}

/// Handles PONG response from server.
fn handlePong(self: *Client) void {
    const now = getNowNs() catch self.last_pong_received_ns.load(.monotonic);
    self.last_pong_received_ns.store(now, .monotonic);
    self.pings_outstanding.store(0, .monotonic);
    dbg.pingPong("PONG_RECEIVED", 0);
}

/// Checks connection health, sends PING if needed.
/// Returns true if connection is stale (should trigger disconnect).
/// Called from io_task loop with throttling.
pub fn checkHealthAndDetectStale(self: *Client) bool {
    if (self.options.ping_interval_ms == 0) return false;
    if (self.state != .connected) return false;

    const now_ns = getNowNs() catch return false;
    const interval_ns: u64 =
        @as(u64, self.options.ping_interval_ms) * 1_000_000;

    // Check if too many PINGs outstanding (connection stale)
    const outstanding = self.pings_outstanding.load(.monotonic);
    if (outstanding >= self.options.max_pings_outstanding) {
        dbg.print(
            "[HEALTH] STALE: pings_outstanding={d} >= max={d}",
            .{ outstanding, self.options.max_pings_outstanding },
        );
        return true;
    }

    // Check if it's time to send PING
    const last_ping = self.last_ping_sent_ns.load(.monotonic);
    if (now_ns - last_ping >= interval_ns) {
        dbg.print(
            "[HEALTH] Sending PING, pings_outstanding={d}",
            .{outstanding},
        );
        self.sendPing() catch |err| {
            dbg.print("[HEALTH] PING failed: {s}", .{@errorName(err)});
            // Write failure indicates disconnection
            if (err == error.BrokenPipe or err == error.ConnectionResetByPeer) {
                return true;
            }
        };
    }

    return false;
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

    // 4. Cancel callback task and free event queue
    // SAFETY: Set event_queue = null BEFORE canceling to signal callback_task
    // to exit. This prevents use-after-free if callback_task is mid-loop.
    const eq = self.event_queue;
    self.event_queue = null; // Signal callback_task to exit
    if (eq) |queue| {
        // Push closed event so callback_task can dispatch final onClose
        _ = queue.push(.{ .closed = {} });
    }
    if (self.callback_task_future) |*future| {
        _ = future.cancel(self.io);
        self.callback_task_future = null;
    }
    // Now safe to free - callback_task has exited (state=closed + null check)
    if (eq) |queue| {
        alloc.destroy(queue);
    }
    if (self.event_queue_buf) |buf| {
        alloc.free(buf);
        self.event_queue_buf = null;
    }

    // 5. Cleanup subscriptions (io_task is now gone)
    self.closeAllQueues();
    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            sub.state = .unsubscribed;
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

    // Free TLS resources
    self.tls_client = null;
    if (self.tls_read_buffer) |buf| {
        alloc.free(buf);
        self.tls_read_buffer = null;
    }
    if (self.tls_write_buffer) |buf| {
        alloc.free(buf);
        self.tls_write_buffer = null;
    }
    if (self.ca_bundle) |*bundle| {
        bundle.deinit(alloc);
        self.ca_bundle = null;
    }

    alloc.free(self.read_buffer);
    alloc.free(self.write_buffer);
    alloc.destroy(self);
}

// -- Reconnection Support

/// Backup all active subscriptions for restoration after reconnect.
/// Stores SID, subject, and queue_group in inline buffers (no allocation).
/// Returns error if any subject > 256 bytes or queue_group > 64 bytes.
pub fn backupSubscriptions(self: *Client) error{SubjectTooLong}!void {
    self.sub_backup_count = 0;

    for (self.sub_ptrs) |maybe_sub| {
        if (maybe_sub) |sub| {
            if (sub.state != .active) continue;
            if (self.sub_backup_count >= MAX_SUBSCRIPTIONS) break;

            // Validate lengths - reject truncation
            if (sub.subject.len > defaults.Limits.max_subject_len) {
                self.pushEvent(.{
                    .err = .{
                        .err = events_mod.Error.SubjectTooLong,
                        .msg = null,
                    },
                });
                return error.SubjectTooLong;
            }
            if (sub.queue_group) |qg| {
                if (qg.len > defaults.Limits.max_queue_group_len) {
                    self.pushEvent(.{
                        .err = .{
                            .err = events_mod.Error.QueueGroupTooLong,
                            .msg = null,
                        },
                    });
                    return error.SubjectTooLong;
                }
            }

            var backup = &self.sub_backups[self.sub_backup_count];
            backup.sid = sub.sid;

            // Copy subject (validated above)
            const subj_len: u8 = @intCast(sub.subject.len);
            @memcpy(backup.subject_buf[0..subj_len], sub.subject);
            backup.subject_len = subj_len;

            // Copy queue_group if present (validated above)
            if (sub.queue_group) |qg| {
                const qg_len: u8 = @intCast(qg.len);
                @memcpy(backup.queue_group_buf[0..qg_len], qg);
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

    const writer = self.active_writer;

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
    if (self.pending_buffer != null) return;

    self.pending_buffer = try allocator.alloc(
        u8,
        self.options.pending_buffer_size,
    );
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
    self.active_writer.writeAll(buf[0..self.pending_buffer_pos]) catch {
        return error.WriteFailed;
    };
    self.active_writer.flush() catch return error.WriteFailed;
    self.pending_buffer_pos = 0;
    dbg.pendingBuffer("FLUSHED", 0, self.pending_buffer_capacity);
}

/// Cleanup client state for reconnection.
/// Closes old stream but preserves subscriptions and pending buffer.
fn cleanupForReconnect(self: *Client) void {
    dbg.stateChange("connected", "reconnecting");

    // Close old stream
    self.stream.close(self.io);

    // Reset TLS state (reuse buffers and CA bundle)
    self.tls_client = null;

    // Clear server info (will be refreshed on reconnect)
    // Don't free - server sends new info on reconnect
}

/// Attempt connection to a single server.
/// Returns true on success, error on failure.
pub fn tryConnect(
    self: *Client,
    allocator: Allocator,
    server: *connection.server_pool.Server,
) !void {
    const raw_host = server.getHost();
    const port = server.port;

    dbg.reconnectEvent(
        "CONNECTING",
        self.reconnect_attempt + 1,
        server.getUrl(),
    );

    // Convert "localhost" to IP (IpAddress.parse only handles numeric IPs)
    const host = if (std.mem.eql(u8, raw_host, "localhost"))
        "127.0.0.1"
    else
        raw_host;

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
    // Reset to TCP reader/writer (upgradeTls will update if needed)
    self.active_reader = &self.reader.interface;
    self.active_writer = &self.writer.interface;

    // Parse URL to get auth info and TLS flag
    const parsed = parseUrl(server.getUrl()) catch ParsedUrl{
        .host = host,
        .port = port,
        .user = null,
        .pass = null,
        .use_tls = self.use_tls,
    };

    // Update TLS host for reconnection (server might have different hostname)
    if (self.use_tls) {
        const actual_host = server.getHost();
        if (actual_host.len > 0 and actual_host.len <= 255) {
            const host_len: u8 = @intCast(actual_host.len);
            @memcpy(self.tls_host[0..host_len], actual_host);
            self.tls_host_len = host_len;
        }
    }

    // TLS-first mode: upgrade to TLS before NATS protocol
    if (self.use_tls and self.options.tls_handshake_first) {
        try self.upgradeTls(allocator, self.options);
    }

    // Perform handshake (includes TLS upgrade after INFO if needed)
    try self.handshake(allocator, self.options, parsed);

    // Initialize health check timestamps (atomics)
    const now_ns = getNowNs() catch 0;
    self.last_ping_sent_ns.store(now_ns, .monotonic);
    self.last_pong_received_ns.store(now_ns, .monotonic);
    self.pings_outstanding.store(0, .monotonic);

    dbg.reconnectEvent(
        "CONNECTED",
        self.reconnect_attempt + 1,
        server.getUrl(),
    );
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

    dbg.print(
        "Backoff wait: {d}ms (attempt {d})",
        .{ final_wait, attempt + 1 },
    );
    self.io.sleep(.fromMilliseconds(final_wait), .awake) catch {};
}

/// Attempt reconnection with exponential backoff.
/// Can be called automatically (from io_task) or manually by user.
pub fn reconnect(self: *Client, allocator: Allocator) !void {
    if (self.state != .disconnected and self.state != .reconnecting) {
        if (self.state == .connected) return;
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

    // Backup subscriptions before cleanup (error = subs won't restore)
    self.backupSubscriptions() catch |err| {
        dbg.print("backupSubscriptions failed: {s}", .{@errorName(err)});
        // Error event already pushed by backupSubscriptions
    };
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
            // Connection succeeded
            self.restoreSubscriptions() catch |err| {
                dbg.print(
                    "Failed to restore subscriptions: {s}",
                    .{@errorName(err)},
                );
                // Notify user - subscriptions may be broken, they can re-sub
                self.pushEvent(.{
                    .err = .{
                        .err = events_mod.Error.SubscriptionRestoreFailed,
                        .msg = null,
                    },
                });
            };
            self.flushPendingBuffer() catch |err| {
                dbg.print(
                    "Failed to flush pending buffer: {s}",
                    .{@errorName(err)},
                );
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
            dbg.reconnectEvent(
                "FAILED",
                self.reconnect_attempt + 1,
                server.getUrl(),
            );
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
    /// msgs_in value when alloc_failed event was last pushed (rate-limit).
    last_alloc_notified_at: u64 = 0,

    // Auto-unsubscribe support
    /// Maximum messages before auto-unsubscribe. Null = no limit.
    /// When set, subscription auto-unsubscribes after this many messages.
    max_msgs: ?u64 = null,
    /// Count of delivered messages for auto-unsubscribe tracking.
    /// Only tracked when max_msgs is set (opt-in for performance).
    delivered_count: u64 = 0,
    /// Flag set when auto-unsubscribe triggered (for io_task to send UNSUB).
    auto_unsub_triggered: bool = false,

    // Pending limits (flow control)
    /// Maximum pending messages allowed in queue. 0 = no limit.
    pending_limit: usize = 0,

    // Pending bytes tracking (for statistics)
    /// Current bytes pending in queue. Updated by io_task on push.
    pending_bytes: u64 = 0,
    /// High watermark for pending message count.
    max_pending_msgs: u64 = 0,
    /// High watermark for pending bytes.
    max_pending_bytes: u64 = 0,

    /// Blocks until a message is available or connection is closed.
    ///
    /// Arguments:
    ///     allocator: Allocator for owned message copy
    ///     io: Io interface for blocking operations
    /// Blocks until a message arrives on this subscription.
    ///
    /// The background io_task handles all socket I/O and routes messages
    /// to subscription queues. This function blocks on the queue.
    /// Lock-free spin-wait for message.
    ///
    /// Returns owned Message that caller must free via msg.deinit(allocator).
    pub fn next(self: *Subscription, allocator: Allocator, io: Io) !Message {
        _ = allocator;
        assert(self.state == .active or self.state == .draining);

        // Hybrid spin-yield: spin for fast path, yield for cancellation support
        var spin_count: u32 = 0;

        while (true) {
            if (self.queue.pop()) |msg| {
                // Use backing_buf.len for speed (avoids 4 field accesses)
                const msg_size = if (msg.backing_buf) |buf| buf.len else msg.size();
                self.pending_bytes -|= msg_size;
                return msg;
            }
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }

            spin_count += 1;
            if (spin_count < defaults.Spin.max_spins) {
                std.atomic.spinLoopHint();
            } else {
                // Yield to I/O runtime - enables cancellation
                io.sleep(.fromNanoseconds(0), .awake) catch |err| {
                    if (err == error.Canceled) return error.Canceled;
                };
                spin_count = 0;
            }
        }
    }

    /// Try receive without blocking. Returns null if no message available.
    pub fn tryNext(self: *Subscription) ?Message {
        if (self.queue.pop()) |msg| {
            const msg_size = if (msg.backing_buf) |buf| buf.len else msg.size();
            self.pending_bytes -|= msg_size;
            return msg;
        }
        return null;
    }

    /// Batch receive - waits for at least 1, returns up to buf.len.
    pub fn nextBatch(self: *Subscription, io: Io, buf: []Message) !usize {
        assert(self.state == .active or self.state == .draining);
        assert(buf.len > 0);

        // Hybrid spin-yield: spin for fast path, yield for cancellation support
        var spin_count: u32 = 0;

        while (true) {
            const count = self.queue.popBatch(buf);
            if (count > 0) {
                // Decrement pending bytes for all popped messages
                for (buf[0..count]) |msg| {
                    const msg_size = if (msg.backing_buf) |buf_| buf_.len else msg.size();
                    self.pending_bytes -|= msg_size;
                }
                return count;
            }
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }

            spin_count += 1;
            if (spin_count < defaults.Spin.max_spins) {
                std.atomic.spinLoopHint();
            } else {
                // Yield to I/O runtime - enables cancellation
                io.sleep(.fromNanoseconds(0), .awake) catch |err| {
                    if (err == error.Canceled) return error.Canceled;
                };
                spin_count = 0;
            }
        }
    }

    /// Non-blocking batch receive.
    pub fn tryNextBatch(self: *Subscription, buf: []Message) usize {
        const count = self.queue.popBatch(buf);
        for (buf[0..count]) |msg| {
            const msg_size = if (msg.backing_buf) |buf_| buf_.len else msg.size();
            self.pending_bytes -|= msg_size;
        }
        return count;
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
            if (self.queue.pop()) |msg| {
                const msg_size = if (msg.backing_buf) |buf| buf.len else msg.size();
                self.pending_bytes -|= msg_size;
                return msg;
            }
            return null;
        };
        const timeout_ns: u64 = @as(u64, timeout_ms) * std.time.ns_per_ms;
        var check_counter: u32 = 0;

        while (true) {
            if (self.queue.pop()) |msg| {
                const msg_size = if (msg.backing_buf) |buf| buf.len else msg.size();
                self.pending_bytes -|= msg_size;
                return msg;
            }
            if (self.state != .active and self.state != .draining) {
                return error.Closed;
            }

            // Check timeout only periodically to reduce syscalls
            check_counter +%= 1;
            if (check_counter >= defaults.Spin.timeout_check_iterations) {
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

    // =========================================================================
    // Subscription Control Methods
    // =========================================================================

    /// Auto-unsubscribe after receiving max messages.
    /// Sends UNSUB with max_msgs to server for server-side enforcement.
    /// Client also tracks and triggers local cleanup.
    pub fn autoUnsubscribe(self: *Subscription, max: u64) !void {
        assert(max > 0);
        if (self.state != .active) return error.InvalidState;
        if (self.client_destroyed) return error.InvalidState;

        self.max_msgs = max;
        self.delivered_count = 0;

        // Send UNSUB with max_msgs to server (server tracks count too)
        const client = self.client;
        if (client.state.canSend()) {
            try client.write_mutex.lock(client.io);
            defer client.write_mutex.unlock(client.io);

            const writer = client.active_writer;
            protocol.Encoder.encodeUnsub(writer, .{
                .sid = self.sid,
                .max_msgs = max,
            }) catch return error.EncodingFailed;
        }
    }

    /// Gracefully drain this subscription.
    /// Stops receiving new messages but delivers already-queued messages.
    pub fn drain(self: *Subscription) !void {
        if (self.state != .active) return;
        if (self.client_destroyed) return error.InvalidState;

        self.state = .draining;

        // Send UNSUB to stop new messages (no max_msgs = immediate unsub)
        const client = self.client;
        if (client.state.canSend()) {
            try client.write_mutex.lock(client.io);
            defer client.write_mutex.unlock(client.io);

            const writer = client.active_writer;
            protocol.Encoder.encodeUnsub(writer, .{
                .sid = self.sid,
                .max_msgs = null,
            }) catch return error.EncodingFailed;
        }
    }

    /// Blocks until the subscription queue is empty (drained) or timeout.
    ///
    /// Call after drain() to wait for all queued messages to be consumed.
    /// Returns error.Timeout if the queue is not empty after timeout_ms.
    /// Returns error.NotDraining if subscription is not in draining state.
    ///
    /// Note: This only waits for the queue to empty. Call unsubscribe() or
    /// deinit() afterward to fully clean up the subscription.
    ///
    /// Example:
    /// ```
    /// try sub.drain();
    /// // ... consume messages with next() ...
    /// try sub.waitDrained(5000); // Wait up to 5 seconds
    /// sub.deinit(allocator);     // Clean up
    /// ```
    pub fn waitDrained(self: *Subscription, timeout_ms: u32) !void {
        assert(timeout_ms > 0);

        if (self.state != .draining) {
            return error.NotDraining;
        }

        // Already drained
        if (self.queue.len() == 0) {
            return;
        }

        const start = std.time.Instant.now() catch {
            // Timer unavailable - single check only
            if (self.queue.len() == 0) {
                return;
            }
            return error.Timeout;
        };
        const timeout_ns: u64 = @as(u64, timeout_ms) * std.time.ns_per_ms;
        var check_counter: u32 = 0;

        while (true) {
            if (self.queue.len() == 0) {
                return;
            }

            // Check timeout periodically to reduce syscalls
            check_counter +%= 1;
            if (check_counter >= defaults.Spin.timeout_check_iterations) {
                check_counter = 0;
                const now = std.time.Instant.now() catch return error.Timeout;
                if (now.since(start) >= timeout_ns) return error.Timeout;
            }

            std.atomic.spinLoopHint();
        }
    }

    /// Returns the number of messages pending in the queue.
    pub fn pending(self: *const Subscription) usize {
        return self.queue.len();
    }

    /// Returns the count of delivered messages.
    /// Only tracked when autoUnsubscribe is set.
    pub fn delivered(self: *const Subscription) u64 {
        return self.delivered_count;
    }

    /// Sets the maximum pending message limit.
    /// When exceeded, new messages are dropped (slow consumer).
    /// Set to 0 for no limit (default).
    pub fn setPendingLimits(self: *Subscription, msg_limit: usize) void {
        self.pending_limit = msg_limit;
    }

    /// Returns the current pending message limit. 0 means no limit.
    pub fn getPendingLimits(self: *const Subscription) usize {
        return self.pending_limit;
    }

    /// Returns true if the subscription is valid and can receive messages.
    pub fn isValid(self: *const Subscription) bool {
        if (self.client_destroyed) return false;
        return self.state == .active or self.state == .draining;
    }

    // =========================================================================
    // Subscription Info Getters
    // =========================================================================

    /// Returns the subscription ID (SID).
    /// Unique within the connection, assigned during subscribe.
    pub fn getSid(self: *const Subscription) u64 {
        assert(self.sid > 0);
        return self.sid;
    }

    /// Returns the subscription subject pattern.
    /// May contain wildcards (* and >).
    pub fn getSubject(self: *const Subscription) []const u8 {
        assert(self.subject.len > 0);
        return self.subject;
    }

    /// Returns the queue group name if subscribed as a queue subscriber.
    /// Returns null for regular subscriptions.
    pub fn getQueueGroup(self: *const Subscription) ?[]const u8 {
        return self.queue_group;
    }

    /// Returns true if this subscription is currently draining.
    /// A draining subscription will deliver queued messages but not receive new ones.
    pub fn isDraining(self: *const Subscription) bool {
        return self.state == .draining;
    }

    // =========================================================================
    // Subscription Statistics
    // =========================================================================

    /// Subscription statistics snapshot.
    pub const SubStats = struct {
        /// Current messages pending in queue.
        pending_msgs: usize,
        /// Current bytes pending in queue.
        pending_bytes: u64,
        /// High watermark for pending message count.
        max_pending_msgs: u64,
        /// High watermark for pending bytes.
        max_pending_bytes: u64,
        /// Total messages delivered to this subscription.
        delivered: u64,
        /// Messages dropped due to slow consumer (queue overflow).
        dropped: u64,
        /// Messages lost due to allocation failure.
        alloc_failed: u64,
    };

    /// Returns current pending bytes in queue.
    pub fn pendingBytes(self: *const Subscription) u64 {
        return self.pending_bytes;
    }

    /// Returns high watermarks for pending messages and bytes.
    pub fn maxPending(self: *const Subscription) struct { msgs: u64, bytes: u64 } {
        return .{
            .msgs = self.max_pending_msgs,
            .bytes = self.max_pending_bytes,
        };
    }

    /// Resets high watermark counters to current values.
    pub fn clearMaxPending(self: *Subscription) void {
        self.max_pending_msgs = self.queue.len();
        self.max_pending_bytes = self.pending_bytes;
    }

    /// Returns a snapshot of subscription statistics.
    pub fn getSubStats(self: *const Subscription) SubStats {
        return .{
            .pending_msgs = self.queue.len(),
            .pending_bytes = self.pending_bytes,
            .max_pending_msgs = self.max_pending_msgs,
            .max_pending_bytes = self.max_pending_bytes,
            .delivered = self.delivered_count,
            .dropped = self.dropped_msgs,
            .alloc_failed = self.alloc_failed_msgs,
        };
    }

    /// Push message to queue (called by io_task).
    /// Lock-free, never blocks.
    /// ⚠️ HOT PATH: called for every message. Keep minimal.
    pub fn pushMessage(self: *Subscription, msg: Message) !void {
        const queue_len = self.queue.len();

        // Check pending limit if set (flow control)
        if (self.pending_limit > 0 and queue_len >= self.pending_limit) {
            return error.QueueFull;
        }

        if (!self.queue.push(msg)) return error.QueueFull;

        // Use backing_buf.len (always set for io_task messages) - avoids
        // 4 field accesses in msg.size()
        const msg_size = if (msg.backing_buf) |buf| buf.len else msg.size();
        self.pending_bytes += msg_size;

        // High watermarks (queue_len + 1 = new length after push)
        const new_len = queue_len + 1;
        if (new_len > self.max_pending_msgs) {
            self.max_pending_msgs = new_len;
        }
        if (self.pending_bytes > self.max_pending_bytes) {
            self.max_pending_bytes = self.pending_bytes;
        }

        // Track delivered count for auto-unsubscribe (opt-in, only if max set)
        if (self.max_msgs != null) {
            self.delivered_count += 1;
            // Check if limit reached - fire subscription_complete event
            if (self.delivered_count >= self.max_msgs.? and !self.auto_unsub_triggered) {
                self.auto_unsub_triggered = true;
                // Notify via event callback that subscription reached its limit
                self.client.pushEvent(.{
                    .subscription_complete = .{ .sid = self.sid },
                });
            }
        }
    }

    /// Unsubscribes from the subject.
    ///
    /// Sends UNSUB to server, removes from client tracking, closes queue.
    /// Idempotent - returns immediately if already unsubscribed.
    /// Does NOT free memory - call deinit() for that.
    ///
    /// Returns error.NotConnected if UNSUB couldn't be sent (local cleanup
    /// still succeeds). Returns error.EncodingFailed for protocol errors.
    pub fn unsubscribe(self: *Subscription) !void {
        // Idempotent - already unsubscribed
        if (self.state == .unsubscribed) return;

        // Client already destroyed, mark state only
        if (self.client_destroyed) {
            self.state = .unsubscribed;
            return;
        }

        const client = self.client;
        const can_send = client.state.canSend();

        // Acquire mutex for thread-safe cleanup
        client.read_mutex.lockUncancelable(client.io);
        defer client.read_mutex.unlock(client.io);

        // Track UNSUB send success
        var send_failed = false;

        // Send UNSUB protocol if connected
        if (can_send) {
            const writer = &client.writer.interface;
            protocol.Encoder.encodeUnsub(writer, .{
                .sid = self.sid,
                .max_msgs = null,
            }) catch {
                send_failed = true;
            };
        }

        // Always remove from client tracking (inside mutex)
        // This must happen even if not connected to prevent use-after-free
        if (client.sidmap.get(self.sid)) |slot_idx| {
            client.sub_ptrs[slot_idx] = null;
            if (client.cached_sub == self) client.cached_sub = null;
            _ = client.sidmap.remove(self.sid);
            client.free_slots[client.free_count] = slot_idx;
            client.free_count += 1;
        }

        // Close queue (inside mutex)
        self.queue.close(client.io);

        // Mark as unsubscribed
        self.state = .unsubscribed;

        // Report errors after cleanup completes
        if (!can_send) return error.NotConnected;
        if (send_failed) return error.EncodingFailed;
    }

    /// Frees all memory resources.
    ///
    /// If not yet unsubscribed, calls unsubscribe() and ignores errors.
    /// Safe to use in defer blocks (like Rust's Drop trait).
    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        // Ensure unsubscribed (errors ignored - like Rust Drop)
        if (self.state != .unsubscribed) {
            self.unsubscribe() catch |err| {
                dbg.print(
                    "deinit: unsubscribe failed: {s}",
                    .{@errorName(err)},
                );
            };
        }

        // Drain remaining messages (return buffers to pool)
        var drain_buf: [1]Message = undefined;
        while (true) {
            const n = self.queue.popBatch(&drain_buf);
            if (n == 0) break;
            drain_buf[0].deinit(allocator);
        }

        // Free subscription resources
        allocator.free(self.queue_buf);
        allocator.free(self.subject);
        if (self.queue_group) |qg| allocator.free(qg);
        allocator.destroy(self);
    }
};

test "parse url" {
    {
        const parsed = try parseUrl("nats://localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(parsed.user == null);
        try std.testing.expect(!parsed.use_tls);
    }

    {
        const parsed = try parseUrl("nats://user:pass@localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expectEqualSlices(u8, "user", parsed.user.?);
        try std.testing.expectEqualSlices(u8, "pass", parsed.pass.?);
        try std.testing.expect(!parsed.use_tls);
    }

    {
        const parsed = try parseUrl("localhost");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(!parsed.use_tls);
    }

    {
        const parsed = try parseUrl("127.0.0.1:4223");
        try std.testing.expectEqualSlices(u8, "127.0.0.1", parsed.host);
        try std.testing.expectEqual(@as(u16, 4223), parsed.port);
        try std.testing.expect(!parsed.use_tls);
    }
}

test "parse url tls scheme" {
    {
        const parsed = try parseUrl("tls://secure.example.com:4222");
        try std.testing.expectEqualSlices(u8, "secure.example.com", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(parsed.use_tls);
    }

    {
        const parsed = try parseUrl("tls://user:pass@secure.example.com:4222");
        try std.testing.expectEqualSlices(u8, "secure.example.com", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expectEqualSlices(u8, "user", parsed.user.?);
        try std.testing.expectEqualSlices(u8, "pass", parsed.pass.?);
        try std.testing.expect(parsed.use_tls);
    }

    {
        const parsed = try parseUrl("tls://localhost");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(parsed.use_tls);
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
    try std.testing.expectEqual(
        defaults.Connection.timeout_ns,
        opts.connect_timeout_ns,
    );
    try std.testing.expectEqual(
        defaults.Memory.queue_size.value(),
        opts.sub_queue_size,
    );
}

test "stats defaults" {
    const stats: Stats = .{};
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_in);
    try std.testing.expectEqual(@as(u64, 0), stats.msgs_out);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_in);
    try std.testing.expectEqual(@as(u64, 0), stats.bytes_out);
    try std.testing.expectEqual(@as(u32, 0), stats.reconnects);
}
