//! Centralized Default Configuration
//!
//! Queue size is the master value. Slab tier counts derive from it.
//! Change queue_size once, all memory allocations adjust automatically.

/// Predefined queue size options (power-of-2, 1K to 512K).
pub const QueueSize = enum(u32) {
    k1 = 1024,
    k2 = 2048,
    k4 = 4096,
    k8 = 8192,
    k16 = 16384,
    k32 = 32768,
    k64 = 65536,
    k128 = 131072,
    k256 = 262144,
    k512 = 524288,

    /// Returns the numeric value.
    pub fn value(self: QueueSize) u32 {
        return @intFromEnum(self);
    }
};

/// Memory and slab configuration.
pub const Memory = struct {
    /// Master queue size (slab tiers derive from this).
    pub const queue_size: QueueSize = .k8;

    /// Slab tier sizes (fixed, power-of-2).
    pub const tier_sizes = [_]u32{ 256, 512, 1024, 4096, 16384 };
    pub const tier_count: usize = tier_sizes.len;

    /// Slab tier counts (derived from queue_size).
    pub const tier_counts = blk: {
        const q = queue_size.value();
        break :blk [_]u32{
            q, // Tier 0: 256B
            q, // Tier 1: 512B
            q / 2, // Tier 2: 1KB
            q / 4, // Tier 3: 4KB
            q / 16, // Tier 4: 16KB
        };
    };

    /// Max slab slice size (larger uses fallback allocator).
    pub const max_slice_size: usize = 16384;

    /// Total pre-allocated slab memory (comptime computed).
    pub const total_memory: usize = blk: {
        var total: usize = 0;
        for (tier_sizes, tier_counts) |size, count| {
            total += @as(usize, size) * count;
        }
        break :blk total;
    };
};

/// Connection settings.
pub const Connection = struct {
    /// Connection timeout (5 seconds).
    pub const timeout_ns: u64 = 5_000_000_000;
    /// Read/write buffer size. Must be > max_payload + protocol overhead.
    /// Derived from Protocol.max_payload + 8KB headroom for MSG/HMSG headers.
    pub const buffer_size: usize = Protocol.max_payload + 8 * 1024;
    /// TCP receive buffer hint (1 MB for high throughput).
    pub const tcp_rcvbuf: u32 = 1024 * 1024;
    /// Ping interval (2 minutes).
    pub const ping_interval_ms: u32 = 120_000;
    /// Max outstanding pings before stale.
    pub const max_pings_outstanding: u8 = 2;
};

/// Reconnection strategy.
pub const Reconnection = struct {
    /// Enable automatic reconnection.
    pub const enabled: bool = true;
    /// Maximum reconnection attempts (0 = infinite).
    pub const max_attempts: u32 = 60;
    /// Initial wait between attempts (2 seconds).
    pub const wait_ms: u32 = 2_000;
    /// Maximum wait with backoff (30 seconds).
    pub const wait_max_ms: u32 = 30_000;
    /// Jitter percentage (0-50).
    pub const jitter_percent: u8 = 10;
    /// Discover servers from INFO connect_urls.
    pub const discover_servers: bool = true;
    /// Buffer for publishes during reconnect (8 MB).
    pub const pending_buffer_size: usize = 8 * 1024 * 1024;
};

/// Server pool limits.
pub const Server = struct {
    /// Max servers in pool.
    pub const max_pool_size: u8 = 16;
    /// Max URL string length.
    pub const max_url_len: u16 = 256;
    /// Cooldown after failure (5 seconds).
    pub const failure_cooldown_ns: u64 = 5_000_000_000;
};

/// Client limits.
pub const Client = struct {
    /// Max concurrent subscriptions per client.
    pub const max_subscriptions: u16 = 256;
    /// SidMap hash table capacity.
    pub const sidmap_capacity: u32 = 512;
};

/// Protocol constants.
pub const Protocol = struct {
    /// Default NATS server port.
    pub const port: u16 = 4222;
    /// Default max payload (1 MB).
    pub const max_payload: u32 = 1048576;
    /// Client version string.
    pub const version: []const u8 = "0.1.0";
};

/// Spin/yield loop tuning constants.
pub const Spin = struct {
    /// Spin iterations before yielding in subscription next() loop.
    /// After this many spins, yields to I/O runtime for cancellation support.
    pub const max_spins: u32 = 4096;
    /// Loop iterations between health check timestamp reads in io_task.
    /// Avoids syscall overhead by only checking time periodically.
    pub const health_check_iterations: u32 = 10000;
    /// Loop iterations between timeout checks in nextWithTimeout().
    /// Reduces syscalls while maintaining reasonable timeout granularity.
    pub const timeout_check_iterations: u32 = 10000;
};

/// Protocol limits for subjects and queue groups.
/// These are compile-time limits that define backup buffer sizes.
pub const Limits = struct {
    /// Max subject length for backup buffers (reconnect support).
    /// Subjects longer than this cannot be restored after reconnect.
    pub const max_subject_len: u16 = 256;
    /// Max queue group length for backup buffers.
    pub const max_queue_group_len: u8 = 64;
};

/// Error reporting configuration.
pub const ErrorReporting = struct {
    /// Messages between rate-limited error notifications.
    /// After first error, subsequent errors only notify every N messages.
    /// This prevents event queue flooding during sustained error conditions.
    pub const notify_interval_msgs: u64 = 100_000;
};

/// TLS configuration.
pub const Tls = struct {
    /// TLS read/write buffer size (must be >= tls.Client.min_buffer_len).
    /// Using 32KB for good performance.
    pub const buffer_size: usize = 32 * 1024;
};
