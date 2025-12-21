//! Connection Options
//!
//! Configuration options for establishing NATS connections.

const std = @import("std");

/// Connection options with sensible defaults.
pub const Options = struct {
    /// Connection name visible in server monitoring.
    name: ?[]const u8 = null,

    /// Enable verbose protocol mode (receive +OK for each message).
    verbose: bool = false,

    /// Enable pedantic protocol checking.
    pedantic: bool = false,

    /// Echo messages back to the sender.
    echo: bool = true,

    /// Enable message headers support.
    headers: bool = true,

    /// Request no_responders notification for requests.
    no_responders: bool = true,

    /// Connection timeout in milliseconds.
    connect_timeout_ms: u32 = 2000,

    /// Ping interval in milliseconds.
    ping_interval_ms: u32 = 120000,

    /// Maximum outstanding pings before connection is considered stale.
    max_pings_outstanding: u32 = 2,

    /// Maximum reconnection attempts. 0 = no limit.
    max_reconnects: u32 = 60,

    /// Initial reconnection wait in milliseconds.
    reconnect_wait_ms: u32 = 2000,

    /// Maximum reconnection wait in milliseconds.
    reconnect_wait_max_ms: u32 = 30000,

    /// Add jitter to reconnection timing.
    reconnect_jitter: bool = true,

    /// Buffer size for pending messages during reconnection.
    reconnect_buffer_size: u32 = 8 * 1024 * 1024,

    /// Flush timeout in milliseconds.
    flush_timeout_ms: u32 = 10000,

    /// Authentication token.
    token: ?[]const u8 = null,

    /// Username for authentication.
    user: ?[]const u8 = null,

    /// Password for authentication.
    pass: ?[]const u8 = null,

    /// NKey seed for authentication.
    nkey_seed: ?[]const u8 = null,

    /// JWT for authentication.
    jwt: ?[]const u8 = null,

    /// Require TLS connection.
    tls_required: bool = false,
};

test "options defaults" {
    const opts: Options = .{};
    try std.testing.expect(!opts.verbose);
    try std.testing.expect(opts.echo);
    try std.testing.expect(opts.headers);
    try std.testing.expectEqual(@as(u32, 2000), opts.connect_timeout_ms);
}

test "options custom" {
    const opts: Options = .{
        .name = "test-client",
        .verbose = true,
        .max_reconnects = 0,
    };
    try std.testing.expectEqualSlices(u8, "test-client", opts.name.?);
    try std.testing.expect(opts.verbose);
    try std.testing.expectEqual(@as(u32, 0), opts.max_reconnects);
}
