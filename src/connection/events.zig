//! Connection Events
//!
//! Event queue for connection lifecycle and message events.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const commands = @import("../protocol/commands.zig");
const ServerInfo = commands.ServerInfo;

/// Connection events that can be polled by the user.
pub const Event = union(enum) {
    /// Successfully connected to server.
    connected: ConnectedInfo,

    /// Disconnected from server.
    disconnected: DisconnectedInfo,

    /// Received a message.
    message: MessageInfo,

    /// Received an error from server.
    server_error: []const u8,

    /// Reconnecting to server.
    reconnecting: ReconnectingInfo,

    /// Lamport drain started.
    drain_started,

    /// Lamport drain completed.
    drain_completed,
};

/// Information about successful connection.
pub const ConnectedInfo = struct {
    /// Server information received during handshake.
    server_id: []const u8,
    server_name: []const u8,
    version: []const u8,
    /// True if this is a reconnection.
    is_reconnect: bool,
};

/// Information about disconnection.
pub const DisconnectedInfo = struct {
    /// Reason for disconnection.
    reason: DisconnectReason,
    /// Error message if applicable.
    error_msg: ?[]const u8,
};

/// Reasons for disconnection.
pub const DisconnectReason = enum {
    /// Normal close requested by user.
    user_close,
    /// Server closed connection.
    server_close,
    /// Network error occurred.
    network_error,
    /// Authentication failed.
    auth_failed,
    /// Protocol error.
    protocol_error,
    /// Connection timeout.
    timeout,
};

/// Information about received message.
pub const MessageInfo = struct {
    /// Message subject.
    subject: []const u8,
    /// Subscription ID that matched.
    sid: u64,
    /// Optional reply-to subject.
    reply_to: ?[]const u8,
    /// Message payload.
    data: []const u8,
    /// Header data if present (HMSG).
    headers: ?[]const u8,
};

/// Information about reconnection attempt.
pub const ReconnectingInfo = struct {
    /// Current attempt number.
    attempt: u32,
    /// Maximum attempts configured.
    max_attempts: u32,
    /// Server being connected to.
    server: []const u8,
};
