//! Connection Errors
//!
//! Error types for connection-related failures including authentication,
//! timeouts, and connection state issues.

const std = @import("std");

/// Connection-related errors.
pub const Error = error{
    /// Connection to server was closed unexpectedly.
    ConnectionClosed,
    /// Connection attempt timed out.
    ConnectionTimeout,
    /// Server refused the connection.
    ConnectionRefused,
    /// Authentication with the server failed.
    AuthenticationFailed,
    /// Stale connection detected.
    StaleConnection,
    /// Server is in lame duck mode and will shut down soon.
    LameDuckMode,
    /// TCP_NODELAY socket option failed
    TcpNoDelayFailed,
    /// TCP receive buffer option failed
    TcpRcvBufFailed,
    /// URL too long
    UrlTooLong,
    /// Queue group too long
    QueueGroupTooLong,
    /// Subject too long
    SubjectTooLong,
};

/// Parses auth-related errors from server -ERR message.
/// Returns null if the message is not an auth error.
pub fn parseAuthError(msg: []const u8) ?Error {
    if (std.mem.indexOf(u8, msg, "Authentication")) |_| {
        return error.AuthenticationFailed;
    }
    if (std.mem.indexOf(u8, msg, "Stale Connection")) |_| {
        return error.StaleConnection;
    }
    return null;
}

/// Returns true if the error is retryable (connection can be re-established).
pub fn isRetryable(err: Error) bool {
    return switch (err) {
        error.ConnectionClosed,
        error.ConnectionTimeout,
        error.StaleConnection,
        => true,
        else => false,
    };
}

test "parseAuthError authentication" {
    const err = parseAuthError("Authentication Timeout");
    try std.testing.expectEqual(error.AuthenticationFailed, err.?);
}

test "parseAuthError stale connection" {
    const err = parseAuthError("Stale Connection");
    try std.testing.expectEqual(error.StaleConnection, err.?);
}

test "parseAuthError non-auth" {
    const err = parseAuthError("Some Other Error");
    try std.testing.expectEqual(@as(?Error, null), err);
}

test "isRetryable connection errors" {
    try std.testing.expect(isRetryable(error.ConnectionClosed));
    try std.testing.expect(isRetryable(error.ConnectionTimeout));
    try std.testing.expect(isRetryable(error.StaleConnection));
}

test "isRetryable non-retryable errors" {
    try std.testing.expect(!isRetryable(error.AuthenticationFailed));
    try std.testing.expect(!isRetryable(error.ConnectionRefused));
    try std.testing.expect(!isRetryable(error.LameDuckMode));
}
