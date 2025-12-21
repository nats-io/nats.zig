//! NATS Error Types
//!
//! Defines all error conditions that can occur during NATS operations.

const std = @import("std");

/// NATS-specific errors.
pub const Error = error{
    /// Connection to server was closed unexpectedly.
    ConnectionClosed,

    /// Connection attempt timed out.
    ConnectionTimeout,

    /// Server refused the connection.
    ConnectionRefused,

    /// Authentication with the server failed.
    AuthenticationFailed,

    /// Authorization for the requested operation was denied.
    AuthorizationViolation,

    /// Server sent an invalid protocol message.
    ProtocolError,

    /// Message payload exceeds server's maximum allowed size.
    MaxPayloadExceeded,

    /// Server sent an error response.
    ServerError,

    /// The requested subject is invalid.
    InvalidSubject,

    /// The subscription ID is not recognized.
    InvalidSubscription,

    /// Request timed out waiting for a response.
    RequestTimeout,

    /// No responders available for the request.
    NoResponders,

    /// JetStream is not enabled on this server.
    JetStreamNotEnabled,

    /// The requested stream was not found.
    StreamNotFound,

    /// The requested consumer was not found.
    ConsumerNotFound,

    /// Message was not acknowledged in time.
    AckTimeout,

    /// Stale connection detected.
    StaleConnection,

    /// Server is in lame duck mode and will shut down soon.
    LameDuckMode,
};

/// Parses a NATS server error message into an Error.
/// Server errors come in the form: -ERR 'message'
pub fn parseServerError(msg: []const u8) Error {
    if (std.mem.indexOf(u8, msg, "Authorization Violation")) |_| {
        return Error.AuthorizationViolation;
    }
    if (std.mem.indexOf(u8, msg, "Authentication")) |_| {
        return Error.AuthenticationFailed;
    }
    if (std.mem.indexOf(u8, msg, "Maximum Payload")) |_| {
        return Error.MaxPayloadExceeded;
    }
    if (std.mem.indexOf(u8, msg, "Invalid Subject")) |_| {
        return Error.InvalidSubject;
    }
    if (std.mem.indexOf(u8, msg, "Stale Connection")) |_| {
        return Error.StaleConnection;
    }
    return Error.ServerError;
}

test "parseServerError authorization" {
    const err = parseServerError("Authorization Violation");
    try std.testing.expectEqual(Error.AuthorizationViolation, err);
}

test "parseServerError authentication" {
    const err = parseServerError("Authentication Timeout");
    try std.testing.expectEqual(Error.AuthenticationFailed, err);
}

test "parseServerError max payload" {
    const err = parseServerError("Maximum Payload Exceeded");
    try std.testing.expectEqual(Error.MaxPayloadExceeded, err);
}

test "parseServerError unknown" {
    const err = parseServerError("Some Unknown Error");
    try std.testing.expectEqual(Error.ServerError, err);
}

test "parseServerError invalid subject" {
    const err = parseServerError("Invalid Subject");
    try std.testing.expectEqual(Error.InvalidSubject, err);
}

test "parseServerError stale connection" {
    const err = parseServerError("Stale Connection");
    try std.testing.expectEqual(Error.StaleConnection, err);
}

/// Returns true if the error is retryable (connection can be re-established).
pub fn isRetryable(err: Error) bool {
    return switch (err) {
        Error.ConnectionClosed,
        Error.ConnectionTimeout,
        Error.StaleConnection,
        Error.RequestTimeout,
        => true,
        else => false,
    };
}

test "isRetryable connection errors" {
    try std.testing.expect(isRetryable(Error.ConnectionClosed));
    try std.testing.expect(isRetryable(Error.ConnectionTimeout));
    try std.testing.expect(isRetryable(Error.StaleConnection));
    try std.testing.expect(isRetryable(Error.RequestTimeout));
}

test "isRetryable non-retryable errors" {
    try std.testing.expect(!isRetryable(Error.AuthenticationFailed));
    try std.testing.expect(!isRetryable(Error.AuthorizationViolation));
    try std.testing.expect(!isRetryable(Error.InvalidSubject));
    try std.testing.expect(!isRetryable(Error.MaxPayloadExceeded));
}

/// Returns true if the error is a permissions/auth error.
pub fn isAuthError(err: Error) bool {
    return switch (err) {
        Error.AuthenticationFailed,
        Error.AuthorizationViolation,
        => true,
        else => false,
    };
}

test "isAuthError" {
    try std.testing.expect(isAuthError(Error.AuthenticationFailed));
    try std.testing.expect(isAuthError(Error.AuthorizationViolation));
    try std.testing.expect(!isAuthError(Error.ConnectionClosed));
    try std.testing.expect(!isAuthError(Error.ServerError));
}
