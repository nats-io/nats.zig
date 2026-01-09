//! Protocol Errors
//!
//! Error types for protocol-related failures including parsing errors,
//! server errors, and authorization violations.

const std = @import("std");

/// Protocol-related errors.
pub const Error = error{
    /// Server sent an invalid protocol message.
    ProtocolError,
    /// Server sent an error response.
    ServerError,
    /// Authorization for the requested operation was denied.
    AuthorizationViolation,
    /// Message payload exceeds server's maximum allowed size.
    MaxPayloadExceeded,
};

/// Parses a NATS server error message into a protocol Error.
/// Server errors come in the form: -ERR 'message'
pub fn parseServerError(msg: []const u8) Error {
    if (std.mem.indexOf(u8, msg, "Authorization Violation")) |_| {
        return error.AuthorizationViolation;
    }
    if (std.mem.indexOf(u8, msg, "Maximum Payload")) |_| {
        return error.MaxPayloadExceeded;
    }
    return error.ServerError;
}

/// Returns true if the error is a permissions error.
pub fn isAuthError(err: Error) bool {
    return err == error.AuthorizationViolation;
}

test "parseServerError authorization" {
    const err = parseServerError("Authorization Violation");
    try std.testing.expectEqual(error.AuthorizationViolation, err);
}

test "parseServerError max payload" {
    const err = parseServerError("Maximum Payload Exceeded");
    try std.testing.expectEqual(error.MaxPayloadExceeded, err);
}

test "parseServerError unknown" {
    const err = parseServerError("Some Unknown Error");
    try std.testing.expectEqual(error.ServerError, err);
}

test "isAuthError" {
    try std.testing.expect(isAuthError(error.AuthorizationViolation));
    try std.testing.expect(!isAuthError(error.ServerError));
    try std.testing.expect(!isAuthError(error.ProtocolError));
}
