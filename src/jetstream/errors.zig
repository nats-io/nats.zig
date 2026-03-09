//! JetStream error types and error codes.
//!
//! Two-layer error handling: Zig error unions for transport/protocol
//! failures, and ApiError struct for JetStream-specific API errors
//! returned by the server.

const std = @import("std");

/// JetStream API error returned by the server.
/// Stored inline on the JetStream context (no allocation).
pub const ApiError = struct {
    code: u16,
    err_code: u16,
    description_buf: [255]u8 = undefined,
    description_len: u8 = 0,

    /// Returns the error description string.
    pub fn description(self: *const ApiError) []const u8 {
        return self.description_buf[0..self.description_len];
    }

    /// Creates an ApiError from a parsed JSON error object.
    pub fn fromJson(json_err: ApiErrorJson) ApiError {
        std.debug.assert(json_err.code > 0);
        var result = ApiError{
            .code = json_err.code,
            .err_code = json_err.err_code,
        };
        if (json_err.description) |desc| {
            const len: u8 = @intCast(@min(
                desc.len,
                result.description_buf.len,
            ));
            @memcpy(
                result.description_buf[0..len],
                desc[0..len],
            );
            result.description_len = len;
        }
        return result;
    }
};

/// JSON-deserializable error object from JetStream API responses.
pub const ApiErrorJson = struct {
    code: u16 = 0,
    err_code: u16 = 0,
    description: ?[]const u8 = null,
};

/// Well-known JetStream error codes (from Go's errors.go).
pub const ErrCode = struct {
    pub const bad_request: u16 = 10003;
    pub const consumer_create: u16 = 10012;
    pub const consumer_not_found: u16 = 10014;
    pub const max_consumers_limit: u16 = 10026;
    pub const message_not_found: u16 = 10037;
    pub const js_not_enabled_for_account: u16 = 10039;
    pub const stream_name_in_use: u16 = 10058;
    pub const stream_not_found: u16 = 10059;
    pub const stream_wrong_last_seq: u16 = 10071;
    pub const js_not_enabled: u16 = 10076;
    pub const consumer_already_exists: u16 = 10105;
    pub const duplicate_filter_subjects: u16 = 10136;
    pub const overlapping_filter_subjects: u16 = 10138;
    pub const consumer_empty_filter: u16 = 10139;
    pub const consumer_exists: u16 = 10148;
    pub const consumer_does_not_exist: u16 = 10149;
};

/// JetStream error set for Zig error unions.
pub const Error = error{
    Timeout,
    NoResponders,
    ApiError,
    JsonParseError,
    SubjectTooLong,
};

test "ApiError.fromJson" {
    const json_err = ApiErrorJson{
        .code = 404,
        .err_code = ErrCode.stream_not_found,
        .description = "stream not found",
    };
    const api_err = ApiError.fromJson(json_err);
    try std.testing.expectEqual(@as(u16, 404), api_err.code);
    try std.testing.expectEqual(
        ErrCode.stream_not_found,
        api_err.err_code,
    );
    try std.testing.expectEqualStrings(
        "stream not found",
        api_err.description(),
    );
}

test "ApiError.fromJson truncates long description" {
    const long = "x" ** 300;
    const json_err = ApiErrorJson{
        .code = 400,
        .err_code = ErrCode.bad_request,
        .description = long,
    };
    const api_err = ApiError.fromJson(json_err);
    try std.testing.expectEqual(@as(u8, 255), api_err.description_len);
    try std.testing.expectEqual(@as(usize, 255), api_err.description().len);
}

test "ApiError.fromJson null description" {
    const json_err = ApiErrorJson{
        .code = 500,
        .err_code = 0,
        .description = null,
    };
    const api_err = ApiError.fromJson(json_err);
    try std.testing.expectEqual(@as(u8, 0), api_err.description_len);
    try std.testing.expectEqualStrings("", api_err.description());
}
