//! NATS Protocol Headers
//!
//! Handles NATS message headers in the NATS/1.0 format.
//! Headers are used with HPUB/HMSG commands for metadata.
//!
//! Format:
//! ```
//! NATS/1.0\r\n
//! Header-Name: value\r\n
//! Another-Header: value\r\n
//! \r\n
//! ```

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Well-known NATS header names.
pub const HeaderName = struct {
    pub const msg_id = "Nats-Msg-Id";
    pub const expected_stream = "Nats-Expected-Stream";
    pub const expected_last_msg_id = "Nats-Expected-Last-Msg-Id";
    pub const expected_last_seq = "Nats-Expected-Last-Sequence";
    pub const expected_last_subj_seq = "Nats-Expected-Last-Subject-Sequence";
    pub const last_consumer = "Nats-Last-Consumer";
    pub const last_stream = "Nats-Last-Stream";
    pub const consumer_stalled = "Nats-Consumer-Stalled";
    pub const rollup = "Nats-Rollup";
    pub const no_responders = "Status";
    pub const description = "Description";
};

/// Status codes returned in headers.
pub const Status = struct {
    pub const no_responders = "503";
    pub const request_timeout = "408";
    pub const no_messages = "404";
    pub const control_message = "100";
};

/// Header entry (key-value pair).
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

/// Parses headers from NATS/1.0 format.
/// Returns entries as slices into the original data (no allocation).
pub fn parse(data: []const u8) ParseResult {
    assert(data.len > 0);
    var result: ParseResult = .{};

    if (!std.mem.startsWith(u8, data, "NATS/1.0")) {
        result.err = .invalid_version;
        return result;
    }

    var pos: usize = 8;

    // Skip optional status code and description on first line
    if (pos < data.len and data[pos] == ' ') {
        // Has status: NATS/1.0 503 No Responders\r\n
        pos += 1;
        const status_end = std.mem.indexOfPos(u8, data, pos, " ") orelse
            std.mem.indexOfPos(u8, data, pos, "\r\n") orelse {
            result.err = .incomplete;
            return result;
        };
        result.status = data[pos..status_end];
        pos = status_end;

        // Skip description if present
        if (pos < data.len and data[pos] == ' ') {
            pos += 1;
            const desc_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse {
                result.err = .incomplete;
                return result;
            };
            result.description = data[pos..desc_end];
            pos = desc_end;
        }
    }

    // Skip \r\n after version line
    if (pos + 2 > data.len or !std.mem.eql(u8, data[pos..][0..2], "\r\n")) {
        result.err = .incomplete;
        return result;
    }
    pos += 2;

    // Parse header entries
    while (pos < data.len) {
        // Empty line marks end of headers
        if (std.mem.startsWith(u8, data[pos..], "\r\n")) {
            result.header_end = pos + 2;
            return result;
        }

        // Find colon separator
        const colon = std.mem.indexOfPos(u8, data, pos, ":") orelse {
            result.err = .invalid_header;
            return result;
        };

        const key = data[pos..colon];

        // Skip colon and optional space
        var value_start = colon + 1;
        if (value_start < data.len and data[value_start] == ' ') {
            value_start += 1;
        }

        // Find end of line
        const crlf = "\r\n";
        const idx = std.mem.indexOfPos(u8, data, value_start, crlf);
        const line_end = idx orelse {
            result.err = .incomplete;
            return result;
        };

        const value = data[value_start..line_end];

        if (result.count < result.entries.len) {
            result.entries[result.count] = .{ .key = key, .value = value };
            result.count += 1;
        }

        pos = line_end + 2;
    }

    result.err = .incomplete;
    return result;
}

/// Result of header parsing.
pub const ParseResult = struct {
    entries: [16]Entry = undefined,
    count: usize = 0,
    status: ?[]const u8 = null,
    description: ?[]const u8 = null,
    header_end: usize = 0,
    err: ?ParseError = null,

    pub const ParseError = enum {
        invalid_version,
        invalid_header,
        incomplete,
    };

    /// Returns slice of parsed entries.
    pub fn items(self: *const ParseResult) []const Entry {
        assert(self.count <= self.entries.len);
        return self.entries[0..self.count];
    }

    /// Gets first value for a header name.
    pub fn get(self: *const ParseResult, name: []const u8) ?[]const u8 {
        for (self.entries[0..self.count]) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.key, name)) {
                return entry.value;
            }
        }
        return null;
    }

    /// Returns true if this is a no-responders status.
    pub fn isNoResponders(self: *const ParseResult) bool {
        if (self.status) |s| {
            return std.mem.eql(u8, s, Status.no_responders);
        }
        return false;
    }
};

/// Encodes headers to NATS/1.0 format.
pub fn encode(
    writer: *Io.Writer,
    entries: []const Entry,
) Io.Writer.Error!void {
    assert(entries.len <= 16);
    try writer.writeAll("NATS/1.0\r\n");

    for (entries) |entry| {
        try writer.writeAll(entry.key);
        try writer.writeAll(": ");
        try writer.writeAll(entry.value);
        try writer.writeAll("\r\n");
    }

    try writer.writeAll("\r\n");
}

/// Calculates the encoded size of headers.
pub fn encodedSize(entries: []const Entry) usize {
    var size: usize = 10; // "NATS/1.0\r\n"

    for (entries) |entry| {
        size += entry.key.len + 2 + entry.value.len + 2;
    }

    size += 2; // final \r\n
    return size;
}

test "parse simple headers" {
    const data = "NATS/1.0\r\nFoo: bar\r\nBaz: qux\r\n\r\n";
    const result = parse(data);

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 2), result.count);
    try std.testing.expectEqualSlices(u8, "Foo", result.entries[0].key);
    try std.testing.expectEqualSlices(u8, "bar", result.entries[0].value);
    try std.testing.expectEqualSlices(u8, "Baz", result.entries[1].key);
    try std.testing.expectEqualSlices(u8, "qux", result.entries[1].value);
}

test "parse with status" {
    const data = "NATS/1.0 503 No Responders\r\n\r\n";
    const result = parse(data);

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqualSlices(u8, "503", result.status.?);
    const desc = "No Responders";
    try std.testing.expectEqualSlices(u8, desc, result.description.?);
    try std.testing.expect(result.isNoResponders());
}

test "parse no headers" {
    const data = "NATS/1.0\r\n\r\n";
    const result = parse(data);

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 0), result.count);
}

test "get header case insensitive" {
    const data = "NATS/1.0\r\nContent-Type: application/json\r\n\r\n";
    const result = parse(data);

    try std.testing.expectEqualSlices(
        u8,
        "application/json",
        result.get("content-type").?,
    );
}

test "encode headers" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]Entry{
        .{ .key = "Foo", .value = "bar" },
        .{ .key = "Baz", .value = "123" },
    };

    try encode(&writer, &entries);

    try std.testing.expectEqualSlices(
        u8,
        "NATS/1.0\r\nFoo: bar\r\nBaz: 123\r\n\r\n",
        writer.buffered(),
    );
}

test "encoded size" {
    const entries = [_]Entry{
        .{ .key = "Foo", .value = "bar" },
    };

    const size = encodedSize(&entries);
    try std.testing.expectEqual(@as(usize, 22), size);
}
