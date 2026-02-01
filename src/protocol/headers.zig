//! NATS Protocol Headers
//!
//! Handles NATS message headers in the NATS/1.0 format.
//! Headers are used with HPUB/HMSG commands for metadata.
//!
//! Features:
//! - Full ownership: ParseResult copies all strings, safe after source freed
//! - Case-insensitive header lookup
//! - API: parse() function, always call deinit()
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

/// Extracts status code from raw header bytes without allocation.
/// Returns null if no status code present or invalid format.
/// Format expected: "NATS/1.0 503 Description\r\n..." or "NATS/1.0\r\n..."
pub fn extractStatus(header_data: []const u8) ?u16 {
    // Minimum: "NATS/1.0\r\n" = 10 chars
    if (header_data.len < 10) return null;

    // Verify NATS/1.0 prefix
    if (!std.mem.startsWith(u8, header_data, "NATS/1.0")) return null;

    // Skip "NATS/1.0"
    const after_version = header_data[8..];

    // If next char is \r, no status code
    if (after_version.len == 0 or after_version[0] == '\r') return null;

    // Expect space before status code
    if (after_version[0] != ' ') return null;

    // Find end of status code (space or \r)
    const status_start = 1; // skip space
    var status_end: usize = status_start;
    while (status_end < after_version.len) : (status_end += 1) {
        const c = after_version[status_end];
        if (c == ' ' or c == '\r') break;
    }

    if (status_end == status_start) return null;

    const status_str = after_version[status_start..status_end];
    return std.fmt.parseInt(u16, status_str, 10) catch null;
}

/// Header entry (key-value pair).
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

/// Result of header parsing.
///
/// Owns all its data - copies strings to heap. Safe to use after source
/// data is freed. Caller MUST call deinit() to free memory.
pub const ParseResult = struct {
    /// Heap-allocated entry array (owns this memory).
    entries: []Entry = &.{},

    /// Heap-allocated string buffer (all key/value strings copied here).
    string_buf: []u8 = &.{},

    /// Number of valid entries.
    count: usize = 0,

    /// Allocator used (needed for deinit).
    allocator: Allocator,

    /// Status code from header line (e.g., "503") - slice into string_buf.
    status: ?[]const u8 = null,

    /// Description from header line - slice into string_buf.
    description: ?[]const u8 = null,

    /// Byte offset where headers end in original data.
    header_end: usize = 0,

    /// Parse error if any.
    err: ?ParseError = null,

    pub const ParseError = enum {
        invalid_version,
        invalid_header,
        incomplete,
        out_of_memory,
    };

    /// Returns all parsed entries. Empty slice if error occurred.
    pub fn items(self: *const ParseResult) []const Entry {
        if (self.err != null) return &.{};
        assert(self.count <= self.entries.len);
        return self.entries[0..self.count];
    }

    /// Gets first value for header name (case-insensitive).
    /// Returns null if error occurred or header not found.
    pub fn get(self: *const ParseResult, name: []const u8) ?[]const u8 {
        if (self.err != null) return null;
        for (self.items()) |entry| {
            if (std.ascii.eqlIgnoreCase(entry.key, name)) {
                return entry.value;
            }
        }
        return null;
    }

    /// Returns true if this is a no-responders status (503).
    pub fn isNoResponders(self: *const ParseResult) bool {
        if (self.err != null) return false;
        if (self.status) |s| {
            return std.mem.eql(u8, s, Status.no_responders);
        }
        return false;
    }

    /// Frees all heap-allocated memory.
    /// Safe to call multiple times. MUST be called after parse().
    pub fn deinit(self: *ParseResult) void {
        if (self.entries.len > 0) {
            self.allocator.free(self.entries);
            self.entries = &.{};
        }
        if (self.string_buf.len > 0) {
            self.allocator.free(self.string_buf);
            self.string_buf = &.{};
        }
        self.count = 0;
        self.status = null;
        self.description = null;
    }
};

/// Parses headers from NATS/1.0 format.
///
/// Allocates memory and copies all header data. ParseResult owns its data
/// and is safe to use after the source data is freed.
///
/// Caller MUST call result.deinit() to free memory.
///
/// On error: result.err is set, items() returns empty, get() returns null.
pub fn parse(allocator: Allocator, data: []const u8) ParseResult {
    assert(data.len > 0);
    return parseImpl(allocator, data);
}

fn parseImpl(allocator: Allocator, data: []const u8) ParseResult {
    var result: ParseResult = .{ .allocator = allocator };

    if (!std.mem.startsWith(u8, data, "NATS/1.0")) {
        result.err = .invalid_version;
        return result;
    }

    // Pass 1: count headers and string bytes
    var header_count: usize = 0;
    var total_string_bytes: usize = 0;
    var status_len: usize = 0;
    var desc_len: usize = 0;

    var pos: usize = 8;

    // Parse optional status and description on first line
    if (pos < data.len and data[pos] == ' ') {
        pos += 1;
        const status_end = std.mem.indexOfPos(u8, data, pos, " ") orelse
            std.mem.indexOfPos(u8, data, pos, "\r\n") orelse {
            result.err = .incomplete;
            return result;
        };
        status_len = status_end - pos;
        total_string_bytes += status_len;
        pos = status_end;

        // Skip description if present
        if (pos < data.len and data[pos] == ' ') {
            pos += 1;
            const desc_end = std.mem.indexOfPos(u8, data, pos, "\r\n") orelse {
                result.err = .incomplete;
                return result;
            };
            desc_len = desc_end - pos;
            total_string_bytes += desc_len;
            pos = desc_end;
        }
    }

    // Skip \r\n after version line
    if (pos + 2 > data.len or !std.mem.eql(u8, data[pos..][0..2], "\r\n")) {
        result.err = .incomplete;
        return result;
    }
    pos += 2;

    // Count header entries and their string sizes
    while (pos < data.len) {
        // Empty line marks end of headers
        if (std.mem.startsWith(u8, data[pos..], "\r\n")) {
            result.header_end = pos + 2;
            break;
        }

        // Find colon separator
        const colon = std.mem.indexOfPos(u8, data, pos, ":") orelse {
            result.err = .invalid_header;
            return result;
        };

        const key_len = colon - pos;

        // Skip colon and optional space
        var value_start = colon + 1;
        if (value_start < data.len and data[value_start] == ' ') {
            value_start += 1;
        }

        // Find end of line
        const line_end = std.mem.indexOfPos(u8, data, value_start, "\r\n") orelse {
            result.err = .incomplete;
            return result;
        };

        const value_len = line_end - value_start;

        header_count += 1;
        total_string_bytes += key_len + value_len;

        pos = line_end + 2;
    } else {
        // Didn't find terminating \r\n\r\n
        result.err = .incomplete;
        return result;
    }

    // Pass 2: allocate and copy
    if (header_count > 0 or status_len > 0 or desc_len > 0) {
        // Allocate entries array
        const entries = allocator.alloc(Entry, header_count) catch {
            result.err = .out_of_memory;
            return result;
        };

        // Allocate string buffer
        const string_buf = allocator.alloc(u8, total_string_bytes) catch {
            allocator.free(entries);
            result.err = .out_of_memory;
            return result;
        };

        result.entries = entries;
        result.string_buf = string_buf;

        // Copy strings into buffer
        var buf_pos: usize = 0;
        var entry_idx: usize = 0;

        pos = 8;

        // Copy status and description
        if (status_len > 0) {
            pos += 1; // skip space
            @memcpy(string_buf[buf_pos..][0..status_len], data[pos..][0..status_len]);
            result.status = string_buf[buf_pos..][0..status_len];
            buf_pos += status_len;
            pos += status_len;

            if (desc_len > 0) {
                pos += 1; // skip space
                @memcpy(
                    string_buf[buf_pos..][0..desc_len],
                    data[pos..][0..desc_len],
                );
                result.description = string_buf[buf_pos..][0..desc_len];
                buf_pos += desc_len;
                pos += desc_len;
            }
        }

        // Skip \r\n after version line
        pos = std.mem.indexOfPos(u8, data, 8, "\r\n").? + 2;

        // Copy header entries
        while (pos < data.len) {
            if (std.mem.startsWith(u8, data[pos..], "\r\n")) {
                break;
            }

            const colon = std.mem.indexOfPos(u8, data, pos, ":").?;
            const key_len = colon - pos;

            // Copy key
            @memcpy(string_buf[buf_pos..][0..key_len], data[pos..][0..key_len]);
            const key_slice = string_buf[buf_pos..][0..key_len];
            buf_pos += key_len;

            // Skip colon and optional space
            var value_start = colon + 1;
            if (value_start < data.len and data[value_start] == ' ') {
                value_start += 1;
            }

            const line_end = std.mem.indexOfPos(u8, data, value_start, "\r\n").?;
            const value_len = line_end - value_start;

            // Copy value
            @memcpy(
                string_buf[buf_pos..][0..value_len],
                data[value_start..][0..value_len],
            );
            const value_slice = string_buf[buf_pos..][0..value_len];
            buf_pos += value_len;

            entries[entry_idx] = .{ .key = key_slice, .value = value_slice };
            entry_idx += 1;

            pos = line_end + 2;
        }

        result.count = entry_idx;
        assert(entry_idx == header_count);
        assert(buf_pos == total_string_bytes);
    }

    return result;
}

/// Encodes headers to NATS/1.0 format.
pub fn encode(
    writer: *Io.Writer,
    entries: []const Entry,
) Io.Writer.Error!void {
    assert(entries.len > 0);
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
    assert(entries.len > 0);
    var size: usize = 10; // "NATS/1.0\r\n"

    for (entries) |entry| {
        size += entry.key.len + 2 + entry.value.len + 2;
    }

    size += 2; // final \r\n
    return size;
}

// Tests

test "parse simple headers" {
    const data = "NATS/1.0\r\nFoo: bar\r\nBaz: qux\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 2), result.count);
    try std.testing.expectEqualSlices(u8, "Foo", result.items()[0].key);
    try std.testing.expectEqualSlices(u8, "bar", result.items()[0].value);
    try std.testing.expectEqualSlices(u8, "Baz", result.items()[1].key);
    try std.testing.expectEqualSlices(u8, "qux", result.items()[1].value);
}

test "parse with status" {
    const data = "NATS/1.0 503 No Responders\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqualSlices(u8, "503", result.status.?);
    try std.testing.expectEqualSlices(u8, "No Responders", result.description.?);
    try std.testing.expect(result.isNoResponders());
}

test "parse no headers" {
    const data = "NATS/1.0\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 0), result.count);
}

test "get header case insensitive" {
    const data = "NATS/1.0\r\nContent-Type: application/json\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expect(result.get("content-type") != null);
    try std.testing.expect(result.get("CONTENT-TYPE") != null);
    try std.testing.expect(result.get("Content-Type") != null);
}

test "parse many headers" {
    // Build 100 headers dynamically
    var data_buf: [4096]u8 = undefined;
    var pos: usize = 0;

    const prefix = "NATS/1.0\r\n";
    @memcpy(data_buf[pos..][0..prefix.len], prefix);
    pos += prefix.len;

    for (0..100) |i| {
        const written = std.fmt.bufPrint(
            data_buf[pos..],
            "H{d:0>3}: value{d}\r\n",
            .{ i, i },
        ) catch unreachable;
        pos += written.len;
    }
    @memcpy(data_buf[pos..][0..2], "\r\n");
    pos += 2;

    var result = parse(std.testing.allocator, data_buf[0..pos]);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 100), result.count);
    try std.testing.expect(result.get("H000") != null);
    try std.testing.expect(result.get("H099") != null);
}

test "parsed data survives after source freed" {
    // This test verifies ownership - parsed data is independent
    const data = try std.testing.allocator.dupe(u8, "NATS/1.0\r\nKey: value\r\n\r\n");

    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    // Free source data
    std.testing.allocator.free(data);

    // ParseResult should still work (owns copies)
    try std.testing.expectEqualSlices(u8, "Key", result.items()[0].key);
    try std.testing.expectEqualSlices(u8, "value", result.items()[0].value);
}

test "error returns empty items" {
    const data = "INVALID\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expect(result.err != null);
    try std.testing.expectEqual(@as(usize, 0), result.items().len);
    try std.testing.expect(result.get("anything") == null);
}

test "deinit is safe to call multiple times" {
    const data = "NATS/1.0\r\nFoo: bar\r\n\r\n";
    var result = parse(std.testing.allocator, data);

    result.deinit();
    result.deinit();
    result.deinit();
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

test "parse status only no description" {
    const data = "NATS/1.0 503\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqualSlices(u8, "503", result.status.?);
    try std.testing.expect(result.description == null);
    try std.testing.expect(result.isNoResponders());
}

test "parse status with headers" {
    const data = "NATS/1.0 100 Idle Heartbeat\r\nNats-Last-Consumer: 42\r\n\r\n";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqualSlices(u8, "100", result.status.?);
    try std.testing.expectEqualSlices(u8, "Idle Heartbeat", result.description.?);
    try std.testing.expectEqual(@as(usize, 1), result.count);
    try std.testing.expectEqualSlices(u8, "42", result.get("Nats-Last-Consumer").?);
}

test "header_end is set correctly" {
    // "NATS/1.0\r\nFoo: bar\r\n\r\n" = 8 + 2 + 8 + 2 + 2 = 22 bytes
    const data = "NATS/1.0\r\nFoo: bar\r\n\r\npayload here";
    var result = parse(std.testing.allocator, data);
    defer result.deinit();

    try std.testing.expectEqual(@as(?ParseResult.ParseError, null), result.err);
    try std.testing.expectEqual(@as(usize, 22), result.header_end);
    try std.testing.expectEqualSlices(u8, "payload here", data[result.header_end..]);
}

test "extractStatus returns 503" {
    const data = "NATS/1.0 503 No Responders\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, 503), extractStatus(data));
}

test "extractStatus returns 408" {
    const data = "NATS/1.0 408 Request Timeout\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, 408), extractStatus(data));
}

test "extractStatus returns 100" {
    const data = "NATS/1.0 100 Idle Heartbeat\r\nHeader: value\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, 100), extractStatus(data));
}

test "extractStatus returns null for no status" {
    const data = "NATS/1.0\r\nFoo: bar\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, null), extractStatus(data));
}

test "extractStatus returns null for invalid prefix" {
    const data = "HTTP/1.0 200 OK\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, null), extractStatus(data));
}

test "extractStatus returns null for short data" {
    const data = "NATS";
    try std.testing.expectEqual(@as(?u16, null), extractStatus(data));
}

test "extractStatus handles status without description" {
    const data = "NATS/1.0 503\r\n\r\n";
    try std.testing.expectEqual(@as(?u16, 503), extractStatus(data));
}
