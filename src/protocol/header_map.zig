//! Header Map Builder
//!
//! Programmatic builder for NATS message headers.
//! Supports multi-value headers (same key, multiple values).
//!
//! Example:
//! ```zig
//! var headers = HeaderMap{};
//! defer headers.deinit(allocator);
//! try headers.set(allocator, "Content-Type", "application/json");
//! try headers.add(allocator, "X-Tag", "important");
//! try headers.add(allocator, "X-Tag", "urgent");  // Multiple values
//!
//! // Encode to NATS format
//! const encoded = try headers.encode(allocator);
//! defer allocator.free(encoded);
//! ```

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// Builder for NATS message headers.
/// Supports multiple values per key.
pub const HeaderMap = struct {
    /// Keys (owned, case-preserved).
    keys: std.ArrayList([]u8) = .{},
    /// Values (owned). Same index as key.
    values: std.ArrayList([]u8) = .{},

    /// Frees all memory.
    pub fn deinit(self: *HeaderMap, allocator: Allocator) void {
        for (self.keys.items) |key| {
            allocator.free(key);
        }
        self.keys.deinit(allocator);

        for (self.values.items) |value| {
            allocator.free(value);
        }
        self.values.deinit(allocator);
    }

    /// Sets a header, replacing any existing values for this key.
    /// Key comparison is case-insensitive.
    pub fn set(
        self: *HeaderMap,
        allocator: Allocator,
        key: []const u8,
        value: []const u8,
    ) Allocator.Error!void {
        assert(key.len > 0);

        // Remove existing values for this key
        self.deleteInternal(allocator, key);

        // Add new entry
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const owned_value = try allocator.dupe(u8, value);
        errdefer allocator.free(owned_value);

        try self.keys.append(allocator, owned_key);
        try self.values.append(allocator, owned_value);
    }

    /// Adds a value to a header (allows multiple values for same key).
    /// Key comparison is case-insensitive for grouping.
    pub fn add(
        self: *HeaderMap,
        allocator: Allocator,
        key: []const u8,
        value: []const u8,
    ) Allocator.Error!void {
        assert(key.len > 0);

        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);

        const owned_value = try allocator.dupe(u8, value);
        errdefer allocator.free(owned_value);

        try self.keys.append(allocator, owned_key);
        try self.values.append(allocator, owned_value);
    }

    /// Gets the first value for a header (case-insensitive lookup).
    /// Returns null if header not found.
    pub fn get(self: *const HeaderMap, key: []const u8) ?[]const u8 {
        assert(key.len > 0);
        for (self.keys.items, 0..) |k, i| {
            if (std.ascii.eqlIgnoreCase(k, key)) {
                return self.values.items[i];
            }
        }
        return null;
    }

    /// Gets the last value for a header (case-insensitive lookup).
    /// Useful when multiple values exist and you want the most recent.
    /// Returns null if header not found.
    pub fn getLast(self: *const HeaderMap, key: []const u8) ?[]const u8 {
        assert(key.len > 0);
        var last: ?[]const u8 = null;
        for (self.keys.items, 0..) |k, i| {
            if (std.ascii.eqlIgnoreCase(k, key)) {
                last = self.values.items[i];
            }
        }
        return last;
    }

    /// Gets all values for a header (case-insensitive lookup).
    /// Caller owns returned slice, must free with allocator.
    pub fn getAll(
        self: *const HeaderMap,
        allocator: Allocator,
        key: []const u8,
    ) Allocator.Error!?[]const []const u8 {
        assert(key.len > 0);

        var match_count: usize = 0;
        for (self.keys.items) |k| {
            if (std.ascii.eqlIgnoreCase(k, key)) {
                match_count += 1;
            }
        }

        if (match_count == 0) return null;

        const result = try allocator.alloc([]const u8, match_count);
        var idx: usize = 0;
        for (self.keys.items, 0..) |k, i| {
            if (std.ascii.eqlIgnoreCase(k, key)) {
                result[idx] = self.values.items[i];
                idx += 1;
            }
        }

        return result;
    }

    /// Deletes all values for a header (case-insensitive).
    pub fn delete(self: *HeaderMap, allocator: Allocator, key: []const u8) void {
        assert(key.len > 0);
        self.deleteInternal(allocator, key);
    }

    fn deleteInternal(self: *HeaderMap, allocator: Allocator, key: []const u8) void {
        var i: usize = 0;
        while (i < self.keys.items.len) {
            if (std.ascii.eqlIgnoreCase(self.keys.items[i], key)) {
                allocator.free(self.keys.orderedRemove(i));
                allocator.free(self.values.orderedRemove(i));
            } else {
                i += 1;
            }
        }
    }

    /// Returns slice of all keys (for iteration).
    /// Note: May contain duplicate keys if add() was used.
    pub fn keys_slice(self: *const HeaderMap) []const []const u8 {
        return @ptrCast(self.keys.items);
    }

    /// Returns the number of header entries.
    /// Note: Same key may appear multiple times.
    pub fn count(self: *const HeaderMap) usize {
        return self.keys.items.len;
    }

    /// Returns true if the map is empty.
    pub fn isEmpty(self: *const HeaderMap) bool {
        return self.keys.items.len == 0;
    }

    /// Encodes headers to NATS/1.0 format.
    /// Caller owns returned memory.
    pub fn encode(self: *const HeaderMap, allocator: Allocator) ![]u8 {
        if (self.keys.items.len == 0) {
            return error.EmptyHeaders;
        }

        // Calculate size
        var size: usize = 10; // "NATS/1.0\r\n"
        for (self.keys.items, 0..) |key, i| {
            // "Key: Value\r\n"
            size += key.len + 2 + self.values.items[i].len + 2;
        }
        size += 2; // final "\r\n"

        const buf = try allocator.alloc(u8, size);
        errdefer allocator.free(buf);

        var pos: usize = 0;
        @memcpy(buf[pos..][0..10], "NATS/1.0\r\n");
        pos += 10;

        for (self.keys.items, 0..) |key, i| {
            const value = self.values.items[i];
            @memcpy(buf[pos..][0..key.len], key);
            pos += key.len;
            @memcpy(buf[pos..][0..2], ": ");
            pos += 2;
            @memcpy(buf[pos..][0..value.len], value);
            pos += value.len;
            @memcpy(buf[pos..][0..2], "\r\n");
            pos += 2;
        }

        @memcpy(buf[pos..][0..2], "\r\n");
        pos += 2;

        assert(pos == size);
        return buf;
    }

    /// Returns the encoded size in bytes.
    pub fn encodedSize(self: *const HeaderMap) usize {
        if (self.keys.items.len == 0) return 0;

        var size: usize = 10; // "NATS/1.0\r\n"
        for (self.keys.items, 0..) |key, i| {
            size += key.len + 2 + self.values.items[i].len + 2;
        }
        size += 2; // final "\r\n"
        return size;
    }
};

// Tests

test "header map set and get" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Content-Type", "application/json");
    try hm.set(allocator, "X-Request-Id", "abc123");

    try std.testing.expectEqualStrings(
        "application/json",
        hm.get("Content-Type").?,
    );
    try std.testing.expectEqualStrings(
        "abc123",
        hm.get("X-Request-Id").?,
    );
    try std.testing.expectEqual(@as(?[]const u8, null), hm.get("Not-Found"));
}

test "header map case insensitive get" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Content-Type", "text/plain");

    try std.testing.expect(hm.get("content-type") != null);
    try std.testing.expect(hm.get("CONTENT-TYPE") != null);
    try std.testing.expect(hm.get("Content-Type") != null);
}

test "header map set replaces existing" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Key", "value1");
    try hm.set(allocator, "Key", "value2");

    try std.testing.expectEqualStrings("value2", hm.get("Key").?);
    try std.testing.expectEqual(@as(usize, 1), hm.count());
}

test "header map add multiple values" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.add(allocator, "X-Tag", "important");
    try hm.add(allocator, "X-Tag", "urgent");
    try hm.add(allocator, "X-Tag", "review");

    try std.testing.expectEqual(@as(usize, 3), hm.count());

    const all = try hm.getAll(allocator, "X-Tag");
    defer allocator.free(all.?);

    try std.testing.expectEqual(@as(usize, 3), all.?.len);
    try std.testing.expectEqualStrings("important", all.?[0]);
    try std.testing.expectEqualStrings("urgent", all.?[1]);
    try std.testing.expectEqualStrings("review", all.?[2]);
}

test "header map getLast" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.add(allocator, "X-Tag", "first");
    try hm.add(allocator, "X-Tag", "second");
    try hm.add(allocator, "X-Tag", "third");

    // get() returns first, getLast() returns last
    try std.testing.expectEqualStrings("first", hm.get("X-Tag").?);
    try std.testing.expectEqualStrings("third", hm.getLast("X-Tag").?);

    // Case insensitive
    try std.testing.expectEqualStrings("third", hm.getLast("x-tag").?);

    // Non-existent key returns null
    try std.testing.expectEqual(@as(?[]const u8, null), hm.getLast("Not-Found"));
}

test "header map delete" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.add(allocator, "X-Tag", "value1");
    try hm.add(allocator, "X-Tag", "value2");
    try hm.add(allocator, "Other", "keep");

    hm.delete(allocator, "X-Tag");

    try std.testing.expectEqual(@as(?[]const u8, null), hm.get("X-Tag"));
    try std.testing.expectEqualStrings("keep", hm.get("Other").?);
    try std.testing.expectEqual(@as(usize, 1), hm.count());
}

test "header map encode" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Foo", "bar");
    try hm.set(allocator, "Baz", "123");

    const encoded = try hm.encode(allocator);
    defer allocator.free(encoded);

    // Order may vary, but should contain both headers
    try std.testing.expect(std.mem.startsWith(u8, encoded, "NATS/1.0\r\n"));
    try std.testing.expect(std.mem.endsWith(u8, encoded, "\r\n\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, encoded, "Foo: bar\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "Baz: 123\r\n") != null);
}

test "header map encoded size" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Foo", "bar");

    // "NATS/1.0\r\n" (10) + "Foo: bar\r\n" (10) + "\r\n" (2) = 22
    try std.testing.expectEqual(@as(usize, 22), hm.encodedSize());

    const encoded = try hm.encode(allocator);
    defer allocator.free(encoded);

    try std.testing.expectEqual(hm.encodedSize(), encoded.len);
}

test "header map empty" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try std.testing.expect(hm.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), hm.count());
    try std.testing.expectEqual(@as(usize, 0), hm.encodedSize());
}

test "header map with empty value" {
    const allocator = std.testing.allocator;
    var hm: HeaderMap = .{};
    defer hm.deinit(allocator);

    try hm.set(allocator, "Empty", "");

    try std.testing.expectEqualStrings("", hm.get("Empty").?);

    const encoded = try hm.encode(allocator);
    defer allocator.free(encoded);

    try std.testing.expect(std.mem.indexOf(u8, encoded, "Empty: \r\n") != null);
}
