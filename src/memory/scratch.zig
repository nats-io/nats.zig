//! Scratchpads for Spanning Message Handling
//!
//! Pre-allocated buffers for the rare case when a message spans multiple
//! slabs or network chunks. Avoids dynamic allocation in hot path.
//!
//! Based on io_uring client design: 4KB header, 1MB payload.

const std = @import("std");
const assert = std.debug.assert;

/// Pre-allocated scratch buffers for rare spanning messages.
/// Header up to 4KB, payload up to 1MB (NATS max message size).
pub const Scratchpads = struct {
    header: [header_size]u8 = undefined,
    payload: [payload_size]u8 = undefined,
    header_len: usize = 0,
    payload_len: usize = 0,

    pub const header_size: usize = 4096;
    pub const payload_size: usize = 1_048_576;

    /// Reset scratchpads for new message.
    pub fn reset(self: *Scratchpads) void {
        self.header_len = 0;
        self.payload_len = 0;
    }

    /// Get header slice (up to header_len bytes).
    pub fn headerSlice(self: *const Scratchpads) []const u8 {
        assert(self.header_len <= header_size);
        return self.header[0..self.header_len];
    }

    /// Get payload slice (up to payload_len bytes).
    pub fn payloadSlice(self: *const Scratchpads) []const u8 {
        assert(self.payload_len <= payload_size);
        return self.payload[0..self.payload_len];
    }

    /// Copy data into header scratchpad.
    /// Returns error if data exceeds header_size.
    pub fn copyToHeader(self: *Scratchpads, data: []const u8) ![]u8 {
        assert(data.len > 0);
        if (data.len > header_size) return error.HeaderTooLarge;

        @memcpy(self.header[0..data.len], data);
        self.header_len = data.len;
        return self.header[0..data.len];
    }

    /// Copy data into payload scratchpad.
    /// Returns error if data exceeds payload_size.
    pub fn copyToPayload(self: *Scratchpads, data: []const u8) ![]u8 {
        assert(data.len > 0);
        if (data.len > payload_size) return error.PayloadTooLarge;

        @memcpy(self.payload[0..data.len], data);
        self.payload_len = data.len;
        return self.payload[0..data.len];
    }

    /// Append data to header scratchpad.
    /// Returns error if total exceeds header_size.
    pub fn appendToHeader(self: *Scratchpads, data: []const u8) ![]u8 {
        const new_len = self.header_len + data.len;
        if (new_len > header_size) return error.HeaderTooLarge;

        @memcpy(self.header[self.header_len..new_len], data);
        self.header_len = new_len;
        return self.header[0..new_len];
    }

    /// Append data to payload scratchpad.
    /// Returns error if total exceeds payload_size.
    pub fn appendToPayload(self: *Scratchpads, data: []const u8) ![]u8 {
        const new_len = self.payload_len + data.len;
        if (new_len > payload_size) return error.PayloadTooLarge;

        @memcpy(self.payload[self.payload_len..new_len], data);
        self.payload_len = new_len;
        return self.payload[0..new_len];
    }
};

/// Copy message fields into a single scratch buffer.
/// Returns slices pointing into the scratch buffer.
pub const CopyResult = struct {
    subject: []const u8,
    reply_to: ?[]const u8,
    headers: ?[]const u8,
    data: []const u8,
};

/// Copy message fields into scratch buffer.
/// All returned slices point into the provided scratch buffer.
pub fn copyMessage(
    scratch: []u8,
    subject: []const u8,
    reply_to: ?[]const u8,
    headers: ?[]const u8,
    data: []const u8,
) !CopyResult {
    assert(subject.len > 0);

    // Calculate required size
    var required: usize = subject.len + data.len;
    if (reply_to) |rt| required += rt.len;
    if (headers) |h| required += h.len;

    if (required > scratch.len) return error.ScratchTooSmall;

    var offset: usize = 0;

    // Copy subject
    const subject_end = offset + subject.len;
    @memcpy(scratch[offset..subject_end], subject);
    const result_subject = scratch[offset..subject_end];
    offset = subject_end;

    // Copy reply_to
    const result_reply_to = if (reply_to) |rt| blk: {
        const rt_end = offset + rt.len;
        @memcpy(scratch[offset..rt_end], rt);
        const result = scratch[offset..rt_end];
        offset = rt_end;
        break :blk result;
    } else null;

    // Copy headers
    const result_headers = if (headers) |h| blk: {
        const h_end = offset + h.len;
        @memcpy(scratch[offset..h_end], h);
        const result = scratch[offset..h_end];
        offset = h_end;
        break :blk result;
    } else null;

    // Copy data
    const data_end = offset + data.len;
    @memcpy(scratch[offset..data_end], data);
    const result_data = scratch[offset..data_end];

    return .{
        .subject = result_subject,
        .reply_to = result_reply_to,
        .headers = result_headers,
        .data = result_data,
    };
}

// Tests

test "Scratchpads basic operations" {
    var scratch = Scratchpads{};

    // Copy to header
    const header_data = "NATS/1.0\r\nX-Custom: value\r\n\r\n";
    const header = try scratch.copyToHeader(header_data);
    try std.testing.expectEqualSlices(u8, header_data, header);
    try std.testing.expectEqual(header_data.len, scratch.header_len);

    // Copy to payload
    const payload_data = "Hello, NATS!";
    const payload = try scratch.copyToPayload(payload_data);
    try std.testing.expectEqualSlices(u8, payload_data, payload);
    try std.testing.expectEqual(payload_data.len, scratch.payload_len);

    // Reset
    scratch.reset();
    try std.testing.expectEqual(@as(usize, 0), scratch.header_len);
    try std.testing.expectEqual(@as(usize, 0), scratch.payload_len);
}

test "Scratchpads append operations" {
    var scratch = Scratchpads{};

    // Append chunks to payload
    _ = try scratch.appendToPayload("Hello, ");
    _ = try scratch.appendToPayload("NATS!");

    try std.testing.expectEqualSlices(u8, "Hello, NATS!", scratch.payloadSlice());
}

test "Scratchpads size limits" {
    var scratch = Scratchpads{};

    // Header too large
    var big_header: [Scratchpads.header_size + 1]u8 = undefined;
    @memset(&big_header, 'H');
    try std.testing.expectError(error.HeaderTooLarge, scratch.copyToHeader(&big_header));

    // Payload too large
    var big_payload: [Scratchpads.payload_size + 1]u8 = undefined;
    @memset(&big_payload, 'P');
    try std.testing.expectError(error.PayloadTooLarge, scratch.copyToPayload(&big_payload));
}

test "copyMessage basic" {
    var scratch: [256]u8 = undefined;

    const result = try copyMessage(
        &scratch,
        "test.subject",
        "_INBOX.123",
        null,
        "payload data",
    );

    try std.testing.expectEqualSlices(u8, "test.subject", result.subject);
    try std.testing.expectEqualSlices(u8, "_INBOX.123", result.reply_to.?);
    try std.testing.expect(result.headers == null);
    try std.testing.expectEqualSlices(u8, "payload data", result.data);

    // Verify slices point into scratch
    const scratch_start = @intFromPtr(&scratch);
    const scratch_end = scratch_start + scratch.len;
    try std.testing.expect(
        @intFromPtr(result.subject.ptr) >= scratch_start,
    );
    try std.testing.expect(
        @intFromPtr(result.subject.ptr) < scratch_end,
    );
}

test "copyMessage with headers" {
    var scratch: [512]u8 = undefined;

    const result = try copyMessage(
        &scratch,
        "test.subject",
        null,
        "NATS/1.0\r\n\r\n",
        "payload",
    );

    try std.testing.expectEqualSlices(u8, "test.subject", result.subject);
    try std.testing.expect(result.reply_to == null);
    try std.testing.expectEqualSlices(u8, "NATS/1.0\r\n\r\n", result.headers.?);
    try std.testing.expectEqualSlices(u8, "payload", result.data);
}

test "copyMessage scratch too small" {
    var scratch: [10]u8 = undefined;

    const result = copyMessage(
        &scratch,
        "long.subject.name",
        null,
        null,
        "some data",
    );

    try std.testing.expectError(error.ScratchTooSmall, result);
}
