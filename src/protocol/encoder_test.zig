//! Protocol Encoder Edge Case Tests
//!
//! Comprehensive test coverage for NATS protocol encoding including:
//! - Integer conversion edge cases
//! - CRLF injection attacks (SECURITY)
//! - Empty/optional field handling
//! - SID boundary values
//! - Payload size edge cases

const std = @import("std");
const Io = std.Io;

const encoder = @import("encoder.zig");
const Encoder = encoder.Encoder;

// Section 1: Existing Tests (moved from encoder.zig)

test "encode PING" {
    var buf: [64]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    try Encoder.encodePing(&writer);
    try std.testing.expectEqualSlices(u8, "PING\r\n", writer.buffered());
}

test "encode PONG" {
    var buf: [64]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    try Encoder.encodePong(&writer);
    try std.testing.expectEqualSlices(u8, "PONG\r\n", writer.buffered());
}

test "encode PUB" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "test.subject",
        .payload = "hello",
    });

    try std.testing.expectEqualSlices(
        u8,
        "PUB test.subject 5\r\nhello\r\n",
        writer.buffered(),
    );
}

test "encode PUB with reply" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "request",
        .reply_to = "_INBOX.123",
        .payload = "data",
    });

    try std.testing.expectEqualSlices(
        u8,
        "PUB request _INBOX.123 4\r\ndata\r\n",
        writer.buffered(),
    );
}

test "encode SUB" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{
        .subject = "events.>",
        .sid = 42,
    });

    try std.testing.expectEqualSlices(
        u8,
        "SUB events.> 42\r\n",
        writer.buffered(),
    );
}

test "encode SUB with queue" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{
        .subject = "orders.*",
        .queue_group = "workers",
        .sid = 1,
    });

    try std.testing.expectEqualSlices(
        u8,
        "SUB orders.* workers 1\r\n",
        writer.buffered(),
    );
}

test "encode UNSUB" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeUnsub(&writer, .{ .sid = 5 });

    try std.testing.expectEqualSlices(u8, "UNSUB 5\r\n", writer.buffered());
}

test "encode UNSUB with max" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeUnsub(&writer, .{ .sid = 5, .max_msgs = 10 });

    try std.testing.expectEqualSlices(u8, "UNSUB 5 10\r\n", writer.buffered());
}

test "encode CONNECT" {
    var buf: [1024]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeConnect(&writer, .{
        .verbose = false,
        .name = "test-client",
    });

    const written = writer.buffered();
    try std.testing.expect(std.mem.startsWith(u8, written, "CONNECT {"));
    try std.testing.expect(std.mem.endsWith(u8, written, "}\r\n"));
    try std.testing.expect(
        std.mem.indexOf(u8, written, "\"name\":\"test-client\"") != null,
    );
}

test "encodePub empty subject rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodePub(&writer, .{
        .subject = "",
        .payload = "hello",
    });
    try std.testing.expectError(Encoder.Error.EmptySubject, result);
}

test "encodeHPub empty subject rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodeHPub(&writer, .{
        .subject = "",
        .headers = "NATS/1.0\r\n\r\n",
        .payload = "hello",
    });
    try std.testing.expectError(Encoder.Error.EmptySubject, result);
}

test "encodeHPub empty headers rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodeHPub(&writer, .{
        .subject = "test",
        .headers = "",
        .payload = "hello",
    });
    try std.testing.expectError(Encoder.Error.EmptyHeaders, result);
}

test "encodeSub empty subject rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodeSub(&writer, .{
        .subject = "",
        .sid = 1,
    });
    try std.testing.expectError(Encoder.Error.EmptySubject, result);
}

test "encodeSub invalid SID rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodeSub(&writer, .{
        .subject = "test",
        .sid = 0,
    });
    try std.testing.expectError(Encoder.Error.InvalidSid, result);
}

test "encodeUnsub invalid SID rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    const result = Encoder.encodeUnsub(&writer, .{ .sid = 0 });
    try std.testing.expectError(Encoder.Error.InvalidSid, result);
}

// Section 2: SID Boundary Value Tests

test "encodeSub SID one" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{ .subject = "test", .sid = 1 });
    try std.testing.expectEqualSlices(u8, "SUB test 1\r\n", writer.buffered());
}

test "encodeSub SID large value" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{ .subject = "test", .sid = 999999999 });
    try std.testing.expectEqualSlices(
        u8,
        "SUB test 999999999\r\n",
        writer.buffered(),
    );
}

test "encodeSub SID u64 max" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const max_sid: u64 = std.math.maxInt(u64);
    try Encoder.encodeSub(&writer, .{ .subject = "t", .sid = max_sid });

    const expected = "SUB t 18446744073709551615\r\n";
    try std.testing.expectEqualSlices(u8, expected, writer.buffered());
}

test "encodeUnsub SID one" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeUnsub(&writer, .{ .sid = 1 });
    try std.testing.expectEqualSlices(u8, "UNSUB 1\r\n", writer.buffered());
}

test "encodeUnsub SID u64 max" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const max_sid: u64 = std.math.maxInt(u64);
    try Encoder.encodeUnsub(&writer, .{ .sid = max_sid });

    const expected = "UNSUB 18446744073709551615\r\n";
    try std.testing.expectEqualSlices(u8, expected, writer.buffered());
}

test "encodeUnsub max_msgs u64 max" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const max_val: u64 = std.math.maxInt(u64);
    try Encoder.encodeUnsub(&writer, .{ .sid = 1, .max_msgs = max_val });

    const expected = "UNSUB 1 18446744073709551615\r\n";
    try std.testing.expectEqualSlices(u8, expected, writer.buffered());
}

// Section 3: Payload Size Edge Cases

test "encodePub empty payload" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "test",
        .payload = "",
    });

    try std.testing.expectEqualSlices(
        u8,
        "PUB test 0\r\n\r\n",
        writer.buffered(),
    );
}

test "encodePub single byte payload" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "test",
        .payload = "X",
    });

    try std.testing.expectEqualSlices(
        u8,
        "PUB test 1\r\nX\r\n",
        writer.buffered(),
    );
}

test "encodePub payload length 9" {
    var buf: [512]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    var payload_buf: [9]u8 = undefined;
    @memset(&payload_buf, 'X');
    try Encoder.encodePub(&writer, .{ .subject = "s", .payload = &payload_buf });
    try std.testing.expect(std.mem.startsWith(u8, writer.buffered(), "PUB s 9\r\n"));
}

test "encodePub payload length 10" {
    var buf: [512]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    var payload_buf: [10]u8 = undefined;
    @memset(&payload_buf, 'X');
    try Encoder.encodePub(&writer, .{ .subject = "s", .payload = &payload_buf });
    try std.testing.expect(std.mem.startsWith(u8, writer.buffered(), "PUB s 10\r\n"));
}

test "encodePub payload length 99" {
    var buf: [512]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    var payload_buf: [99]u8 = undefined;
    @memset(&payload_buf, 'X');
    try Encoder.encodePub(&writer, .{ .subject = "s", .payload = &payload_buf });
    try std.testing.expect(std.mem.startsWith(u8, writer.buffered(), "PUB s 99\r\n"));
}

test "encodePub payload length 100" {
    var buf: [512]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);
    var payload_buf: [100]u8 = undefined;
    @memset(&payload_buf, 'X');
    try Encoder.encodePub(&writer, .{ .subject = "s", .payload = &payload_buf });
    try std.testing.expect(std.mem.startsWith(u8, writer.buffered(), "PUB s 100\r\n"));
}

test "encodeHPub empty payload with headers" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeHPub(&writer, .{
        .subject = "test",
        .headers = "NATS/1.0\r\n\r\n",
        .payload = "",
    });

    // headers.len = 12, total_len = 12 (headers only)
    try std.testing.expectEqualSlices(
        u8,
        "HPUB test 12 12\r\nNATS/1.0\r\n\r\n\r\n",
        writer.buffered(),
    );
}

test "encodeHPub headers and payload lengths" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeHPub(&writer, .{
        .subject = "test",
        .headers = "NATS/1.0\r\nX:Y\r\n\r\n", // 17 bytes
        .payload = "hello", // 5 bytes
    });

    // headers.len = 17, total_len = 22
    try std.testing.expectEqualSlices(
        u8,
        "HPUB test 17 22\r\nNATS/1.0\r\nX:Y\r\n\r\nhello\r\n",
        writer.buffered(),
    );
}

// Section 4: Subject Edge Cases

test "encodePub single char subject" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{ .subject = "x", .payload = "y" });
    try std.testing.expectEqualSlices(u8, "PUB x 1\r\ny\r\n", writer.buffered());
}

test "encodePub subject with dots" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "foo.bar.baz",
        .payload = "",
    });
    try std.testing.expectEqualSlices(
        u8,
        "PUB foo.bar.baz 0\r\n\r\n",
        writer.buffered(),
    );
}

test "encodeSub subject with wildcards" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{ .subject = "foo.*.bar", .sid = 1 });
    try std.testing.expectEqualSlices(
        u8,
        "SUB foo.*.bar 1\r\n",
        writer.buffered(),
    );
}

test "encodeSub subject with full wildcard" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{ .subject = "foo.>", .sid = 1 });
    try std.testing.expectEqualSlices(
        u8,
        "SUB foo.> 1\r\n",
        writer.buffered(),
    );
}

test "encodeSub subject only wildcard" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{ .subject = ">", .sid = 1 });
    try std.testing.expectEqualSlices(u8, "SUB > 1\r\n", writer.buffered());
}

// Section 5: CRLF Injection Tests (SECURITY) - FIXED

test "encodePub subject with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // CRLF injection attempt - must be rejected
    const malicious_subject = "test\r\nUNSUB 1\r\nPUB foo";
    const result = Encoder.encodePub(&writer, .{
        .subject = malicious_subject,
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodePub reply_to with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const malicious_reply = "_INBOX\r\nUNSUB 1\r\nPUB foo";
    const result = Encoder.encodePub(&writer, .{
        .subject = "test",
        .reply_to = malicious_reply,
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeSub subject with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const malicious_subject = "test\r\nUNSUB 1";
    const result = Encoder.encodeSub(&writer, .{
        .subject = malicious_subject,
        .sid = 1,
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeSub queue_group with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const malicious_queue = "workers\r\nUNSUB 1";
    const result = Encoder.encodeSub(&writer, .{
        .subject = "test",
        .queue_group = malicious_queue,
        .sid = 1,
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeHPub subject with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const malicious_subject = "test\r\nUNSUB 1";
    const result = Encoder.encodeHPub(&writer, .{
        .subject = malicious_subject,
        .headers = "NATS/1.0\r\n\r\n",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeHPub reply_to with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const malicious_reply = "_INBOX\r\nUNSUB 1";
    const result = Encoder.encodeHPub(&writer, .{
        .subject = "test",
        .reply_to = malicious_reply,
        .headers = "NATS/1.0\r\n\r\n",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

// Section 6: Space in Fields Tests - FIXED

test "encodePub subject with space rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Space in subject must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test subject",
        .payload = "x",
    });

    try std.testing.expectError(error.SpaceInSubject, result);
}

test "encodeSub queue_group with space rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Space in queue_group must be rejected
    const result = Encoder.encodeSub(&writer, .{
        .subject = "test",
        .queue_group = "worker group",
        .sid = 1,
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

// Section 7: Empty Optional Field Tests - FIXED

test "encodePub empty reply_to treated as null" {
    // Empty string reply_to is now treated as null (skipped)
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "test",
        .reply_to = "", // Empty string treated as null
        .payload = "x",
    });

    // Should produce same output as no reply_to
    try std.testing.expectEqualSlices(
        u8,
        "PUB test 1\r\nx\r\n",
        writer.buffered(),
    );
}

test "encodeSub empty queue_group treated as null" {
    // Empty string queue_group is now treated as null (skipped)
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeSub(&writer, .{
        .subject = "test",
        .queue_group = "", // Empty string treated as null
        .sid = 1,
    });

    // Should produce same output as no queue_group
    try std.testing.expectEqualSlices(
        u8,
        "SUB test 1\r\n",
        writer.buffered(),
    );
}

test "encodeHPub empty reply_to treated as null" {
    // Empty string reply_to is now treated as null (skipped)
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeHPub(&writer, .{
        .subject = "test",
        .reply_to = "",
        .headers = "NATS/1.0\r\n\r\n",
        .payload = "x",
    });

    // Should produce same output as no reply_to
    try std.testing.expectEqualSlices(
        u8,
        "HPUB test 12 13\r\nNATS/1.0\r\n\r\nx\r\n",
        writer.buffered(),
    );
}

// Section 8: Null Byte Tests - FIXED

test "encodePub subject with null byte rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Null byte in subject must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test\x00inject",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodePub payload with null byte allowed" {
    // Payload CAN contain null bytes (binary data)
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodePub(&writer, .{
        .subject = "test",
        .payload = "hel\x00lo",
    });

    const written = writer.buffered();
    // Null byte should be in payload
    try std.testing.expectEqualSlices(
        u8,
        "PUB test 6\r\nhel\x00lo\r\n",
        written,
    );
}

// Section 9: Control Character Tests - FIXED

test "encodePub subject with tab rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Tab in subject must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test\tsubject",
        .payload = "x",
    });

    try std.testing.expectError(error.SpaceInSubject, result);
}

test "encodePub subject with CR only rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // CR alone must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test\rsubject",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodePub subject with LF only rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // LF alone must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test\nsubject",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodePub subject with DEL char rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // DEL (0x7F) must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test\x7fsubject",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodePub reply_to with control char rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Control char in reply_to must be rejected
    const result = Encoder.encodePub(&writer, .{
        .subject = "test",
        .reply_to = "_INBOX\x01inject",
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeSub queue_group with control char rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Control char in queue_group must be rejected
    const result = Encoder.encodeSub(&writer, .{
        .subject = "test",
        .queue_group = "workers\x01group",
        .sid = 1,
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

// Section 10: UNSUB max_msgs Edge Cases

test "encodeUnsub max_msgs zero" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // max_msgs = 0 might have special meaning or be invalid
    try Encoder.encodeUnsub(&writer, .{ .sid = 1, .max_msgs = 0 });
    try std.testing.expectEqualSlices(
        u8,
        "UNSUB 1 0\r\n",
        writer.buffered(),
    );
}

test "encodeUnsub max_msgs one" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeUnsub(&writer, .{ .sid = 1, .max_msgs = 1 });
    try std.testing.expectEqualSlices(
        u8,
        "UNSUB 1 1\r\n",
        writer.buffered(),
    );
}

// Section 11: CONNECT Edge Cases

test "encodeConnect minimal options" {
    var buf: [1024]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeConnect(&writer, .{});

    const written = writer.buffered();
    try std.testing.expect(std.mem.startsWith(u8, written, "CONNECT {"));
    try std.testing.expect(std.mem.endsWith(u8, written, "}\r\n"));
}

test "encodeConnect with all options" {
    var buf: [2048]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    try Encoder.encodeConnect(&writer, .{
        .verbose = true,
        .pedantic = true,
        .name = "full-client",
        .lang = "zig",
        .version = "1.0.0",
        .protocol = 1,
        .echo = false,
        .headers = true,
        .no_responders = true,
    });

    const written = writer.buffered();
    try std.testing.expect(std.mem.startsWith(u8, written, "CONNECT {"));
    try std.testing.expect(std.mem.endsWith(u8, written, "}\r\n"));
    try std.testing.expect(
        std.mem.indexOf(u8, written, "\"verbose\":true") != null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, written, "\"echo\":false") != null,
    );
}

// Section 12: Long Subject/Payload Tests

test "encodePub long subject" {
    var buf: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Create 200-char subject
    var subject_buf: [200]u8 = undefined;
    @memset(&subject_buf, 'x');

    try Encoder.encodePub(&writer, .{
        .subject = &subject_buf,
        .payload = "y",
    });

    const written = writer.buffered();
    try std.testing.expect(std.mem.startsWith(u8, written, "PUB "));
    try std.testing.expect(written.len > 200);
}

test "encodeSub long queue_group" {
    var buf: [4096]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    var queue_buf: [100]u8 = undefined;
    @memset(&queue_buf, 'q');

    try Encoder.encodeSub(&writer, .{
        .subject = "test",
        .queue_group = &queue_buf,
        .sid = 1,
    });

    const written = writer.buffered();
    try std.testing.expect(written.len > 100);
}

// Section 13: Binary Payload Tests

test "encodePub payload with all byte values" {
    var buf: [1024]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    // Payload containing all 256 byte values
    var payload: [256]u8 = undefined;
    for (&payload, 0..) |*p, i| {
        p.* = @intCast(i);
    }

    try Encoder.encodePub(&writer, .{
        .subject = "binary",
        .payload = &payload,
    });

    const written = writer.buffered();
    try std.testing.expect(
        std.mem.startsWith(u8, written, "PUB binary 256\r\n"),
    );
    // Verify payload is intact
    const payload_start = std.mem.indexOf(u8, written, "\r\n").? + 2;
    const payload_end = written.len - 2; // Exclude trailing \r\n
    try std.testing.expectEqualSlices(
        u8,
        &payload,
        written[payload_start..payload_end],
    );
}

test "encodeHPub binary headers and payload" {
    var buf: [1024]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const headers = "NATS/1.0\r\nBin: \x00\x01\x02\r\n\r\n";
    const payload = "\xFF\xFE\xFD";

    try Encoder.encodeHPub(&writer, .{
        .subject = "binary",
        .headers = headers,
        .payload = payload,
    });

    const written = writer.buffered();
    // Verify headers and payload are in output
    try std.testing.expect(std.mem.indexOf(u8, written, headers) != null);
}

// Section 14: HPUB with Entries Tests

const headers_mod = @import("headers.zig");

test "encodeHPubWithEntries basic" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "Foo", .value = "bar" },
    };

    try Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "test",
        .headers = &entries,
        .payload = "hello",
    });

    // NATS/1.0\r\nFoo: bar\r\n\r\n = 22 bytes
    // total = 22 + 5 = 27 bytes
    try std.testing.expectEqualSlices(
        u8,
        "HPUB test 22 27\r\nNATS/1.0\r\nFoo: bar\r\n\r\nhello\r\n",
        writer.buffered(),
    );
}

test "encodeHPubWithEntries with reply_to" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "X", .value = "Y" },
    };

    try Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "request",
        .reply_to = "_INBOX.123",
        .headers = &entries,
        .payload = "data",
    });

    // NATS/1.0\r\nX: Y\r\n\r\n = 18 bytes
    // total = 18 + 4 = 22 bytes
    try std.testing.expectEqualSlices(
        u8,
        "HPUB request _INBOX.123 18 22\r\nNATS/1.0\r\nX: Y\r\n\r\ndata\r\n",
        writer.buffered(),
    );
}

test "encodeHPubWithEntries multiple headers" {
    var buf: [512]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "Content-Type", .value = "application/json" },
        .{ .key = "Nats-Msg-Id", .value = "abc123" },
    };

    try Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "api.request",
        .headers = &entries,
        .payload = "{}",
    });

    const written = writer.buffered();
    try std.testing.expect(std.mem.startsWith(u8, written, "HPUB api.request "));
    try std.testing.expect(std.mem.indexOf(
        u8,
        written,
        "Content-Type: application/json",
    ) != null);
    try std.testing.expect(std.mem.indexOf(
        u8,
        written,
        "Nats-Msg-Id: abc123",
    ) != null);
}

test "encodeHPubWithEntries empty payload" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "Status", .value = "100" },
    };

    try Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "notify",
        .headers = &entries,
        .payload = "",
    });

    // NATS/1.0\r\n (10) + Status: 100\r\n (13) + \r\n (2) = 25 bytes
    // total = 25 + 0 = 25 bytes
    try std.testing.expectEqualSlices(
        u8,
        "HPUB notify 25 25\r\nNATS/1.0\r\nStatus: 100\r\n\r\n\r\n",
        writer.buffered(),
    );
}

test "encodeHPubWithEntries empty headers rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{};

    const result = Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "test",
        .headers = &entries,
        .payload = "x",
    });

    try std.testing.expectError(Encoder.Error.EmptyHeaders, result);
}

test "encodeHPubWithEntries empty subject rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "X", .value = "Y" },
    };

    const result = Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "",
        .headers = &entries,
        .payload = "x",
    });

    try std.testing.expectError(Encoder.Error.EmptySubject, result);
}

test "encodeHPubWithEntries subject with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "X", .value = "Y" },
    };

    const result = Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "test\r\nUNSUB 1",
        .headers = &entries,
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeHPubWithEntries reply_to with CRLF rejected" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "X", .value = "Y" },
    };

    const result = Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "test",
        .reply_to = "_INBOX\r\nUNSUB 1",
        .headers = &entries,
        .payload = "x",
    });

    try std.testing.expectError(error.InvalidCharacter, result);
}

test "encodeHPubWithEntries empty reply_to treated as null" {
    var buf: [256]u8 = undefined;
    var writer = Io.Writer.fixed(&buf);

    const entries = [_]headers_mod.Entry{
        .{ .key = "X", .value = "Y" },
    };

    try Encoder.encodeHPubWithEntries(&writer, .{
        .subject = "test",
        .reply_to = "",
        .headers = &entries,
        .payload = "x",
    });

    // Should produce same as no reply_to
    try std.testing.expectEqualSlices(
        u8,
        "HPUB test 18 19\r\nNATS/1.0\r\nX: Y\r\n\r\nx\r\n",
        writer.buffered(),
    );
}
