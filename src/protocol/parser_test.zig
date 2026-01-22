//! Parser Edge Case Tests
//!
//! Comprehensive test coverage for NATS protocol parser including:
//! - Integer parsing edge cases (overflow, boundaries, invalid chars)
//! - MSG/HMSG parsing (truncated, malformed, edge values)
//! - INFO JSON parsing (invalid JSON, type mismatches, overflow)
//! - Command dispatch (case sensitivity, prefix validation)
//! - CRLF verification and buffer boundaries

const std = @import("std");
const parser = @import("parser.zig");
const Parser = parser.Parser;
const parseU64Fast = parser.parseU64Fast;
const parseUsizeFast = parser.parseUsizeFast;
const ServerCommand = @import("commands.zig").ServerCommand;

// Section 1: Existing Tests (moved from parser.zig)

test "parse PING" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "PING\r\n",
        &consumed,
    );

    try std.testing.expectEqual(@as(usize, 6), consumed);
    try std.testing.expectEqual(ServerCommand.ping, result.?);
}

test "parse PONG" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "PONG\r\n",
        &consumed,
    );

    try std.testing.expectEqual(@as(usize, 6), consumed);
    try std.testing.expectEqual(ServerCommand.pong, result.?);
}

test "parse +OK" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "+OK\r\n",
        &consumed,
    );

    try std.testing.expectEqual(ServerCommand.ok, result.?);
}

test "parse -ERR" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "-ERR 'Authorization Violation'\r\n",
        &consumed,
    );

    try std.testing.expectEqualSlices(
        u8,
        "'Authorization Violation'",
        result.?.err,
    );
}

test "parse MSG without payload" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG test.subject 1 0\r\n\r\n",
        &consumed,
    );

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqual(@as(u64, 1), msg.sid);
    try std.testing.expectEqual(@as(usize, 0), msg.payload_len);
}

test "parse MSG with payload" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "MSG test.subject 42 5\r\nhello\r\n";

    const result = try p.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqual(@as(u64, 42), msg.sid);
    try std.testing.expectEqualSlices(u8, "hello", msg.payload);
    try std.testing.expectEqual(@as(usize, 30), consumed);
}

test "parse MSG with payload - partial data" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const partial = "MSG test.subject 42 5\r\nhel";
    var result = try p.parse(std.testing.allocator, partial, &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);

    const full = "MSG test.subject 42 5\r\nhello\r\n";
    result = try p.parse(std.testing.allocator, full, &consumed);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "hello", result.?.msg.payload);
}

test "parse MSG with reply-to" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "MSG test.subject 1 _INBOX.123 5\r\nworld\r\n";
    const result = try p.parse(std.testing.allocator, data, &consumed);

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "_INBOX.123", msg.reply_to.?);
    try std.testing.expectEqualSlices(u8, "world", msg.payload);
}

test "parse incomplete data returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(std.testing.allocator, "PIN", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "parse INFO" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const info_json = "INFO {\"server_id\":\"test\"," ++
        "\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n";

    const alloc = std.testing.allocator;
    const result = try p.parse(alloc, info_json, &consumed);
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }

    try std.testing.expect(result != null);
    const info = result.?.info;
    try std.testing.expectEqualSlices(u8, "test", info.server_id);
    try std.testing.expectEqualSlices(u8, "2.10.0", info.version);
    try std.testing.expectEqual(@as(u32, 1048576), info.max_payload);
}

test "parse HMSG without payload" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "HMSG test.subject 1 12 12\r\nNATS/1.0\r\n\r\n\r\n";

    const result = try p.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 1), hmsg.sid);
    try std.testing.expectEqual(@as(usize, 12), hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 12), hmsg.total_len);
}

test "parse HMSG with payload" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "HMSG test.subject 42 12 17\r\nNATS/1.0\r\n\r\nhello\r\n";

    const result = try p.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 42), hmsg.sid);
    try std.testing.expectEqualSlices(u8, "NATS/1.0\r\n\r\n", hmsg.headers);
    try std.testing.expectEqualSlices(u8, "hello", hmsg.payload);
}

test "parse invalid command" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const alloc = std.testing.allocator;
    const result = p.parse(alloc, "INVALID\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parseU64Fast valid numbers" {
    try std.testing.expectEqual(@as(u64, 0), try parseU64Fast("0"));
    try std.testing.expectEqual(@as(u64, 1), try parseU64Fast("1"));
    try std.testing.expectEqual(@as(u64, 123), try parseU64Fast("123"));
    try std.testing.expectEqual(@as(u64, 999999), try parseU64Fast("999999"));
}

test "parseU64Fast invalid input" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("abc"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12a3"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("-1"));
}

test "parseUsizeFast valid numbers" {
    try std.testing.expectEqual(@as(usize, 0), try parseUsizeFast("0"));
    try std.testing.expectEqual(@as(usize, 42), try parseUsizeFast("42"));
    const large: usize = 1048576;
    try std.testing.expectEqual(large, try parseUsizeFast("1048576"));
}

test "parseUsizeFast invalid input" {
    try std.testing.expectError(error.InvalidCharacter, parseUsizeFast("xyz"));
    try std.testing.expectError(error.InvalidCharacter, parseUsizeFast("1 2"));
}

test "parseU64Fast overflow protection" {
    // 21 digits - guaranteed overflow, should be rejected
    try std.testing.expectError(
        error.Overflow,
        parseU64Fast("123456789012345678901"),
    );
    // 20 digits at max value edge - should work
    _ = try parseU64Fast("18446744073709551615");
    // 20 zeros - should work (equals 0)
    try std.testing.expectEqual(
        @as(u64, 0),
        try parseU64Fast("00000000000000000000"),
    );
}

test "parseUsizeFast overflow protection" {
    // 21 digits - guaranteed overflow, should be rejected
    try std.testing.expectError(
        error.Overflow,
        parseUsizeFast("123456789012345678901"),
    );
    // 20 digits - should work
    _ = try parseUsizeFast("18446744073709551615");
}

test "parse MSG with SID=0 rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;
    const result = p.parse(
        std.testing.allocator,
        "MSG test.subject 0 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "parse HMSG with SID=0 rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;
    const result = p.parse(
        std.testing.allocator,
        "HMSG test.subject 0 12 12\r\nNATS/1.0\r\n\r\n\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

// Section 2: Integer Parsing Edge Cases (parseU64Fast / parseUsizeFast)

test "parseU64Fast u64 max value" {
    // u64 max = 18446744073709551615 (exactly 20 digits)
    const result = try parseU64Fast("18446744073709551615");
    try std.testing.expectEqual(std.math.maxInt(u64), result);
}

test "parseU64Fast u64 max plus one wraps" {
    // 18446744073709551616 wraps to 0 due to wrapping arithmetic
    // This is accepted because it's 20 digits (passes length check)
    const result = try parseU64Fast("18446744073709551616");
    try std.testing.expectEqual(@as(u64, 0), result);
}

test "parseU64Fast 19 digit large value" {
    const result = try parseU64Fast("9999999999999999999");
    try std.testing.expectEqual(@as(u64, 9999999999999999999), result);
}

test "parseU64Fast leading zeros preserved value" {
    // Leading zeros should parse correctly
    try std.testing.expectEqual(@as(u64, 1), try parseU64Fast("0000000000000000001"));
    try std.testing.expectEqual(@as(u64, 42), try parseU64Fast("0000000000000000042"));
    try std.testing.expectEqual(@as(u64, 0), try parseU64Fast("0000000000000000000"));
}

test "parseU64Fast 20 leading zeros" {
    // Exactly 20 zeros = 0
    try std.testing.expectEqual(
        @as(u64, 0),
        try parseU64Fast("00000000000000000000"),
    );
}

test "parseU64Fast 21 zeros overflow" {
    // 21 characters should error regardless of value
    try std.testing.expectError(
        error.Overflow,
        parseU64Fast("000000000000000000000"),
    );
}

test "parseU64Fast invalid first char" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("a123"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("x999"));
}

test "parseU64Fast invalid middle char" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12a34"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("99x99"));
}

test "parseU64Fast invalid last char" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("1234a"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("9999z"));
}

test "parseU64Fast space in middle" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12 34"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("1 2"));
}

test "parseU64Fast tab character" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12\t34"));
}

test "parseU64Fast newline character" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12\n34"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("123\r\n"));
}

test "parseU64Fast null byte" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12\x0034"));
}

test "parseU64Fast negative sign" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("-123"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("-1"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("-0"));
}

test "parseU64Fast positive sign" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("+123"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("+1"));
}

test "parseU64Fast minus in middle" {
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("12-34"));
}

test "parseU64Fast exactly 20 chars boundary" {
    // All valid 20-digit numbers should work
    _ = try parseU64Fast("10000000000000000000");
    _ = try parseU64Fast("12345678901234567890");
    _ = try parseU64Fast("99999999999999999999");
}

test "parseU64Fast exactly 21 chars overflow" {
    try std.testing.expectError(
        error.Overflow,
        parseU64Fast("100000000000000000000"),
    );
    try std.testing.expectError(
        error.Overflow,
        parseU64Fast("999999999999999999999"),
    );
}

test "parseU64Fast single digit all values" {
    try std.testing.expectEqual(@as(u64, 0), try parseU64Fast("0"));
    try std.testing.expectEqual(@as(u64, 1), try parseU64Fast("1"));
    try std.testing.expectEqual(@as(u64, 5), try parseU64Fast("5"));
    try std.testing.expectEqual(@as(u64, 9), try parseU64Fast("9"));
}

test "parseUsizeFast boundaries" {
    // Test platform-dependent boundaries
    _ = try parseUsizeFast("18446744073709551615");
    try std.testing.expectEqual(@as(usize, 0), try parseUsizeFast("0"));
    try std.testing.expectEqual(@as(usize, 1), try parseUsizeFast("1"));
}

test "parseU64Fast special ASCII near digits" {
    // Characters just before '0' (ASCII 48) and after '9' (ASCII 57)
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("/"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast(":"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("1/2"));
    try std.testing.expectError(error.InvalidCharacter, parseU64Fast("1:2"));
}

// Section 3: MSG Parsing Edge Cases

test "MSG header only no CRLF returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // No \r\n means incomplete - should return null
    const result = try p.parse(std.testing.allocator, "MSG subject 1 5", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "MSG header with CRLF but no payload returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Header complete but payload not yet arrived
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\n",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "MSG partial payload returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Only 3 of 5 payload bytes
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\nhel",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "MSG payload without trailing CRLF returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Payload complete but missing trailing \r\n
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\nhello",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "MSG one byte short of complete returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Missing final \n
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\nhello\r",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "MSG empty subject rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Double space = empty subject
    const result = p.parse(
        std.testing.allocator,
        "MSG  1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG non-numeric SID rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject abc 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG non-numeric payload length rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1 abc\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG float SID rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1.5 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG float payload length rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1 5.5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG negative SID rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject -1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG negative payload length rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1 -5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG SID one is valid" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 1), result.?.msg.sid);
}

test "MSG SID large value" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Large but valid SID
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 999999999999 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 999999999999), result.?.msg.sid);
}

test "MSG SID overflow 21 digits rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 123456789012345678901 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG zero payload valid" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 0\r\n\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.msg.payload_len);
    try std.testing.expectEqualSlices(u8, "", result.?.msg.payload);
}

test "MSG payload length overflow 21 digits rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1 123456789012345678901\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "MSG subject with dots" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG foo.bar.baz 1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "foo.bar.baz", result.?.msg.subject);
}

test "MSG single char subject" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG x 1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "x", result.?.msg.subject);
}

test "MSG long subject" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // 256 character subject
    const long_subject = "a" ** 256;
    const data = "MSG " ++ long_subject ++ " 1 5\r\nhello\r\n";

    const result = try p.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 256), result.?.msg.subject.len);
}

test "MSG reply-to with dots" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 _INBOX.abc.def 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(
        u8,
        "_INBOX.abc.def",
        result.?.msg.reply_to.?,
    );
}

test "MSG tab instead of space rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Tab is not a valid delimiter
    const result = p.parse(
        std.testing.allocator,
        "MSG\tsubject\t1\t5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "MSG missing SID field rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Only subject and length, no SID
    const result = p.parse(
        std.testing.allocator,
        "MSG subject 5\r\nhello\r\n",
        &consumed,
    );
    // This parses "5" as SID and then fails with missing length
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

// Section 4: HMSG Parsing Edge Cases

test "HMSG header length zero" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // hdr_len=0 means no headers, only payload
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 0 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.hmsg.header_len);
    try std.testing.expectEqualSlices(u8, "", result.?.hmsg.headers);
    try std.testing.expectEqualSlices(u8, "hello", result.?.hmsg.payload);
}

test "HMSG header length exceeds total rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // hdr_len > total_len is invalid
    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 1 100 50\r\n" ++ "x" ** 50 ++ "\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "HMSG header length equals total" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // hdr_len == total_len means headers only, no payload
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 12 12\r\nNATS/1.0\r\n\r\n\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 12), result.?.hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 12), result.?.hmsg.total_len);
    try std.testing.expectEqualSlices(u8, "", result.?.hmsg.payload);
}

test "HMSG header length one less than total" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // 1 byte payload
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 12 13\r\nNATS/1.0\r\n\r\nX\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "X", result.?.hmsg.payload);
}

test "HMSG zero total length" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Both hdr_len and total_len are 0
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 0 0\r\n\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 0), result.?.hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 0), result.?.hmsg.total_len);
}

test "HMSG total length overflow rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 1 0 123456789012345678901\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "HMSG header length overflow rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 1 123456789012345678901 100\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "HMSG partial headers returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Need 12 bytes of headers but only have 5
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 12 12\r\nNATS/",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "HMSG headers complete payload partial returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Headers complete (12 bytes) but payload (5 bytes) incomplete
    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 12 17\r\nNATS/1.0\r\n\r\nhel",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "HMSG with reply-to" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "HMSG subject 1 _INBOX.reply 12 17\r\nNATS/1.0\r\n\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "_INBOX.reply", result.?.hmsg.reply_to.?);
}

test "HMSG SID overflow rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 123456789012345678901 12 12\r\nNATS/1.0\r\n\r\n\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "HMSG non-numeric header length rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 1 abc 12\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

test "HMSG non-numeric total length rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "HMSG subject 1 12 abc\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidArguments, result);
}

// Section 5: INFO Parsing Edge Cases

test "INFO minimal valid json" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Minimal valid JSON - needs at least server_id or version
    const result = try p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"test\"}\r\n",
        &consumed,
    );
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "test", result.?.info.server_id);
}

test "INFO malformed json rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "INFO {invalid}\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO truncated json rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO array instead of object rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "INFO []\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO null rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "INFO null\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO string instead of object rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "INFO \"test\"\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO max_payload zero rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // max_payload = 0 is invalid
    const result = p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"test\",\"max_payload\":0}\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidServerInfo, result);
}

test "INFO max_payload exceeds limit rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // max_payload > 1GB is invalid
    const result = p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"test\",\"max_payload\":2000000000}\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidServerInfo, result);
}

test "INFO empty json rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Empty JSON has no server_id or version
    const result = p.parse(std.testing.allocator, "INFO {}\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidServerInfo, result);
}

test "INFO empty server_id with version valid" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Empty server_id is valid if version is present
    const result = try p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"\",\"version\":\"2.10.0\"}\r\n",
        &consumed,
    );
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "", result.?.info.server_id);
    try std.testing.expectEqualSlices(u8, "2.10.0", result.?.info.version);
}

test "INFO with unknown fields ignored" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"test\",\"unknown_field\":\"value\"," ++
            "\"another_unknown\":123}\r\n",
        &consumed,
    );
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "test", result.?.info.server_id);
}

test "INFO boolean as string type mismatch" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // headers should be bool, not string
    const result = p.parse(
        std.testing.allocator,
        "INFO {\"headers\":\"true\"}\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO number as string type mismatch" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // server_id should be string, not number
    const result = p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":123}\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidJson, result);
}

test "INFO string as number coerced" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Zig JSON parser coerces string "1000" to number 1000
    const result = try p.parse(
        std.testing.allocator,
        "INFO {\"server_id\":\"test\",\"max_payload\":\"1000\"}\r\n",
        &consumed,
    );
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u32, 1000), result.?.info.max_payload);
}

test "INFO with all valid fields" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const json = "INFO {" ++
        "\"server_id\":\"NATS123\"," ++
        "\"server_name\":\"my-nats\"," ++
        "\"version\":\"2.10.0\"," ++
        "\"proto\":1," ++
        "\"host\":\"localhost\"," ++
        "\"port\":4222," ++
        "\"max_payload\":1048576," ++
        "\"headers\":true," ++
        "\"jetstream\":true" ++
        "}\r\n";

    const result = try p.parse(std.testing.allocator, json, &consumed);
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }
    try std.testing.expect(result != null);
    const info = result.?.info;
    try std.testing.expectEqualSlices(u8, "NATS123", info.server_id);
    try std.testing.expectEqualSlices(u8, "my-nats", info.server_name);
    try std.testing.expectEqual(@as(u16, 4222), info.port);
    try std.testing.expectEqual(true, info.headers);
    try std.testing.expectEqual(true, info.jetstream);
}

// Section 6: Command Dispatch Edge Cases

test "parse MSG without space rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "MSG123\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse HMSG without space rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "HMSG123\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse INFO without space rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "INFO{}\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse lowercase msg rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(
        std.testing.allocator,
        "msg subject 1 5\r\nhello\r\n",
        &consumed,
    );
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse lowercase ping rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "ping\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse mixed case Ping rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "Ping\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse PING with trailing data rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "PING extra\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse PONG with trailing data rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "PONG extra\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse +OK with trailing data rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "+OK extra\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse -ERR empty message" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // -ERR with just a space and nothing after
    const result = try p.parse(std.testing.allocator, "-ERR \r\n", &consumed);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "", result.?.err);
}

test "parse -ERR no space rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "-ERRmessage\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse -ERR long message" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const long_msg = "x" ** 1000;
    const data = "-ERR " ++ long_msg ++ "\r\n";

    const result = try p.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 1000), result.?.err.len);
}

test "parse empty buffer returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(std.testing.allocator, "", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "parse single byte returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(std.testing.allocator, "M", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "parse CR only returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(std.testing.allocator, "\r", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "parse LF only returns null" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = try p.parse(std.testing.allocator, "\n", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "parse unknown command rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "UNKNOWN\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse binary garbage rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "\x00\x01\x02\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

test "parse high ASCII rejected" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const result = p.parse(std.testing.allocator, "\xFF\xFE\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}

// Section 7: CRLF Verification Edge Cases

test "MSG with LF only line ending incomplete" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // \n alone is not recognized as line ending, returns null
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\nhello\n",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "MSG with CR only line ending incomplete" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // \r alone is not recognized as line ending
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 5\rhello\r",
        &consumed,
    );
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
}

test "MSG payload contains CRLF" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // CRLF in payload is valid - payload is binary
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 7\r\nhel\r\nlo\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "hel\r\nlo", result.?.msg.payload);
}

test "MSG payload is all CRLF" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Payload of just \r\n\r\n (4 bytes)
    const result = try p.parse(
        std.testing.allocator,
        "MSG subject 1 4\r\n\r\n\r\n\r\n",
        &consumed,
    );
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "\r\n\r\n", result.?.msg.payload);
}

test "MSG wrong CRLF order in payload trailing" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // \n\r instead of \r\n at end - should fail verification
    const result = p.parse(
        std.testing.allocator,
        "MSG subject 1 5\r\nhello\n\r",
        &consumed,
    );
    // Either returns null (incomplete) or InvalidArguments
    if (result) |r| {
        try std.testing.expectEqual(@as(?ServerCommand, null), r);
    } else |err| {
        try std.testing.expectEqual(Parser.Error.InvalidArguments, err);
    }
}

// Section 8: Buffer Boundary Edge Cases

test "MSG exactly fills buffer" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "MSG subject 1 5\r\nhello\r\n";
    const result = try p.parse(std.testing.allocator, data, &consumed);

    try std.testing.expect(result != null);
    try std.testing.expectEqual(data.len, consumed);
}

test "multiple commands in buffer first only" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Two commands: MSG then PING
    const data = "MSG subject 1 5\r\nhello\r\nPING\r\n";
    const result = try p.parse(std.testing.allocator, data, &consumed);

    try std.testing.expect(result != null);
    // Should only consume the MSG, not the PING
    try std.testing.expectEqual(@as(usize, 24), consumed);
    try std.testing.expectEqualSlices(u8, "hello", result.?.msg.payload);

    // Parse again for PING
    const remaining = data[consumed..];
    var consumed2: usize = 0;
    const result2 = try p.parse(std.testing.allocator, remaining, &consumed2);
    try std.testing.expect(result2 != null);
    try std.testing.expectEqual(ServerCommand.ping, result2.?);
}

test "partial parse consumed is zero" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // Incomplete command
    const result = try p.parse(std.testing.allocator, "MSG subject 1 5\r\nhel", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "consumed never exceeds data length" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "PING\r\n";
    _ = try p.parse(std.testing.allocator, data, &consumed);

    try std.testing.expect(consumed <= data.len);
}

test "MSG with extra data after" {
    var p: Parser = .{};
    var consumed: usize = 0;

    // MSG followed by garbage
    const data = "MSG subject 1 5\r\nhello\r\ngarbage";
    const result = try p.parse(std.testing.allocator, data, &consumed);

    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(usize, 24), consumed);
    // Garbage should remain unparsed
    try std.testing.expectEqualSlices(u8, "garbage", data[consumed..]);
}

test "INFO consumed includes CRLF" {
    var p: Parser = .{};
    var consumed: usize = 0;

    const data = "INFO {\"server_id\":\"test\"}\r\n";
    const result = try p.parse(std.testing.allocator, data, &consumed);
    defer {
        if (result) |cmd| {
            switch (cmd) {
                .info => |*info| {
                    var info_mut = info.*;
                    info_mut.deinit(std.testing.allocator);
                },
                else => {},
            }
        }
    }

    try std.testing.expect(result != null);
    try std.testing.expectEqual(data.len, consumed);
}
