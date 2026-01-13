//! NATS Protocol Parser
//!
//! Parses incoming data from NATS server into structured commands.
//! Handles streaming data that may arrive in partial chunks.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const commands = @import("commands.zig");
const ServerCommand = commands.ServerCommand;
const RawServerInfo = commands.RawServerInfo;
const ServerInfo = commands.ServerInfo;
const MsgArgs = commands.MsgArgs;
const HMsgArgs = commands.HMsgArgs;

/// Fast decimal parser for u64. Inlined for hot path performance.
/// Uses wrapping math with length guard to prevent overflow.
pub inline fn parseU64Fast(s: []const u8) error{ InvalidCharacter, Overflow }!u64 {
    assert(s.len > 0);
    if (s.len > 20) return error.Overflow; // u64 max is 20 digits
    var v: u64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.InvalidCharacter;
        v = v *% 10 +% @as(u64, c - '0');
    }
    return v;
}

/// Fast decimal parser for usize. Inlined for hot path performance.
/// Uses wrapping math with length guard to prevent overflow.
pub inline fn parseUsizeFast(s: []const u8) error{ InvalidCharacter, Overflow }!usize {
    assert(s.len > 0);
    if (s.len > 20) return error.Overflow; // usize max is 20 digits (64-bit)
    var v: usize = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.InvalidCharacter;
        v = v *% 10 +% @as(usize, c - '0');
    }
    return v;
}

/// Protocol parser for NATS server commands.
///
/// Stateless single-pass parser - no multi-stage state machine needed.
/// Handles partial data by returning null (need more bytes).
/// Allocates only for INFO command (ServerInfo string copies).
pub const Parser = struct {
    /// Parse error types.
    pub const Error = error{
        InvalidCommand,
        InvalidArguments,
        PayloadTooLarge,
        InvalidJson,
    };

    /// Creates a new parser.
    pub fn init() Parser {
        return .{};
    }

    /// Resets the parser to initial state (no-op for stateless parser).
    pub fn reset(self: *Parser) void {
        _ = self;
    }

    /// Parses data and returns a command if complete.
    /// Returns null if no complete command is available (need more data).
    /// Sets consumed to the number of bytes consumed (0 if need more data).
    pub fn parse(
        self: *Parser,
        allocator: Allocator,
        data: []const u8,
        consumed: *usize,
    ) (Error || Allocator.Error)!?ServerCommand {
        _ = self;
        consumed.* = 0;

        if (data.len == 0) return null;

        // Find end of first line
        const line_end = std.mem.indexOf(u8, data, "\r\n") orelse return null;
        const line = data[0..line_end];
        const header_len = line_end + 2;

        assert(line.len > 0);

        // First-byte dispatch for fast command detection
        switch (line[0]) {
            'M' => {
                // MSG <subject> <sid> [reply-to] <size>
                if (line.len >= 4 and line[1] == 'S' and
                    line[2] == 'G' and line[3] == ' ')
                {
                    return parseFullMsgFast(
                        data,
                        line[4..],
                        header_len,
                        consumed,
                    );
                }
                return Error.InvalidCommand;
            },
            'H' => {
                // HMSG <subject> <sid> [reply-to] <hdr_len> <total_len>
                if (line.len >= 5 and line[1] == 'M' and line[2] == 'S' and
                    line[3] == 'G' and line[4] == ' ')
                {
                    return parseFullHMsgFast(
                        data,
                        line[5..],
                        header_len,
                        consumed,
                    );
                }
                return Error.InvalidCommand;
            },
            'P' => {
                // PING or PONG
                if (line.len == 4) {
                    if (line[1] == 'I' and line[2] == 'N' and line[3] == 'G') {
                        consumed.* = header_len;
                        return .ping;
                    }
                    if (line[1] == 'O' and line[2] == 'N' and line[3] == 'G') {
                        consumed.* = header_len;
                        return .pong;
                    }
                }
                return Error.InvalidCommand;
            },
            '+' => {
                // +OK
                if (line.len == 3 and line[1] == 'O' and line[2] == 'K') {
                    consumed.* = header_len;
                    return .ok;
                }
                return Error.InvalidCommand;
            },
            '-' => {
                // -ERR <message>
                if (line.len >= 5 and line[1] == 'E' and line[2] == 'R' and
                    line[3] == 'R' and line[4] == ' ')
                {
                    consumed.* = header_len;
                    return .{ .err = line[5..] };
                }
                return Error.InvalidCommand;
            },
            'I' => {
                // INFO <json>
                if (line.len >= 5 and line[1] == 'N' and line[2] == 'F' and
                    line[3] == 'O' and line[4] == ' ')
                {
                    const json_data = line[5..];
                    var parsed = std.json.parseFromSlice(
                        RawServerInfo,
                        allocator,
                        json_data,
                        .{ .ignore_unknown_fields = true },
                    ) catch return Error.InvalidJson;
                    defer parsed.deinit();

                    const owned = ServerInfo.fromParsed(
                        allocator,
                        parsed,
                    ) catch return error.OutOfMemory;
                    consumed.* = header_len;
                    return .{ .info = owned };
                }
                return Error.InvalidCommand;
            },
            else => return Error.InvalidCommand,
        }
    }
};

/// Verify trailing CRLF using u16 comparison (little-endian).
inline fn verifyCRLF(data: []const u8, offset: usize) bool {
    if (offset + 2 > data.len) return false;
    const word = @as(u16, @bitCast([2]u8{ data[offset], data[offset + 1] }));
    return word == 0x0A0D; // '\r\n' in little-endian
}

/// Parse complete MSG using manual byte scanning (no iterator allocation).
/// Returns null if payload not yet available.
inline fn parseFullMsgFast(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    assert(args_line.len > 0);
    assert(header_len > 0);

    var i: usize = 0;

    // Parse subject (scan to first space)
    const subj_start = i;
    while (i < args_line.len and args_line[i] != ' ') : (i += 1) {}
    if (i == subj_start or i >= args_line.len) {
        return Parser.Error.InvalidArguments;
    }
    const subject = args_line[subj_start..i];
    i += 1; // skip space

    // Parse SID inline (avoids separate function call overhead)
    var sid: u64 = 0;
    var sid_digits: u8 = 0;
    while (i < args_line.len and args_line[i] != ' ') : (i += 1) {
        const c = args_line[i];
        if (c < '0' or c > '9') return Parser.Error.InvalidArguments;
        sid_digits += 1;
        if (sid_digits > 20) return Parser.Error.InvalidArguments; // overflow guard
        sid = sid *% 10 +% @as(u64, c - '0');
    }
    if (sid_digits == 0) return Parser.Error.InvalidArguments;

    // Check if there's more to parse
    if (i >= args_line.len) return Parser.Error.InvalidArguments;
    i += 1; // skip space

    // Parse third token
    const t3_start = i;
    while (i < args_line.len and args_line[i] != ' ') : (i += 1) {}
    if (i == t3_start) return Parser.Error.InvalidArguments;
    const third = args_line[t3_start..i];

    // Check for optional fourth token (reply-to case)
    var reply_to: ?[]const u8 = null;
    var payload_len_slice: []const u8 = undefined;

    if (i < args_line.len and args_line[i] == ' ') {
        i += 1; // skip space
        reply_to = third;
        const t4_start = i;
        while (i < args_line.len and args_line[i] != ' ') : (i += 1) {}
        if (i == t4_start) return Parser.Error.InvalidArguments;
        payload_len_slice = args_line[t4_start..i];
    } else {
        payload_len_slice = third;
    }

    // Parse payload length inline (with overflow guard)
    if (payload_len_slice.len > 20) return Parser.Error.InvalidArguments;
    var payload_len: usize = 0;
    for (payload_len_slice) |c| {
        if (c < '0' or c > '9') return Parser.Error.InvalidArguments;
        payload_len = payload_len *% 10 +% @as(usize, c - '0');
    }

    // Calculate total message size: header + payload + trailing \r\n
    const total_len = header_len + payload_len + 2;

    // Check if we have the complete message
    if (data.len < total_len) return null;

    // Verify trailing CRLF with u16 comparison
    if (!verifyCRLF(data, header_len + payload_len)) {
        return Parser.Error.InvalidArguments;
    }

    // Extract payload - it's right after the header
    const payload = data[header_len..][0..payload_len];

    consumed.* = total_len;
    assert(consumed.* <= data.len);
    assert(subject.len > 0);
    assert(sid > 0);

    return .{ .msg = .{
        .subject = subject,
        .sid = sid,
        .reply_to = reply_to,
        .payload_len = payload_len,
        .payload = payload,
    } };
}

/// Parse complete MSG in single pass (legacy, kept for test compatibility).
/// Returns null if payload not yet available.
inline fn parseFullMsg(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    return parseFullMsgFast(data, args_line, header_len, consumed);
}

/// Parse complete HMSG using manual byte scanning (no iterator allocation).
/// Returns null if headers/payload not yet available.
inline fn parseFullHMsgFast(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    assert(args_line.len > 0);
    assert(header_len > 0);

    var i: usize = 0;

    // Parse subject (scan to first space)
    const subj_start = i;
    while (i < args_line.len and args_line[i] != ' ') : (i += 1) {}
    if (i == subj_start or i >= args_line.len) {
        return Parser.Error.InvalidArguments;
    }
    const subject = args_line[subj_start..i];
    i += 1; // skip space

    // Parse SID inline (with overflow guard)
    var sid: u64 = 0;
    var sid_digits: u8 = 0;
    while (i < args_line.len and args_line[i] != ' ') : (i += 1) {
        const c = args_line[i];
        if (c < '0' or c > '9') return Parser.Error.InvalidArguments;
        sid_digits += 1;
        if (sid_digits > 20) return Parser.Error.InvalidArguments; // overflow guard
        sid = sid *% 10 +% @as(u64, c - '0');
    }
    if (sid_digits == 0 or i >= args_line.len) return Parser.Error.InvalidArguments;
    i += 1; // skip space

    // Collect remaining tokens (2 or 3: [reply-to] hdr_len total_len)
    var tokens: [3][]const u8 = undefined;
    var token_count: usize = 0;

    while (i < args_line.len and token_count < 3) {
        const t_start = i;
        while (i < args_line.len and args_line[i] != ' ') : (i += 1) {}
        if (i == t_start) break;
        tokens[token_count] = args_line[t_start..i];
        token_count += 1;
        if (i < args_line.len and args_line[i] == ' ') i += 1;
    }

    if (token_count < 2) return Parser.Error.InvalidArguments;

    var reply_to: ?[]const u8 = null;
    var hdr_len_slice: []const u8 = undefined;
    var total_len_slice: []const u8 = undefined;

    if (token_count == 3) {
        reply_to = tokens[0];
        hdr_len_slice = tokens[1];
        total_len_slice = tokens[2];
    } else {
        hdr_len_slice = tokens[0];
        total_len_slice = tokens[1];
    }

    // Parse hdr_len inline (with overflow guard)
    if (hdr_len_slice.len > 20) return Parser.Error.InvalidArguments;
    var hdr_len: usize = 0;
    for (hdr_len_slice) |c| {
        if (c < '0' or c > '9') return Parser.Error.InvalidArguments;
        hdr_len = hdr_len *% 10 +% @as(usize, c - '0');
    }

    // Parse total_content_len inline (with overflow guard)
    if (total_len_slice.len > 20) return Parser.Error.InvalidArguments;
    var total_content_len: usize = 0;
    for (total_len_slice) |c| {
        if (c < '0' or c > '9') return Parser.Error.InvalidArguments;
        total_content_len = total_content_len *% 10 +% @as(usize, c - '0');
    }

    if (hdr_len > total_content_len) return Parser.Error.InvalidArguments;

    // Calculate total message size: header line + content + trailing \r\n
    const total_len = header_len + total_content_len + 2;

    // Check if we have the complete message
    if (data.len < total_len) return null;

    // Verify trailing CRLF with u16 comparison
    if (!verifyCRLF(data, header_len + total_content_len)) {
        return Parser.Error.InvalidArguments;
    }

    // Extract headers and payload - they're right after the header line
    const headers = data[header_len..][0..hdr_len];
    const payload_len = total_content_len - hdr_len;
    const payload = data[header_len + hdr_len ..][0..payload_len];

    consumed.* = total_len;
    assert(consumed.* <= data.len);
    assert(subject.len > 0);
    assert(sid > 0);
    assert(hdr_len <= total_content_len);

    return .{ .hmsg = .{
        .subject = subject,
        .sid = sid,
        .reply_to = reply_to,
        .header_len = hdr_len,
        .total_len = total_content_len,
        .headers = headers,
        .payload = payload,
    } };
}

/// Parse complete HMSG in single pass (legacy, kept for test compatibility).
/// Returns null if headers/payload not yet available.
inline fn parseFullHMsg(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    return parseFullHMsgFast(data, args_line, header_len, consumed);
}

test "parse PING" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(
        std.testing.allocator,
        "PING\r\n",
        &consumed,
    );

    try std.testing.expectEqual(@as(usize, 6), consumed);
    try std.testing.expectEqual(ServerCommand.ping, result.?);
}

test "parse PONG" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(
        std.testing.allocator,
        "PONG\r\n",
        &consumed,
    );

    try std.testing.expectEqual(@as(usize, 6), consumed);
    try std.testing.expectEqual(ServerCommand.pong, result.?);
}

test "parse +OK" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(
        std.testing.allocator,
        "+OK\r\n",
        &consumed,
    );

    try std.testing.expectEqual(ServerCommand.ok, result.?);
}

test "parse -ERR" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(
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
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(
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
    var parser: Parser = .{};
    var consumed: usize = 0;

    const data = "MSG test.subject 42 5\r\nhello\r\n";

    // Single pass - returns complete message when all data available
    const result = try parser.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqual(@as(u64, 42), msg.sid);
    try std.testing.expectEqualSlices(u8, "hello", msg.payload);
    try std.testing.expectEqual(@as(usize, 30), consumed);
}

test "parse MSG with payload - partial data" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    // Only header, no payload yet
    const partial = "MSG test.subject 42 5\r\nhel";
    var result = try parser.parse(std.testing.allocator, partial, &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);

    // Full data
    const full = "MSG test.subject 42 5\r\nhello\r\n";
    result = try parser.parse(std.testing.allocator, full, &consumed);
    try std.testing.expect(result != null);
    try std.testing.expectEqualSlices(u8, "hello", result.?.msg.payload);
}

test "parse MSG with reply-to" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const data = "MSG test.subject 1 _INBOX.123 5\r\nworld\r\n";
    const result = try parser.parse(std.testing.allocator, data, &consumed);

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "_INBOX.123", msg.reply_to.?);
    try std.testing.expectEqualSlices(u8, "world", msg.payload);
}

test "parse incomplete data returns null" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const result = try parser.parse(std.testing.allocator, "PIN", &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 0), consumed);
}

test "parseMsgLine" {
    var consumed: usize = 0;
    const data = "MSG test.subject 42 11\r\n01234567890\r\n";
    const result = try parseFullMsg(data, "test.subject 42 11", 24, &consumed);
    try std.testing.expect(result != null);
    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqual(@as(u64, 42), msg.sid);
    try std.testing.expectEqual(@as(?[]const u8, null), msg.reply_to);
    try std.testing.expectEqual(@as(usize, 11), msg.payload_len);
}

test "parseMsgLine with reply" {
    var consumed: usize = 0;
    const data = "MSG foo 1 _INBOX.x 5\r\nhello\r\n";
    const result = try parseFullMsg(data, "foo 1 _INBOX.x 5", 22, &consumed);
    try std.testing.expect(result != null);
    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "foo", msg.subject);
    try std.testing.expectEqualSlices(u8, "_INBOX.x", msg.reply_to.?);
    try std.testing.expectEqual(@as(usize, 5), msg.payload_len);
}

test "parse INFO" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const info_json = "INFO {\"server_id\":\"test\"," ++
        "\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n";

    const alloc = std.testing.allocator;
    const result = try parser.parse(alloc, info_json, &consumed);
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
    var parser: Parser = .{};
    var consumed: usize = 0;

    const data = "HMSG test.subject 1 12 12\r\nNATS/1.0\r\n\r\n\r\n";

    // Single pass - returns complete message when all data available
    const result = try parser.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 1), hmsg.sid);
    try std.testing.expectEqual(@as(usize, 12), hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 12), hmsg.total_len);
}

test "parse HMSG with payload" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const data = "HMSG test.subject 42 12 17\r\nNATS/1.0\r\n\r\nhello\r\n";

    // Single pass - returns complete message when all data available
    const result = try parser.parse(std.testing.allocator, data, &consumed);
    try std.testing.expect(result != null);

    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 42), hmsg.sid);
    try std.testing.expectEqualSlices(u8, "NATS/1.0\r\n\r\n", hmsg.headers);
    try std.testing.expectEqualSlices(u8, "hello", hmsg.payload);
}

test "parseHMsgLine" {
    var consumed: usize = 0;
    const header = "HMSG test.subject 42 10 25\r\n";
    const data = header ++ "H" ** 10 ++ "P" ** 15 ++ "\r\n";
    const args = "test.subject 42 10 25";
    const result = try parseFullHMsg(data, args, 28, &consumed);
    try std.testing.expect(result != null);
    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 42), hmsg.sid);
    try std.testing.expectEqual(@as(?[]const u8, null), hmsg.reply_to);
    try std.testing.expectEqual(@as(usize, 10), hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 25), hmsg.total_len);
}

test "parseHMsgLine with reply" {
    var consumed: usize = 0;
    const header = "HMSG foo 1 _INBOX.reply 15 30\r\n";
    const data = header ++ "H" ** 15 ++ "P" ** 15 ++ "\r\n";
    const result = try parseFullHMsg(
        data,
        "foo 1 _INBOX.reply 15 30",
        31,
        &consumed,
    );
    try std.testing.expect(result != null);
    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "foo", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 1), hmsg.sid);
    try std.testing.expectEqualSlices(u8, "_INBOX.reply", hmsg.reply_to.?);
    try std.testing.expectEqual(@as(usize, 15), hmsg.header_len);
    try std.testing.expectEqual(@as(usize, 30), hmsg.total_len);
}

test "parse invalid command" {
    var parser: Parser = .{};
    var consumed: usize = 0;

    const alloc = std.testing.allocator;
    const result = parser.parse(alloc, "INVALID\r\n", &consumed);
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
    try std.testing.expectError(error.Overflow, parseU64Fast("123456789012345678901"));
    // 20 digits at max value edge - should work
    _ = try parseU64Fast("18446744073709551615");
    // 20 zeros - should work (equals 0)
    try std.testing.expectEqual(@as(u64, 0), try parseU64Fast("00000000000000000000"));
}

test "parseUsizeFast overflow protection" {
    // 21 digits - guaranteed overflow, should be rejected
    try std.testing.expectError(error.Overflow, parseUsizeFast("123456789012345678901"));
    // 20 digits - should work
    _ = try parseUsizeFast("18446744073709551615");
}
