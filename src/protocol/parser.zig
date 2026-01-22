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
pub inline fn parseU64Fast(s: []const u8) error{
    InvalidCharacter,
    Overflow,
}!u64 {
    assert(s.len > 0);
    if (s.len > 20) return error.Overflow;
    var v: u64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.InvalidCharacter;
        v = v *% 10 +% @as(u64, c - '0');
    }
    return v;
}

/// Fast decimal parser for usize. Inlined for hot path performance.
/// Uses wrapping math with length guard to prevent overflow.
pub inline fn parseUsizeFast(s: []const u8) error{
    InvalidCharacter,
    Overflow,
}!usize {
    assert(s.len > 0);
    if (s.len > 20) return error.Overflow;
    var v: usize = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.InvalidCharacter;
        v = v *% 10 +% @as(usize, c - '0');
    }
    return v;
}

/// Fast \r\n finder optimized for short NATS lines (~30 bytes).
/// Scans for '\r' then checks next byte, avoiding 2-byte pattern overhead.
inline fn findCRLF(data: []const u8) ?usize {
    if (data.len < 2) return null;
    const end = data.len - 1;
    var i: usize = 0;
    while (i < end) : (i += 1) {
        if (data[i] == '\r' and data[i + 1] == '\n') return i;
    }
    return null;
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
        InvalidServerInfo,
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

        const line_end = findCRLF(data) orelse return null;
        const line = data[0..line_end];
        const header_len = line_end + 2;

        assert(line.len > 0);

        // u32 word comparison for dispatch
        const CMD_MSG: u32 = 0x2047534D; // "MSG " in little-endian
        const CMD_PING: u32 = 0x474E4950; // "PING" in little-endian
        const CMD_PONG: u32 = 0x474E4F50; // "PONG" in little-endian
        const CMD_INFO: u32 = 0x4F464E49; // "INFO" in little-endian
        const CMD_HMSG: u32 = 0x47534D48; // "HMSG" in little-endian
        const CMD_ERR: u32 = 0x5252452D; // "-ERR" in little-endian

        if (line.len >= 4) {
            const cmd = std.mem.readInt(u32, line[0..4], .little);

            // MSG <subject> <sid> [reply-to] <size>
            if (cmd == CMD_MSG) {
                return parseFullMsgFast(data, line[4..], header_len, consumed);
            }
            // PING (exact 4 chars)
            if (cmd == CMD_PING and line.len == 4) {
                consumed.* = header_len;
                return .ping;
            }
            // PONG (exact 4 chars)
            if (cmd == CMD_PONG and line.len == 4) {
                consumed.* = header_len;
                return .pong;
            }
            // INFO <json> (need 5th char to be space)
            if (cmd == CMD_INFO and line.len >= 5 and line[4] == ' ') {
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
                ) catch |err| return switch (err) {
                    error.OutOfMemory => error.OutOfMemory,
                    error.InvalidServerInfo => Error.InvalidServerInfo,
                };
                consumed.* = header_len;
                return .{ .info = owned };
            }
            // HMSG <subject> <sid> [reply-to] <hdr_len> <total_len>
            if (cmd == CMD_HMSG and line.len >= 5 and line[4] == ' ') {
                return parseFullHMsgFast(data, line[5..], header_len, consumed);
            }
            // -ERR <message>
            if (cmd == CMD_ERR and line.len >= 5 and line[4] == ' ') {
                consumed.* = header_len;
                return .{ .err = line[5..] };
            }
        }

        if (line.len == 3 and line[0] == '+' and line[1] == 'O' and
            line[2] == 'K')
        {
            consumed.* = header_len;
            return .ok;
        }

        return Error.InvalidCommand;
    }
};

/// Verify trailing CRLF using u16 comparison (little-endian).
inline fn verifyCRLF(data: []const u8, offset: usize) bool {
    if (offset + 2 > data.len) return false;
    const word = @as(u16, @bitCast([2]u8{ data[offset], data[offset + 1] }));
    return word == 0x0A0D;
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
        // u64 max is 20 digits; reject at >= 20 to prevent overflow
        if (sid_digits >= 20) return Parser.Error.InvalidArguments;
        sid = sid *% 10 +% @as(u64, c - '0');
    }
    if (sid_digits == 0 or sid == 0) return Parser.Error.InvalidArguments;

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

    // Check for complete message
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
        // u64 max is 20 digits; reject at >= 20 to prevent overflow
        if (sid_digits >= 20) return Parser.Error.InvalidArguments;
        sid = sid *% 10 +% @as(u64, c - '0');
    }
    if (sid_digits == 0 or sid == 0 or i >= args_line.len)
        return Parser.Error.InvalidArguments;
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

    // Check for complete message
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

test {
    _ = @import("parser_test.zig");
}
