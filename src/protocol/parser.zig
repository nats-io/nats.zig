//! NATS Protocol Parser
//!
//! Parses incoming data from NATS server into structured commands.
//! Handles streaming data that may arrive in partial chunks.
//! Uses single-pass parsing - no multi-stage state machine.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const commands = @import("commands.zig");
const ServerCommand = commands.ServerCommand;
const ServerInfo = commands.ServerInfo;
const OwnedServerInfo = commands.OwnedServerInfo;
const MsgArgs = commands.MsgArgs;
const HMsgArgs = commands.HMsgArgs;

/// Protocol parser for NATS server commands.
/// Stateless single-pass parser - no multi-stage state machine.
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

        // Parse based on command type
        if (std.mem.startsWith(u8, line, "INFO ")) {
            const json_data = line[5..];
            var parsed = std.json.parseFromSlice(
                ServerInfo,
                allocator,
                json_data,
                .{ .ignore_unknown_fields = true },
            ) catch return Error.InvalidJson;
            defer parsed.deinit();

            const owned = OwnedServerInfo.fromParsed(allocator, parsed) catch
                return error.OutOfMemory;
            consumed.* = header_len;
            return .{ .info = owned };
        }

        if (std.mem.eql(u8, line, "PING")) {
            consumed.* = header_len;
            return .ping;
        }

        if (std.mem.eql(u8, line, "PONG")) {
            consumed.* = header_len;
            return .pong;
        }

        if (std.mem.eql(u8, line, "+OK")) {
            consumed.* = header_len;
            return .ok;
        }

        if (std.mem.startsWith(u8, line, "-ERR ")) {
            consumed.* = header_len;
            return .{ .err = line[5..] };
        }

        if (std.mem.startsWith(u8, line, "MSG ")) {
            return parseFullMsg(data, line[4..], header_len, consumed);
        }

        if (std.mem.startsWith(u8, line, "HMSG ")) {
            return parseFullHMsg(data, line[5..], header_len, consumed);
        }

        return Error.InvalidCommand;
    }
};

/// Parse complete MSG in single pass.
/// Returns null if payload not yet available.
fn parseFullMsg(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    assert(args_line.len > 0);
    assert(header_len > 0);

    var it = std.mem.splitScalar(u8, args_line, ' ');

    const subject = it.next() orelse return Parser.Error.InvalidArguments;
    const sid_str = it.next() orelse return Parser.Error.InvalidArguments;

    const sid = std.fmt.parseInt(u64, sid_str, 10) catch
        return Parser.Error.InvalidArguments;

    var reply_to: ?[]const u8 = null;
    var payload_len_str: []const u8 = undefined;

    if (it.next()) |third| {
        if (it.next()) |fourth| {
            reply_to = third;
            payload_len_str = fourth;
        } else {
            payload_len_str = third;
        }
    } else {
        return Parser.Error.InvalidArguments;
    }

    const payload_len = std.fmt.parseInt(usize, payload_len_str, 10) catch
        return Parser.Error.InvalidArguments;

    // Calculate total message size: header + payload + trailing \r\n
    const total_len = header_len + payload_len + 2;

    // Check if we have the complete message
    if (data.len < total_len) {
        return null;
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

/// Parse complete HMSG in single pass.
/// Returns null if headers/payload not yet available.
fn parseFullHMsg(
    data: []const u8,
    args_line: []const u8,
    header_len: usize,
    consumed: *usize,
) Parser.Error!?ServerCommand {
    assert(args_line.len > 0);
    assert(header_len > 0);

    var it = std.mem.splitScalar(u8, args_line, ' ');

    const subject = it.next() orelse return Parser.Error.InvalidArguments;
    const sid_str = it.next() orelse return Parser.Error.InvalidArguments;

    const sid = std.fmt.parseInt(u64, sid_str, 10) catch
        return Parser.Error.InvalidArguments;

    const parts = blk: {
        const p1 = it.next() orelse return Parser.Error.InvalidArguments;
        const p2 = it.next() orelse return Parser.Error.InvalidArguments;
        const p3 = it.next();

        if (p3) |third| {
            break :blk .{ p1, p2, third };
        } else {
            break :blk .{ null, p1, p2 };
        }
    };

    const reply_to = parts[0];
    const hdr_len = std.fmt.parseInt(usize, parts[1], 10) catch
        return Parser.Error.InvalidArguments;
    const total_content_len = std.fmt.parseInt(usize, parts[2], 10) catch
        return Parser.Error.InvalidArguments;

    // Calculate total message size: header line + content + trailing \r\n
    const total_len = header_len + total_content_len + 2;

    // Check if we have the complete message
    if (data.len < total_len) {
        return null;
    }

    // Extract headers and payload - they're right after the header line
    const headers = data[header_len..][0..hdr_len];
    const payload = data[header_len + hdr_len ..][0 .. total_content_len - hdr_len];

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

test "parse PING" {
    var parser = Parser.init();
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
    var parser = Parser.init();
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
    var parser = Parser.init();
    var consumed: usize = 0;

    const result = try parser.parse(
        std.testing.allocator,
        "+OK\r\n",
        &consumed,
    );

    try std.testing.expectEqual(ServerCommand.ok, result.?);
}

test "parse -ERR" {
    var parser = Parser.init();
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
    var parser = Parser.init();
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
    var parser = Parser.init();
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
    var parser = Parser.init();
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
    var parser = Parser.init();
    var consumed: usize = 0;

    const data = "MSG test.subject 1 _INBOX.123 5\r\nworld\r\n";
    const result = try parser.parse(std.testing.allocator, data, &consumed);

    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "_INBOX.123", msg.reply_to.?);
    try std.testing.expectEqualSlices(u8, "world", msg.payload);
}

test "parse incomplete data returns null" {
    var parser = Parser.init();
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
    var parser = Parser.init();
    var consumed: usize = 0;

    const info_json =
        \\INFO {"server_id":"test","version":"2.10.0","proto":1,"max_payload":1048576}
    ++ "\r\n";

    const result = try parser.parse(std.testing.allocator, info_json, &consumed);
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
    var parser = Parser.init();
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
    var parser = Parser.init();
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
    const data = "HMSG test.subject 42 10 25\r\n" ++ "H" ** 10 ++ "P" ** 15 ++ "\r\n";
    const result = try parseFullHMsg(data, "test.subject 42 10 25", 28, &consumed);
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
    const data = "HMSG foo 1 _INBOX.reply 15 30\r\n" ++ "H" ** 15 ++ "P" ** 15 ++ "\r\n";
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
    var parser = Parser.init();
    var consumed: usize = 0;

    const result = parser.parse(std.testing.allocator, "INVALID\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}
