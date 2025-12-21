//! NATS Protocol Parser
//!
//! Parses incoming data from NATS server into structured commands.
//! Handles streaming data that may arrive in partial chunks.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const commands = @import("commands.zig");
const ServerCommand = commands.ServerCommand;
const ServerInfo = commands.ServerInfo;
const OwnedServerInfo = commands.OwnedServerInfo;
const MsgArgs = commands.MsgArgs;
const HMsgArgs = commands.HMsgArgs;

/// Parser state machine states.
pub const State = enum {
    /// Waiting for command line.
    command,
    /// Reading MSG payload.
    msg_payload,
    /// Reading HMSG headers and payload.
    hmsg_payload,
};

/// Protocol parser for NATS server commands.
/// Does not store allocator - pass to functions that need it.
pub const Parser = struct {
    state: State = .command,
    pending_msg: ?MsgArgs = null,
    pending_hmsg: ?HMsgArgs = null,

    /// Result of parsing: either a command or need more data.
    pub const Result = union(enum) {
        command: ServerCommand,
        need_more_data,
    };

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

    /// Resets the parser to initial state.
    pub fn reset(self: *Parser) void {
        self.state = .command;
        self.pending_msg = null;
        self.pending_hmsg = null;
        assert(self.state == .command);
    }

    /// Parses data and returns a command or indicates more data is needed.
    /// Returns null if no complete command is available.
    pub fn parse(
        self: *Parser,
        allocator: Allocator,
        data: []const u8,
        consumed: *usize,
    ) (Error || Allocator.Error)!?ServerCommand {
        consumed.* = 0;

        switch (self.state) {
            .command => return self.parseCommand(allocator, data, consumed),
            .msg_payload => return self.parseMsgPayload(data, consumed),
            .hmsg_payload => return self.parseHMsgPayload(data, consumed),
        }
    }

    fn parseCommand(
        self: *Parser,
        allocator: Allocator,
        data: []const u8,
        consumed: *usize,
    ) (Error || Allocator.Error)!?ServerCommand {
        assert(self.state == .command);
        const line_end = std.mem.indexOf(u8, data, "\r\n") orelse return null;
        const line = data[0..line_end];
        consumed.* = line_end + 2;
        assert(consumed.* <= data.len);

        if (std.mem.startsWith(u8, line, "INFO ")) {
            // INFO json is on same line: INFO {...}\r\n
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
            return .{ .info = owned };
        }

        if (std.mem.eql(u8, line, "PING")) {
            return .ping;
        }

        if (std.mem.eql(u8, line, "PONG")) {
            return .pong;
        }

        if (std.mem.eql(u8, line, "+OK")) {
            return .ok;
        }

        if (std.mem.startsWith(u8, line, "-ERR ")) {
            return .{ .err = line[5..] };
        }

        if (std.mem.startsWith(u8, line, "MSG ")) {
            const args = try parseMsgLine(line[4..]);
            if (args.payload_len == 0) {
                return .{ .msg = args };
            }
            self.pending_msg = args;
            self.state = .msg_payload;
            return null;
        }

        if (std.mem.startsWith(u8, line, "HMSG ")) {
            const args = try parseHMsgLine(line[5..]);
            if (args.total_len == 0) {
                return .{ .hmsg = args };
            }
            self.pending_hmsg = args;
            self.state = .hmsg_payload;
            return null;
        }

        return Error.InvalidCommand;
    }

    fn parseMsgPayload(
        self: *Parser,
        data: []const u8,
        consumed: *usize,
    ) Error!?ServerCommand {
        assert(self.state == .msg_payload);
        var args = self.pending_msg orelse return Error.InvalidArguments;
        const needed = args.payload_len + 2;

        if (data.len < needed) return null;

        assert(args.payload_len <= data.len);
        args.payload = data[0..args.payload_len];
        consumed.* = needed;
        assert(consumed.* <= data.len);
        self.pending_msg = null;
        self.state = .command;

        return .{ .msg = args };
    }

    fn parseHMsgPayload(
        self: *Parser,
        data: []const u8,
        consumed: *usize,
    ) Error!?ServerCommand {
        assert(self.state == .hmsg_payload);
        var args = self.pending_hmsg orelse return Error.InvalidArguments;
        const needed = args.total_len + 2;

        if (data.len < needed) return null;

        assert(args.header_len <= args.total_len);
        assert(args.total_len <= data.len);
        args.headers = data[0..args.header_len];
        args.payload = data[args.header_len..args.total_len];
        consumed.* = needed;
        assert(consumed.* <= data.len);
        self.pending_hmsg = null;
        self.state = .command;

        return .{ .hmsg = args };
    }
};

/// Parses MSG command arguments: subject sid [reply-to] payload_len
fn parseMsgLine(line: []const u8) Parser.Error!MsgArgs {
    assert(line.len > 0);
    var it = std.mem.splitScalar(u8, line, ' ');

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

    assert(subject.len > 0);
    assert(sid > 0);
    return .{
        .subject = subject,
        .sid = sid,
        .reply_to = reply_to,
        .payload_len = payload_len,
    };
}

/// Parses HMSG command arguments: subject sid [reply-to] hdr_len total_len
fn parseHMsgLine(line: []const u8) Parser.Error!HMsgArgs {
    assert(line.len > 0);
    var it = std.mem.splitScalar(u8, line, ' ');

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
    const header_len = std.fmt.parseInt(usize, parts[1], 10) catch
        return Parser.Error.InvalidArguments;
    const total_len = std.fmt.parseInt(usize, parts[2], 10) catch
        return Parser.Error.InvalidArguments;

    assert(subject.len > 0);
    assert(sid > 0);
    assert(header_len <= total_len);
    return .{
        .subject = subject,
        .sid = sid,
        .reply_to = reply_to,
        .header_len = header_len,
        .total_len = total_len,
    };
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

    var result = try parser.parse(std.testing.allocator, data, &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);
    try std.testing.expectEqual(@as(usize, 23), consumed);

    result = try parser.parse(
        std.testing.allocator,
        data[consumed..],
        &consumed,
    );
    const msg = result.?.msg;
    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqual(@as(u64, 42), msg.sid);
    try std.testing.expectEqualSlices(u8, "hello", msg.payload);
}

test "parse MSG with reply-to" {
    var parser = Parser.init();
    var consumed: usize = 0;

    const data = "MSG test.subject 1 _INBOX.123 5\r\nworld\r\n";

    _ = try parser.parse(std.testing.allocator, data, &consumed);
    const result = try parser.parse(
        std.testing.allocator,
        data[consumed..],
        &consumed,
    );

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
    const args = try parseMsgLine("test.subject 42 11");
    try std.testing.expectEqualSlices(u8, "test.subject", args.subject);
    try std.testing.expectEqual(@as(u64, 42), args.sid);
    try std.testing.expectEqual(@as(?[]const u8, null), args.reply_to);
    try std.testing.expectEqual(@as(usize, 11), args.payload_len);
}

test "parseMsgLine with reply" {
    const args = try parseMsgLine("foo 1 _INBOX.x 5");
    try std.testing.expectEqualSlices(u8, "foo", args.subject);
    try std.testing.expectEqualSlices(u8, "_INBOX.x", args.reply_to.?);
    try std.testing.expectEqual(@as(usize, 5), args.payload_len);
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

    var result = try parser.parse(std.testing.allocator, data, &consumed);
    try std.testing.expectEqual(@as(?ServerCommand, null), result);

    result = try parser.parse(std.testing.allocator, data[consumed..], &consumed);
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

    _ = try parser.parse(std.testing.allocator, data, &consumed);
    const result = try parser.parse(
        std.testing.allocator,
        data[consumed..],
        &consumed,
    );

    const hmsg = result.?.hmsg;
    try std.testing.expectEqualSlices(u8, "test.subject", hmsg.subject);
    try std.testing.expectEqual(@as(u64, 42), hmsg.sid);
    try std.testing.expectEqualSlices(u8, "NATS/1.0\r\n\r\n", hmsg.headers);
    try std.testing.expectEqualSlices(u8, "hello", hmsg.payload);
}

test "parseHMsgLine" {
    const args = try parseHMsgLine("test.subject 42 10 25");
    try std.testing.expectEqualSlices(u8, "test.subject", args.subject);
    try std.testing.expectEqual(@as(u64, 42), args.sid);
    try std.testing.expectEqual(@as(?[]const u8, null), args.reply_to);
    try std.testing.expectEqual(@as(usize, 10), args.header_len);
    try std.testing.expectEqual(@as(usize, 25), args.total_len);
}

test "parseHMsgLine with reply" {
    const args = try parseHMsgLine("foo 1 _INBOX.reply 15 30");
    try std.testing.expectEqualSlices(u8, "foo", args.subject);
    try std.testing.expectEqual(@as(u64, 1), args.sid);
    try std.testing.expectEqualSlices(u8, "_INBOX.reply", args.reply_to.?);
    try std.testing.expectEqual(@as(usize, 15), args.header_len);
    try std.testing.expectEqual(@as(usize, 30), args.total_len);
}

test "parse invalid command" {
    var parser = Parser.init();
    var consumed: usize = 0;

    const result = parser.parse(std.testing.allocator, "INVALID\r\n", &consumed);
    try std.testing.expectError(Parser.Error.InvalidCommand, result);
}
