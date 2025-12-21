//! NATS Protocol Encoder
//!
//! Encodes client commands into NATS wire protocol format.

const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;
const commands = @import("commands.zig");
const ConnectOptions = commands.ConnectOptions;
const PubArgs = commands.PubArgs;
const HPubArgs = commands.HPubArgs;
const SubArgs = commands.SubArgs;
const UnsubArgs = commands.UnsubArgs;

/// Protocol encoder for client commands.
pub const Encoder = struct {
    /// Encodes CONNECT command with JSON options.
    pub fn encodeConnect(
        writer: *Io.Writer,
        opts: ConnectOptions,
    ) Io.Writer.Error!void {
        try writer.writeAll("CONNECT ");
        try std.json.Stringify.value(opts, .{}, writer);
        try writer.writeAll("\r\n");
    }

    /// Encodes PUB command.
    pub fn encodePub(
        writer: *Io.Writer,
        args: PubArgs,
    ) Io.Writer.Error!void {
        assert(args.subject.len > 0);
        try writer.writeAll("PUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            try writer.writeByte(' ');
            try writer.writeAll(reply);
        }

        try writer.print(" {d}\r\n", .{args.payload.len});
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes HPUB command (publish with headers).
    pub fn encodeHPub(
        writer: *Io.Writer,
        args: HPubArgs,
    ) Io.Writer.Error!void {
        assert(args.subject.len > 0);
        assert(args.headers.len > 0);
        try writer.writeAll("HPUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            try writer.writeByte(' ');
            try writer.writeAll(reply);
        }

        const total_len = args.headers.len + args.payload.len;
        try writer.print(" {d} {d}\r\n", .{ args.headers.len, total_len });
        try writer.writeAll(args.headers);
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes SUB command.
    pub fn encodeSub(
        writer: *Io.Writer,
        args: SubArgs,
    ) Io.Writer.Error!void {
        assert(args.subject.len > 0);
        assert(args.sid > 0);
        try writer.writeAll("SUB ");
        try writer.writeAll(args.subject);

        if (args.queue_group) |queue| {
            try writer.writeByte(' ');
            try writer.writeAll(queue);
        }

        try writer.print(" {d}\r\n", .{args.sid});
    }

    /// Encodes UNSUB command.
    pub fn encodeUnsub(
        writer: *Io.Writer,
        args: UnsubArgs,
    ) Io.Writer.Error!void {
        assert(args.sid > 0);
        try writer.print("UNSUB {d}", .{args.sid});

        if (args.max_msgs) |max| {
            try writer.print(" {d}", .{max});
        }

        try writer.writeAll("\r\n");
    }

    /// Encodes PING command.
    pub fn encodePing(writer: *Io.Writer) Io.Writer.Error!void {
        try writer.writeAll("PING\r\n");
    }

    /// Encodes PONG command.
    pub fn encodePong(writer: *Io.Writer) Io.Writer.Error!void {
        try writer.writeAll("PONG\r\n");
    }
};

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
