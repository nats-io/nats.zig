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

/// Fast integer-to-string conversion (avoids std.fmt overhead).
/// Writes digits directly to buffer, returns slice of written digits.
fn writeUsizeToBuffer(buf: *[20]u8, value: usize) []const u8 {
    if (value == 0) {
        buf[19] = '0';
        return buf[19..20];
    }

    var v = value;
    var i: usize = 20;
    while (v > 0) : (v /= 10) {
        i -= 1;
        buf[i] = @intCast((v % 10) + '0');
    }
    return buf[i..20];
}

/// Protocol encoder for client commands.
pub const Encoder = struct {
    /// Encoding validation errors.
    pub const Error = error{
        EmptySubject,
        EmptyHeaders,
        InvalidSid,
    };

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
    ) (Error || Io.Writer.Error)!void {
        if (args.subject.len == 0) return Error.EmptySubject;
        assert(args.subject.len > 0);
        try writer.writeAll("PUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            try writer.writeByte(' ');
            try writer.writeAll(reply);
        }

        var num_buf: [20]u8 = undefined;
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, args.payload.len));
        try writer.writeAll("\r\n");
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes HPUB command (publish with headers).
    pub fn encodeHPub(
        writer: *Io.Writer,
        args: HPubArgs,
    ) (Error || Io.Writer.Error)!void {
        if (args.subject.len == 0) return Error.EmptySubject;
        if (args.headers.len == 0) return Error.EmptyHeaders;
        assert(args.subject.len > 0);
        assert(args.headers.len > 0);
        try writer.writeAll("HPUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            try writer.writeByte(' ');
            try writer.writeAll(reply);
        }

        const total_len = args.headers.len + args.payload.len;
        var num_buf: [20]u8 = undefined;
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, args.headers.len));
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, total_len));
        try writer.writeAll("\r\n");
        try writer.writeAll(args.headers);
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes SUB command.
    pub fn encodeSub(
        writer: *Io.Writer,
        args: SubArgs,
    ) (Error || Io.Writer.Error)!void {
        if (args.subject.len == 0) return Error.EmptySubject;
        if (args.sid == 0) return Error.InvalidSid;
        assert(args.subject.len > 0);
        assert(args.sid > 0);
        try writer.writeAll("SUB ");
        try writer.writeAll(args.subject);

        if (args.queue_group) |queue| {
            try writer.writeByte(' ');
            try writer.writeAll(queue);
        }

        var num_buf: [20]u8 = undefined;
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, args.sid));
        try writer.writeAll("\r\n");
    }

    /// Encodes UNSUB command.
    pub fn encodeUnsub(
        writer: *Io.Writer,
        args: UnsubArgs,
    ) (Error || Io.Writer.Error)!void {
        if (args.sid == 0) return Error.InvalidSid;
        assert(args.sid > 0);
        var num_buf: [20]u8 = undefined;
        try writer.writeAll("UNSUB ");
        try writer.writeAll(writeUsizeToBuffer(&num_buf, args.sid));

        if (args.max_msgs) |max| {
            try writer.writeByte(' ');
            try writer.writeAll(writeUsizeToBuffer(&num_buf, max));
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
