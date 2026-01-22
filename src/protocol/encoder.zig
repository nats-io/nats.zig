//! NATS Protocol Encoder
//!
//! Encodes client commands into NATS wire protocol format.
//! All string fields are validated against CRLF injection and control chars.

const std = @import("std");
const assert = std.debug.assert;

const Io = std.Io;

const commands = @import("commands.zig");
const ConnectOptions = commands.ConnectOptions;
const PubArgs = commands.PubArgs;
const HPubArgs = commands.HPubArgs;
const HPubWithEntriesArgs = commands.HPubWithEntriesArgs;
const SubArgs = commands.SubArgs;
const UnsubArgs = commands.UnsubArgs;

const headers = @import("headers.zig");

const subject = @import("../pubsub/subject.zig");
const ValidationError = subject.ValidationError;

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
    /// Includes ValidationError for subject/reply-to/queue-group validation.
    pub const Error = error{
        EmptyHeaders,
        InvalidSid,
    } || ValidationError;

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
    /// Validates subject and reply_to for CRLF injection and control chars.
    pub fn encodePub(
        writer: *Io.Writer,
        args: PubArgs,
    ) (Error || Io.Writer.Error)!void {
        try subject.validatePublish(args.subject);
        assert(args.subject.len > 0);

        try writer.writeAll("PUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            if (reply.len > 0) {
                try subject.validateReplyTo(reply);
                try writer.writeByte(' ');
                try writer.writeAll(reply);
            }
        }

        var num_buf: [20]u8 = undefined;
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, args.payload.len));
        try writer.writeAll("\r\n");
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes HPUB command (publish with headers).
    /// Validates subject and reply_to for CRLF injection and control chars.
    pub fn encodeHPub(
        writer: *Io.Writer,
        args: HPubArgs,
    ) (Error || Io.Writer.Error)!void {
        try subject.validatePublish(args.subject);
        if (args.headers.len == 0) return Error.EmptyHeaders;
        assert(args.subject.len > 0);
        assert(args.headers.len > 0);

        try writer.writeAll("HPUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            if (reply.len > 0) {
                try subject.validateReplyTo(reply);
                try writer.writeByte(' ');
                try writer.writeAll(reply);
            }
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

    /// Encodes HPUB command with structured header entries.
    /// Calculates header size and encodes headers inline.
    pub fn encodeHPubWithEntries(
        writer: *Io.Writer,
        args: HPubWithEntriesArgs,
    ) (Error || Io.Writer.Error)!void {
        try subject.validatePublish(args.subject);
        if (args.headers.len == 0) return Error.EmptyHeaders;
        assert(args.subject.len > 0);
        assert(args.headers.len > 0);
        assert(args.headers.len <= 1024);

        try writer.writeAll("HPUB ");
        try writer.writeAll(args.subject);

        if (args.reply_to) |reply| {
            if (reply.len > 0) {
                try subject.validateReplyTo(reply);
                try writer.writeByte(' ');
                try writer.writeAll(reply);
            }
        }

        const hdr_len = headers.encodedSize(args.headers);
        const total_len = hdr_len + args.payload.len;
        var num_buf: [20]u8 = undefined;
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, hdr_len));
        try writer.writeByte(' ');
        try writer.writeAll(writeUsizeToBuffer(&num_buf, total_len));
        try writer.writeAll("\r\n");
        try headers.encode(writer, args.headers);
        try writer.writeAll(args.payload);
        try writer.writeAll("\r\n");
    }

    /// Encodes SUB command.
    /// Validates subject and queue_group for CRLF injection and control chars.
    pub fn encodeSub(
        writer: *Io.Writer,
        args: SubArgs,
    ) (Error || Io.Writer.Error)!void {
        try subject.validateSubscribe(args.subject);
        if (args.sid == 0) return Error.InvalidSid;
        assert(args.subject.len > 0);
        assert(args.sid > 0);

        try writer.writeAll("SUB ");
        try writer.writeAll(args.subject);

        if (args.queue_group) |queue| {
            if (queue.len > 0) {
                try subject.validateQueueGroup(queue);
                try writer.writeByte(' ');
                try writer.writeAll(queue);
            }
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

test {
    _ = @import("encoder_test.zig");
}
