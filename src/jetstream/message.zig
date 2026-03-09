//! JetStream message wrapper with acknowledgment protocol.
//!
//! Wraps a core NATS Message and adds JetStream ack/nak/wpi/term
//! methods that publish to the message's reply_to subject.

const std = @import("std");
const nats = @import("../nats.zig");
const Client = nats.Client;

/// JetStream message with ack protocol support.
pub const JsMsg = struct {
    msg: Client.Message,
    client: *Client,
    acked: bool = false,

    /// Acknowledges the message (+ACK).
    pub fn ack(self: *JsMsg) !void {
        std.debug.assert(!self.acked);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        try self.client.publish(reply, "+ACK");
        self.acked = true;
    }

    /// Negatively acknowledges -- triggers redelivery (-NAK).
    pub fn nak(self: *JsMsg) !void {
        std.debug.assert(!self.acked);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        try self.client.publish(reply, "-NAK");
        self.acked = true;
    }

    /// Negatively acknowledges with a redelivery delay.
    pub fn nakWithDelay(
        self: *JsMsg,
        delay_ns: i64,
    ) !void {
        std.debug.assert(!self.acked);
        std.debug.assert(delay_ns > 0);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "-NAK {{\"delay\":{d}}}",
            .{delay_ns},
        ) catch unreachable;
        try self.client.publish(reply, payload);
        self.acked = true;
    }

    /// Signals work in progress (+WPI). Can be called
    /// repeatedly to extend the ack deadline.
    pub fn inProgress(self: *JsMsg) !void {
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        try self.client.publish(reply, "+WPI");
    }

    /// Terminates message processing (+TERM).
    pub fn term(self: *JsMsg) !void {
        std.debug.assert(!self.acked);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        try self.client.publish(reply, "+TERM");
        self.acked = true;
    }

    /// Terminates with a reason string.
    pub fn termWithReason(
        self: *JsMsg,
        reason: []const u8,
    ) !void {
        std.debug.assert(!self.acked);
        std.debug.assert(reason.len > 0);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        var buf: [512]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "+TERM {s}",
            .{reason},
        ) catch unreachable;
        try self.client.publish(reply, payload);
        self.acked = true;
    }

    /// Returns the message data payload.
    pub fn data(self: *const JsMsg) []const u8 {
        return self.msg.data;
    }

    /// Returns the message subject.
    pub fn subject(self: *const JsMsg) []const u8 {
        return self.msg.subject;
    }

    /// Returns raw headers if present.
    pub fn headers(self: *const JsMsg) ?[]const u8 {
        return self.msg.headers;
    }

    /// Returns the reply-to subject (ack subject).
    pub fn replyTo(self: *const JsMsg) ?[]const u8 {
        return self.msg.reply_to;
    }

    /// Frees the underlying message.
    pub fn deinit(self: *JsMsg) void {
        self.msg.deinit();
    }
};
