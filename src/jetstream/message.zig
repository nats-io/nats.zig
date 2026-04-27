//! JetStream message wrapper with acknowledgment protocol.
//!
//! Wraps a core NATS Message and adds JetStream ack/nak/wpi/term
//! methods that publish to the message's reply_to subject.

const std = @import("std");
const nats = @import("../nats.zig");
const Client = nats.Client;

/// JetStream message wrapper with ack protocol support.
///
/// Ownership model (mirrors Client.Message.owned):
/// - Pull/fetch path: `owned = true`. The caller receives a
///   JsMsg by value and MUST call `deinit()` when finished to
///   free the underlying backing buffer.
/// - Push callback path: `owned = false`. The subscription
///   passes a stack-local JsMsg to the handler; `deinit()`
///   is a no-op. Slice fields (subject, data, headers,
///   reply_to via the inner Client.Message) are valid ONLY
///   during the callback invocation. Do NOT copy the struct
///   out of the callback scope or save pointers past return
///   -- the backing buffer is reclaimed by the subscription
///   right after the handler returns.
///
/// This matches the existing contract for `*const
/// Client.Message` in core NATS callbacks.
pub const JsMsg = struct {
    msg: Client.Message,
    client: *Client,
    acked: bool = false,
    /// See the type-level doc comment for the lifetime
    /// contract. Default is `true` (owned) so pull-path
    /// constructions do not need to specify it.
    owned: bool = true,

    /// Acknowledges the message (+ACK).
    pub fn ack(self: *JsMsg) !void {
        std.debug.assert(!self.acked);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        try self.client.publish(reply, "+ACK");
        self.acked = true;
    }

    /// Acknowledges and waits for server confirmation.
    /// Slower than ack() but guarantees the server
    /// processed the acknowledgment.
    pub fn doubleAck(
        self: *JsMsg,
        timeout_ms: u32,
    ) !void {
        std.debug.assert(!self.acked);
        std.debug.assert(timeout_ms > 0);
        const reply = self.msg.reply_to orelse
            return;
        std.debug.assert(reply.len > 0);
        const resp = self.client.request(
            reply,
            "+ACK",
            timeout_ms,
        ) catch |err| return err;
        if (resp) |r| {
            var m = r;
            m.deinit();
        }
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
        // "+TERM " = 6 overhead, 512 - 6 = 506 max
        std.debug.assert(reason.len <= 506);
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

    /// Parses JetStream metadata from the reply subject.
    /// Returns null if the reply subject is missing or
    /// not in the expected `$JS.ACK.*` format.
    /// Returned slices point into the reply subject
    /// string (owned by the underlying Message).
    pub fn metadata(
        self: *const JsMsg,
    ) ?MsgMetadata {
        const reply = self.msg.reply_to orelse
            return null;
        std.debug.assert(reply.len > 0);
        return parseMsgMetadata(reply);
    }

    /// Frees the underlying message. No-op when `owned` is
    /// false (push-callback path -- subscription handles it).
    pub fn deinit(self: *JsMsg) void {
        if (!self.owned) return;
        self.msg.deinit();
    }
};

/// Metadata parsed from a JetStream message reply subject.
/// Format: `$JS.ACK.<stream>.<consumer>.<nDel>.<sSeq>
///          .<cSeq>.<timestamp>.<nPending>`
/// With domain: `$JS.<domain>.ACK.<stream>.<consumer>
///              .<nDel>.<sSeq>.<cSeq>.<timestamp>.<nPending>`
pub const MsgMetadata = struct {
    stream: []const u8,
    consumer: []const u8,
    num_delivered: u64,
    stream_seq: u64,
    consumer_seq: u64,
    timestamp: i64,
    num_pending: u64,
    domain: ?[]const u8,
};

/// Parses JetStream metadata from a reply subject string.
/// Returns null if the format is invalid.
fn parseMsgMetadata(
    reply: []const u8,
) ?MsgMetadata {
    std.debug.assert(reply.len > 0);
    var it = std.mem.splitScalar(u8, reply, '.');

    // Token 0: must be "$JS"
    const t0 = it.next() orelse return null;
    if (!std.mem.eql(u8, t0, "$JS")) return null;

    // Token 1: "ACK" or domain name
    const t1 = it.next() orelse return null;

    var domain: ?[]const u8 = null;
    if (!std.mem.eql(u8, t1, "ACK")) {
        domain = t1;
        const ack_tok = it.next() orelse return null;
        if (!std.mem.eql(u8, ack_tok, "ACK"))
            return null;
    }

    const stream = it.next() orelse return null;
    const consumer = it.next() orelse return null;
    const n_del = it.next() orelse return null;
    const s_seq = it.next() orelse return null;
    const c_seq = it.next() orelse return null;
    const ts = it.next() orelse return null;
    const n_pend = it.next() orelse return null;

    return MsgMetadata{
        .stream = stream,
        .consumer = consumer,
        .num_delivered = parseU64(n_del) orelse
            return null,
        .stream_seq = parseU64(s_seq) orelse
            return null,
        .consumer_seq = parseU64(c_seq) orelse
            return null,
        .timestamp = parseI64(ts) orelse return null,
        .num_pending = parseU64(n_pend) orelse
            return null,
        .domain = domain,
    };
}

fn parseU64(s: []const u8) ?u64 {
    return std.fmt.parseInt(u64, s, 10) catch null;
}

fn parseI64(s: []const u8) ?i64 {
    return std.fmt.parseInt(i64, s, 10) catch null;
}

// -- Tests --

test "parse standard reply subject" {
    const reply =
        "$JS.ACK.ORDERS.worker.1.42.42.1710000000.5";
    const md = parseMsgMetadata(reply).?;
    try std.testing.expectEqualStrings(
        "ORDERS",
        md.stream,
    );
    try std.testing.expectEqualStrings(
        "worker",
        md.consumer,
    );
    try std.testing.expectEqual(@as(u64, 1), md.num_delivered);
    try std.testing.expectEqual(@as(u64, 42), md.stream_seq);
    try std.testing.expectEqual(@as(u64, 42), md.consumer_seq);
    try std.testing.expectEqual(
        @as(i64, 1710000000),
        md.timestamp,
    );
    try std.testing.expectEqual(@as(u64, 5), md.num_pending);
    try std.testing.expect(md.domain == null);
}

test "parse reply with domain" {
    const reply =
        "$JS.hub.ACK.ORDERS.worker.1.42.42.1710000000.5";
    const md = parseMsgMetadata(reply).?;
    try std.testing.expectEqualStrings("hub", md.domain.?);
    try std.testing.expectEqualStrings(
        "ORDERS",
        md.stream,
    );
    try std.testing.expectEqualStrings(
        "worker",
        md.consumer,
    );
    try std.testing.expectEqual(@as(u64, 42), md.stream_seq);
}

test "invalid reply returns null" {
    try std.testing.expect(
        parseMsgMetadata("_INBOX.abc.def") == null,
    );
    try std.testing.expect(
        parseMsgMetadata("$JS.ACK.STREAM") == null,
    );
    try std.testing.expect(
        parseMsgMetadata("$JS.ACK") == null,
    );
    try std.testing.expect(
        parseMsgMetadata("NATS.something") == null,
    );
}

test "edge cases: zero values" {
    const reply =
        "$JS.ACK.S.C.0.0.0.0.0";
    const md = parseMsgMetadata(reply).?;
    try std.testing.expectEqual(@as(u64, 0), md.stream_seq);
    try std.testing.expectEqual(@as(u64, 0), md.consumer_seq);
    try std.testing.expectEqual(@as(u64, 0), md.num_pending);
    try std.testing.expectEqual(@as(u64, 0), md.num_delivered);
    try std.testing.expectEqual(@as(i64, 0), md.timestamp);
}
