//! NATS Message
//!
//! Represents a message received from or to be published to NATS.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// A NATS message with subject, optional reply-to, and payload.
pub const Message = struct {
    /// Subject the message was received on or will be published to.
    subject: []const u8,

    /// Optional reply subject for request-reply pattern.
    reply_to: ?[]const u8,

    /// Message payload data.
    data: []const u8,

    /// Subscription ID that received this message.
    sid: u64,

    /// Whether this message owns its data (needs freeing).
    owned: bool,

    /// Creates a message that does not own its data.
    /// Use this for messages with borrowed data that outlives the message.
    pub fn initBorrowed(
        subject: []const u8,
        data: []const u8,
        reply_to: ?[]const u8,
        sid: u64,
    ) Message {
        assert(subject.len > 0);
        assert(sid > 0);
        return .{
            .subject = subject,
            .reply_to = reply_to,
            .data = data,
            .sid = sid,
            .owned = false,
        };
    }

    /// Creates a message with copied data that the message owns.
    /// Caller must call deinit() with the same allocator to free.
    pub fn initOwned(
        allocator: Allocator,
        subject: []const u8,
        data: []const u8,
        reply_to: ?[]const u8,
        sid: u64,
    ) Allocator.Error!Message {
        assert(subject.len > 0);
        assert(sid > 0);
        const subject_copy = try allocator.dupe(u8, subject);
        errdefer allocator.free(subject_copy);

        const data_copy = try allocator.dupe(u8, data);
        errdefer allocator.free(data_copy);

        const reply_copy = if (reply_to) |r|
            try allocator.dupe(u8, r)
        else
            null;

        return .{
            .subject = subject_copy,
            .reply_to = reply_copy,
            .data = data_copy,
            .sid = sid,
            .owned = true,
        };
    }

    /// Frees owned data. Only call if message was created with initOwned.
    /// Allocator must be the same one passed to initOwned.
    pub fn deinit(self: *Message, allocator: Allocator) void {
        if (!self.owned) return;
        assert(self.subject.len > 0);

        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply_to) |r| {
            allocator.free(r);
        }
        self.* = undefined;
    }

    /// Returns true if this is a request expecting a reply.
    pub fn isRequest(self: Message) bool {
        return self.reply_to != null;
    }

    /// Returns the data as a string slice.
    pub fn dataAsString(self: Message) []const u8 {
        return self.data;
    }
};

test "message borrowed" {
    const subject = "test.subject";
    const data = "hello world";
    var msg = Message.initBorrowed(subject, data, null, 1);

    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqualSlices(u8, "hello world", msg.data);
    try std.testing.expectEqual(@as(?[]const u8, null), msg.reply_to);
    try std.testing.expectEqual(@as(u64, 1), msg.sid);
    try std.testing.expect(!msg.owned);
    try std.testing.expect(!msg.isRequest());
}

test "message owned" {
    const allocator = std.testing.allocator;
    var msg = try Message.initOwned(
        allocator,
        "test.subject",
        "hello world",
        "_INBOX.123",
        42,
    );
    defer msg.deinit(allocator);

    try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
    try std.testing.expectEqualSlices(u8, "hello world", msg.data);
    try std.testing.expectEqualSlices(u8, "_INBOX.123", msg.reply_to.?);
    try std.testing.expect(msg.owned);
    try std.testing.expect(msg.isRequest());
}

test "message deinit borrowed is no-op" {
    const allocator = std.testing.allocator;
    var msg = Message.initBorrowed("test", "data", null, 1);
    msg.deinit(allocator);
}
