//! NATS Subscription
//!
//! Represents an active subscription to a NATS subject pattern.
//! Follows idiomatic Zig: allocator is passed to functions, not stored.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// An active subscription to a subject pattern.
pub const Subscription = struct {
    /// Unique subscription ID assigned by the client.
    sid: u64,

    /// Subject pattern this subscription matches.
    subject: []const u8,

    /// Optional queue group for load balancing.
    queue: ?[]const u8,

    /// Maximum messages to receive before auto-unsubscribe (0 = unlimited).
    max_msgs: u64,

    /// Number of messages received so far.
    received: u64,

    /// Whether this subscription is still active.
    active: bool,

    /// Whether this subscription owns its string data.
    owned: bool,

    /// Creates a subscription with borrowed data.
    pub fn initBorrowed(
        sid: u64,
        subject: []const u8,
        queue: ?[]const u8,
    ) Subscription {
        assert(sid > 0);
        assert(subject.len > 0);
        return .{
            .sid = sid,
            .subject = subject,
            .queue = queue,
            .max_msgs = 0,
            .received = 0,
            .active = true,
            .owned = false,
        };
    }

    /// Creates a subscription with copied data that it owns.
    /// Caller must call deinit() with same allocator to free.
    pub fn initOwned(
        allocator: Allocator,
        sid: u64,
        subject: []const u8,
        queue: ?[]const u8,
    ) Allocator.Error!Subscription {
        assert(sid > 0);
        assert(subject.len > 0);
        const subject_copy = try allocator.dupe(u8, subject);
        errdefer allocator.free(subject_copy);

        const queue_copy = if (queue) |q|
            try allocator.dupe(u8, q)
        else
            null;

        return .{
            .sid = sid,
            .subject = subject_copy,
            .queue = queue_copy,
            .max_msgs = 0,
            .received = 0,
            .active = true,
            .owned = true,
        };
    }

    /// Frees owned data. Only call if created with initOwned.
    /// Allocator must be the same one passed to initOwned.
    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        if (!self.owned) return;
        assert(self.subject.len > 0);

        allocator.free(self.subject);
        if (self.queue) |q| {
            allocator.free(q);
        }
        self.* = undefined;
    }

    /// Records a message received, returns true if subscription should
    /// auto-unsubscribe.
    pub fn recordMessage(self: *Subscription) bool {
        assert(self.active);
        self.received += 1;
        if (self.max_msgs > 0 and self.received >= self.max_msgs) {
            self.active = false;
            return true;
        }
        return false;
    }

    /// Sets auto-unsubscribe after receiving n more messages.
    pub fn autoUnsubscribe(self: *Subscription, max: u64) void {
        self.max_msgs = self.received + max;
    }

    /// Returns true if this is a queue subscription.
    pub fn isQueue(self: Subscription) bool {
        return self.queue != null;
    }
};

test "subscription borrowed" {
    var sub = Subscription.initBorrowed(1, "events.>", null);

    try std.testing.expectEqual(@as(u64, 1), sub.sid);
    try std.testing.expectEqualSlices(u8, "events.>", sub.subject);
    try std.testing.expectEqual(@as(?[]const u8, null), sub.queue);
    try std.testing.expect(sub.active);
    try std.testing.expect(!sub.isQueue());
}

test "subscription owned" {
    const allocator = std.testing.allocator;
    var sub = try Subscription.initOwned(allocator, 42, "orders.*", "workers");
    defer sub.deinit(allocator);

    try std.testing.expectEqual(@as(u64, 42), sub.sid);
    try std.testing.expectEqualSlices(u8, "orders.*", sub.subject);
    try std.testing.expectEqualSlices(u8, "workers", sub.queue.?);
    try std.testing.expect(sub.isQueue());
}

test "subscription auto unsubscribe" {
    var sub = Subscription.initBorrowed(1, "test", null);
    sub.autoUnsubscribe(3);

    try std.testing.expect(!sub.recordMessage());
    try std.testing.expect(!sub.recordMessage());
    try std.testing.expect(sub.recordMessage());
    try std.testing.expect(!sub.active);
}
