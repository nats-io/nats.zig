//! Subscription Management
//!
//! Tracks active subscriptions and provides message delivery.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const subject_mod = @import("subject.zig");

/// Subscription state.
pub const State = enum {
    /// Subscription is active and receiving messages.
    active,
    /// Subscription is draining (finishing pending messages).
    draining,
    /// Subscription has been unsubscribed.
    unsubscribed,
};

/// Statistics for a subscription.
pub const Stats = struct {
    /// Number of messages received.
    messages: u64 = 0,
    /// Total bytes received.
    bytes: u64 = 0,
    /// Number of messages dropped (queue full).
    dropped: u64 = 0,
};

/// Subscription metadata.
/// Does not own subject/queue_group memory - caller manages lifetime.
pub const Subscription = struct {
    /// Subscription ID assigned by client.
    sid: u64,
    /// Subject pattern (may contain wildcards).
    subject: []const u8,
    /// Optional queue group for load balancing.
    queue_group: ?[]const u8,
    /// Maximum messages to receive (0 = unlimited).
    max_msgs: u64,
    /// Current state.
    state: State,
    /// Statistics.
    stats: Stats,
    /// Whether subject/queue_group are owned (allocated).
    owned: bool,

    /// Creates a new subscription.
    pub fn init(
        sid: u64,
        subject: []const u8,
        queue_group: ?[]const u8,
    ) Subscription {
        assert(sid > 0);
        assert(subject.len > 0);
        return .{
            .sid = sid,
            .subject = subject,
            .queue_group = queue_group,
            .max_msgs = 0,
            .state = .active,
            .stats = .{},
            .owned = false,
        };
    }

    /// Creates a subscription with owned (duplicated) strings.
    pub fn initOwned(
        allocator: Allocator,
        sid: u64,
        subject: []const u8,
        queue_group: ?[]const u8,
    ) Allocator.Error!Subscription {
        assert(sid > 0);
        assert(subject.len > 0);
        const owned_subject = try allocator.dupe(u8, subject);
        errdefer allocator.free(owned_subject);

        const owned_queue = if (queue_group) |qg|
            try allocator.dupe(u8, qg)
        else
            null;

        return .{
            .sid = sid,
            .subject = owned_subject,
            .queue_group = owned_queue,
            .max_msgs = 0,
            .state = .active,
            .stats = .{},
            .owned = true,
        };
    }

    /// Free owned memory.
    pub fn deinit(self: *Subscription, allocator: Allocator) void {
        if (!self.owned) return;
        assert(self.subject.len > 0);

        allocator.free(self.subject);
        if (self.queue_group) |qg| {
            allocator.free(qg);
        }
        self.* = undefined;
    }

    /// Checks if this subscription matches a subject.
    pub fn matches(self: *const Subscription, msg_subject: []const u8) bool {
        assert(self.subject.len > 0);
        assert(msg_subject.len > 0);
        return subject_mod.matches(self.subject, msg_subject);
    }

    /// Records a received message.
    pub fn recordMessage(self: *Subscription, payload_len: usize) void {
        self.stats.messages += 1;
        self.stats.bytes += payload_len;
    }

    /// Records a dropped message.
    pub fn recordDropped(self: *Subscription) void {
        self.stats.dropped += 1;
    }

    /// Checks if subscription should auto-unsubscribe.
    pub fn shouldUnsubscribe(self: *const Subscription) bool {
        if (self.max_msgs == 0) return false;
        return self.stats.messages >= self.max_msgs;
    }

    /// Marks subscription as draining.
    pub fn drain(self: *Subscription) void {
        assert(self.state != .unsubscribed);
        if (self.state == .active) {
            self.state = .draining;
        }
    }

    /// Marks subscription as unsubscribed.
    pub fn unsubscribe(self: *Subscription) void {
        self.state = .unsubscribed;
    }

    /// Checks if subscription is active.
    pub fn isActive(self: *const Subscription) bool {
        return self.state == .active;
    }
};

/// Map of subscriptions by SID.
pub const SubscriptionMap = struct {
    items: std.AutoHashMapUnmanaged(u64, Subscription) = .empty,

    pub fn deinit(self: *SubscriptionMap, allocator: Allocator) void {
        var iter = self.items.valueIterator();
        while (iter.next()) |sub| {
            sub.deinit(allocator);
        }
        self.items.deinit(allocator);
    }

    pub fn put(
        self: *SubscriptionMap,
        allocator: Allocator,
        sub: Subscription,
    ) Allocator.Error!void {
        try self.items.put(allocator, sub.sid, sub);
    }

    pub fn get(self: *SubscriptionMap, sid: u64) ?*Subscription {
        return self.items.getPtr(sid);
    }

    pub fn remove(self: *SubscriptionMap, allocator: Allocator, sid: u64) void {
        if (self.items.fetchRemove(sid)) |kv| {
            var sub = kv.value;
            sub.deinit(allocator);
        }
    }

    pub fn count(self: *const SubscriptionMap) usize {
        return self.items.count();
    }

    /// Finds subscription matching a subject.
    pub fn findBySubject(
        self: *SubscriptionMap,
        msg_subject: []const u8,
    ) ?*Subscription {
        var iter = self.items.valueIterator();
        while (iter.next()) |sub| {
            if (sub.matches(msg_subject) and sub.isActive()) {
                return sub;
            }
        }
        return null;
    }
};

test "subscription basic" {
    var sub = Subscription.init(1, "foo.bar", null);

    try std.testing.expectEqual(@as(u64, 1), sub.sid);
    try std.testing.expectEqualSlices(u8, "foo.bar", sub.subject);
    try std.testing.expect(sub.queue_group == null);
    try std.testing.expect(sub.isActive());
}

test "subscription owned" {
    const allocator = std.testing.allocator;

    var sub = try Subscription.initOwned(allocator, 1, "foo.bar", "myqueue");
    defer sub.deinit(allocator);

    try std.testing.expectEqualSlices(u8, "foo.bar", sub.subject);
    try std.testing.expectEqualSlices(u8, "myqueue", sub.queue_group.?);
    try std.testing.expect(sub.owned);
}

test "subscription matches" {
    var sub = Subscription.init(1, "foo.*", null);

    try std.testing.expect(sub.matches("foo.bar"));
    try std.testing.expect(sub.matches("foo.baz"));
    try std.testing.expect(!sub.matches("foo.bar.baz"));
}

test "subscription stats" {
    var sub = Subscription.init(1, "foo", null);

    sub.recordMessage(100);
    sub.recordMessage(50);
    sub.recordDropped();

    try std.testing.expectEqual(@as(u64, 2), sub.stats.messages);
    try std.testing.expectEqual(@as(u64, 150), sub.stats.bytes);
    try std.testing.expectEqual(@as(u64, 1), sub.stats.dropped);
}

test "subscription auto unsubscribe" {
    var sub = Subscription.init(1, "foo", null);
    sub.max_msgs = 2;

    try std.testing.expect(!sub.shouldUnsubscribe());

    sub.recordMessage(10);
    try std.testing.expect(!sub.shouldUnsubscribe());

    sub.recordMessage(10);
    try std.testing.expect(sub.shouldUnsubscribe());
}

test "subscription map" {
    const allocator = std.testing.allocator;
    var map: SubscriptionMap = .{};
    defer map.deinit(allocator);

    const sub1 = Subscription.init(1, "foo", null);
    const sub2 = Subscription.init(2, "bar.*", null);

    try map.put(allocator, sub1);
    try map.put(allocator, sub2);

    try std.testing.expectEqual(@as(usize, 2), map.count());
    try std.testing.expect(map.get(1) != null);
    try std.testing.expect(map.get(3) == null);

    const found = map.findBySubject("bar.baz");
    try std.testing.expect(found != null);
    try std.testing.expectEqual(@as(u64, 2), found.?.sid);
}
