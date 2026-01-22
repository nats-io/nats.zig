//! Subscription Types (for embedded/zero-allocation use)
//!
//! This module contains types for embedded/no-alloc scenarios.
//! For normal use, see client.zig Subscription type.

const std = @import("std");
const assert = std.debug.assert;

const subject_mod = @import("subject.zig");

/// Subscription state.
pub const State = enum {
    active,
    draining,
    unsubscribed,
};

/// Subscription-related errors.
pub const Error = error{
    InvalidSubject,
    InvalidSubscription,
    SubscriptionClosed,
};

/// Fixed-size ring buffer queue (zero allocations).
/// For embedded use cases where dynamic allocation is not allowed.
pub fn FixedQueue(comptime T: type, comptime capacity: u16) type {
    return struct {
        items: [capacity]T = undefined,
        head: u16 = 0,
        tail: u16 = 0,
        count: u16 = 0,

        const Self = @This();

        pub fn push(self: *Self, item: T) !void {
            if (self.count >= capacity) return error.QueueFull;
            self.items[self.tail] = item;
            self.tail = (self.tail + 1) % capacity;
            self.count += 1;
        }

        pub fn tryPop(self: *Self) ?T {
            if (self.count == 0) return null;
            const item = self.items[self.head];
            self.head = (self.head + 1) % capacity;
            self.count -= 1;
            return item;
        }

        pub fn clear(self: *Self) void {
            self.head = 0;
            self.tail = 0;
            self.count = 0;
        }
    };
}

/// FixedSubscription slot configuration.
pub const FixedSubConfig = struct {
    max_subject_len: u16 = 256,
    max_queue_group_len: u16 = 64,
    queue_capacity: u16 = 256,
};

/// Zero-allocation subscription slot (for embedded use).
/// Uses fixed buffers for subject, queue_group, and message queue.
/// Designed for embedding in fixed arrays (no heap allocation).
/// Message type must be provided by user as it's defined in client.zig.
pub fn FixedSubscription(
    comptime ClientType: type,
    comptime MessageType: type,
    comptime config: FixedSubConfig,
) type {
    return struct {
        client: *ClientType,
        sid: u64,
        subject_buf: [config.max_subject_len]u8,
        subject_len: u16,
        queue_group_buf: [config.max_queue_group_len]u8,
        queue_group_len: u16,
        messages: FixedQueue(MessageType, config.queue_capacity),
        state: State,
        max_msgs: u64,
        received_msgs: u64,
        active: bool,

        const Self = @This();

        /// Initialize an inactive slot.
        pub fn initEmpty() Self {
            return .{
                .client = undefined,
                .sid = 0,
                .subject_buf = undefined,
                .subject_len = 0,
                .queue_group_buf = undefined,
                .queue_group_len = 0,
                .messages = .{},
                .state = .unsubscribed,
                .max_msgs = 0,
                .received_msgs = 0,
                .active = false,
            };
        }

        /// Activation errors.
        pub const ActivateError = error{
            EmptySubject,
            SubjectTooLong,
            QueueGroupTooLong,
        };

        /// Activate slot with subscription data.
        pub fn activate(
            self: *Self,
            client_ptr: *ClientType,
            sid_val: u64,
            subj: []const u8,
            queue_grp: ?[]const u8,
        ) ActivateError!void {
            if (subj.len == 0) return error.EmptySubject;
            if (subj.len > config.max_subject_len) {
                return error.SubjectTooLong;
            }
            assert(subj.len > 0);
            assert(subj.len <= config.max_subject_len);

            self.client = client_ptr;
            self.sid = sid_val;
            self.subject_len = @intCast(subj.len);
            @memcpy(self.subject_buf[0..subj.len], subj);

            if (queue_grp) |qg| {
                if (qg.len > config.max_queue_group_len) {
                    return error.QueueGroupTooLong;
                }
                self.queue_group_len = @intCast(qg.len);
                @memcpy(self.queue_group_buf[0..qg.len], qg);
            } else {
                self.queue_group_len = 0;
            }

            self.messages.clear();
            self.state = .active;
            self.max_msgs = 0;
            self.received_msgs = 0;
            self.active = true;
        }

        /// Deactivate slot (returns to pool).
        pub fn deactivate(self: *Self) void {
            self.active = false;
            self.state = .unsubscribed;
            self.sid = 0;
        }

        /// Get subject slice.
        pub fn subject(self: *const Self) []const u8 {
            return self.subject_buf[0..self.subject_len];
        }

        /// Get queue group slice (null if not set).
        pub fn queueGroup(self: *const Self) ?[]const u8 {
            if (self.queue_group_len == 0) return null;
            return self.queue_group_buf[0..self.queue_group_len];
        }

        /// Returns pending message count.
        pub fn pending(self: *const Self) u16 {
            return self.messages.count;
        }

        /// Start draining.
        pub fn drain(self: *Self) void {
            if (self.state == .active) {
                self.state = .draining;
            }
        }

        /// Check if active.
        pub fn isActive(self: *const Self) bool {
            return self.state == .active and self.active;
        }

        /// Match subject pattern.
        pub fn matches(self: *const Self, msg_subject: []const u8) bool {
            return subject_mod.matches(self.subject(), msg_subject);
        }
    };
}

test {
    _ = @import("subscription_test.zig");
}
