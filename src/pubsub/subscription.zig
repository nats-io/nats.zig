//! Subscription Management
//!
//! Tracks active subscriptions and provides message delivery.
//! Uses comptime generics for zero-overhead Go-style API.
//! Messages are delivered via client.poll(), which uses posix.poll()
//! for efficient timeout handling without background threads.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const subject_mod = @import("subject.zig");
const sync = @import("../sync.zig");

/// Message received on a subscription.
/// Data is copied and owned - caller must call deinit() to free.
pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,
    allocator: ?Allocator = null,

    /// Frees owned message data.
    pub fn deinit(self: *const Message) void {
        if (self.allocator) |alloc| {
            alloc.free(self.subject);
            alloc.free(self.data);
            if (self.reply_to) |rt| alloc.free(rt);
            if (self.headers) |h| alloc.free(h);
        }
    }
};

/// Options for receiving messages.
pub const ReceiveOptions = struct {
    timeout_ms: ?u64 = null,
};

/// Subscription state.
pub const State = enum {
    active,
    draining,
    unsubscribed,
};

/// Ring buffer for messages. Dynamically allocated.
pub const MessageQueue = struct {
    messages: []Message,
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,

    /// Creates a new message queue with given capacity.
    pub fn init(allocator: Allocator, queue_capacity: usize) !MessageQueue {
        assert(queue_capacity > 0);
        const messages = try allocator.alloc(Message, queue_capacity);
        return .{ .messages = messages };
    }

    /// Frees the queue buffer.
    pub fn deinit(self: *MessageQueue, allocator: Allocator) void {
        allocator.free(self.messages);
        self.* = undefined;
    }

    /// Pushes a message to the queue. Returns error if full.
    pub fn push(self: *MessageQueue, msg: Message) !void {
        if (self.count >= self.messages.len) return error.QueueFull;

        self.messages[self.tail] = msg;
        self.tail = (self.tail + 1) % self.messages.len;
        self.count += 1;
    }

    /// Pops a message from the queue. Returns null if empty.
    pub fn pop(self: *MessageQueue) ?Message {
        if (self.count == 0) return null;

        const msg = self.messages[self.head];
        self.head = (self.head + 1) % self.messages.len;
        self.count -= 1;
        return msg;
    }

    pub fn len(self: *const MessageQueue) usize {
        return self.count;
    }

    pub fn isEmpty(self: *const MessageQueue) bool {
        return self.count == 0;
    }

    pub fn isFull(self: *const MessageQueue) bool {
        return self.count >= self.messages.len;
    }

    pub fn capacity(self: *const MessageQueue) usize {
        return self.messages.len;
    }
};

/// Comptime generic subscription - Go-style API with per-sub message queue.
/// Uses client.poll() for efficient timeout handling without threads.
pub fn Subscription(comptime ClientType: type) type {
    return struct {
        client: *ClientType,
        sid: u64,
        subject: []const u8,
        queue_group: ?[]const u8,
        messages: sync.ThreadSafeQueue(Message),
        state: State,
        max_msgs: u64,
        received_msgs: u64,

        const Self = @This();

        /// Go-style receive with optional timeout.
        /// Uses posix.poll() for efficient waiting - no background threads.
        /// Returns null on timeout or if subscription is closed/draining.
        pub fn nextMessage(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            assert(self.state == .active or self.state == .draining);

            // Setup timeout tracking using Instant
            const has_timeout = opts.timeout_ms != null;
            const start: std.time.Instant = if (has_timeout)
                std.time.Instant.now() catch return error.TimerUnavailable
            else
                undefined;
            const timeout_ns: u64 = if (opts.timeout_ms) |ms|
                ms * std.time.ns_per_ms
            else
                0;

            while (true) {
                // Check queue first (fast path)
                if (self.messages.tryPop()) |m| {
                    self.received_msgs += 1;
                    return m;
                }

                // Check if draining and queue empty
                if (self.state == .draining) return null;

                // Calculate remaining timeout
                const remaining_ms: ?u32 = if (has_timeout) blk: {
                    const now = std.time.Instant.now() catch {
                        return error.TimerUnavailable;
                    };
                    const elapsed_ns = now.since(start);
                    if (elapsed_ns >= timeout_ns) return null;
                    const remaining_ns = timeout_ns - elapsed_ns;
                    break :blk @intCast(remaining_ns / std.time.ns_per_ms);
                } else null;

                // Poll for more data
                const got_data = self.client.poll(
                    allocator,
                    remaining_ms,
                ) catch |err| {
                    return err;
                };

                // If no data and we have a timeout, check again
                if (!got_data and has_timeout) {
                    const now = std.time.Instant.now() catch {
                        return error.TimerUnavailable;
                    };
                    const elapsed_ns = now.since(start);
                    if (elapsed_ns >= timeout_ns) return null;
                }
            }
        }

        /// Returns number of pending messages.
        pub fn pending(self: *Self) usize {
            return self.messages.len();
        }

        /// Unsubscribe from the subject.
        pub fn unsubscribe(self: *Self) !void {
            if (self.state == .unsubscribed) return;
            self.state = .unsubscribed;
            try self.client.unsubscribeSid(self.sid);
        }

        /// Start draining - finish pending messages then unsubscribe.
        pub fn drain(self: *Self) void {
            if (self.state == .active) {
                self.state = .draining;
            }
        }

        /// Check if subscription is active.
        pub fn isActive(self: *const Self) bool {
            return self.state == .active;
        }

        /// Check if subject matches a pattern.
        pub fn matches(self: *const Self, msg_subject: []const u8) bool {
            return subject_mod.matches(self.subject, msg_subject);
        }

        /// Clean up subscription resources.
        /// Removes self from client's subscription map to prevent double-free.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            // Remove from client's map first (prevents double-free)
            _ = self.client.subscriptions.remove(self.sid);

            // Close queue to wake any waiting threads
            self.messages.close();
            self.messages.deinit(allocator);
            allocator.free(self.subject);
            if (self.queue_group) |qg| {
                allocator.free(qg);
            }
            allocator.destroy(self);
        }
    };
}

test "message queue basic" {
    const allocator = std.testing.allocator;

    var queue = try MessageQueue.init(allocator, 4);
    defer queue.deinit(allocator);

    try std.testing.expect(queue.isEmpty());
    try std.testing.expectEqual(@as(usize, 4), queue.capacity());

    const msg1 = Message{
        .subject = "test",
        .sid = 1,
        .reply_to = null,
        .data = "hello",
        .headers = null,
    };

    try queue.push(msg1);
    try std.testing.expectEqual(@as(usize, 1), queue.len());

    const popped = queue.pop();
    try std.testing.expect(popped != null);
    try std.testing.expectEqualSlices(u8, "test", popped.?.subject);
    try std.testing.expect(queue.isEmpty());
}

test "message queue full" {
    const allocator = std.testing.allocator;

    var queue = try MessageQueue.init(allocator, 2);
    defer queue.deinit(allocator);

    const msg = Message{
        .subject = "x",
        .sid = 1,
        .reply_to = null,
        .data = "",
        .headers = null,
    };

    try queue.push(msg);
    try queue.push(msg);
    try std.testing.expect(queue.isFull());

    try std.testing.expectError(error.QueueFull, queue.push(msg));

    _ = queue.pop();
    try std.testing.expect(!queue.isFull());
    try queue.push(msg);
}
