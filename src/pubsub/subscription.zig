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
/// For zero-copy (owned=false): slices point into read buffer, valid until
/// next nextMessage() call. For owned (owned=true): data is allocated and
/// must be freed via deinit().
pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,
    owned: bool = false,

    /// Frees owned message data. No-op for zero-copy messages.
    /// Caller must pass same allocator used to create owned message.
    pub fn deinit(self: *const Message, allocator: Allocator) void {
        if (!self.owned) return;
        allocator.free(self.subject);
        allocator.free(self.data);
        if (self.reply_to) |rt| allocator.free(rt);
        if (self.headers) |h| allocator.free(h);
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

        /// Zero-copy receive with optional timeout.
        /// Returns slices directly into read buffer - valid until next call.
        /// For owned copies, use nextMessageOwned().
        pub fn nextMessage(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            assert(self.state == .active or self.state == .draining);

            // Check queue first for any pending owned messages
            if (self.messages.tryPop()) |m| {
                self.received_msgs += 1;
                return m;
            }

            // Check if draining and queue empty
            if (self.state == .draining) return null;

            // Setup timeout tracking
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
                // Calculate remaining timeout
                const remaining_ms: ?u32 = if (has_timeout) blk: {
                    const now = std.time.Instant.now() catch {
                        return error.TimerUnavailable;
                    };
                    const elapsed = now.since(start);
                    if (elapsed >= timeout_ns) return null;
                    break :blk @intCast((timeout_ns - elapsed) / std.time.ns_per_ms);
                } else null;

                // Poll for a single message (zero-copy)
                const direct = self.client.pollDirect(
                    allocator,
                    remaining_ms,
                ) catch |err| {
                    return err;
                };

                if (direct) |d| {
                    // Check if message is for this subscription
                    if (d.sid == self.sid) {
                        self.received_msgs += 1;
                        return Message{
                            .subject = d.subject,
                            .sid = d.sid,
                            .reply_to = d.reply_to,
                            .data = d.data,
                            .headers = d.headers,
                            .owned = false,
                        };
                    } else {
                        // Message for different subscription - queue it (copied)
                        if (self.client.subscriptions.get(d.sid)) |other_sub| {
                            const alloc = allocator;
                            const subject = alloc.dupe(u8, d.subject) catch {
                                self.client.tossPending();
                                continue;
                            };
                            const data = alloc.dupe(u8, d.data) catch {
                                alloc.free(subject);
                                self.client.tossPending();
                                continue;
                            };
                            const reply_to = if (d.reply_to) |rt|
                                alloc.dupe(u8, rt) catch {
                                    alloc.free(subject);
                                    alloc.free(data);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;
                            const headers = if (d.headers) |h|
                                alloc.dupe(u8, h) catch {
                                    alloc.free(subject);
                                    alloc.free(data);
                                    if (reply_to) |rt| alloc.free(rt);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;

                            other_sub.messages.push(.{
                                .subject = subject,
                                .sid = d.sid,
                                .reply_to = reply_to,
                                .data = data,
                                .headers = headers,
                                .owned = true,
                            }) catch {
                                alloc.free(subject);
                                alloc.free(data);
                                if (reply_to) |rt| alloc.free(rt);
                                if (headers) |h| alloc.free(h);
                            };
                        }
                        // Toss and continue looking for our message
                        self.client.tossPending();
                    }
                } else {
                    // Timeout or no data
                    return null;
                }
            }
        }

        /// Owned receive - copies message data, caller owns memory.
        /// Use when message data needs to outlive the next nextMessage() call.
        pub fn nextMessageOwned(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            const msg = try self.nextMessage(allocator, opts);
            if (msg) |m| {
                if (m.owned) return m;

                // Copy zero-copy message to owned
                const subject = try allocator.dupe(u8, m.subject);
                errdefer allocator.free(subject);

                const data = try allocator.dupe(u8, m.data);
                errdefer allocator.free(data);

                const reply_to = if (m.reply_to) |rt|
                    try allocator.dupe(u8, rt)
                else
                    null;
                errdefer if (reply_to) |rt| allocator.free(rt);

                const headers = if (m.headers) |h|
                    try allocator.dupe(u8, h)
                else
                    null;

                return Message{
                    .subject = subject,
                    .sid = m.sid,
                    .reply_to = reply_to,
                    .data = data,
                    .headers = headers,
                    .owned = true,
                };
            }
            return null;
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
