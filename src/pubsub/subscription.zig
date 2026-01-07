//! Subscription Management
//!
//! Tracks active subscriptions and provides message delivery.
//! Uses comptime generics for zero-overhead Go-style API.
//! Messages are delivered via client.poll(), which uses posix.poll()
//! for efficient timeout handling without background threads.
//!
//! Connection-scoped: Client stores Io, Reader, Writer for connection lifetime.

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
    /// Single backing buffer (when set, all slices point into this).
    /// Enables single allocation per message for async client.
    backing_buf: ?[]u8 = null,

    /// Frees owned message data. No-op for zero-copy messages.
    pub fn deinit(self: *const Message, allocator: Allocator) void {
        if (!self.owned) return;
        // Fast path: single backing buffer (async client optimization)
        if (self.backing_buf) |buf| {
            allocator.free(buf);
            return;
        }
        // Legacy path: separate allocations (sync client routed messages)
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

/// Fixed-size message queue with zero allocations.
/// Pre-allocated ring buffer for high-throughput message handling.
pub fn FixedMessageQueue(comptime capacity: u16) type {
    return struct {
        messages: [capacity]Message = undefined,
        head: u16 = 0,
        tail: u16 = 0,
        count: u16 = 0,

        const Self = @This();
        pub const CAPACITY = capacity;

        /// Pushes a message to the queue. Returns error if full.
        pub fn push(self: *Self, msg: Message) error{QueueFull}!void {
            if (self.count >= capacity) return error.QueueFull;

            self.messages[self.tail] = msg;
            self.tail = (self.tail + 1) % capacity;
            self.count += 1;
        }

        /// Pops a message from the queue. Returns null if empty.
        pub fn pop(self: *Self) ?Message {
            if (self.count == 0) return null;

            const msg = self.messages[self.head];
            self.head = (self.head + 1) % capacity;
            self.count -= 1;
            return msg;
        }

        /// Try pop without blocking (same as pop for fixed queue).
        pub fn tryPop(self: *Self) ?Message {
            return self.pop();
        }

        pub fn len(self: *const Self) u16 {
            return self.count;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.count == 0;
        }

        pub fn isFull(self: *const Self) bool {
            return self.count >= capacity;
        }

        /// Clear all messages without deallocation.
        pub fn clear(self: *Self) void {
            self.head = 0;
            self.tail = 0;
            self.count = 0;
        }

        /// No-op for fixed queue (no allocations to free).
        pub fn close(self: *Self) void {
            _ = self;
        }

        /// No-op for fixed queue (no allocations to free).
        pub fn deinit(self: *Self, allocator: Allocator) void {
            _ = self;
            _ = allocator;
        }
    };
}

/// FixedSubscription slot configuration.
pub const FixedSubConfig = struct {
    max_subject_len: u16 = 256,
    max_queue_group_len: u16 = 64,
    queue_capacity: u16 = 256,
};

/// Zero-allocation subscription slot.
/// Uses fixed buffers for subject, queue_group, and message queue.
/// Designed for embedding in fixed arrays (no heap allocation).
pub fn FixedSubscription(
    comptime ClientType: type,
    comptime config: FixedSubConfig,
) type {
    return struct {
        client: *ClientType,
        sid: u64,
        subject_buf: [config.max_subject_len]u8,
        subject_len: u8,
        queue_group_buf: [config.max_queue_group_len]u8,
        queue_group_len: u8,
        messages: FixedMessageQueue(config.queue_capacity),
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

        /// Activate slot with subscription data.
        pub fn activate(
            self: *Self,
            client_ptr: *ClientType,
            sid_val: u64,
            subj: []const u8,
            queue_grp: ?[]const u8,
        ) error{SubjectTooLong}!void {
            assert(subj.len > 0);
            assert(subj.len <= config.max_subject_len);

            if (subj.len > config.max_subject_len) {
                return error.SubjectTooLong;
            }

            self.client = client_ptr;
            self.sid = sid_val;
            self.subject_len = @intCast(subj.len);
            @memcpy(self.subject_buf[0..subj.len], subj);

            if (queue_grp) |qg| {
                assert(qg.len <= config.max_queue_group_len);
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

        /// Zero-copy receive with optional timeout.
        pub fn nextMessage(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            assert(self.active);
            // Return error if subscription is closed
            if (self.state != .active and self.state != .draining) {
                return error.SubscriptionClosed;
            }

            // Check queue first
            if (self.messages.tryPop()) |m| {
                self.received_msgs += 1;
                return m;
            }

            if (self.state == .draining) return null;

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
                const remaining_ms: ?u32 = if (has_timeout) blk: {
                    const now = std.time.Instant.now() catch {
                        return error.TimerUnavailable;
                    };
                    const elapsed = now.since(start);
                    if (elapsed >= timeout_ns) return null;
                    break :blk @intCast((timeout_ns - elapsed) /
                        std.time.ns_per_ms);
                } else null;

                const direct = self.client.pollDirect(
                    allocator,
                    remaining_ms,
                ) catch |err| {
                    return err;
                };

                if (direct) |d| {
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
                        // Route to other subscription
                        self.client.routeToSubscription(allocator, d);
                        self.client.tossPending();
                    }
                } else {
                    return null;
                }
            }
        }

        /// Owned receive - copies message data so it outlives read buffer.
        pub fn nextMessageOwned(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            const msg = try self.nextMessage(allocator, opts);
            if (msg) |m| {
                if (m.owned) return m;

                const subj = try allocator.dupe(u8, m.subject);
                errdefer allocator.free(subj);

                const data = try allocator.dupe(u8, m.data);
                errdefer allocator.free(data);

                const reply = if (m.reply_to) |rt|
                    try allocator.dupe(u8, rt)
                else
                    null;
                errdefer if (reply) |rt| allocator.free(rt);

                const hdrs = if (m.headers) |h|
                    try allocator.dupe(u8, h)
                else
                    null;

                return Message{
                    .subject = subj,
                    .sid = m.sid,
                    .reply_to = reply,
                    .data = data,
                    .headers = hdrs,
                    .owned = true,
                };
            }
            return null;
        }

        /// Returns pending message count.
        pub fn pending(self: *const Self) u16 {
            return self.messages.count;
        }

        /// Unsubscribe from subject (protocol only, no memory cleanup).
        pub fn unsubscribe(self: *Self) !void {
            if (self.state == .unsubscribed) return;
            self.state = .unsubscribed;
            try self.client.unsubscribeSid(self.sid);
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

        /// Clean up subscription: unsubscribe and release slot.
        /// This is the single cleanup function - use with defer.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            _ = allocator;
            // Unsubscribe from server (ignore errors during cleanup)
            self.unsubscribe() catch {};
            self.client.releaseSubscriptionSlot(self);
        }
    };
}

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
            assert(self.sid > 0);
            // Return error if subscription is closed
            if (self.state != .active and self.state != .draining) {
                return error.SubscriptionClosed;
            }

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
                    const ns_per_ms = std.time.ns_per_ms;
                    const remaining = (timeout_ns - elapsed) / ns_per_ms;
                    break :blk @intCast(remaining);
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
                        // Message for different subscription - queue it
                        // O(1) lookup via SidMap
                        const other = self.client.getSubscriptionBySid(d.sid);
                        if (other) |other_sub| {
                            const alloc = allocator;
                            const subj = alloc.dupe(u8, d.subject) catch {
                                self.client.tossPending();
                                continue;
                            };
                            const data = alloc.dupe(u8, d.data) catch {
                                alloc.free(subj);
                                self.client.tossPending();
                                continue;
                            };
                            const reply_to = if (d.reply_to) |rt|
                                alloc.dupe(u8, rt) catch {
                                    alloc.free(subj);
                                    alloc.free(data);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;
                            const headers = if (d.headers) |h|
                                alloc.dupe(u8, h) catch {
                                    alloc.free(subj);
                                    alloc.free(data);
                                    if (reply_to) |rt| alloc.free(rt);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;

                            other_sub.messages.push(.{
                                .subject = subj,
                                .sid = d.sid,
                                .reply_to = reply_to,
                                .data = data,
                                .headers = headers,
                                .owned = true,
                            }) catch {
                                alloc.free(subj);
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

        /// Owned receive - copies message data so it outlives read buffer.
        pub fn nextMessageOwned(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            const msg = try self.nextMessage(allocator, opts);
            if (msg) |m| {
                if (m.owned) return m;

                const subj = try allocator.dupe(u8, m.subject);
                errdefer allocator.free(subj);

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
                    .subject = subj,
                    .sid = m.sid,
                    .reply_to = reply_to,
                    .data = data,
                    .headers = headers,
                    .owned = true,
                };
            }
            return null;
        }

        /// Async message receive - returns Future for true async/await.
        /// Usage:
        ///   var future = sub.nextMessageAsync(allocator);
        ///   defer if (future.cancel(io)) |m| {
        ///       if (m) |msg| msg.deinit(allocator);
        ///   } else |_| {};
        ///   if (try future.await(io)) |msg| { ... }
        pub fn nextMessageAsync(
            self: *Self,
            allocator: Allocator,
        ) std.Io.Future(anyerror!?Message) {
            return self.client.io.async(asyncNextMessageImpl, .{
                self.client.io,
                self,
                allocator,
            });
        }

        /// Internal async implementation (standalone fn for io.async).
        fn asyncNextMessageImpl(
            io: std.Io,
            self: *Self,
            allocator: Allocator,
        ) anyerror!?Message {
            _ = io; // Captured for cancellation points in pollDirect

            // Poll until message received or cancelled
            while (self.state == .active or self.state == .draining) {
                // Check queue first
                if (self.messages.tryPop()) |m| {
                    self.received_msgs += 1;
                    return m;
                }

                if (self.state == .draining) return null;

                // Poll with 100ms timeout - acts as cancellation point
                const direct = self.client.pollDirect(allocator, 100) catch |err| {
                    return err;
                };

                if (direct) |d| {
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
                        // Route to other subscription
                        const other = self.client.getSubscriptionBySid(d.sid);
                        if (other) |other_sub| {
                            const subj = allocator.dupe(u8, d.subject) catch {
                                self.client.tossPending();
                                continue;
                            };
                            const data = allocator.dupe(u8, d.data) catch {
                                allocator.free(subj);
                                self.client.tossPending();
                                continue;
                            };
                            const reply_to = if (d.reply_to) |rt|
                                allocator.dupe(u8, rt) catch {
                                    allocator.free(subj);
                                    allocator.free(data);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;
                            const headers = if (d.headers) |h|
                                allocator.dupe(u8, h) catch {
                                    allocator.free(subj);
                                    allocator.free(data);
                                    if (reply_to) |rt| allocator.free(rt);
                                    self.client.tossPending();
                                    continue;
                                }
                            else
                                null;

                            other_sub.messages.push(.{
                                .subject = subj,
                                .sid = d.sid,
                                .reply_to = reply_to,
                                .data = data,
                                .headers = headers,
                                .owned = true,
                            }) catch {
                                allocator.free(subj);
                                allocator.free(data);
                                if (reply_to) |rt| allocator.free(rt);
                                if (headers) |h| allocator.free(h);
                            };
                        }
                        self.client.tossPending();
                    }
                }
                // No message yet, loop - cancellation can happen at pollDirect
            }
            return error.SubscriptionClosed;
        }

        /// Returns number of pending messages.
        pub fn pending(self: *Self) usize {
            return self.messages.len();
        }

        /// Unsubscribe from the subject (protocol only, no memory cleanup).
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

        /// Clean up subscription: unsubscribe and free all resources.
        /// This is the single cleanup function - use with defer.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            // Unsubscribe from server (ignore errors during cleanup)
            self.unsubscribe() catch {};

            // Remove from SidMap and sub_ptrs
            if (self.client.sidmap.get(self.sid)) |slot_idx| {
                self.client.sub_ptrs[slot_idx] = null;
                _ = self.client.sidmap.remove(self.sid);
                // Return slot to free stack
                self.client.free_slots[self.client.free_count] = slot_idx;
                self.client.free_count += 1;
            }

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
