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
const memory = @import("../memory.zig");

/// Message received on a subscription.
/// For zero-copy (owned=false): slices point into read buffer, valid until
/// next nextMessage() call. For owned (owned=true): data is allocated and
/// must be freed via deinit(). Slab-based owned uses SlabPool for fast
/// allocation.
pub const Message = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8,
    data: []const u8,
    headers: ?[]const u8,
    owned: bool = false,
    slab: ?*memory.Slab = null,
    pool: ?*memory.SlabPool = null,

    /// Frees owned message data. No-op for zero-copy messages.
    /// For slab-based: releases slab back to pool.
    /// For allocator-based: frees with provided allocator.
    pub fn deinit(self: *const Message, allocator: Allocator) void {
        if (!self.owned) return;

        if (self.slab) |s| {
            // Slab-based: release slab (returns to pool when refcount=0)
            assert(self.pool != null);
            if (s.release()) {
                self.pool.?.returnSlab(s);
            }
        } else {
            // Allocator-based
            allocator.free(self.subject);
            allocator.free(self.data);
            if (self.reply_to) |rt| allocator.free(rt);
            if (self.headers) |h| allocator.free(h);
        }
    }
};

/// Options for receiving messages.
pub const ReceiveOptions = struct {
    timeout_ms: ?u64 = null,
    slab_pool: ?*memory.SlabPool = null,
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

/// Tiger Style: Fixed-size message queue with zero allocations.
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

/// Tiger Style subscription slot configuration.
pub const FixedSubConfig = struct {
    max_subject_len: u16 = 256,
    max_queue_group_len: u16 = 64,
    queue_capacity: u16 = 256,
};

/// Tiger Style: Zero-allocation subscription slot.
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
            assert(self.state == .active or self.state == .draining);

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

        /// Owned receive with optional slab pool.
        pub fn nextMessageOwned(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            const msg = try self.nextMessage(allocator, opts);
            if (msg) |m| {
                if (m.owned) return m;

                if (opts.slab_pool) |pool| {
                    return copyToSlabFixed(pool, m);
                }

                // Allocator fallback
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
                    .slab = null,
                    .pool = null,
                };
            }
            return null;
        }

        /// Returns pending message count.
        pub fn pending(self: *const Self) u16 {
            return self.messages.count;
        }

        /// Unsubscribe from subject.
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

        /// No-op deinit (fixed slots don't need cleanup).
        /// Just deactivates the slot.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            _ = allocator;
            self.client.releaseSubscriptionSlot(self);
        }
    };
}

/// Copy message to slab (for FixedSubscription).
fn copyToSlabFixed(pool: *memory.SlabPool, m: Message) ?Message {
    assert(m.subject.len > 0);

    var total: usize = m.subject.len + m.data.len;
    if (m.reply_to) |rt| total += rt.len;
    if (m.headers) |h| total += h.len;

    if (total > pool.slab_size) return null;

    const slab = pool.acquireSlab() orelse return null;
    const buf = slab.slice();
    var offset: usize = 0;

    const subj_end = offset + m.subject.len;
    @memcpy(buf[offset..subj_end], m.subject);
    const subject_slice = buf[offset..subj_end];
    offset = subj_end;

    const reply_slice = if (m.reply_to) |rt| blk: {
        const rt_end = offset + rt.len;
        @memcpy(buf[offset..rt_end], rt);
        const slice = buf[offset..rt_end];
        offset = rt_end;
        break :blk slice;
    } else null;

    const headers_slice = if (m.headers) |h| blk: {
        const h_end = offset + h.len;
        @memcpy(buf[offset..h_end], h);
        const slice = buf[offset..h_end];
        offset = h_end;
        break :blk slice;
    } else null;

    const data_end = offset + m.data.len;
    @memcpy(buf[offset..data_end], m.data);
    const data_slice = buf[offset..data_end];

    return Message{
        .subject = subject_slice,
        .sid = m.sid,
        .reply_to = reply_slice,
        .data = data_slice,
        .headers = headers_slice,
        .owned = true,
        .slab = slab,
        .pool = pool,
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
                        // Tiger Style: O(1) lookup via SidMap
                        const other = self.client.getSubscriptionBySid(d.sid);
                        if (other) |other_sub| {
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
        /// If opts.slab_pool is set, uses fast slab-based allocation.
        pub fn nextMessageOwned(
            self: *Self,
            allocator: Allocator,
            opts: ReceiveOptions,
        ) !?Message {
            const msg = try self.nextMessage(allocator, opts);
            if (msg) |m| {
                if (m.owned) return m;

                // If slab_pool provided, use fast slab path
                if (opts.slab_pool) |pool| {
                    return copyToSlab(pool, m);
                }

                // Fallback: allocator-based copy
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
                    .slab = null,
                    .pool = null,
                };
            }
            return null;
        }

        /// Copy message into slab for fast owned allocation.
        /// Falls back to null if message too large or pool exhausted.
        fn copyToSlab(pool: *memory.SlabPool, m: Message) ?Message {
            assert(m.subject.len > 0);

            // Calculate total size needed
            var total: usize = m.subject.len + m.data.len;
            if (m.reply_to) |rt| total += rt.len;
            if (m.headers) |h| total += h.len;

            // Check if fits in slab
            if (total > pool.slab_size) return null;

            // Acquire slab
            const slab = pool.acquireSlab() orelse return null;
            const buf = slab.slice();
            var offset: usize = 0;

            // Copy subject
            const subj_end = offset + m.subject.len;
            @memcpy(buf[offset..subj_end], m.subject);
            const subject_slice = buf[offset..subj_end];
            offset = subj_end;

            // Copy reply_to
            const reply_to_slice = if (m.reply_to) |rt| blk: {
                const rt_end = offset + rt.len;
                @memcpy(buf[offset..rt_end], rt);
                const slice = buf[offset..rt_end];
                offset = rt_end;
                break :blk slice;
            } else null;

            // Copy headers
            const headers_slice = if (m.headers) |h| blk: {
                const h_end = offset + h.len;
                @memcpy(buf[offset..h_end], h);
                const slice = buf[offset..h_end];
                offset = h_end;
                break :blk slice;
            } else null;

            // Copy data
            const data_end = offset + m.data.len;
            @memcpy(buf[offset..data_end], m.data);
            const data_slice = buf[offset..data_end];

            return Message{
                .subject = subject_slice,
                .sid = m.sid,
                .reply_to = reply_to_slice,
                .data = data_slice,
                .headers = headers_slice,
                .owned = true,
                .slab = slab,
                .pool = pool,
            };
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
        /// Removes self from client's maps to prevent double-free.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            // Remove from Tiger Style SidMap and sub_ptrs
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
