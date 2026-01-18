//! Subscription Message Queue
//!
//! Fixed-size ring buffer for subscription messages with O(1) operations.
//! Drops messages when full (like official Go/C/Rust NATS clients).
//! Tracks delivery statistics for monitoring slow consumers.

const std = @import("std");
const assert = std.debug.assert;

/// Statistics for queue monitoring.
pub const Stats = struct {
    delivered: u64 = 0,
    dropped: u64 = 0,
    peak_pending: u32 = 0,
};

/// Fixed-size ring buffer queue that drops on full.
///
/// Designed for high-throughput message delivery where dropping is
/// preferable to blocking or allocation. All official NATS clients
/// use this pattern for subscription queues.
pub fn SubQueue(comptime T: type, comptime capacity: u32) type {
    return struct {
        buffer: [capacity]T = undefined,
        head: u32 = 0,
        tail: u32 = 0,
        count: u32 = 0,
        stats: Stats = .{},

        const Self = @This();

        /// Push message to queue. Returns true if queued, false if dropped.
        /// O(1) operation, never blocks or allocates.
        pub fn push(self: *Self, item: T) bool {
            assert(self.count <= capacity);
            assert(self.head < capacity);
            assert(self.tail < capacity);

            if (self.count >= capacity) {
                self.stats.dropped += 1;
                return false;
            }

            self.buffer[self.tail] = item;
            self.tail = (self.tail + 1) % capacity;
            self.count += 1;

            if (self.count > self.stats.peak_pending) {
                self.stats.peak_pending = self.count;
            }

            assert(self.count <= capacity);
            return true;
        }

        /// Pop message from queue. Returns null if empty.
        /// O(1) operation, never blocks.
        pub fn pop(self: *Self) ?T {
            assert(self.count <= capacity);
            assert(self.head < capacity);

            if (self.count == 0) return null;

            const item = self.buffer[self.head];
            self.head = (self.head + 1) % capacity;
            self.count -= 1;
            self.stats.delivered += 1;

            assert(self.count <= capacity);
            return item;
        }

        /// Peek at front item without removing.
        pub fn peek(self: *const Self) ?*const T {
            if (self.count == 0) return null;
            return &self.buffer[self.head];
        }

        /// Returns number of items in queue.
        pub fn len(self: *const Self) u32 {
            return self.count;
        }

        /// Returns true if queue is empty.
        pub fn isEmpty(self: *const Self) bool {
            return self.count == 0;
        }

        /// Returns true if queue is full (next push will drop).
        pub fn isFull(self: *const Self) bool {
            return self.count >= capacity;
        }

        /// Clear all items from queue (does not call deinit on items).
        pub fn clear(self: *Self) void {
            self.head = 0;
            self.tail = 0;
            self.count = 0;
        }

        /// Get queue statistics.
        pub fn getStats(self: *const Self) Stats {
            return self.stats;
        }

        /// Reset statistics counters.
        pub fn resetStats(self: *Self) void {
            self.stats = .{};
        }

        /// Returns queue capacity.
        pub fn getCapacity(_: *const Self) u32 {
            return capacity;
        }
    };
}

test "SubQueue push/pop" {
    const Queue = SubQueue(u32, 4);
    var q: Queue = .{};

    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), q.len());

    try std.testing.expect(q.push(1));
    try std.testing.expect(q.push(2));
    try std.testing.expect(q.push(3));
    try std.testing.expect(q.push(4));

    try std.testing.expect(q.isFull());
    try std.testing.expectEqual(@as(u32, 4), q.len());

    try std.testing.expectEqual(@as(?u32, 1), q.pop());
    try std.testing.expectEqual(@as(?u32, 2), q.pop());
    try std.testing.expectEqual(@as(?u32, 3), q.pop());
    try std.testing.expectEqual(@as(?u32, 4), q.pop());

    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(?u32, null), q.pop());
}

test "SubQueue drops on full" {
    const Queue = SubQueue(u32, 2);
    var q: Queue = .{};

    try std.testing.expect(q.push(1));
    try std.testing.expect(q.push(2));
    try std.testing.expect(!q.push(3)); // dropped
    try std.testing.expect(!q.push(4)); // dropped

    try std.testing.expectEqual(@as(u64, 2), q.stats.dropped);
    try std.testing.expectEqual(@as(u32, 2), q.stats.peak_pending);

    try std.testing.expectEqual(@as(?u32, 1), q.pop());
    try std.testing.expectEqual(@as(?u32, 2), q.pop());

    try std.testing.expectEqual(@as(u64, 2), q.stats.delivered);
}

test "SubQueue wraparound" {
    const Queue = SubQueue(u32, 3);
    var q: Queue = .{};

    // Fill and partial drain
    try std.testing.expect(q.push(1));
    try std.testing.expect(q.push(2));
    try std.testing.expectEqual(@as(?u32, 1), q.pop());
    try std.testing.expectEqual(@as(?u32, 2), q.pop());

    // Now head/tail are at position 2, test wraparound
    try std.testing.expect(q.push(3));
    try std.testing.expect(q.push(4));
    try std.testing.expect(q.push(5));
    try std.testing.expect(!q.push(6)); // full, dropped

    try std.testing.expectEqual(@as(?u32, 3), q.pop());
    try std.testing.expectEqual(@as(?u32, 4), q.pop());
    try std.testing.expectEqual(@as(?u32, 5), q.pop());
    try std.testing.expectEqual(@as(?u32, null), q.pop());
}

test "SubQueue peek" {
    const Queue = SubQueue(u32, 4);
    var q: Queue = .{};

    try std.testing.expectEqual(@as(?*const u32, null), q.peek());

    try std.testing.expect(q.push(42));
    try std.testing.expectEqual(@as(u32, 42), q.peek().?.*);
    try std.testing.expectEqual(@as(u32, 1), q.len()); // peek doesn't remove

    try std.testing.expectEqual(@as(?u32, 42), q.pop());
    try std.testing.expectEqual(@as(?*const u32, null), q.peek());
}

test "SubQueue stats tracking" {
    const Queue = SubQueue(u32, 2);
    var q: Queue = .{};

    try std.testing.expect(q.push(1));
    try std.testing.expectEqual(@as(u32, 1), q.stats.peak_pending);

    try std.testing.expect(q.push(2));
    try std.testing.expectEqual(@as(u32, 2), q.stats.peak_pending);

    _ = q.pop();
    try std.testing.expectEqual(@as(u32, 2), q.stats.peak_pending);

    try std.testing.expect(q.push(3));
    try std.testing.expectEqual(@as(u32, 2), q.stats.peak_pending);

    const stats = q.getStats();
    try std.testing.expectEqual(@as(u64, 1), stats.delivered);
    try std.testing.expectEqual(@as(u64, 0), stats.dropped);
    try std.testing.expectEqual(@as(u32, 2), stats.peak_pending);
}

test "SubQueue clear" {
    const Queue = SubQueue(u32, 4);
    var q: Queue = .{};

    try std.testing.expect(q.push(1));
    try std.testing.expect(q.push(2));
    try std.testing.expectEqual(@as(u32, 2), q.len());

    q.clear();
    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), q.len());
    try std.testing.expectEqual(@as(?u32, null), q.pop());

    // Stats preserved after clear
    try std.testing.expectEqual(@as(u32, 2), q.stats.peak_pending);
}
