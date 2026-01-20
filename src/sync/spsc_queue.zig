//! Lock-free Single Producer Single Consumer Queue
//!
//! Zero syscalls, zero mutex, maximum throughput.
//! Designed for cross-thread message passing between io_task and subscriber.

const std = @import("std");
const assert = std.debug.assert;

/// Lock-free SPSC queue with runtime-sized buffer.
/// Producer (io_task) and consumer (subscriber) can run on different threads.
pub fn SpscQueue(comptime T: type) type {
    return struct {
        buffer: []T,
        capacity: usize,
        head: std.atomic.Value(usize), // Producer writes here
        tail: std.atomic.Value(usize), // Consumer reads here

        const Self = @This();

        /// Initialize queue with pre-allocated buffer.
        /// Buffer length MUST be a power of 2 for bitwise AND optimization.
        pub fn init(buffer: []T) Self {
            assert(buffer.len > 0);
            assert(std.math.isPowerOfTwo(buffer.len));
            return .{
                .buffer = buffer,
                .capacity = buffer.len,
                .head = std.atomic.Value(usize).init(0),
                .tail = std.atomic.Value(usize).init(0),
            };
        }

        /// Push item (producer only). Returns false if full.
        /// O(1), lock-free, never blocks.
        pub fn push(self: *Self, item: T) bool {
            const head = self.head.load(.monotonic);
            const tail = self.tail.load(.acquire);

            // Full check: head has wrapped around to tail
            if (head -% tail >= self.capacity) return false;

            // Bitwise AND is faster than modulo (power-of-2 capacity)
            const mask = self.capacity - 1;
            self.buffer[head & mask] = item;
            // Release ensures item write is visible before head increment
            self.head.store(head +% 1, .release);
            return true;
        }

        /// Pop item (consumer only). Returns null if empty.
        /// O(1), lock-free, never blocks.
        pub fn pop(self: *Self) ?T {
            const tail = self.tail.load(.monotonic);
            const head = self.head.load(.acquire);

            // Empty check
            if (tail == head) return null;

            // Bitwise AND is faster than modulo (power-of-2 capacity)
            const mask = self.capacity - 1;
            const item = self.buffer[tail & mask];
            // Release ensures item read completes before tail increment
            self.tail.store(tail +% 1, .release);
            return item;
        }

        /// Pop multiple items into output buffer. Returns count popped.
        /// O(n), lock-free, never blocks.
        pub fn popBatch(self: *Self, out: []T) usize {
            const tail = self.tail.load(.monotonic);
            const head = self.head.load(.acquire);

            const available = head -% tail;
            if (available == 0) return 0;

            // Bitwise AND is faster than modulo (power-of-2 capacity)
            const mask = self.capacity - 1;
            const count = @min(available, out.len);
            for (0..count) |i| {
                out[i] = self.buffer[(tail +% i) & mask];
            }
            self.tail.store(tail +% count, .release);
            return count;
        }

        /// Number of items in queue (approximate, may be stale).
        pub fn len(self: *const Self) usize {
            const head = self.head.load(.acquire);
            const tail = self.tail.load(.acquire);
            return head -% tail;
        }

        /// True if queue appears empty (may be stale).
        pub fn isEmpty(self: *const Self) bool {
            return self.len() == 0;
        }

        /// Close queue (no-op for compatibility with Io.Queue API).
        pub fn close(self: *Self, io: anytype) void {
            _ = self;
            _ = io;
            // No-op - SPSC doesn't need close signaling
        }
    };
}

test "SpscQueue push/pop" {
    var buffer: [4]u32 = undefined;
    var q = SpscQueue(u32).init(&buffer);

    try std.testing.expect(q.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), q.len());

    try std.testing.expect(q.push(1));
    try std.testing.expect(q.push(2));
    try std.testing.expect(q.push(3));
    try std.testing.expect(q.push(4));
    try std.testing.expect(!q.push(5)); // Full

    try std.testing.expectEqual(@as(usize, 4), q.len());

    try std.testing.expectEqual(@as(?u32, 1), q.pop());
    try std.testing.expectEqual(@as(?u32, 2), q.pop());
    try std.testing.expectEqual(@as(?u32, 3), q.pop());
    try std.testing.expectEqual(@as(?u32, 4), q.pop());
    try std.testing.expectEqual(@as(?u32, null), q.pop()); // Empty

    try std.testing.expect(q.isEmpty());
}

test "SpscQueue popBatch" {
    var buffer: [8]u32 = undefined;
    var q = SpscQueue(u32).init(&buffer);

    _ = q.push(1);
    _ = q.push(2);
    _ = q.push(3);
    _ = q.push(4);
    _ = q.push(5);

    var out: [3]u32 = undefined;
    const count = q.popBatch(&out);
    try std.testing.expectEqual(@as(usize, 3), count);
    try std.testing.expectEqual(@as(u32, 1), out[0]);
    try std.testing.expectEqual(@as(u32, 2), out[1]);
    try std.testing.expectEqual(@as(u32, 3), out[2]);

    try std.testing.expectEqual(@as(usize, 2), q.len());
}

test "SpscQueue wraparound" {
    var buffer: [4]u32 = undefined;
    var q = SpscQueue(u32).init(&buffer);

    // Fill and drain several times to test wraparound
    for (0..10) |cycle| {
        const base: u32 = @intCast(cycle * 4);
        try std.testing.expect(q.push(base + 1));
        try std.testing.expect(q.push(base + 2));
        try std.testing.expect(q.push(base + 3));
        try std.testing.expect(q.push(base + 4));
        try std.testing.expect(!q.push(base + 5)); // Full

        try std.testing.expectEqual(@as(?u32, base + 1), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 2), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 3), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 4), q.pop());
        try std.testing.expectEqual(@as(?u32, null), q.pop());
    }
}
