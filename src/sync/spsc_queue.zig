//! Lock-free Single Producer Single Consumer Queue
//!
//! Zero syscalls, zero mutex, maximum throughput.
//! Designed for cross-thread message passing between io_task and subscriber.
//!
//! ## Memory Ordering Rationale
//!
//! This SPSC queue uses a carefully chosen memory ordering strategy that is
//! both correct on weakly-ordered architectures (ARM) and optimal for
//! strongly-ordered architectures (x86_64).
//!
//! **Key insight**: Each index (head/tail) has exactly ONE writer thread.
//! - `head`: written only by producer, read by both
//! - `tail`: written only by consumer, read by both
//!
//! **Ordering rules applied**:
//! - Reading your OWN index: `.monotonic` (you're the only writer, no sync needed)
//! - Reading OTHER's index: `.acquire` (must see their prior writes to buffer)
//! - Writing your index: `.release` (your buffer writes must be visible first)
//!
//! **The release-acquire pairing**:
//! ```
//! Producer:                          Consumer:
//!   buffer[head] = item;               head = head.load(.acquire);  // sees data
//!   head.store(new, .release);  ---->  item = buffer[tail];
//!                                      tail.store(new, .release);
//!                               <----  tail.load(.acquire);  // sees consumption
//! ```
//!
//! This ensures: when consumer sees updated head, the data write is visible.
//! When producer sees updated tail, the slot is safe to reuse.

const std = @import("std");
const assert = std.debug.assert;

/// Lock-free SPSC queue with runtime-sized buffer.
/// Producer (io_task) and consumer (subscriber) can run on different threads.
/// See module doc comment for memory ordering rationale.
pub fn SpscQueue(comptime T: type) type {
    return struct {
        buffer: []T,
        capacity: usize,
        head: std.atomic.Value(usize),
        tail: std.atomic.Value(usize),

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
            // .monotonic: single head writer, no sync needed for own read
            const head = self.head.load(.monotonic);
            // .acquire: must see consumer's tail updates to know slots are free
            const tail = self.tail.load(.acquire);

            if (head -% tail >= self.capacity) return false;

            const mask = self.capacity - 1;
            self.buffer[head & mask] = item;
            // .release: ensures item write is visible BEFORE head increment
            // Consumer's .acquire on head will see this data
            self.head.store(head +% 1, .release);
            return true;
        }

        /// Pop item (consumer only). Returns null if empty.
        /// O(1), lock-free, never blocks.
        pub fn pop(self: *Self) ?T {
            // .monotonic: single tail writer, no sync needed for own read
            const tail = self.tail.load(.monotonic);
            // .acquire: must see producer's buffer writes that happened before
            // their .release store to head
            const head = self.head.load(.acquire);

            if (tail == head) return null;

            const mask = self.capacity - 1;
            const item = self.buffer[tail & mask];
            // .release: ensures item read completes BEFORE tail increment
            // Producer's .acquire on tail will see slot is now free
            self.tail.store(tail +% 1, .release);
            return item;
        }

        /// Pop multiple items into output buffer. Returns count popped.
        /// O(n), lock-free, never blocks.
        /// Same memory ordering rationale as pop() - see module doc.
        pub fn popBatch(self: *Self, out: []T) usize {
            // .monotonic: single tail writer
            const tail = self.tail.load(.monotonic);
            // .acquire: must see producer's buffer writes
            const head = self.head.load(.acquire);

            const available = head -% tail;
            if (available == 0) return 0;

            const mask = self.capacity - 1;
            const count = @min(available, out.len);
            for (0..count) |i| {
                out[i] = self.buffer[(tail +% i) & mask];
            }
            // .release: ensures all reads complete before tail update
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
    try std.testing.expect(!q.push(5));

    try std.testing.expectEqual(@as(usize, 4), q.len());

    try std.testing.expectEqual(@as(?u32, 1), q.pop());
    try std.testing.expectEqual(@as(?u32, 2), q.pop());
    try std.testing.expectEqual(@as(?u32, 3), q.pop());
    try std.testing.expectEqual(@as(?u32, 4), q.pop());
    try std.testing.expectEqual(@as(?u32, null), q.pop());

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

    for (0..10) |cycle| {
        const base: u32 = @intCast(cycle * 4);
        try std.testing.expect(q.push(base + 1));
        try std.testing.expect(q.push(base + 2));
        try std.testing.expect(q.push(base + 3));
        try std.testing.expect(q.push(base + 4));
        try std.testing.expect(!q.push(base + 5));

        try std.testing.expectEqual(@as(?u32, base + 1), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 2), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 3), q.pop());
        try std.testing.expectEqual(@as(?u32, base + 4), q.pop());
        try std.testing.expectEqual(@as(?u32, null), q.pop());
    }
}
