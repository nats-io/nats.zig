//! Lock-free SPSC Byte Ring Buffer
//!
//! Variable-length message passing between producer and consumer threads.
//! Each entry is: [4-byte little-endian length][payload bytes].
//! Length=0 is a padding sentinel meaning "skip to ring start".
//!
//! ## Memory Ordering
//!
//! Same release-acquire pattern as SpscQueue (see spsc_queue.zig):
//! - `head`: written by producer (.release), read by consumer (.acquire)
//! - `tail`: written by consumer (.release), read by producer (.acquire)
//! - Producer's buffer writes are visible to consumer via head release-acquire.
//! - Consumer's consumption is visible to producer via tail release-acquire.

const std = @import("std");
const assert = std.debug.assert;

/// Length header size (4 bytes, little-endian u32).
pub const HDR_SIZE: usize = 4;

/// Lock-free SPSC ring buffer for variable-length byte messages.
/// Producer reserves space, writes data, commits. Consumer peeks, reads, advances.
pub const ByteRing = struct {
    buffer: []u8,
    capacity: usize,
    head: std.atomic.Value(usize),
    tail: std.atomic.Value(usize),

    const Self = @This();

    /// Initialize with pre-allocated buffer.
    /// Capacity must be a power of 2 and >= 64.
    pub fn init(buffer: []u8) Self {
        assert(buffer.len >= 64);
        assert(std.math.isPowerOfTwo(buffer.len));
        return .{
            .buffer = buffer,
            .capacity = buffer.len,
            .head = std.atomic.Value(usize).init(0),
            .tail = std.atomic.Value(usize).init(0),
        };
    }

    /// Returns available space in the ring (approximate).
    pub fn available(self: *const Self) usize {
        const head = self.head.load(.monotonic);
        const tail = self.tail.load(.acquire);
        return self.capacity -% (head -% tail);
    }

    /// Producer: calculate encoded size for a message.
    /// Returns total ring bytes needed (header + payload).
    pub fn entrySize(data_len: usize) usize {
        return HDR_SIZE + data_len;
    }

    /// Producer: reserve contiguous space for `data_len` bytes.
    /// Returns writable slice (header + payload area) or null if full.
    /// If the entry doesn't fit before wrap, inserts a padding
    /// sentinel and wraps to ring start.
    pub fn reserve(
        self: *Self,
        data_len: usize,
    ) ?[]u8 {
        const total = entrySize(data_len);
        assert(total <= self.capacity / 2);

        const head = self.head.load(.monotonic);
        const tail = self.tail.load(.acquire);
        const used = head -% tail;

        if (used + total > self.capacity) return null;

        const offset = head % self.capacity;
        const remaining = self.capacity - offset;

        if (remaining >= total) {
            // Fits before wrap
            return self.buffer[offset .. offset + total];
        }

        // Doesn't fit — insert padding sentinel at wrap point
        if (remaining >= HDR_SIZE) {
            // Write zero-length marker
            const pad = self.buffer[offset..][0..HDR_SIZE];
            std.mem.writeInt(u32, pad, 0, .little);
        }

        // Check if wrapping still fits
        const new_used = used + remaining + total;
        if (new_used > self.capacity) return null;

        // Advance head past the padding
        self.head.store(head +% remaining, .release);

        // Now at ring start
        return self.buffer[0..total];
    }

    /// Producer: commit an entry. Writes the length header and
    /// advances head atomically (.release).
    pub fn commit(self: *Self, entry: []u8, data_len: usize) void {
        assert(entry.len >= HDR_SIZE + data_len);
        assert(data_len > 0);

        // Write length header
        const hdr = entry[0..HDR_SIZE];
        std.mem.writeInt(u32, hdr, @intCast(data_len), .little);

        // Advance head past this entry
        const head = self.head.load(.monotonic);
        self.head.store(
            head +% entrySize(data_len),
            .release,
        );
    }

    /// Consumer: peek at the next entry's data.
    /// Returns the data slice (after the 4-byte header).
    /// Skips padding sentinels (length=0) automatically.
    /// Returns null if ring is empty.
    pub fn peek(self: *Self) ?[]const u8 {
        var tail = self.tail.load(.monotonic);
        const head = self.head.load(.acquire);

        while (tail != head) {
            const offset = tail % self.capacity;

            // Need at least HDR_SIZE bytes to read length
            if (self.capacity - offset < HDR_SIZE) {
                // Not enough space for a header — skip to start
                tail = tail +% (self.capacity - offset);
                self.tail.store(tail, .release);
                continue;
            }

            const hdr = self.buffer[offset..][0..HDR_SIZE];
            const entry_len = std.mem.readInt(
                u32,
                hdr,
                .little,
            );

            if (entry_len == 0) {
                // Padding sentinel — skip to wrap
                const remaining = self.capacity - offset;
                tail = tail +% remaining;
                self.tail.store(tail, .release);
                continue;
            }

            const data_start = offset + HDR_SIZE;
            return self.buffer[data_start .. data_start + entry_len];
        }

        return null;
    }

    /// Consumer: advance past the current entry.
    /// Must be called after peek() returned non-null.
    pub fn advance(self: *Self) void {
        const tail = self.tail.load(.monotonic);
        const offset = tail % self.capacity;
        const hdr = self.buffer[offset..][0..HDR_SIZE];
        const entry_len = std.mem.readInt(
            u32,
            hdr,
            .little,
        );
        assert(entry_len > 0);
        self.tail.store(
            tail +% entrySize(entry_len),
            .release,
        );
    }

    /// Consumer: drain all entries, writing each to the writer.
    /// Returns number of entries drained.
    pub fn drainToWriter(
        self: *Self,
        writer: anytype,
    ) !usize {
        var count: usize = 0;
        while (self.peek()) |data| {
            try writer.writeAll(data);
            self.advance();
            count += 1;
        }
        return count;
    }

    /// Returns true if ring appears empty.
    pub fn isEmpty(self: *const Self) bool {
        const head = self.head.load(.acquire);
        const tail = self.tail.load(.acquire);
        return head == tail;
    }

    /// Returns approximate number of bytes used.
    pub fn len(self: *const Self) usize {
        const head = self.head.load(.acquire);
        const tail = self.tail.load(.acquire);
        return head -% tail;
    }

    /// Reset ring to empty state (not thread-safe).
    pub fn clear(self: *Self) void {
        self.head.store(0, .release);
        self.tail.store(0, .release);
    }
};

// --- Tests ---

test "ByteRing basic write/read" {
    var buf: [256]u8 = undefined;
    var ring = ByteRing.init(&buf);

    // Reserve and write
    const entry = ring.reserve(5).?;
    @memcpy(entry[HDR_SIZE..][0..5], "hello");
    ring.commit(entry, 5);

    // Read back
    const data = ring.peek().?;
    try std.testing.expectEqualStrings("hello", data);
    ring.advance();

    // Now empty
    try std.testing.expect(ring.peek() == null);
    try std.testing.expect(ring.isEmpty());
}

test "ByteRing multiple entries" {
    var buf: [1024]u8 = undefined;
    var ring = ByteRing.init(&buf);

    const messages = [_][]const u8{
        "one", "two",   "three", "four", "five",
        "six", "seven", "eight", "nine", "ten",
    };

    for (messages) |msg| {
        const entry = ring.reserve(msg.len).?;
        @memcpy(entry[HDR_SIZE..][0..msg.len], msg);
        ring.commit(entry, msg.len);
    }

    for (messages) |msg| {
        const data = ring.peek().?;
        try std.testing.expectEqualStrings(msg, data);
        ring.advance();
    }

    try std.testing.expect(ring.peek() == null);
}

test "ByteRing wraparound" {
    var buf: [128]u8 = undefined;
    var ring = ByteRing.init(&buf);

    // Fill to ~90% (each entry = HDR + 20 = 24 bytes, ~5 fit)
    for (0..5) |i| {
        const entry = ring.reserve(20).?;
        @memset(entry[HDR_SIZE..][0..20], @intCast(i + 1));
        ring.commit(entry, 20);
    }

    // Drain all
    for (0..5) |i| {
        const data = ring.peek().?;
        try std.testing.expectEqual(
            @as(u8, @intCast(i + 1)),
            data[0],
        );
        ring.advance();
    }

    // Fill again — wraps around
    for (0..5) |i| {
        const entry = ring.reserve(20).?;
        @memset(entry[HDR_SIZE..][0..20], @intCast(i + 10));
        ring.commit(entry, 20);
    }

    for (0..5) |i| {
        const data = ring.peek().?;
        try std.testing.expectEqual(
            @as(u8, @intCast(i + 10)),
            data[0],
        );
        ring.advance();
    }

    try std.testing.expect(ring.isEmpty());
}

test "ByteRing padding sentinel" {
    // 64-byte ring. Write entries until one forces a wrap.
    var buf: [64]u8 = undefined;
    var ring = ByteRing.init(&buf);

    // Entry of 20 bytes = 24 with header. Fits twice (48/64 used).
    const e1 = ring.reserve(20).?;
    @memset(e1[HDR_SIZE..][0..20], 'A');
    ring.commit(e1, 20);

    const e2 = ring.reserve(20).?;
    @memset(e2[HDR_SIZE..][0..20], 'B');
    ring.commit(e2, 20);

    // Consume first to free space
    const d1 = ring.peek().?;
    try std.testing.expectEqual(@as(u8, 'A'), d1[0]);
    ring.advance();

    // Now 24 bytes free at start, 16 bytes free at end.
    // An entry of 20 (24 total) won't fit in the 16 bytes at end.
    // Should pad and wrap to start.
    const e3 = ring.reserve(20).?;
    @memset(e3[HDR_SIZE..][0..20], 'C');
    ring.commit(e3, 20);

    // Read B then C
    const d2 = ring.peek().?;
    try std.testing.expectEqual(@as(u8, 'B'), d2[0]);
    ring.advance();

    const d3 = ring.peek().?;
    try std.testing.expectEqual(@as(u8, 'C'), d3[0]);
    ring.advance();

    try std.testing.expect(ring.isEmpty());
}

test "ByteRing full returns null" {
    var buf: [64]u8 = undefined;
    var ring = ByteRing.init(&buf);

    // Fill: 2 entries of 20 = 48 bytes used
    const e1 = ring.reserve(20).?;
    ring.commit(e1, 20);
    const e2 = ring.reserve(20).?;
    ring.commit(e2, 20);

    // Third should fail (48 + 24 = 72 > 64)
    try std.testing.expect(ring.reserve(20) == null);

    // Drain one, now should fit
    _ = ring.peek().?;
    ring.advance();
    try std.testing.expect(ring.reserve(20) != null);
}

test "ByteRing empty returns null" {
    var buf: [64]u8 = undefined;
    var ring = ByteRing.init(&buf);
    try std.testing.expect(ring.peek() == null);
    try std.testing.expect(ring.isEmpty());
}

test "ByteRing concurrent stress" {
    const NUM_MSGS = 100_000;
    const allocator = std.testing.allocator;

    const ring_buf = try allocator.alloc(u8, 65536);
    defer allocator.free(ring_buf);

    var ring = ByteRing.init(ring_buf);

    var received: usize = 0;
    var corrupt: bool = false;

    const consumer = try std.Thread.spawn(.{}, struct {
        fn run(
            r: *ByteRing,
            recv: *usize,
            bad: *bool,
        ) void {
            var count: usize = 0;
            while (count < NUM_MSGS) {
                if (r.peek()) |data| {
                    // Verify pattern: first byte = count & 0xFF
                    const expected: u8 = @truncate(count);
                    if (data.len < 1 or data[0] != expected) {
                        bad.* = true;
                    }
                    r.advance();
                    count += 1;
                } else {
                    std.atomic.spinLoopHint();
                }
            }
            recv.* = count;
        }
    }.run, .{ &ring, &received, &corrupt });

    // Producer
    for (0..NUM_MSGS) |i| {
        while (true) {
            if (ring.reserve(16)) |entry| {
                const pattern: u8 = @truncate(i);
                @memset(entry[HDR_SIZE..][0..16], pattern);
                ring.commit(entry, 16);
                break;
            } else {
                std.atomic.spinLoopHint();
            }
        }
    }

    consumer.join();

    try std.testing.expectEqual(NUM_MSGS, received);
    try std.testing.expect(!corrupt);
}

test "ByteRing max message size" {
    const allocator = std.testing.allocator;
    // Ring of 4096, message of ~2000 bytes (< capacity/2)
    const ring_buf = try allocator.alloc(u8, 4096);
    defer allocator.free(ring_buf);

    var ring = ByteRing.init(ring_buf);

    const msg_len = 2000;
    const entry = ring.reserve(msg_len).?;
    for (0..msg_len) |j| {
        entry[HDR_SIZE + j] = @truncate(j);
    }
    ring.commit(entry, msg_len);

    const data = ring.peek().?;
    try std.testing.expectEqual(msg_len, data.len);
    for (0..msg_len) |j| {
        const expected: u8 = @truncate(j);
        try std.testing.expectEqual(expected, data[j]);
    }
    ring.advance();
    try std.testing.expect(ring.isEmpty());
}

test "ByteRing alternating sizes" {
    const allocator = std.testing.allocator;
    const ring_buf = try allocator.alloc(u8, 131072);
    defer allocator.free(ring_buf);

    var ring = ByteRing.init(ring_buf);

    // Alternate tiny (10B) and large (1000B) entries
    const sizes = [_]usize{ 10, 1000 };
    const COUNT = 200;

    for (0..COUNT) |i| {
        const sz = sizes[i % 2];
        const entry = ring.reserve(sz).?;
        @memset(entry[HDR_SIZE..][0..sz], @truncate(i));
        ring.commit(entry, sz);
    }

    for (0..COUNT) |i| {
        const sz = sizes[i % 2];
        const data = ring.peek().?;
        try std.testing.expectEqual(sz, data.len);
        try std.testing.expectEqual(@as(u8, @truncate(i)), data[0]);
        ring.advance();
    }

    try std.testing.expect(ring.isEmpty());
}

test "ByteRing drainToWriter" {
    var buf: [256]u8 = undefined;
    var ring = ByteRing.init(&buf);

    const e1 = ring.reserve(3).?;
    @memcpy(e1[HDR_SIZE..][0..3], "abc");
    ring.commit(e1, 3);

    const e2 = ring.reserve(3).?;
    @memcpy(e2[HDR_SIZE..][0..3], "def");
    ring.commit(e2, 3);

    var out_buf: [64]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&out_buf);

    const count = try ring.drainToWriter(fbs.writer());
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings(
        "abcdef",
        fbs.getWritten(),
    );
    try std.testing.expect(ring.isEmpty());
}
