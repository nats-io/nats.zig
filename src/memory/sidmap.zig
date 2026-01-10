//! SidMap - Zero-Alloc Subscription ID Router
//!
//! Pre-allocated open-addressing hash map optimized for O(1) subscription
//! routing. Uses splitmix64 hash and power-of-two capacity for fast lookups.
//! Inspired by io_uring NATS client design.
//!
//! Zero allocations - caller provides pre-allocated arrays.

const std = @import("std");
const assert = std.debug.assert;

/// Sentinel values for slot state.
pub const EMPTY: u16 = 0xFFFF;
pub const TOMB: u16 = 0xFFFE;

/// Maximum valid slot index (leaves room for EMPTY/TOMB sentinels).
pub const MAX_SLOT: u16 = 0xFFFD;

/// Zero-allocation subscription ID to slot index map.
/// Caller provides pre-allocated keys/vals arrays at init.
pub const SidMap = struct {
    keys: []u64,
    vals: []u16,
    cap: u32,
    len: u32,

    /// Initialize SidMap with pre-allocated arrays.
    /// Capacity must be power of 2 and match array lengths.
    pub fn init(keys: []u64, vals: []u16) SidMap {
        assert(keys.len == vals.len);
        assert(keys.len > 0);
        assert(isPowerOfTwo(keys.len));
        assert(keys.len <= std.math.maxInt(u32));

        // Initialize all slots to EMPTY
        @memset(vals, EMPTY);

        return .{
            .keys = keys,
            .vals = vals,
            .cap = @intCast(keys.len),
            .len = 0,
        };
    }

    /// O(1) lookup - returns slot index for SID or null if not found.
    /// Marked inline for hot path performance.
    pub inline fn get(self: *const SidMap, sid: u64) ?u16 {
        assert(self.cap > 0);
        assert(isPowerOfTwo(self.cap));

        const mask = self.cap - 1;
        var idx: u32 = @intCast(mix64(sid) & mask);
        var probes: u32 = 0;

        while (probes < self.cap) : (probes += 1) {
            const v = self.vals[idx];

            if (v == EMPTY) {
                return null;
            }

            if (v != TOMB and self.keys[idx] == sid) {
                return v;
            }

            idx = (idx + 1) & mask;
        }

        return null;
    }

    /// Insert or update SID -> slot mapping.
    /// Returns error if map is full (load > 70%).
    pub fn put(self: *SidMap, sid: u64, slot: u16) error{MapFull}!void {
        assert(slot <= MAX_SLOT);
        assert(self.cap > 0);

        // Check load factor (max 70%)
        const max_load = self.cap * 7 / 10;
        if (self.len >= max_load) {
            return error.MapFull;
        }

        const mask = self.cap - 1;
        var idx: u32 = @intCast(mix64(sid) & mask);
        var tomb_idx: ?u32 = null;
        var probes: u32 = 0;

        while (probes < self.cap) : (probes += 1) {
            const v = self.vals[idx];

            if (v == EMPTY) {
                // Use tombstone slot if we found one
                const insert_idx = tomb_idx orelse idx;
                self.keys[insert_idx] = sid;
                self.vals[insert_idx] = slot;
                self.len += 1;
                return;
            }

            if (v == TOMB) {
                // Remember first tombstone for reuse
                if (tomb_idx == null) {
                    tomb_idx = idx;
                }
            } else if (self.keys[idx] == sid) {
                // Update existing
                self.vals[idx] = slot;
                return;
            }

            idx = (idx + 1) & mask;
        }

        // Should not reach here if load factor is respected
        unreachable;
    }

    /// Remove SID from map. Returns true if found and removed.
    pub fn remove(self: *SidMap, sid: u64) bool {
        assert(self.cap > 0);

        const mask = self.cap - 1;
        var idx: u32 = @intCast(mix64(sid) & mask);
        var probes: u32 = 0;

        while (probes < self.cap) : (probes += 1) {
            const v = self.vals[idx];

            if (v == EMPTY) {
                return false;
            }

            if (v != TOMB and self.keys[idx] == sid) {
                self.vals[idx] = TOMB;
                self.len -= 1;
                return true;
            }

            idx = (idx + 1) & mask;
        }

        return false;
    }

    /// Returns current number of entries.
    pub fn count(self: *const SidMap) u32 {
        return self.len;
    }

    /// Returns true if map is empty.
    pub fn isEmpty(self: *const SidMap) bool {
        return self.len == 0;
    }

    /// Clear all entries (reset to initial state).
    pub fn clear(self: *SidMap) void {
        @memset(self.vals, EMPTY);
        self.len = 0;
    }
};

/// splitmix64 hash function - fast 64-bit mixer.
/// 5 operations, excellent avalanche properties.
inline fn mix64(x0: u64) u64 {
    var x = x0 +% 0x9E3779B97F4A7C15;
    x = (x ^ (x >> 30)) *% 0xBF58476D1CE4E5B9;
    x = (x ^ (x >> 27)) *% 0x94D049BB133111EB;
    return x ^ (x >> 31);
}

/// Check if n is a power of two.
inline fn isPowerOfTwo(n: usize) bool {
    return n > 0 and (n & (n - 1)) == 0;
}

// Tests

test "SidMap basic operations" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;

    var map: SidMap = .init(&keys, &vals);

    try std.testing.expect(map.isEmpty());
    try std.testing.expectEqual(@as(u32, 0), map.count());

    // Insert
    try map.put(100, 0);
    try map.put(200, 1);
    try map.put(300, 2);

    try std.testing.expectEqual(@as(u32, 3), map.count());

    // Lookup
    try std.testing.expectEqual(@as(u16, 0), map.get(100).?);
    try std.testing.expectEqual(@as(u16, 1), map.get(200).?);
    try std.testing.expectEqual(@as(u16, 2), map.get(300).?);
    try std.testing.expect(map.get(999) == null);

    // Update
    try map.put(200, 42);
    try std.testing.expectEqual(@as(u16, 42), map.get(200).?);
    try std.testing.expectEqual(@as(u32, 3), map.count());

    // Remove
    try std.testing.expect(map.remove(200));
    try std.testing.expect(map.get(200) == null);
    try std.testing.expectEqual(@as(u32, 2), map.count());

    // Remove non-existent
    try std.testing.expect(!map.remove(999));
}

test "SidMap tombstone reuse" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;

    var map: SidMap = .init(&keys, &vals);

    // Fill slots
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);

    // Remove middle
    try std.testing.expect(map.remove(2));

    // Insert new - should reuse tombstone
    try map.put(4, 3);
    try std.testing.expectEqual(@as(u32, 3), map.count());

    // Verify all present
    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
    try std.testing.expect(map.get(2) == null);
    try std.testing.expectEqual(@as(u16, 2), map.get(3).?);
    try std.testing.expectEqual(@as(u16, 3), map.get(4).?);
}

test "SidMap load factor limit" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;

    var map: SidMap = .init(&keys, &vals);

    // 70% of 8 = 5 entries max
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);
    try map.put(4, 3);
    try map.put(5, 4);

    // 6th should fail
    try std.testing.expectError(error.MapFull, map.put(6, 5));
}

test "SidMap clear" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;

    var map: SidMap = .init(&keys, &vals);

    try map.put(1, 0);
    try map.put(2, 1);
    try std.testing.expectEqual(@as(u32, 2), map.count());

    map.clear();
    try std.testing.expect(map.isEmpty());
    try std.testing.expect(map.get(1) == null);
    try std.testing.expect(map.get(2) == null);
}

test "SidMap large capacity" {
    var keys: [512]u64 = undefined;
    var vals: [512]u16 = undefined;

    var map: SidMap = .init(&keys, &vals);

    // Insert many entries
    var i: u64 = 0;
    while (i < 300) : (i += 1) {
        try map.put(i * 1000, @intCast(i));
    }

    try std.testing.expectEqual(@as(u32, 300), map.count());

    // Verify all present
    i = 0;
    while (i < 300) : (i += 1) {
        try std.testing.expectEqual(@as(u16, @intCast(i)), map.get(i * 1000).?);
    }
}

test "splitmix64 distribution" {
    // Verify hash produces different values
    const h1 = mix64(1);
    const h2 = mix64(2);
    const h3 = mix64(1000);

    try std.testing.expect(h1 != h2);
    try std.testing.expect(h2 != h3);
    try std.testing.expect(h1 != h3);

    // Verify deterministic
    try std.testing.expectEqual(h1, mix64(1));
}
