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
///
/// Uses open-addressing with linear probing and splitmix64 hash.
/// Caller provides pre-allocated keys/vals arrays at init.
/// Maximum 70% load factor enforced to maintain O(1) performance.
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
                const insert_idx = tomb_idx orelse idx;
                self.keys[insert_idx] = sid;
                self.vals[insert_idx] = slot;
                self.len += 1;
                return;
            }

            if (v == TOMB) {
                if (tomb_idx == null) {
                    tomb_idx = idx;
                }
            } else if (self.keys[idx] == sid) {
                self.vals[idx] = slot;
                return;
            }

            idx = (idx + 1) & mask;
        }

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
/// 5 operations, good avalanche properties.
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

test {
    _ = @import("sidmap_test.zig");
}
