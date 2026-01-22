//! SidMap Edge Case Tests
//!
//! Comprehensive test coverage for SidMap including:
//! - Sentinel value handling (EMPTY/TOMB corruption)
//! - Hash collision stress testing
//! - Tombstone accumulation and reuse
//! - Load factor boundary conditions
//! - Edge values (SID=0, slot=0, max values)

const std = @import("std");
const sidmap = @import("sidmap.zig");
const SidMap = sidmap.SidMap;
const EMPTY = sidmap.EMPTY;
const TOMB = sidmap.TOMB;
const MAX_SLOT = sidmap.MAX_SLOT;

// Section 1: Existing Tests (moved from sidmap.zig)

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

// Section 2: Edge Value Tests (SID boundaries)

test "SidMap SID zero" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // SID = 0 should be valid
    try map.put(0, 42);
    try std.testing.expectEqual(@as(u16, 42), map.get(0).?);
    try std.testing.expectEqual(@as(u32, 1), map.count());

    // Remove SID = 0
    try std.testing.expect(map.remove(0));
    try std.testing.expect(map.get(0) == null);
}

test "SidMap SID u64 max" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    const max_sid: u64 = std.math.maxInt(u64);
    try map.put(max_sid, 99);
    try std.testing.expectEqual(@as(u16, 99), map.get(max_sid).?);
}

test "SidMap SID one" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(1, 0);
    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
}

test "SidMap consecutive SIDs" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Insert consecutive SIDs 1-5
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);
    try map.put(4, 3);
    try map.put(5, 4);

    // Verify all accessible
    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
    try std.testing.expectEqual(@as(u16, 1), map.get(2).?);
    try std.testing.expectEqual(@as(u16, 2), map.get(3).?);
    try std.testing.expectEqual(@as(u16, 3), map.get(4).?);
    try std.testing.expectEqual(@as(u16, 4), map.get(5).?);
}

// Section 3: Edge Value Tests (Slot boundaries)

test "SidMap slot zero" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Slot = 0 should be valid (not confused with EMPTY)
    try map.put(100, 0);
    try std.testing.expectEqual(@as(u16, 0), map.get(100).?);
}

test "SidMap slot MAX_SLOT" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // MAX_SLOT = 0xFFFD should be valid
    try map.put(100, MAX_SLOT);
    try std.testing.expectEqual(MAX_SLOT, map.get(100).?);
}

// Sentinel protection: put() asserts slot <= MAX_SLOT to prevent TOMB/EMPTY
// corruption. Debug builds fail assertion; release builds strip assert.

test "SidMap sentinel values documented" {
    try std.testing.expectEqual(@as(u16, 0xFFFF), EMPTY);
    try std.testing.expectEqual(@as(u16, 0xFFFE), TOMB);
    try std.testing.expectEqual(@as(u16, 0xFFFD), MAX_SLOT);
}

test "SidMap valid slot range" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Verify full valid range: 0 to MAX_SLOT (0xFFFD)
    try map.put(1, 0); // Minimum valid
    try map.put(2, MAX_SLOT); // Maximum valid

    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
    try std.testing.expectEqual(MAX_SLOT, map.get(2).?);
}

// Section 4: Load Factor Boundary Tests

test "SidMap exact load factor boundary" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 8 = 5.6, truncated to 5
    // So max_load = 5, inserts allowed while len < 5
    try map.put(1, 0); // len=1
    try map.put(2, 1); // len=2
    try map.put(3, 2); // len=3
    try map.put(4, 3); // len=4
    try map.put(5, 4); // len=5

    try std.testing.expectEqual(@as(u32, 5), map.count());

    // Next insert should fail (5 >= 5)
    try std.testing.expectError(error.MapFull, map.put(6, 5));
}

test "SidMap load factor after removes" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Fill to capacity
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);
    try map.put(4, 3);
    try map.put(5, 4);

    // Remove some
    try std.testing.expect(map.remove(1));
    try std.testing.expect(map.remove(2));
    try std.testing.expectEqual(@as(u32, 3), map.count());

    // Should be able to insert again (len=3, max_load=5)
    try map.put(6, 5);
    try map.put(7, 6);
    try std.testing.expectEqual(@as(u32, 5), map.count());

    // Now at limit again
    try std.testing.expectError(error.MapFull, map.put(8, 7));
}

test "SidMap load factor 16 capacity" {
    var keys: [16]u64 = undefined;
    var vals: [16]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 16 = 11.2, truncated to 11
    var i: u64 = 0;
    while (i < 11) : (i += 1) {
        try map.put(i, @intCast(i));
    }

    try std.testing.expectEqual(@as(u32, 11), map.count());

    // 12th should fail
    try std.testing.expectError(error.MapFull, map.put(11, 11));
}

test "SidMap load factor 256 capacity" {
    var keys: [256]u64 = undefined;
    var vals: [256]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 256 = 179.2, truncated to 179
    var i: u64 = 0;
    while (i < 179) : (i += 1) {
        try map.put(i * 7, @intCast(i)); // Spread out SIDs
    }

    try std.testing.expectEqual(@as(u32, 179), map.count());

    // 180th should fail
    try std.testing.expectError(error.MapFull, map.put(9999, 179));
}

// Section 5: Tombstone Behavior Tests

test "SidMap tombstone lookup continues probing" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Insert entries that will cluster (hash not controllable, insert many)
    try map.put(100, 0);
    try map.put(200, 1);
    try map.put(300, 2);

    // Remove middle entry
    try std.testing.expect(map.remove(200));

    // Lookup of 300 should still work (must probe past tombstone)
    try std.testing.expectEqual(@as(u16, 2), map.get(300).?);
}

test "SidMap tombstone reuse on insert" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Fill to limit
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);
    try map.put(4, 3);
    try map.put(5, 4);

    // Remove all
    try std.testing.expect(map.remove(1));
    try std.testing.expect(map.remove(2));
    try std.testing.expect(map.remove(3));
    try std.testing.expect(map.remove(4));
    try std.testing.expect(map.remove(5));

    try std.testing.expectEqual(@as(u32, 0), map.count());

    // Insert new entries - should reuse tombstones
    try map.put(10, 0);
    try map.put(20, 1);
    try map.put(30, 2);
    try map.put(40, 3);
    try map.put(50, 4);

    try std.testing.expectEqual(@as(u32, 5), map.count());

    // Verify all accessible
    try std.testing.expectEqual(@as(u16, 0), map.get(10).?);
    try std.testing.expectEqual(@as(u16, 4), map.get(50).?);
}

test "SidMap many insert remove cycles" {
    var keys: [64]u64 = undefined;
    var vals: [64]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 64 = 44
    const max_entries = 44;

    // Cycle 1: fill and empty
    var i: u64 = 0;
    while (i < max_entries) : (i += 1) {
        try map.put(i, @intCast(i));
    }
    i = 0;
    while (i < max_entries) : (i += 1) {
        try std.testing.expect(map.remove(i));
    }

    // Cycle 2: fill with different SIDs
    i = 1000;
    while (i < 1000 + max_entries) : (i += 1) {
        try map.put(i, @intCast(i - 1000));
    }

    // Verify cycle 2 entries
    try std.testing.expectEqual(@as(u32, max_entries), map.count());
    try std.testing.expectEqual(@as(u16, 0), map.get(1000).?);
    try std.testing.expectEqual(@as(u16, 43), map.get(1043).?);

    // Old entries should not exist
    try std.testing.expect(map.get(0) == null);
    try std.testing.expect(map.get(43) == null);
}

test "SidMap tombstone does not affect count" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(1, 0);
    try map.put(2, 1);
    try std.testing.expectEqual(@as(u32, 2), map.count());

    try std.testing.expect(map.remove(1));
    try std.testing.expectEqual(@as(u32, 1), map.count());

    // Tombstone exists but count reflects only live entries
    try std.testing.expect(map.get(1) == null);
    try std.testing.expectEqual(@as(u16, 1), map.get(2).?);
}

// Section 6: Double Operation Tests

test "SidMap double remove same SID" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(100, 42);
    try std.testing.expectEqual(@as(u32, 1), map.count());

    // First remove succeeds
    try std.testing.expect(map.remove(100));
    try std.testing.expectEqual(@as(u32, 0), map.count());

    // Second remove returns false (not found)
    try std.testing.expect(!map.remove(100));
    try std.testing.expectEqual(@as(u32, 0), map.count());
}

test "SidMap update existing does not change count" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(100, 0);
    try std.testing.expectEqual(@as(u32, 1), map.count());

    // Update same SID multiple times
    try map.put(100, 1);
    try map.put(100, 2);
    try map.put(100, 42);

    // Count should still be 1
    try std.testing.expectEqual(@as(u32, 1), map.count());
    try std.testing.expectEqual(@as(u16, 42), map.get(100).?);
}

test "SidMap put after remove same SID" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(100, 0);
    try std.testing.expect(map.remove(100));
    try std.testing.expect(map.get(100) == null);

    // Re-insert same SID
    try map.put(100, 99);
    try std.testing.expectEqual(@as(u16, 99), map.get(100).?);
    try std.testing.expectEqual(@as(u32, 1), map.count());
}

// Section 7: Empty Map Operations

test "SidMap get on empty" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try std.testing.expect(map.get(0) == null);
    try std.testing.expect(map.get(1) == null);
    try std.testing.expect(map.get(std.math.maxInt(u64)) == null);
}

test "SidMap remove on empty" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try std.testing.expect(!map.remove(0));
    try std.testing.expect(!map.remove(100));
    try std.testing.expectEqual(@as(u32, 0), map.count());
}

test "SidMap clear on empty" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    map.clear(); // Should not crash
    try std.testing.expect(map.isEmpty());
}

test "SidMap clear after operations" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try map.put(1, 0);
    try map.put(2, 1);
    try std.testing.expect(map.remove(1)); // Creates tombstone

    map.clear();

    // After clear, map should be pristine
    try std.testing.expect(map.isEmpty());
    try std.testing.expect(map.get(1) == null);
    try std.testing.expect(map.get(2) == null);

    // Should be able to fill again
    try map.put(10, 0);
    try map.put(20, 1);
    try map.put(30, 2);
    try map.put(40, 3);
    try map.put(50, 4);
    try std.testing.expectEqual(@as(u32, 5), map.count());
}

// Section 8: Probe Chain Integrity Tests

test "SidMap remove does not break probe chain" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Insert 5 entries
    try map.put(10, 0);
    try map.put(20, 1);
    try map.put(30, 2);
    try map.put(40, 3);
    try map.put(50, 4);

    // Remove entries from different positions
    try std.testing.expect(map.remove(20));
    try std.testing.expect(map.remove(40));

    // Remaining entries should still be findable
    try std.testing.expectEqual(@as(u16, 0), map.get(10).?);
    try std.testing.expectEqual(@as(u16, 2), map.get(30).?);
    try std.testing.expectEqual(@as(u16, 4), map.get(50).?);

    // Removed entries should not be found
    try std.testing.expect(map.get(20) == null);
    try std.testing.expect(map.get(40) == null);
}

test "SidMap insert after remove maintains integrity" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Fill
    try map.put(1, 0);
    try map.put(2, 1);
    try map.put(3, 2);
    try map.put(4, 3);
    try map.put(5, 4);

    // Remove some
    try std.testing.expect(map.remove(2));
    try std.testing.expect(map.remove(4));

    // Insert new
    try map.put(6, 5);
    try map.put(7, 6);

    // All live entries should be accessible
    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
    try std.testing.expectEqual(@as(u16, 2), map.get(3).?);
    try std.testing.expectEqual(@as(u16, 4), map.get(5).?);
    try std.testing.expectEqual(@as(u16, 5), map.get(6).?);
    try std.testing.expectEqual(@as(u16, 6), map.get(7).?);

    // Removed should not be found
    try std.testing.expect(map.get(2) == null);
    try std.testing.expect(map.get(4) == null);
}

// Section 9: Capacity Variations

test "SidMap minimum capacity 2" {
    var keys: [2]u64 = undefined;
    var vals: [2]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 2 = 1.4, truncated to 1
    try map.put(100, 0);
    try std.testing.expectEqual(@as(u32, 1), map.count());

    // 2nd should fail
    try std.testing.expectError(error.MapFull, map.put(200, 1));
}

test "SidMap capacity 4" {
    var keys: [4]u64 = undefined;
    var vals: [4]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 4 = 2.8, truncated to 2
    try map.put(1, 0);
    try map.put(2, 1);
    try std.testing.expectEqual(@as(u32, 2), map.count());

    // 3rd should fail
    try std.testing.expectError(error.MapFull, map.put(3, 2));
}

test "SidMap capacity 1024" {
    var keys: [1024]u64 = undefined;
    var vals: [1024]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // 70% of 1024 = 716.8, truncated to 716
    var i: u64 = 0;
    while (i < 716) : (i += 1) {
        try map.put(i, @intCast(i & 0xFFFF));
    }
    try std.testing.expectEqual(@as(u32, 716), map.count());

    // 717th should fail
    try std.testing.expectError(error.MapFull, map.put(99999, 0));
}

// Section 10: Lookup Performance After Tombstones

test "SidMap lookup non-existent after many tombstones" {
    var keys: [64]u64 = undefined;
    var vals: [64]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Fill to limit (70% of 64 = 44)
    var i: u64 = 0;
    while (i < 44) : (i += 1) {
        try map.put(i, @intCast(i));
    }

    // Remove all - creates 44 tombstones
    i = 0;
    while (i < 44) : (i += 1) {
        try std.testing.expect(map.remove(i));
    }

    try std.testing.expectEqual(@as(u32, 0), map.count());

    // Lookup non-existent keys - must probe through all tombstones
    // This tests that get() terminates correctly
    try std.testing.expect(map.get(100) == null);
    try std.testing.expect(map.get(999) == null);
    try std.testing.expect(map.get(0) == null); // Was removed

    // Insert new entry - should reuse tombstone
    try map.put(1000, 42);
    try std.testing.expectEqual(@as(u16, 42), map.get(1000).?);
}

test "SidMap alternating insert remove stress" {
    var keys: [32]u64 = undefined;
    var vals: [32]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Stress test: many insert/remove cycles
    var cycle: u32 = 0;
    while (cycle < 100) : (cycle += 1) {
        const sid = @as(u64, cycle) * 1000;
        try map.put(sid, @intCast(cycle & 0xFFFF));
        try std.testing.expect(map.remove(sid));
    }

    try std.testing.expectEqual(@as(u32, 0), map.count());

    // Map should still be functional
    try map.put(1, 0);
    try std.testing.expectEqual(@as(u16, 0), map.get(1).?);
}

// Section 11: Hash Distribution Tests

test "SidMap sequential SIDs distribute well" {
    var keys: [256]u64 = undefined;
    var vals: [256]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Insert sequential SIDs - good hash should distribute them
    var i: u64 = 1;
    while (i <= 170) : (i += 1) { // ~66% load
        try map.put(i, @intCast(i));
    }

    // All should be accessible
    i = 1;
    while (i <= 170) : (i += 1) {
        try std.testing.expectEqual(@as(u16, @intCast(i)), map.get(i).?);
    }
}

test "SidMap sparse SIDs" {
    var keys: [64]u64 = undefined;
    var vals: [64]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    // Insert SIDs with large gaps
    const sids = [_]u64{
        1,
        1000,
        1000000,
        1000000000,
        std.math.maxInt(u64),
        std.math.maxInt(u64) - 1,
        std.math.maxInt(u64) / 2,
    };

    for (sids, 0..) |sid, idx| {
        try map.put(sid, @intCast(idx));
    }

    // All should be accessible
    for (sids, 0..) |sid, idx| {
        try std.testing.expectEqual(@as(u16, @intCast(idx)), map.get(sid).?);
    }
}

// Section 12: Edge Cases in State

test "SidMap isEmpty after fill and empty" {
    var keys: [8]u64 = undefined;
    var vals: [8]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try std.testing.expect(map.isEmpty());

    try map.put(1, 0);
    try std.testing.expect(!map.isEmpty());

    try std.testing.expect(map.remove(1));
    try std.testing.expect(map.isEmpty());
}

test "SidMap count accuracy through operations" {
    var keys: [16]u64 = undefined;
    var vals: [16]u16 = undefined;
    var map: SidMap = .init(&keys, &vals);

    try std.testing.expectEqual(@as(u32, 0), map.count());

    try map.put(1, 0);
    try std.testing.expectEqual(@as(u32, 1), map.count());

    try map.put(2, 1);
    try std.testing.expectEqual(@as(u32, 2), map.count());

    try map.put(1, 99); // Update, not insert
    try std.testing.expectEqual(@as(u32, 2), map.count());

    try std.testing.expect(map.remove(1));
    try std.testing.expectEqual(@as(u32, 1), map.count());

    try std.testing.expect(!map.remove(1)); // Already removed
    try std.testing.expectEqual(@as(u32, 1), map.count());

    try std.testing.expect(!map.remove(999)); // Never existed
    try std.testing.expectEqual(@as(u32, 1), map.count());

    try std.testing.expect(map.remove(2));
    try std.testing.expectEqual(@as(u32, 0), map.count());
}
