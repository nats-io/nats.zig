//! Tiered Slab Allocator
//!
//! High-performance message buffer allocator with O(1) alloc/free.
//! Uses tiered slabs with embedded free lists for zero-overhead tracking.
//! Falls back to provided allocator for oversized allocations.

const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const defaults = @import("../defaults.zig");

/// Configuration for tiered slab allocator (derived from defaults.Memory).
pub const Config = struct {
    pub const TIER_COUNT = defaults.Memory.tier_count;

    /// Slice sizes per tier (power-of-2 for efficient selection).
    pub const tier_sizes = defaults.Memory.tier_sizes;

    /// Slice counts per tier (derived from queue_size).
    pub const tier_counts = defaults.Memory.tier_counts;

    /// Maximum slice size handled by slab (larger uses fallback).
    pub const max_slice_size: usize = defaults.Memory.max_slice_size;

    /// Total pre-allocated memory.
    pub const total_memory: usize = defaults.Memory.total_memory;
};

/// Single-tier slab with embedded free list.
///
/// Each free slice stores the index of the next free slice in its first
/// 4 bytes. This eliminates separate tracking overhead.
pub const Slab = struct {
    memory: []align(4096) u8,
    slice_size: u32,
    slice_count: u32,
    free_head: u32,
    alloc_count: u32,

    const NONE: u32 = 0xFFFF_FFFF;

    /// Initialize slab with mmap'd memory.
    pub fn init(slice_size: u32, slice_count: u32) !Slab {
        assert(slice_size >= 4);
        assert(slice_count > 0);
        assert(slice_size <= Config.max_slice_size);

        const total = @as(usize, slice_size) * slice_count;

        const memory = std.posix.mmap(
            null,
            total,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        ) catch return error.MmapFailed;

        var slab = Slab{
            .memory = @alignCast(memory),
            .slice_size = slice_size,
            .slice_count = slice_count,
            .free_head = 0,
            .alloc_count = 0,
        };

        var i: u32 = 0;
        while (i < slice_count) : (i += 1) {
            const slice = slab.getSliceByIndex(i);
            const next: u32 = if (i + 1 < slice_count) i + 1 else NONE;
            @as(*u32, @ptrCast(@alignCast(slice.ptr))).* = next;
        }

        return slab;
    }

    /// Release mmap'd memory.
    pub fn deinit(self: *Slab) void {
        std.posix.munmap(self.memory);
        self.* = undefined;
    }

    /// O(1) allocation - pop from embedded free list.
    pub inline fn alloc(self: *Slab) ?[]u8 {
        if (self.free_head == NONE) return null;

        const idx = self.free_head;
        const slice = self.getSliceByIndex(idx);

        self.free_head = @as(*u32, @ptrCast(@alignCast(slice.ptr))).*;
        self.alloc_count += 1;

        return slice;
    }

    /// O(1) deallocation - push to embedded free list.
    /// Debug builds detect double-free by walking the free list.
    pub inline fn free(self: *Slab, ptr: [*]u8) void {
        const idx = self.ptrToIndex(ptr);
        assert(idx < self.slice_count);

        if (builtin.mode == .Debug) {
            assert(!self.isInFreeList(idx));
        }

        @as(*u32, @ptrCast(@alignCast(ptr))).* = self.free_head;
        self.free_head = idx;
        self.alloc_count -= 1;
    }

    /// Debug helper: check if index is already in free list (O(n)).
    fn isInFreeList(self: *Slab, target_idx: u32) bool {
        var current = self.free_head;
        while (current != NONE) {
            if (current == target_idx) return true;
            const slice = self.getSliceByIndex(current);
            current = @as(*u32, @ptrCast(@alignCast(slice.ptr))).*;
        }
        return false;
    }

    inline fn getSliceByIndex(self: *Slab, idx: u32) []u8 {
        const offset = @as(usize, idx) * self.slice_size;
        return self.memory[offset..][0..self.slice_size];
    }

    inline fn ptrToIndex(self: *Slab, ptr: [*]u8) u32 {
        const ptr_addr = @intFromPtr(ptr);
        const mem_start = @intFromPtr(self.memory.ptr);
        const mem_end = mem_start + self.memory.len;

        assert(ptr_addr >= mem_start);
        assert(ptr_addr < mem_end);

        const offset = ptr_addr - mem_start;
        assert(offset % self.slice_size == 0);

        return @intCast(offset / self.slice_size);
    }

    /// Check if pointer belongs to this slab.
    pub inline fn contains(self: *const Slab, ptr: [*]u8) bool {
        const addr = @intFromPtr(ptr);
        const base = @intFromPtr(self.memory.ptr);
        return addr >= base and addr < base + self.memory.len;
    }

    /// Returns number of currently allocated slices.
    pub fn getAllocCount(self: *const Slab) u32 {
        return self.alloc_count;
    }

    /// Returns total capacity in slices.
    pub fn getCapacity(self: *const Slab) u32 {
        return self.slice_count;
    }
};

/// Multi-tier slab allocator with fallback.
///
/// Selects appropriate tier based on requested size. Falls back to
/// provided allocator for sizes exceeding max tier.
pub const TieredSlab = struct {
    tiers: [Config.TIER_COUNT]Slab,
    fallback: Allocator,
    fallback_count: u32,

    /// Initialize all tiers with mmap'd memory.
    pub fn init(fallback_allocator: Allocator) !TieredSlab {
        var ts: TieredSlab = undefined;
        ts.fallback = fallback_allocator;
        ts.fallback_count = 0;

        var initialized: usize = 0;
        errdefer {
            for (ts.tiers[0..initialized]) |*tier| {
                tier.deinit();
            }
        }

        for (Config.tier_sizes, Config.tier_counts, 0..) |size, count, i| {
            ts.tiers[i] = try Slab.init(size, count);
            initialized += 1;
        }

        return ts;
    }

    /// Release all mmap'd memory.
    pub fn deinit(self: *TieredSlab) void {
        for (&self.tiers) |*tier| {
            tier.deinit();
        }
    }

    /// O(1) tier selection based on size.
    inline fn selectTier(size: usize) ?usize {
        if (size <= 256) return 0;
        if (size <= 512) return 1;
        if (size <= 1024) return 2;
        if (size <= 4096) return 3;
        if (size <= 16384) return 4;
        return null;
    }

    /// Allocate from appropriate tier or fallback.
    pub fn alloc(self: *TieredSlab, size: usize) ?[]u8 {
        assert(size > 0);

        if (selectTier(size)) |tier_idx| {
            if (self.tiers[tier_idx].alloc()) |slice| {
                return slice[0..size];
            }
        }

        self.fallback_count += 1;
        return self.fallback.alloc(u8, size) catch null;
    }

    /// Free to appropriate tier or fallback.
    pub fn free(self: *TieredSlab, buf: []u8) void {
        assert(buf.len > 0);

        const ptr = buf.ptr;

        inline for (&self.tiers) |*tier| {
            if (tier.contains(ptr)) {
                tier.free(ptr);
                return;
            }
        }

        self.fallback_count -= 1;
        self.fallback.free(buf);
    }

    /// Check if pointer belongs to any slab tier.
    pub fn containsPtr(self: *const TieredSlab, ptr: [*]u8) bool {
        inline for (&self.tiers) |*tier| {
            if (tier.contains(ptr)) return true;
        }
        return false;
    }

    /// Get diagnostic statistics.
    pub fn getStats(self: *const TieredSlab) Stats {
        var stats = Stats{};
        for (self.tiers, 0..) |tier, i| {
            stats.tier_alloc_counts[i] = tier.alloc_count;
            stats.tier_capacities[i] = tier.slice_count;
        }
        stats.fallback_count = self.fallback_count;
        return stats;
    }

    pub const Stats = struct {
        tier_alloc_counts: [Config.TIER_COUNT]u32 = .{0} ** Config.TIER_COUNT,
        tier_capacities: [Config.TIER_COUNT]u32 = .{0} ** Config.TIER_COUNT,
        fallback_count: u32 = 0,

        /// Total allocated across all tiers.
        pub fn totalAllocated(self: Stats) u32 {
            var total: u32 = 0;
            for (self.tier_alloc_counts) |count| {
                total += count;
            }
            return total + self.fallback_count;
        }

        /// Total capacity across all tiers.
        pub fn totalCapacity(self: Stats) u32 {
            var total: u32 = 0;
            for (self.tier_capacities) |cap| {
                total += cap;
            }
            return total;
        }
    };
};

/// Wrapper that implements std.mem.Allocator interface.
///
/// This allows TieredSlab to be used transparently with code expecting
/// an Allocator, such as Message.deinit().
pub const SlabAllocator = struct {
    slab: *TieredSlab,

    /// Return std.mem.Allocator interface.
    pub fn allocator(self: *SlabAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    const vtable: Allocator.VTable = .{
        .alloc = allocFn,
        .resize = resizeFn,
        .remap = remapFn,
        .free = freeFn,
    };

    fn allocFn(
        ctx: *anyopaque,
        len: usize,
        alignment: std.mem.Alignment,
        ret_addr: usize,
    ) ?[*]u8 {
        _ = alignment;
        _ = ret_addr;
        const self: *SlabAllocator = @ptrCast(@alignCast(ctx));
        const buf = self.slab.alloc(len) orelse return null;
        return buf.ptr;
    }

    fn resizeFn(
        ctx: *anyopaque,
        buf: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) bool {
        _ = ctx;
        _ = alignment;
        _ = ret_addr;
        if (new_len <= buf.len) return true;
        return false;
    }

    fn remapFn(
        ctx: *anyopaque,
        memory: []u8,
        alignment: std.mem.Alignment,
        new_len: usize,
        ret_addr: usize,
    ) ?[*]u8 {
        _ = ctx;
        _ = memory;
        _ = alignment;
        _ = new_len;
        _ = ret_addr;
        return null;
    }

    fn freeFn(
        ctx: *anyopaque,
        buf: []u8,
        alignment: std.mem.Alignment,
        ret_addr: usize,
    ) void {
        _ = alignment;
        _ = ret_addr;
        const self: *SlabAllocator = @ptrCast(@alignCast(ctx));
        self.slab.free(buf);
    }
};

test "Slab basic alloc/free" {
    var slab = try Slab.init(256, 16);
    defer slab.deinit();

    var ptrs: [16][]u8 = undefined;
    for (&ptrs) |*p| {
        p.* = slab.alloc() orelse unreachable;
    }

    try std.testing.expect(slab.alloc() == null);
    try std.testing.expectEqual(@as(u32, 16), slab.getAllocCount());

    for (ptrs) |p| {
        slab.free(p.ptr);
    }

    try std.testing.expectEqual(@as(u32, 0), slab.getAllocCount());

    const p = slab.alloc() orelse unreachable;
    try std.testing.expect(p.len == 256);
}

test "TieredSlab tier selection" {
    const fallback = std.testing.allocator;
    var ts = try TieredSlab.init(fallback);
    defer ts.deinit();

    const small = ts.alloc(100) orelse unreachable;
    try std.testing.expect(ts.containsPtr(small.ptr));

    const medium = ts.alloc(800) orelse unreachable;
    try std.testing.expect(ts.containsPtr(medium.ptr));

    const large = ts.alloc(20000) orelse unreachable;
    try std.testing.expect(!ts.containsPtr(large.ptr));

    ts.free(small);
    ts.free(medium);
    ts.free(large);

    const stats = ts.getStats();
    try std.testing.expectEqual(@as(u32, 0), stats.totalAllocated());
}

test "SlabAllocator interface" {
    const fallback = std.testing.allocator;
    var ts = try TieredSlab.init(fallback);
    defer ts.deinit();

    var sa = SlabAllocator{ .slab = &ts };
    const alloc = sa.allocator();

    const buf = try alloc.alloc(u8, 200);
    try std.testing.expect(buf.len == 200);

    alloc.free(buf);
}
