//! Slab Pool for Zero-Allocation Message Ownership
//!
//! Provides reference-counted memory slabs for owned message paths.
//! Single-threaded design uses non-atomic refcounts (faster than Rust).
//!
//! Flow: SlabPool -> Slab (refcounted) -> RefSlice (owned slice)
//!
//! When all RefSlices into a Slab are released, the Slab returns to pool.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const posix = std.posix;

/// Reference-counted memory region from SlabPool.
/// Single-threaded: non-atomic refcount (faster than Rust's Arc).
pub const Slab = struct {
    data: [*]u8,
    len: u32,
    refcount: u32,
    index: u32,

    /// Increment reference count. Call when creating new RefSlice.
    pub fn retain(self: *Slab) void {
        assert(self.refcount > 0);
        assert(self.refcount < std.math.maxInt(u32));
        self.refcount += 1;
    }

    /// Decrement reference count.
    /// Returns true if refcount hit zero (caller should return to pool).
    pub fn release(self: *Slab) bool {
        assert(self.refcount > 0);
        self.refcount -= 1;
        return self.refcount == 0;
    }

    /// Get the full slab memory as a slice.
    pub fn slice(self: *const Slab) []u8 {
        assert(self.len > 0);
        return self.data[0..self.len];
    }
};

/// Owned slice into a Slab. Holds reference to keep slab alive.
/// When RefSlice is released, decrements slab refcount.
pub const RefSlice = struct {
    ptr: [*]const u8,
    len: usize,
    slab: *Slab,
    pool: *SlabPool,

    /// Get slice as []const u8.
    pub fn slice(self: RefSlice) []const u8 {
        return self.ptr[0..self.len];
    }

    /// Release reference to underlying slab.
    pub fn deinit(self: *RefSlice) void {
        assert(self.slab.refcount > 0);
        if (self.slab.release()) {
            self.pool.returnSlab(self.slab);
        }
        self.* = undefined;
    }

    /// Create sub-slice. Retains slab (increments refcount).
    pub fn subslice(self: RefSlice, start: usize, end: usize) RefSlice {
        assert(start <= end);
        assert(end <= self.len);
        assert(self.slab.refcount > 0);

        self.slab.retain();
        return .{
            .ptr = self.ptr + start,
            .len = end - start,
            .slab = self.slab,
            .pool = self.pool,
        };
    }

    /// Clone this RefSlice. Retains slab (increments refcount).
    pub fn clone(self: RefSlice) RefSlice {
        assert(self.slab.refcount > 0);
        self.slab.retain();
        return self;
    }
};

/// Pre-allocated pool of reference-counted slabs.
/// Uses mmap for page-aligned memory with optional page warming.
pub const SlabPool = struct {
    slabs: []Slab,
    free_stack: []u32,
    free_count: u32,
    slab_size: u32,
    slab_count: u32,
    memory: []align(4096) u8,

    pub const Options = struct {
        slab_count: u32 = 256,
        slab_size: u32 = 65536,
        enable_page_warming: bool = true,
    };

    /// Initialize slab pool with mmap'd memory.
    pub fn init(allocator: Allocator, opts: Options) !SlabPool {
        assert(opts.slab_count > 0);
        assert(opts.slab_size > 0);
        assert(opts.slab_size % 4096 == 0);

        const total_size = @as(usize, opts.slab_count) *
            @as(usize, opts.slab_size);
        assert(total_size > 0);

        // mmap for page-aligned memory
        const memory = try posix.mmap(
            null,
            total_size,
            posix.PROT.READ | posix.PROT.WRITE,
            .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
            -1,
            0,
        );

        // Allocate slab metadata
        const slabs = try allocator.alloc(Slab, opts.slab_count);
        errdefer allocator.free(slabs);

        // Allocate free stack
        const free_stack = try allocator.alloc(u32, opts.slab_count);
        errdefer allocator.free(free_stack);

        const aligned_memory: []align(4096) u8 = @alignCast(memory);

        // Initialize slabs and free stack
        for (0..opts.slab_count) |i| {
            const idx: u32 = @intCast(i);
            const offset = @as(usize, idx) * @as(usize, opts.slab_size);

            slabs[i] = .{
                .data = aligned_memory.ptr + offset,
                .len = opts.slab_size,
                .refcount = 0,
                .index = idx,
            };

            // All slabs start free
            free_stack[i] = idx;
        }

        var pool = SlabPool{
            .slabs = slabs,
            .free_stack = free_stack,
            .free_count = opts.slab_count,
            .slab_size = opts.slab_size,
            .slab_count = opts.slab_count,
            .memory = aligned_memory,
        };

        // Page warming
        if (opts.enable_page_warming) {
            pool.warmPages();
        }

        // Postconditions
        assert(pool.memory.len == total_size);
        assert(pool.free_count == opts.slab_count);
        assert(@intFromPtr(pool.memory.ptr) % 4096 == 0);

        return pool;
    }

    /// Clean up pool. All slabs must be returned first.
    pub fn deinit(self: *SlabPool, allocator: Allocator) void {
        assert(self.free_count == self.slab_count);

        posix.munmap(self.memory);
        allocator.free(self.slabs);
        allocator.free(self.free_stack);
        self.* = undefined;
    }

    /// Acquire a slab from pool. Returns null if pool exhausted.
    pub fn acquireSlab(self: *SlabPool) ?*Slab {
        if (self.free_count == 0) return null;

        self.free_count -= 1;
        const idx = self.free_stack[self.free_count];

        assert(idx < self.slab_count);
        const slab = &self.slabs[idx];

        assert(slab.refcount == 0);
        slab.refcount = 1;

        return slab;
    }

    /// Return slab to pool. Called by RefSlice when refcount hits 0.
    pub fn returnSlab(self: *SlabPool, slab: *Slab) void {
        assert(slab.refcount == 0);
        assert(slab.index < self.slab_count);
        assert(self.free_count < self.slab_count);

        self.free_stack[self.free_count] = slab.index;
        self.free_count += 1;
    }

    /// Create RefSlice from data, copying into acquired slab.
    /// Returns null if pool exhausted.
    pub fn createRefSlice(self: *SlabPool, data: []const u8) ?RefSlice {
        assert(data.len <= self.slab_size);

        const slab = self.acquireSlab() orelse return null;
        const dest = slab.slice();

        @memcpy(dest[0..data.len], data);

        return .{
            .ptr = dest.ptr,
            .len = data.len,
            .slab = slab,
            .pool = self,
        };
    }

    /// Get number of free slabs available.
    pub fn availableSlabs(self: *const SlabPool) u32 {
        return self.free_count;
    }

    /// Get total pool capacity.
    pub fn capacity(self: *const SlabPool) u32 {
        return self.slab_count;
    }

    /// Warm all pages by touching first byte of each 4KB page.
    fn warmPages(self: *SlabPool) void {
        const page_size: usize = 4096;
        var i: usize = 0;

        while (i < self.memory.len) : (i += page_size) {
            self.memory[i] = 0;
        }
    }
};

// Tests

test "Slab retain and release" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 4,
        .slab_size = 4096,
        .enable_page_warming = false,
    });
    defer pool.deinit(allocator);

    const slab = pool.acquireSlab().?;
    try std.testing.expectEqual(@as(u32, 1), slab.refcount);
    try std.testing.expectEqual(@as(u32, 3), pool.availableSlabs());

    slab.retain();
    try std.testing.expectEqual(@as(u32, 2), slab.refcount);

    const should_return = slab.release();
    try std.testing.expect(!should_return);
    try std.testing.expectEqual(@as(u32, 1), slab.refcount);

    const should_return2 = slab.release();
    try std.testing.expect(should_return2);
    pool.returnSlab(slab);

    try std.testing.expectEqual(@as(u32, 4), pool.availableSlabs());
}

test "RefSlice basic operations" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 4,
        .slab_size = 4096,
        .enable_page_warming = false,
    });
    defer pool.deinit(allocator);

    const data = "Hello, NATS!";
    var ref = pool.createRefSlice(data).?;
    defer ref.deinit();

    try std.testing.expectEqualSlices(u8, data, ref.slice());
    try std.testing.expectEqual(data.len, ref.len);
}

test "RefSlice subslice retains slab" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 4,
        .slab_size = 4096,
        .enable_page_warming = false,
    });
    defer pool.deinit(allocator);

    const data = "Hello, NATS!";
    var ref = pool.createRefSlice(data).?;

    // Create subslice
    var sub = ref.subslice(0, 5);
    try std.testing.expectEqualSlices(u8, "Hello", sub.slice());
    try std.testing.expectEqual(@as(u32, 2), ref.slab.refcount);

    // Release original
    ref.deinit();
    try std.testing.expectEqual(@as(u32, 1), sub.slab.refcount);

    // Release subslice
    sub.deinit();
    try std.testing.expectEqual(@as(u32, 4), pool.availableSlabs());
}

test "SlabPool exhaustion and recovery" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 2,
        .slab_size = 4096,
        .enable_page_warming = false,
    });
    defer pool.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 2), pool.availableSlabs());

    // Acquire all slabs
    const s1 = pool.acquireSlab().?;
    const s2 = pool.acquireSlab().?;
    try std.testing.expectEqual(@as(?*Slab, null), pool.acquireSlab());
    try std.testing.expectEqual(@as(u32, 0), pool.availableSlabs());

    // Return one
    _ = s1.release();
    pool.returnSlab(s1);
    try std.testing.expectEqual(@as(u32, 1), pool.availableSlabs());

    // Can acquire again
    const s3 = pool.acquireSlab().?;
    try std.testing.expect(s3 == s1);

    // Cleanup
    _ = s2.release();
    pool.returnSlab(s2);
    _ = s3.release();
    pool.returnSlab(s3);
}

test "SlabPool page warming" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 4,
        .slab_size = 4096,
        .enable_page_warming = true,
    });
    defer pool.deinit(allocator);

    // Memory should be accessible
    const slab = pool.acquireSlab().?;

    const slice = slab.slice();
    slice[0] = 42;
    try std.testing.expectEqual(@as(u8, 42), slice[0]);

    _ = slab.release();
    pool.returnSlab(slab);
}

test "RefSlice clone" {
    const allocator = std.testing.allocator;

    var pool = try SlabPool.init(allocator, .{
        .slab_count = 4,
        .slab_size = 4096,
        .enable_page_warming = false,
    });
    defer pool.deinit(allocator);

    const data = "test data";
    var ref = pool.createRefSlice(data).?;

    var cloned = ref.clone();
    try std.testing.expectEqual(@as(u32, 2), ref.slab.refcount);

    ref.deinit();
    try std.testing.expectEqual(@as(u32, 1), cloned.slab.refcount);

    cloned.deinit();
    try std.testing.expectEqual(@as(u32, 4), pool.availableSlabs());
}
