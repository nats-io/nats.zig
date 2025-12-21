//! Memory Management for Zero-Allocation Message Paths
//!
//! Provides slab pool for owned messages, scratchpads for spanning,
//! and SidMap for O(1) subscription routing.
//! Single-threaded design: non-atomic refcounts (faster than Rust).

pub const slab = @import("memory/slab.zig");
pub const scratch = @import("memory/scratch.zig");
pub const sidmap = @import("memory/sidmap.zig");

pub const Slab = slab.Slab;
pub const SlabPool = slab.SlabPool;
pub const RefSlice = slab.RefSlice;
pub const Scratchpads = scratch.Scratchpads;
pub const copyMessage = scratch.copyMessage;
pub const CopyResult = scratch.CopyResult;
pub const SidMap = sidmap.SidMap;

test {
    _ = slab;
    _ = scratch;
    _ = sidmap;
}
