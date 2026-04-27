//! Memory Management
//!
//! Provides SidMap for O(1) subscription routing and TieredSlab for
//! high-performance message buffer allocation.

pub const sidmap = @import("memory/sidmap.zig");
pub const SidMap = sidmap.SidMap;

pub const slab = @import("memory/slab.zig");
pub const TieredSlab = slab.TieredSlab;
pub const SlabConfig = slab.Config;

test {
    _ = sidmap;
    _ = slab;
}
