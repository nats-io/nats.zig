//! Memory Management
//!
//! Provides SidMap for O(1) subscription routing.

pub const sidmap = @import("memory/sidmap.zig");
pub const SidMap = sidmap.SidMap;

test {
    _ = sidmap;
}
