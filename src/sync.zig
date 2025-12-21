//! Synchronization Primitives
//!
//! Thread-safe data structures for concurrent NATS client operations.

pub const queue = @import("sync/queue.zig");
pub const ThreadSafeQueue = queue.ThreadSafeQueue;
