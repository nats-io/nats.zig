//! Lightweight atomic spinlock for short critical sections.
//!
//! Used where Io.Mutex is unavailable (e.g., Message.deinit()
//! which has no io parameter). Lock hold time must be very
//! short (nanoseconds) — only for single slot writes.

const std = @import("std");
const assert = std.debug.assert;

/// Atomic spinlock using compare-and-swap.
pub const SpinLock = struct {
    locked: std.atomic.Value(u8) =
        std.atomic.Value(u8).init(0),

    /// Acquire the lock. Spins until successful.
    pub fn lock(self: *SpinLock) void {
        while (self.locked.cmpxchgWeak(
            0,
            1,
            .acquire,
            .monotonic,
        ) != null) {
            std.atomic.spinLoopHint();
        }
    }

    /// Release the lock.
    pub fn unlock(self: *SpinLock) void {
        assert(self.locked.load(.monotonic) == 1);
        self.locked.store(0, .release);
    }
};

test "SpinLock basic" {
    var sl: SpinLock = .{};
    sl.lock();
    sl.unlock();
}

test "SpinLock concurrent" {
    const NUM_THREADS = 4;
    const ITERS = 100_000;

    var sl: SpinLock = .{};
    var counter: usize = 0;

    const threads = blk: {
        var t: [NUM_THREADS]std.Thread = undefined;
        for (&t) |*thread| {
            thread.* = try std.Thread.spawn(.{}, struct {
                fn run(
                    s: *SpinLock,
                    c: *usize,
                ) void {
                    for (0..ITERS) |_| {
                        s.lock();
                        c.* += 1;
                        s.unlock();
                    }
                }
            }.run, .{ &sl, &counter });
        }
        break :blk t;
    };

    for (&threads) |*t| t.join();

    try std.testing.expectEqual(
        NUM_THREADS * ITERS,
        counter,
    );
}
