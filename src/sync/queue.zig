//! Thread-Safe Message Queue
//!
//! Provides a bounded queue with condition variable for efficient
//! blocking waits with timeout support. Used for per-subscription
//! message delivery in the NATS client.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

/// Thread-safe queue with condition variable for timed waits.
/// Uses a ring buffer internally for bounded, allocation-free operation.
pub fn ThreadSafeQueue(comptime T: type) type {
    return struct {
        mutex: std.Thread.Mutex = .{},
        condition: std.Thread.Condition = .{},
        items: []T,
        head: usize = 0,
        tail: usize = 0,
        count: usize = 0,
        closed: bool = false,

        const Self = @This();

        /// Creates a new thread-safe queue with the given capacity.
        pub fn init(allocator: Allocator, queue_capacity: usize) !Self {
            assert(queue_capacity > 0);
            const items = try allocator.alloc(T, queue_capacity);
            return .{ .items = items };
        }

        /// Frees the queue buffer.
        pub fn deinit(self: *Self, allocator: Allocator) void {
            allocator.free(self.items);
            self.* = undefined;
        }

        /// Pushes an item to the queue and signals waiting threads.
        /// Returns error.QueueFull if the queue is at capacity.
        pub fn push(self: *Self, item: T) error{QueueFull}!void {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (self.count >= self.items.len) return error.QueueFull;

            self.items[self.tail] = item;
            self.tail = (self.tail + 1) % self.items.len;
            self.count += 1;

            self.condition.signal();
        }

        /// Tries to pop an item without waiting.
        /// Returns null if queue is empty.
        pub fn tryPop(self: *Self) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.popLocked();
        }

        /// Waits for an item with optional timeout.
        /// Returns null on timeout or if queue is closed.
        pub fn popOrWait(self: *Self, timeout_ns: ?u64) ?T {
            self.mutex.lock();
            defer self.mutex.unlock();

            while (self.count == 0 and !self.closed) {
                if (timeout_ns) |ns| {
                    self.condition.timedWait(&self.mutex, ns) catch {
                        return null; // Timeout
                    };
                } else {
                    self.condition.wait(&self.mutex);
                }
            }

            return self.popLocked();
        }

        /// Pops item while already holding the lock.
        fn popLocked(self: *Self) ?T {
            if (self.count == 0) return null;

            const item = self.items[self.head];
            self.head = (self.head + 1) % self.items.len;
            self.count -= 1;
            return item;
        }

        /// Closes the queue, waking all waiting threads.
        pub fn close(self: *Self) void {
            self.mutex.lock();
            defer self.mutex.unlock();

            self.closed = true;
            self.condition.broadcast();
        }

        /// Returns current number of items in queue.
        pub fn len(self: *Self) usize {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.count;
        }

        /// Returns true if queue is empty.
        pub fn isEmpty(self: *Self) bool {
            return self.len() == 0;
        }

        /// Returns the queue capacity.
        pub fn capacity(self: *const Self) usize {
            return self.items.len;
        }
    };
}

test "thread safe queue basic" {
    const allocator = std.testing.allocator;

    var queue = try ThreadSafeQueue(u32).init(allocator, 4);
    defer queue.deinit(allocator);

    try queue.push(1);
    try queue.push(2);
    try queue.push(3);

    try std.testing.expectEqual(@as(usize, 3), queue.len());

    try std.testing.expectEqual(@as(?u32, 1), queue.tryPop());
    try std.testing.expectEqual(@as(?u32, 2), queue.tryPop());
    try std.testing.expectEqual(@as(?u32, 3), queue.tryPop());
    try std.testing.expectEqual(@as(?u32, null), queue.tryPop());
}

test "thread safe queue full" {
    const allocator = std.testing.allocator;

    var queue = try ThreadSafeQueue(u32).init(allocator, 2);
    defer queue.deinit(allocator);

    try queue.push(1);
    try queue.push(2);
    try std.testing.expectError(error.QueueFull, queue.push(3));
}

test "thread safe queue timeout" {
    const allocator = std.testing.allocator;

    var queue = try ThreadSafeQueue(u32).init(allocator, 4);
    defer queue.deinit(allocator);

    // Should timeout immediately on empty queue
    const result = queue.popOrWait(1_000_000); // 1ms timeout
    try std.testing.expectEqual(@as(?u32, null), result);
}
