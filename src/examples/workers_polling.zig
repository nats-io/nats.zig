//! Queue Groups - Polling Loop Pattern
//!
//! Production-style polling loop that checks workers round-robin.
//! Each worker gets a short timeout before moving to the next.
//! Run with: zig build example-workers-polling
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "workers-polling" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Create 3 workers in queue group
    const worker1 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker1.deinit(allocator);

    const worker2 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker2.deinit(allocator);

    const worker3 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker3.deinit(allocator);

    try client.flush();

    std.debug.print("Created 3 workers in queue group 'workers'\n", .{});

    // Publish messages
    const message_count = 9;
    std.debug.print("\nPublishing {d} messages...\n", .{message_count});

    for (0..message_count) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Task {d}", .{i + 1}) catch "Task";
        try client.publish("tasks", msg);
    }
    try client.flush();

    // Wait for messages to be distributed
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    // Production-style polling loop (round-robin)
    const workers = [_]*nats.Client.Sub{ worker1, worker2, worker3 };
    var counts = [3]u32{ 0, 0, 0 };
    var total_received: u32 = 0;
    var worker_idx: usize = 0;

    std.debug.print("\nPolling loop (round-robin with 20ms timeout):\n", .{});

    while (total_received < message_count) {
        const worker = workers[worker_idx];

        // Short timeout - check this worker, then move to next
        if (worker.nextWithTimeout(allocator, 20) catch null) |msg| {
            defer msg.deinit(allocator);
            counts[worker_idx] += 1;
            total_received += 1;
            std.debug.print(
                "  Worker {d} received: {s}\n",
                .{ worker_idx + 1, msg.data },
            );
        }

        // Round-robin to next worker
        worker_idx = (worker_idx + 1) % 3;
    }

    std.debug.print("\nDistribution summary:\n", .{});
    for (counts, 0..) |count, idx| {
        std.debug.print("  Worker {d}: {d} messages\n", .{ idx + 1, count });
    }

    std.debug.print("\nDone!\n", .{});
}
