//! Queue Groups (Load-Balanced Workers)
//!
//! Demonstrates horizontal scaling with queue groups. Multiple subscribers
//! to the same subject with the same queue group name share the message load.
//! NATS distributes messages round-robin among the workers.
//! Run with: zig build run-workers
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

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
        .{ .name = "workers-example" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Create 3 workers - all subscribing to same subject with same queue group
    // NATS will distribute messages among them
    const worker1 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker1.deinit(allocator);

    const worker2 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker2.deinit(allocator);

    const worker3 = try client.subscribeQueue(allocator, "tasks", "workers");
    defer worker3.deinit(allocator);

    try client.flush();

    std.debug.print(
        "Created 3 workers (sids: {d}, {d}, {d}) in queue group 'workers'\n",
        .{ worker1.sid, worker2.sid, worker3.sid },
    );

    // Publish 9 messages - each worker should get approximately 3
    const message_count = 9;
    std.debug.print("\nPublishing {d} messages to 'tasks'...\n", .{message_count});

    for (0..message_count) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Task {d}", .{i + 1}) catch "Task";
        try client.publish("tasks", msg);
    }
    try client.flush();

    // Count how many messages each worker receives
    var counts = [3]u32{ 0, 0, 0 };
    const workers = [_]*nats.Subscription{ worker1, worker2, worker3 };

    std.debug.print("\nProcessing messages:\n", .{});

    // Poll each worker for messages
    var total_received: u32 = 0;
    while (total_received < message_count) {
        var received_any = false;

        for (workers, 0..) |worker, idx| {
            if (worker.tryNext()) |msg| {
                defer msg.deinit(allocator);
                counts[idx] += 1;
                total_received += 1;
                received_any = true;
                std.debug.print(
                    "  Worker {d} received: {s}\n",
                    .{ idx + 1, msg.data },
                );
            }
        }

        if (!received_any) {
            // Brief wait for messages to arrive
            io.sleep(.fromMilliseconds(10), .awake) catch {};
        }
    }

    std.debug.print("\nDistribution summary:\n", .{});
    for (counts, 0..) |count, idx| {
        std.debug.print("  Worker {d}: {d} messages\n", .{ idx + 1, count });
    }

    std.debug.print("\nDone! (Messages were distributed among workers)\n", .{});
}
