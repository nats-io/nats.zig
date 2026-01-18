//! Queue Groups - Load-Balanced Workers
//!
//! Demonstrates horizontal scaling with NATS queue groups. Multiple workers
//! subscribe to the same subject with the same queue group name - NATS
//! distributes messages round-robin among them.
//!
//! This example uses io.concurrent() to run workers in parallel threads,
//! pushing results to a shared Io.Queue for the main loop to consume.
//!
//! Run with: zig build run-queue-groups
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Allocator = std.mem.Allocator;

const WorkerResult = struct {
    worker_id: u8,
    data: []const u8,
    msg: nats.Message,

    fn deinit(self: WorkerResult, allocator: Allocator) void {
        self.msg.deinit(allocator);
    }
};

fn workerTask(
    io: Io,
    worker_id: u8,
    sub: *nats.Client.Sub,
    allocator: Allocator,
    queue: *Io.Queue(WorkerResult),
    done: *std.atomic.Value(bool),
) void {
    while (!done.load(.acquire)) {
        const msg = sub.nextWithTimeout(allocator, 100) catch return orelse continue;
        queue.putOne(io, .{
            .worker_id = worker_id,
            .data = msg.data,
            .msg = msg,
        }) catch {
            msg.deinit(allocator);
            return;
        };
    }
}

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
        .{ .name = "queue-groups-example" },
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

    try client.flush(allocator);

    std.debug.print("Created 3 workers in queue group 'workers'\n", .{});

    // Shared queue for results
    var queue_buf: [32]WorkerResult = undefined;
    var queue: Io.Queue(WorkerResult) = .init(&queue_buf);
    var done: std.atomic.Value(bool) = .init(false);

    // Launch workers in TRUE parallel threads (return void, so no catch)
    var w1 = try io.concurrent(workerTask, .{
        io, 1, worker1, allocator, &queue, &done,
    });
    defer w1.cancel(io);

    var w2 = try io.concurrent(workerTask, .{
        io, 2, worker2, allocator, &queue, &done,
    });
    defer w2.cancel(io);

    var w3 = try io.concurrent(workerTask, .{
        io, 3, worker3, allocator, &queue, &done,
    });
    defer w3.cancel(io);

    // Publish messages
    const message_count: u32 = 9;
    std.debug.print("\nPublishing {d} messages...\n\n", .{message_count});

    for (0..message_count) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Task {d}", .{i + 1}) catch "Task";
        try client.publish("tasks", msg);
    }
    try client.flush(allocator);

    // Consume results from queue
    var counts = [3]u32{ 0, 0, 0 };
    var total_received: u32 = 0;

    std.debug.print("Receiving from concurrent workers:\n", .{});

    while (total_received < message_count) {
        const result = queue.getOne(io) catch break;
        defer result.deinit(allocator);
        counts[result.worker_id - 1] += 1;
        total_received += 1;
        std.debug.print(
            "  Worker {d} received: {s}\n",
            .{ result.worker_id, result.data },
        );
    }

    // Signal workers to stop
    done.store(true, .release);

    std.debug.print("\nDistribution summary:\n", .{});
    for (counts, 0..) |count, idx| {
        std.debug.print("  Worker {d}: {d} messages\n", .{ idx + 1, count });
    }

    std.debug.print("\nDone!\n", .{});
}
