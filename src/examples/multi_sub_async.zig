//! Multiple Subscriptions - Async Pattern (io.concurrent + Io.Queue)
//!
//! Demonstrates handling multiple independent subscriptions using true
//! parallelism. Each subscription runs in its own thread, pushing messages
//! to a shared queue for the main loop to consume.
//!
//! For the simpler polling version, see multi_sub.zig.
//!
//! Run with: zig build example-multi-sub-async
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Allocator = std.mem.Allocator;
const Sub = nats.Client.Sub;

const SubResult = struct {
    name: []const u8,
    sub_idx: u8,
    data: []const u8,
    msg: nats.Message,

    fn deinit(self: SubResult, allocator: Allocator) void {
        self.msg.deinit(allocator);
    }
};

fn subWorker(
    io: Io,
    name: []const u8,
    sub_idx: u8,
    sub: *Sub,
    allocator: Allocator,
    queue: *Io.Queue(SubResult),
    done: *std.atomic.Value(bool),
) void {
    while (!done.load(.acquire)) {
        const msg = sub.nextWithTimeout(allocator, 100) catch return orelse continue;
        queue.putOne(io, .{
            .name = name,
            .sub_idx = sub_idx,
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
        .{ .name = "multi-sub-async" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Subscribe to three independent subjects
    const orders = try client.subscribe(allocator, "orders");
    defer orders.deinit(allocator);

    const users = try client.subscribe(allocator, "users");
    defer users.deinit(allocator);

    const system = try client.subscribe(allocator, "system");
    defer system.deinit(allocator);

    try client.flush();

    std.debug.print("Subscribed to: orders, users, system\n\n", .{});

    // Shared queue for results from all workers
    var queue_buf: [32]SubResult = undefined;
    var queue: Io.Queue(SubResult) = .init(&queue_buf);
    var done: std.atomic.Value(bool) = .init(false);

    // Launch workers in parallel threads
    var w_orders = try io.concurrent(subWorker, .{
        io, "orders", 0, orders, allocator, &queue, &done,
    });
    defer w_orders.cancel(io);

    var w_users = try io.concurrent(subWorker, .{
        io, "users", 1, users, allocator, &queue, &done,
    });
    defer w_users.cancel(io);

    var w_system = try io.concurrent(subWorker, .{
        io, "system", 2, system, allocator, &queue, &done,
    });
    defer w_system.cancel(io);

    // Publish test messages to each subject
    try client.publish("orders", "Order #1001 created");
    try client.publish("users", "User alice logged in");
    try client.publish("system", "CPU usage: 45%");
    try client.publish("orders", "Order #1002 shipped");
    try client.publish("users", "User bob registered");
    try client.publish("system", "Memory: 2.1GB");
    try client.flush();

    std.debug.print("Published 6 messages (2 per subject)\n\n", .{});

    // Consume results from queue
    var counts = [3]u32{ 0, 0, 0 };
    var total: u32 = 0;

    std.debug.print("Receiving from concurrent workers:\n", .{});

    while (total < 6) {
        const result = queue.getOne(io) catch break;
        defer result.deinit(allocator);
        counts[result.sub_idx] += 1;
        total += 1;
        std.debug.print("  [{s}] {s}\n", .{ result.name, result.data });
    }

    // Signal workers to stop
    done.store(true, .release);

    std.debug.print("\nReceived: orders={d}, users={d}, system={d}\n", .{
        counts[0],
        counts[1],
        counts[2],
    });
    std.debug.print("Done!\n", .{});
}
