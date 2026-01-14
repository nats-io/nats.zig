//! Multiple Subscriptions - Polling Pattern
//!
//! Demonstrates handling multiple independent subscriptions using round-robin
//! polling. Simple and reliable - no message loss.
//!
//! For the async version using io.concurrent(), see multi_sub_async.zig.
//!
//! Run with: zig build example-multi-sub
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
        .{ .name = "multi-sub-polling" },
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

    // Publish test messages to each subject
    try client.publish("orders", "Order #1001 created");
    try client.publish("users", "User alice logged in");
    try client.publish("system", "CPU usage: 45%");
    try client.publish("orders", "Order #1002 shipped");
    try client.publish("users", "User bob registered");
    try client.publish("system", "Memory: 2.1GB");
    try client.flush();

    std.debug.print("Published 6 messages (2 per subject)\n\n", .{});

    // Poll all subscriptions round-robin
    const subs = [_]*nats.Client.Sub{ orders, users, system };
    const names = [_][]const u8{ "orders", "users", "system" };
    var counts = [3]u32{ 0, 0, 0 };
    var total: u32 = 0;
    var idx: usize = 0;

    std.debug.print("Polling subscriptions (round-robin):\n", .{});

    while (total < 6) {
        if (subs[idx].nextWithTimeout(allocator, 20) catch null) |msg| {
            defer msg.deinit(allocator);
            counts[idx] += 1;
            total += 1;
            std.debug.print("  [{s}] {s}\n", .{ names[idx], msg.data });
        }
        idx = (idx + 1) % 3;
    }

    std.debug.print("\nReceived: orders={d}, users={d}, system={d}\n", .{
        counts[0],
        counts[1],
        counts[2],
    });
    std.debug.print("Done!\n", .{});
}
