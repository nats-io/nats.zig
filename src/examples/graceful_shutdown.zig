//! Graceful Shutdown Pattern
//!
//! Demonstrates production-ready lifecycle management:
//! - drain() for graceful subscription cleanup
//! - Proper resource cleanup order
//! - Monitoring dropped messages before shutdown
//!
//! Run with: zig build run-graceful-shutdown
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Allocator = std.mem.Allocator;

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
        .{ .name = "graceful-shutdown-example" },
    );
    // Using drain() for graceful cleanup instead of deinit()

    std.debug.print("Connected to NATS!\n", .{});

    // Create multiple subscriptions
    const orders = try client.subscribe(allocator, "orders.*");
    defer orders.deinit(allocator);

    const events = try client.subscribe(allocator, "events.>");
    defer events.deinit(allocator);

    try client.flush(allocator);

    std.debug.print("Subscriptions active:\n", .{});
    std.debug.print("  - orders.* (sid={d})\n", .{orders.sid});
    std.debug.print("  - events.> (sid={d})\n", .{events.sid});

    // Simulate some activity
    std.debug.print("\nSimulating activity...\n", .{});

    for (0..5) |i| {
        var buf: [64]u8 = undefined;
        const order = std.fmt.bufPrint(&buf, "Order #{d}", .{i + 1000}) catch "Order";
        try client.publish("orders.new", order);

        const event = std.fmt.bufPrint(&buf, "Event {d}", .{i + 1}) catch "Event";
        try client.publish("events.user.login", event);
    }
    try client.flush(allocator);

    std.debug.print("Published 10 messages (5 orders, 5 events)\n", .{});

    // Process some messages
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    var orders_count: u32 = 0;
    while (orders.tryNext()) |msg| {
        defer msg.deinit(allocator);
        orders_count += 1;
    }

    var events_count: u32 = 0;
    while (events.tryNext()) |msg| {
        defer msg.deinit(allocator);
        events_count += 1;
    }

    std.debug.print("Processed: {d} orders, {d} events\n", .{ orders_count, events_count });

    // CHECK FOR DROPPED MESSAGES before shutdown
    std.debug.print("\nPre-shutdown health check:\n", .{});

    const orders_dropped = orders.getDroppedCount();
    const events_dropped = events.getDroppedCount();

    if (orders_dropped > 0 or events_dropped > 0) {
        std.debug.print("  WARNING: Messages were dropped!\n", .{});
        std.debug.print("    orders: {d} dropped\n", .{orders_dropped});
        std.debug.print("    events: {d} dropped\n", .{events_dropped});
    } else {
        std.debug.print("  No messages dropped - healthy!\n", .{});
    }

    // GRACEFUL SHUTDOWN with drain()
    std.debug.print("\nInitiating graceful shutdown...\n", .{});

    // drain() does the following:
    // 1. Unsubscribes all active subscriptions
    // 2. Drains any remaining messages from queues (frees memory)
    // 3. Flushes pending writes to server
    // 4. Closes connection and transitions to closed state
    const drain_result = client.drain(allocator) catch |err| {
        std.debug.print("Drain failed: {}\n", .{err});
        client.deinit(allocator);
        return err;
    };

    std.debug.print("Drain completed:\n", .{});
    if (drain_result.unsub_failures > 0) {
        std.debug.print("  WARNING: {d} unsub commands failed\n", .{
            drain_result.unsub_failures,
        });
    } else {
        std.debug.print("  All subscriptions unsubscribed successfully\n", .{});
    }
    if (drain_result.flush_failed) {
        std.debug.print("  WARNING: Final flush failed\n", .{});
    } else {
        std.debug.print("  Final flush succeeded\n", .{});
    }

    // Final stats
    const stats = client.getStats();
    std.debug.print("\nFinal statistics:\n", .{});
    std.debug.print("  Messages sent: {d}\n", .{stats.msgs_out});
    std.debug.print("  Messages received: {d}\n", .{stats.msgs_in});
    std.debug.print("  Bytes sent: {d}\n", .{stats.bytes_out});
    std.debug.print("  Bytes received: {d}\n", .{stats.bytes_in});

    // Now safe to deinit
    client.deinit(allocator);

    std.debug.print("\nGraceful shutdown complete!\n", .{});
}
