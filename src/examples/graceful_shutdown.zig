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

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "graceful-shutdown-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    // Create multiple subscriptions
    const orders = try client.subscribeSync("orders.*");
    defer orders.deinit();

    const events = try client.subscribeSync("events.>");
    defer events.deinit();

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

    std.debug.print("Published 10 messages (5 orders, 5 events)\n", .{});

    // Flush to ensure messages have been delivered
    try client.flush(1_000_000_000);

    var orders_count: u32 = 0;
    while (orders.tryNextMsg()) |msg| {
        defer msg.deinit();
        orders_count += 1;
    }

    var events_count: u32 = 0;
    while (events.tryNextMsg()) |msg| {
        defer msg.deinit();
        events_count += 1;
    }

    std.debug.print(
        "Processed: {d} orders, {d} events\n",
        .{ orders_count, events_count },
    );

    // CHECK FOR DROPPED MESSAGES before shutdown
    std.debug.print("\nPre-shutdown health check:\n", .{});

    const orders_dropped = orders.dropped();
    const events_dropped = events.dropped();

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
    const drain_result = client.drain() catch |err| {
        std.debug.print("Drain failed: {}\n", .{err});
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
    const stats = client.stats();
    std.debug.print("\nFinal statistics:\n", .{});
    std.debug.print("  Messages sent: {d}\n", .{stats.msgs_out});
    std.debug.print("  Messages received: {d}\n", .{stats.msgs_in});
    std.debug.print("  Bytes sent: {d}\n", .{stats.bytes_out});
    std.debug.print("  Bytes received: {d}\n", .{stats.bytes_in});

    std.debug.print("\nGraceful shutdown complete!\n", .{});
}
