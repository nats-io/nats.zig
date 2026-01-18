//! Non-Blocking Polling Pattern
//!
//! Demonstrates non-blocking message processing with tryNext():
//! - Event loop integration (check messages, do other work)
//! - Multiple subscriptions with round-robin polling
//! - Mixed workloads (NATS + other tasks)
//!
//! Use this pattern when you need to:
//! - Integrate NATS into an existing event loop
//! - Handle multiple subscriptions without threads
//! - Do other work between message processing
//!
//! Run with: zig build run-polling-loop
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

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
        .{ .name = "polling-loop-example" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Subscribe to multiple subjects
    const high_priority = try client.subscribe(allocator, "priority.high");
    defer high_priority.deinit(allocator);

    const normal = try client.subscribe(allocator, "priority.normal");
    defer normal.deinit(allocator);

    const low_priority = try client.subscribe(allocator, "priority.low");
    defer low_priority.deinit(allocator);

    try client.flush(allocator);

    std.debug.print("Subscribed to: priority.high, priority.normal, priority.low\n\n", .{});

    // Publish test messages with different priorities
    try client.publish("priority.high", "URGENT: System alert!");
    try client.publish("priority.normal", "Info: User logged in");
    try client.publish("priority.low", "Debug: Cache refreshed");
    try client.publish("priority.high", "URGENT: Disk space low!");
    try client.publish("priority.normal", "Info: Report generated");
    try client.publish("priority.low", "Debug: Metrics collected");
    try client.flush(allocator);

    std.debug.print("Published 6 messages (2 high, 2 normal, 2 low)\n\n", .{});

    // Let messages arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // PRIORITY POLLING: Check high priority first, then others
    std.debug.print("Priority polling (high -> normal -> low):\n", .{});

    var high_count: u32 = 0;
    var normal_count: u32 = 0;
    var low_count: u32 = 0;
    var iterations: u32 = 0;
    const max_iterations: u32 = 20;

    while (iterations < max_iterations) : (iterations += 1) {
        var processed_any = false;

        // Always check high priority first (drain completely)
        while (high_priority.tryNext()) |msg| {
            defer msg.deinit(allocator);
            high_count += 1;
            processed_any = true;
            std.debug.print("  [HIGH] {s}\n", .{msg.data});
        }

        // Then check normal priority (one at a time)
        if (normal.tryNext()) |msg| {
            defer msg.deinit(allocator);
            normal_count += 1;
            processed_any = true;
            std.debug.print("  [NORMAL] {s}\n", .{msg.data});
        }

        // Finally check low priority (one at a time)
        if (low_priority.tryNext()) |msg| {
            defer msg.deinit(allocator);
            low_count += 1;
            processed_any = true;
            std.debug.print("  [LOW] {s}\n", .{msg.data});
        }

        // Do other work if no messages
        if (!processed_any) {
            // In a real app, this is where you'd do other event loop work
            if (iterations < 5) {
                std.debug.print("  (no messages - doing other work...)\n", .{});
            }
            io.sleep(.fromMilliseconds(10), .awake) catch {};
        }

        // Exit when all messages processed
        if (high_count >= 2 and normal_count >= 2 and low_count >= 2) {
            break;
        }
    }

    std.debug.print("\nProcessed: {d} high, {d} normal, {d} low\n", .{
        high_count,
        normal_count,
        low_count,
    });

    // ROUND-ROBIN POLLING: Fair scheduling across subscriptions
    std.debug.print("\n--- Round-Robin Polling Demo ---\n\n", .{});

    // Publish more messages
    for (0..3) |i| {
        var buf: [64]u8 = undefined;
        const high_msg = std.fmt.bufPrint(&buf, "High {d}", .{i + 1}) catch "High";
        try client.publish("priority.high", high_msg);

        const norm_msg = std.fmt.bufPrint(&buf, "Normal {d}", .{i + 1}) catch "Normal";
        try client.publish("priority.normal", norm_msg);

        const low_msg = std.fmt.bufPrint(&buf, "Low {d}", .{i + 1}) catch "Low";
        try client.publish("priority.low", low_msg);
    }
    try client.flush(allocator);
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    std.debug.print("Published 9 more messages (3 each)\n", .{});
    std.debug.print("Round-robin processing:\n", .{});

    const subs = [_]*nats.Client.Sub{ high_priority, normal, low_priority };
    const names = [_][]const u8{ "HIGH", "NORMAL", "LOW" };
    var idx: usize = 0;
    var total: u32 = 0;

    while (total < 9) {
        if (subs[idx].tryNext()) |msg| {
            defer msg.deinit(allocator);
            total += 1;
            std.debug.print("  [{s}] {s}\n", .{ names[idx], msg.data });
        }
        idx = (idx + 1) % 3; // Round-robin to next subscription

        // Safety: prevent infinite loop if messages don't arrive
        if (idx == 0) {
            io.sleep(.fromMilliseconds(10), .awake) catch {};
        }
    }

    std.debug.print("\nTotal processed: {d}\n", .{total});
    std.debug.print("\nDone!\n", .{});
}
