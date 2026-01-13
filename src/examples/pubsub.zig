//! Pub/Sub Example
//!
//! Demonstrates publishing and subscribing to NATS subjects.
//! Run with: zig build run-pubsub
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");

const nats = @import("nats");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create I/O system
    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("Connecting to NATS...\n", .{});

    // Connect to NATS
    const url = "nats://localhost:4222";
    const client = nats.Client.connect(allocator, io, url, .{
        .name = "zig-pubsub-example",
    }) catch |err| {
        std.debug.print("Connection failed: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    std.debug.print("Connected!\n", .{});

    // Print server info
    if (client.getServerInfo()) |info| {
        const has_name = info.server_name.len > 0;
        const name = if (has_name) info.server_name else "unknown";
        const ver = if (info.version.len > 0) info.version else "unknown";
        std.debug.print("Server: {s} v{s}\n", .{ name, ver });
    }

    // Subscribe to a subject - returns *Subscription for Go-style polling
    const sub = try client.subscribe(allocator, "demo.>");
    defer sub.deinit(allocator);
    std.debug.print("Subscribed to 'demo.>' with sid={d}\n", .{sub.sid});

    // Flush to ensure subscription is active
    try client.flush();

    // Publish some messages
    try client.publish("demo.hello", "Hello from Zig!");
    std.debug.print("Published to 'demo.hello'\n", .{});

    try client.publish("demo.world", "World message");
    std.debug.print("Published to 'demo.world'\n", .{});

    try client.publish("demo.test.nested", "Nested subject");
    std.debug.print("Published to 'demo.test.nested'\n", .{});

    // Flush all messages
    try client.flush();

    // Receive messages with timeout
    std.debug.print("\nReceiving messages...\n", .{});
    var count: u32 = 0;
    while (count < 3) {
        if (try sub.nextWithTimeout(allocator, 1000)) |msg| {
            defer msg.deinit(allocator);
            std.debug.print("  [{s}] {s}\n", .{ msg.subject, msg.data });
            count += 1;
        } else {
            std.debug.print("  Timeout\n", .{});
            break;
        }
    }

    // Unsubscribe
    try sub.unsubscribe();
    try client.flush();
    std.debug.print("Unsubscribed from sid={d}\n", .{sub.sid});

    std.debug.print("\nDone!\n", .{});
}
