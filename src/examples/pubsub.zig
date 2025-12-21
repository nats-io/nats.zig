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
    var threaded = std.Io.Threaded.init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("Connecting to NATS...\n", .{});

    // Connect to NATS (pass io to client)
    var client = nats.Client.connect(allocator, io, "nats://localhost:4222", .{
        .name = "zig-pubsub-example",
    }) catch |err| {
        std.debug.print("Connection failed: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    std.debug.print("Connected!\n", .{});

    // Print server info
    if (client.getServerInfo()) |info| {
        const name = if (info.server_name.len > 0) info.server_name else "unknown";
        const ver = if (info.version.len > 0) info.version else "unknown";
        std.debug.print("Server: {s} v{s}\n", .{ name, ver });
    }

    // Subscribe to a subject
    const sid = try client.subscribe(allocator, "demo.>");
    std.debug.print("Subscribed to 'demo.>' with sid={d}\n", .{sid});

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

    std.debug.print("\nMessages published! Check with:\n", .{});
    std.debug.print("  nats sub 'demo.>'\n", .{});

    // Ping server
    try client.ping();
    try client.flush();
    std.debug.print("\nPING sent\n", .{});

    // Unsubscribe
    try client.unsubscribe(allocator, sid);
    try client.flush();
    std.debug.print("Unsubscribed from sid={d}\n", .{sid});

    std.debug.print("\nDone!\n", .{});
}
