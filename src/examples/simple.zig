//! Simple NATS Example
//!
//! Minimal "hello world" - connect, subscribe, publish, receive one message.
//! A starting point for learning the NATS Zig client.
//! Run with: zig build run-simple
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const nats = @import("nats");

/// Main entry point using Zig 0.16's std.process.Init.
/// Init provides: gpa (allocator), io (async I/O), arena, args, environ.
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create async I/O runtime (same pattern as bench tools)
    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Connect to NATS server
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Subscribe to a subject
    const sub = try client.subscribe(allocator, "hello");
    defer sub.deinit(allocator);

    // Publish a message
    try client.publish("hello", "Hello, NATS!");
    try client.flush(allocator);

    // Receive the message
    if (try sub.nextWithTimeout(allocator, 1000)) |msg| {
        defer msg.deinit(allocator);
        std.debug.print("Received: {s}\n", .{msg.data});
    }

    std.debug.print("Done!\n", .{});
}
