//! Simple NATS Example
//!
//! Minimal "hello world" - connect, subscribe, publish, receive one message.
//! A starting point for learning the NATS Zig client.
//! Run with: zig build run-simple
//!   or:    zig build run-simple -Dio_backend=evented
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const nats = @import("nats");
const io_backend = @import("io_backend");

/// Main entry point using Zig 0.16's std.process.Init.
/// Init provides: gpa (allocator), io (async I/O), arena, args, environ.
///
/// IMPORTANT: each Client needs its own Io. We create the backend
/// here next to the Client so it owns its own execution context.
/// We deliberately do NOT reuse `init.io` so the build option
/// `-Dio_backend=...` can pick between Threaded and Evented.
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var backend: io_backend.Backend = undefined;
    try io_backend.init(&backend, allocator);
    defer backend.deinit();
    const io = backend.io();

    // Connect to NATS server
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    // Subscribe to a subject
    const sub = try client.subscribeSync("hello");
    defer sub.deinit();

    // Publish a message
    try client.publish("hello", "Hello, NATS!");

    // Receive the message
    if (try sub.nextMsgTimeout(1000)) |msg| {
        defer msg.deinit();
        std.debug.print("Received: {s}\n", .{msg.data});
    }

    std.debug.print("Done!\n", .{});
}
