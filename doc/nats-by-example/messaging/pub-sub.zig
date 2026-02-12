//! Publish-Subscribe
//!
//! This example demonstrates the core NATS publish-subscribe pattern.
//! Pub/Sub is the fundamental messaging pattern in NATS where publishers
//! send messages to subjects and subscribers receive them.
//!
//! Key concepts shown:
//! - At-most-once delivery: if no subscriber is listening, messages
//!   are silently discarded (like UDP, or MQTT QoS 0)
//! - Wildcard subscriptions: "greet.*" matches "greet.joe",
//!   "greet.pam", etc.
//! - Subject-based routing: messages are routed by their subject
//!
//! Based on: https://natsbyexample.com/examples/messaging/pub-sub/go
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server
//!
//! Run with: zig build run-nbe-messaging-pub-sub

const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const io = init.io;

    // Set up buffered stdout writer for output
    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(
        io,
        &stdout_buf,
    );
    const stdout = &stdout_writer.interface;

    // Connect to NATS server
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit();

    // Publish a message BEFORE subscribing.
    // This message will be lost because NATS provides
    // at-most-once delivery - there are no subscribers
    // listening on this subject yet.
    try client.publish("greet.joe", "hello");

    // Subscribe using a wildcard subject. "greet.*" will
    // match any subject with exactly one token after "greet.",
    // for example: "greet.joe", "greet.pam", "greet.bob"
    const sub = try client.subscribeSync("greet.*");
    defer sub.deinit();

    // Try to receive the message published before subscribing.
    // The short timeout (10ms) confirms no message is available -
    // it was published before our subscription existed.
    const msg = try sub.nextWithTimeout(10);
    try stdout.print("subscribed after a publish...\n", .{});
    try stdout.print("msg is null? {}\n", .{msg == null});
    try stdout.flush();

    // Now publish two messages AFTER subscribing.
    // These will be received because the subscription is active.
    try client.publish("greet.joe", "hello");
    try client.publish("greet.pam", "hello");

    // Receive both messages. The wildcard subscription
    // matches both "greet.joe" and "greet.pam".
    if (try sub.nextWithTimeout(1000)) |m| {
        defer m.deinit();
        try stdout.print(
            "msg data: \"{s}\" on subject \"{s}\"\n",
            .{ m.data, m.subject },
        );
    }
    if (try sub.nextWithTimeout(1000)) |m| {
        defer m.deinit();
        try stdout.print(
            "msg data: \"{s}\" on subject \"{s}\"\n",
            .{ m.data, m.subject },
        );
    }

    // Publish one more to a different subject that still
    // matches our wildcard pattern.
    try client.publish("greet.bob", "hello");

    if (try sub.nextWithTimeout(1000)) |m| {
        defer m.deinit();
        try stdout.print(
            "msg data: \"{s}\" on subject \"{s}\"\n",
            .{ m.data, m.subject },
        );
    }

    try stdout.flush();
}
