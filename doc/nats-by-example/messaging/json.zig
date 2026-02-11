//! JSON for Message Payloads
//!
//! NATS message payloads are opaque byte sequences - the application
//! decides how to serialize and deserialize them. JSON is a common
//! choice for its cross-language compatibility and readability.
//!
//! This example demonstrates:
//! - Defining a struct type for the message payload
//! - Serializing a struct to JSON using Stringify.value
//! - Receiving and deserializing JSON back to a struct
//! - Gracefully handling invalid JSON payloads
//!
//! Based on: https://natsbyexample.com/examples/messaging/json/go
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server
//!
//! Run with: zig build run-nbe-messaging-json

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

/// Application payload type. Zig's std.json will serialize
/// field names directly ("foo", "bar").
const Payload = struct {
    foo: []const u8,
    bar: i32,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var threaded: Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(
        io,
        &stdout_buf,
    );
    const stdout = &stdout_writer.interface;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit(allocator);

    const sub = try client.subscribe(allocator, "greet");
    defer sub.deinit(allocator);

    // Create a payload and serialize it to JSON.
    // Stringify.value writes JSON into a fixed buffer writer -
    // no heap allocation needed.
    const payload = Payload{ .foo = "bar", .bar = 27 };
    var json_buf: [256]u8 = undefined;
    var json_writer = Io.Writer.fixed(&json_buf);
    try std.json.Stringify.value(
        payload,
        .{},
        &json_writer,
    );
    const json = json_writer.buffered();

    // Publish the valid JSON payload
    try client.publish("greet", json);

    // Publish an invalid (non-JSON) payload
    try client.publish("greet", "not json");

    // Receive the first message - valid JSON.
    // parseFromSlice deserializes it back into a Payload struct.
    if (try sub.nextWithTimeout(allocator, 1000)) |msg| {
        defer msg.deinit(allocator);
        if (std.json.parseFromSlice(
            Payload,
            allocator,
            msg.data,
            .{},
        )) |parsed| {
            defer parsed.deinit();
            try stdout.print(
                "received valid payload: " ++
                    "foo={s}, bar={d}\n",
                .{ parsed.value.foo, parsed.value.bar },
            );
        } else |_| {
            try stdout.print(
                "received invalid payload: {s}\n",
                .{msg.data},
            );
        }
    }

    // Receive the second message - invalid JSON.
    // parseFromSlice returns an error, so we print raw data.
    if (try sub.nextWithTimeout(allocator, 1000)) |msg| {
        defer msg.deinit(allocator);
        if (std.json.parseFromSlice(
            Payload,
            allocator,
            msg.data,
            .{},
        )) |parsed| {
            defer parsed.deinit();
            try stdout.print(
                "received valid payload: " ++
                    "foo={s}, bar={d}\n",
                .{ parsed.value.foo, parsed.value.bar },
            );
        } else |_| {
            try stdout.print(
                "received invalid payload: {s}\n",
                .{msg.data},
            );
        }
    }

    try stdout.flush();
}
