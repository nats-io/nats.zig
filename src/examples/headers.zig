//! NATS Headers
//!
//! Demonstrates publishing messages with headers and parsing
//! received header metadata.
//!
//! Run with: zig build run-headers
//!   or:    zig build run-headers -Dio_backend=evented
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");
const io_backend = @import("io_backend");

const headers = nats.protocol.headers;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var backend: io_backend.Backend = undefined;
    try io_backend.init(&backend, allocator);
    defer backend.deinit();
    const io = backend.io();

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "headers-example" },
    );
    defer client.deinit();

    const sub = try client.subscribeSync("headers.demo");
    defer sub.deinit();

    try client.flush(std.time.ns_per_s);

    const hdrs = [_]headers.Entry{
        .{ .key = "Content-Type", .value = "application/json" },
        .{ .key = "X-Request-Id", .value = "req-42" },
        .{ .key = "X-Trace", .value = "alpha" },
        .{ .key = "X-Trace", .value = "beta" },
    };

    try client.publishWithHeaders(
        "headers.demo",
        &hdrs,
        "{\"message\":\"hello\"}",
    );

    if (try sub.nextMsgTimeout(1000)) |msg| {
        defer msg.deinit();

        std.debug.print("Received payload: {s}\n", .{msg.data});

        const raw = msg.headers orelse return error.MissingHeaders;
        var parsed = headers.parse(allocator, raw);
        defer parsed.deinit();

        if (parsed.err) |err| {
            std.debug.print("Header parse error: {}\n", .{err});
            return error.InvalidHeaders;
        }

        if (parsed.get("content-type")) |content_type| {
            std.debug.print("Content-Type: {s}\n", .{content_type});
        }

        std.debug.print("Headers:\n", .{});
        for (parsed.items()) |entry| {
            std.debug.print("  {s}: {s}\n", .{
                entry.key,
                entry.value,
            });
        }
    } else {
        std.debug.print("Timed out waiting for message\n", .{});
    }
}
