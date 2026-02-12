//! Request/Reply Pattern
//!
//! Demonstrates RPC-style request/reply communication.
//! Run with: zig build example-request-reply
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Service client
    const service_client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "service" },
    );
    defer service_client.deinit();

    // Requester client
    const requester = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "requester" },
    );
    defer requester.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    // Service subscribes to handle requests
    const service = try service_client.subscribe("math.double");
    defer service.deinit();

    std.debug.print("Service listening on 'math.double'\n", .{});

    // Run service handler in background (returns void, so no catch)
    var service_future = io.async(handleService, .{
        service_client,
        service,
    });
    defer service_future.cancel(io);

    // Wait for service subscription to be ready
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Send request using client.request() - handles inbox automatically
    std.debug.print("\nRequester: What is 21 * 2?\n", .{});

    if (try requester.request("math.double", "21", 1000)) |reply| {
        defer reply.deinit();
        std.debug.print("Reply: {s}\n", .{reply.data});
    } else {
        std.debug.print("Request timed out\n", .{});
    }

    std.debug.print("\nDone!\n", .{});
}

fn handleService(
    client: *nats.Client,
    service: *nats.Client.Sub,
) void {
    const req = service.nextWithTimeout(2000) catch return;
    if (req) |r| {
        defer r.deinit();

        const num = std.fmt.parseInt(i32, r.data, 10) catch 0;
        var buf: [32]u8 = undefined;
        const result = std.fmt.bufPrint(&buf, "{d}", .{num * 2}) catch "error";

        std.debug.print("Service: {d} * 2 = {s}\n", .{ num, result });

        if (r.reply_to) |reply_to| {
            client.publish(reply_to, result) catch {};
        }
    }
}
