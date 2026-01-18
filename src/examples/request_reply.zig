//! Request/Reply Pattern
//!
//! Demonstrates RPC-style request/reply communication.
//! Run with: zig build example-request-reply
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Service client
    const service_client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "service" },
    );
    defer service_client.deinit(allocator);

    // Requester client
    const requester = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "requester" },
    );
    defer requester.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Service subscribes to handle requests
    const service = try service_client.subscribe(allocator, "math.double");
    defer service.deinit(allocator);
    try service_client.flush(allocator);

    std.debug.print("Service listening on 'math.double'\n", .{});

    // Run service handler in background (returns void, so no catch)
    var service_future = io.async(handleService, .{
        service_client,
        service,
        allocator,
    });
    defer service_future.cancel(io);

    // Wait for service subscription to be ready
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Send request using client.request() - handles inbox automatically
    std.debug.print("\nRequester: What is 21 * 2?\n", .{});

    if (try requester.request(allocator, "math.double", "21", 1000)) |reply| {
        defer reply.deinit(allocator);
        std.debug.print("Reply: {s}\n", .{reply.data});
    } else {
        std.debug.print("Request timed out\n", .{});
    }

    std.debug.print("\nDone!\n", .{});
}

fn handleService(
    client: *nats.Client,
    service: *nats.Client.Sub,
    allocator: std.mem.Allocator,
) void {
    const req = service.nextWithTimeout(allocator, 2000) catch return;
    if (req) |r| {
        defer r.deinit(allocator);

        const num = std.fmt.parseInt(i32, r.data, 10) catch 0;
        var buf: [32]u8 = undefined;
        const result = std.fmt.bufPrint(&buf, "{d}", .{num * 2}) catch "error";

        std.debug.print("Service: {d} * 2 = {s}\n", .{ num, result });

        if (r.reply_to) |reply_to| {
            client.publish(reply_to, result) catch {};
            client.flush(allocator) catch {};
        }
    }
}
