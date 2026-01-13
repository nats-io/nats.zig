//! Request/Reply Pattern
//!
//! Demonstrates the request/reply pattern for RPC-style communication.
//! A single process acts as both the service (responder) and client (requester).
//! Run with: zig build run-request-reply
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const nats = @import("nats");

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
        .{ .name = "request-reply-example" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    // Subscribe to the service subject - this is our "responder"
    const service_sub = try client.subscribe(allocator, "service.echo");
    defer service_sub.deinit(allocator);
    try client.flush();

    std.debug.print("Service listening on 'service.echo'\n", .{});

    // Send a request using client.request() - handles inbox creation
    std.debug.print("\nSending request: 'Hello, Service!'\n", .{});

    // First, check for the incoming request on service subscription
    // Then respond to it before the request() timeout expires
    //
    // In real applications, the service would run in a separate process.
    // Here we interleave to demonstrate both sides in one example.

    // Publish request manually so we can respond before timeout
    const inbox = try nats.newInbox(allocator, io);
    defer allocator.free(inbox);

    const reply_sub = try client.subscribe(allocator, inbox);
    defer reply_sub.deinit(allocator);

    try client.publishRequest("service.echo", inbox, "Hello!");
    try client.flush();

    // Service receives the request
    if (try service_sub.nextWithTimeout(allocator, 1000)) |request| {
        defer request.deinit(allocator);
        std.debug.print(
            "Service received: '{s}' (reply-to: {s})\n",
            .{ request.data, request.reply_to orelse "none" },
        );

        // Service sends reply to the inbox
        if (request.reply_to) |reply_to| {
            try client.publish(reply_to, "Echo: Hello!");
            try client.flush();
            std.debug.print("Service sent reply\n", .{});
        }
    }

    // Client receives the reply
    if (try reply_sub.nextWithTimeout(allocator, 1000)) |reply| {
        defer reply.deinit(allocator);
        std.debug.print("Client received reply: '{s}'\n", .{reply.data});
    }

    // Demonstrate the convenience method client.request()
    std.debug.print("\n--- Using client.request() convenience method ---\n", .{});
    std.debug.print("(This will timeout since no service is responding)\n", .{});

    if (try client.request(allocator, "service.other", "ping", 500)) |reply| {
        defer reply.deinit(allocator);
        std.debug.print("Reply: {s}\n", .{reply.data});
    } else {
        std.debug.print("Request timed out (expected)\n", .{});
    }

    std.debug.print("\nDone!\n", .{});
}
