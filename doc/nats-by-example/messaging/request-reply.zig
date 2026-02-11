//! Request-Reply
//!
//! The request-reply pattern allows a client to send a request and
//! wait for a response. Under the hood, NATS implements this as an
//! optimized pair of publish-subscribe operations using an auto-
//! generated inbox subject for the reply.
//!
//! Key concepts shown:
//! - Subscribing to handle requests in a background task
//! - Extracting info from the subject (e.g. a name)
//! - Responding to requests with msg.respond()
//! - Detecting "no responders" when no handler is available
//!
//! Based on: https://natsbyexample.com/examples/messaging/request-reply/go
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server
//!
//! Run with: zig build run-nbe-messaging-request-reply

const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const io = init.io;

    var stdout_buf: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(
        io,
        &stdout_buf,
    );
    const stdout = &stdout_writer.interface;

    // Connect to NATS
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit();

    // Subscribe to "greet.*" to handle incoming requests.
    // The handler extracts the name from the subject and
    // responds with a greeting.
    const sub = try client.subscribe("greet.*");
    defer sub.deinit();

    // Run the request handler in a background async task.
    // It will process exactly 3 requests then exit.
    var handler = io.async(handleRequests, .{
        client,
        sub,
    });
    defer handler.cancel(io);

    // Give the subscription time to register on the server
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Send 3 requests - each will be handled by our
    // background task and we'll get a personalized greeting.
    if (try client.request(
        "greet.joe",
        "",
        1000,
    )) |reply| {
        defer reply.deinit();
        if (reply.isNoResponders()) {
            try stdout.print("no responders\n", .{});
        } else {
            try stdout.print("{s}\n", .{reply.data});
        }
    }

    if (try client.request(
        "greet.sue",
        "",
        1000,
    )) |reply| {
        defer reply.deinit();
        if (reply.isNoResponders()) {
            try stdout.print("no responders\n", .{});
        } else {
            try stdout.print("{s}\n", .{reply.data});
        }
    }

    if (try client.request(
        "greet.bob",
        "",
        1000,
    )) |reply| {
        defer reply.deinit();
        if (reply.isNoResponders()) {
            try stdout.print("no responders\n", .{});
        } else {
            try stdout.print("{s}\n", .{reply.data});
        }
    }

    // Unsubscribe the handler so no one is listening anymore
    try sub.unsubscribe();

    // This request will fail with "no responders" because
    // we just unsubscribed the only handler.
    if (try client.request(
        "greet.joe",
        "",
        1000,
    )) |reply| {
        defer reply.deinit();
        if (reply.isNoResponders()) {
            try stdout.print("no responders\n", .{});
        } else {
            try stdout.print("{s}\n", .{reply.data});
        }
    }

    try stdout.flush();
}

/// Background handler that processes incoming requests.
/// Extracts the name from the subject ("greet.joe" -> "joe")
/// and responds with "hello, <name>".
fn handleRequests(
    client: *nats.Client,
    sub: *nats.Client.Sub,
) void {
    for (0..3) |_| {
        const req = sub.nextWithTimeout(
            2000,
        ) catch return;
        if (req) |r| {
            defer r.deinit();
            // "greet.joe" -> "joe"
            const name = r.subject[6..];
            var buf: [64]u8 = undefined;
            const reply = std.fmt.bufPrint(
                &buf,
                "hello, {s}",
                .{name},
            ) catch return;
            r.respond(client, reply) catch {};
        }
    }
}
