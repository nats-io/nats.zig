//! JetStream Pull Consumer -- fetch, iterate, and consume
//! patterns.
//!
//! Demonstrates three ways to receive messages from a pull
//! consumer: batch fetch, single next(), and continuous
//! MessagesContext iteration. Pull consumers are the
//! recommended pattern for most workloads -- the client
//! controls the pace.
//!
//! Run with: zig build run-jetstream-consume
//!
//! Prerequisites: nats-server -js

const std = @import("std");
const nats = @import("nats");
const js_mod = nats.jetstream;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://127.0.0.1:4222",
        .{ .name = "js-consume-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    var js = try js_mod.JetStream.init(client, .{});

    // Create a stream to hold our task messages
    var stream_resp = try js.createStream(.{
        .name = "DEMO_CONSUME",
        .subjects = &.{"tasks.>"},
        .storage = .memory,
    });
    stream_resp.deinit();

    // Publish 10 task messages before consuming.
    // In production, publishers and consumers run
    // independently -- messages are persisted in the
    // stream until acknowledged.
    for (0..10) |i| {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "task {d}",
            .{i + 1},
        ) catch "task";
        var ack = try js.publish(
            "tasks.work",
            payload,
        );
        ack.deinit();
    }
    std.debug.print("Published 10 tasks.\n\n", .{});

    // Create a durable pull consumer named "worker".
    // Explicit ack means the server waits for each
    // message to be acknowledged before considering
    // it delivered. Unacked messages are redelivered.
    var cons_resp = try js.createConsumer(
        "DEMO_CONSUME",
        .{
            .name = "worker",
            .ack_policy = .explicit,
        },
    );
    cons_resp.deinit();

    // PullSubscription is a lightweight handle that
    // binds to the stream + consumer pair. No heap
    // allocation -- safe to copy/move.
    var pull = js_mod.PullSubscription{
        .js = &js,
        .stream = "DEMO_CONSUME",
    };
    try pull.setConsumer("worker");

    // Pattern 1: Batch fetch -- get up to N messages
    // in one round-trip. Efficient for bulk processing.
    std.debug.print("-- Pattern 1: fetch --\n", .{});

    var result = try pull.fetch(.{
        .max_messages = 5,
        .timeout_ms = 5000,
    });
    defer result.deinit();

    var total: usize = 0;
    for (result.messages) |*msg| {
        std.debug.print(
            "  [{d}] {s}\n",
            .{ total + 1, msg.data() },
        );
        // Always ack to tell the server we're done
        // with this message. Without ack, the server
        // will redeliver after ack_wait expires.
        try msg.ack();
        total += 1;
    }
    std.debug.print(
        "  Fetched {d} messages.\n\n",
        .{result.count()},
    );

    // Pattern 2: next() -- fetch a single message.
    // Good for request-at-a-time processing or when
    // you need fine-grained control.
    std.debug.print(
        "-- Pattern 2: next --\n",
        .{},
    );

    if (try pull.next(3000)) |*msg| {
        var m = msg.*;
        defer m.deinit();
        std.debug.print(
            "  Got: {s}\n\n",
            .{m.data()},
        );
        try m.ack();
        total += 1;
    }

    // Pattern 3: MessagesContext -- continuous iterator
    // that auto-fetches new batches as needed. Best
    // for long-running workers that process messages
    // in a loop.
    std.debug.print(
        "-- Pattern 3: messages --\n",
        .{},
    );

    var msgs = try pull.messages(.{
        .max_messages = 10,
        .expires_ms = 5000,
    });
    defer msgs.deinit();

    // Read remaining messages (4 left from our 10)
    while (try msgs.next()) |*msg| {
        var m = msg.*;
        defer m.deinit();
        std.debug.print(
            "  [{d}] {s}\n",
            .{ total + 1, m.data() },
        );
        try m.ack();
        total += 1;
    }

    std.debug.print(
        "\nTotal processed: {d}\n",
        .{total},
    );

    // Clean up stream
    var del = try js.deleteStream("DEMO_CONSUME");
    del.deinit();

    std.debug.print("Done!\n", .{});
}
