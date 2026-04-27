//! JetStream Publish -- stream CRUD and publish with
//! acknowledgment.
//!
//! Creates a stream, publishes messages with server-side
//! acknowledgment, demonstrates deduplication via msg-id,
//! and queries stream info.
//!
//! Run with: zig-out/bin/example-jetstream-publish
//!
//! Prerequisites: nats-server -js

const std = @import("std");
const nats = @import("nats");

// JetStream is accessed through the nats.jetstream module.
const js_mod = nats.jetstream;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Connect to NATS with JetStream enabled on the
    // server. JetStream uses the same TCP connection
    // as core NATS -- no extra ports needed.
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://127.0.0.1:4222",
        .{ .name = "js-publish-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    // JetStream context is stack-allocated -- it holds
    // a pointer to the client plus config (no heap).
    var js = js_mod.JetStream.init(client, .{});

    // Create a memory-backed stream named DEMO_PUBLISH
    // that captures all subjects matching "demo.>".
    // Memory storage is fast but not persistent across
    // server restarts.
    var create_resp = try js.createStream(.{
        .name = "DEMO_PUBLISH",
        .subjects = &.{"demo.>"},
        .storage = .memory,
    });
    create_resp.deinit();

    std.debug.print(
        "Stream 'DEMO_PUBLISH' created.\n\n",
        .{},
    );

    // Publish 5 messages. Each publish returns a PubAck
    // from the server confirming storage. The ack
    // contains the stream name and sequence number.
    for (0..5) |i| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "order #{d}",
            .{i + 1},
        ) catch "order";

        var ack = try js.publish(
            "demo.orders",
            payload,
        );
        defer ack.deinit();

        std.debug.print(
            "Published seq={d} stream={s}\n",
            .{
                ack.value.seq,
                ack.value.stream orelse "?",
            },
        );
    }

    // Deduplication: publish with a msg-id header.
    // If the same msg-id is sent within the stream's
    // duplicate_window (default 2min), the server
    // returns duplicate=true without storing again.
    std.debug.print(
        "\n-- Deduplication test --\n",
        .{},
    );

    var ack1 = try js.publishWithOpts(
        "demo.orders",
        "unique payload",
        .{ .msg_id = "order-abc-123" },
    );
    defer ack1.deinit();
    std.debug.print("First:  seq={d} dup={}\n", .{
        ack1.value.seq,
        ack1.value.duplicate orelse false,
    });

    // Same msg-id again -- server detects duplicate
    var ack2 = try js.publishWithOpts(
        "demo.orders",
        "unique payload",
        .{ .msg_id = "order-abc-123" },
    );
    defer ack2.deinit();
    std.debug.print("Second: seq={d} dup={}\n", .{
        ack2.value.seq,
        ack2.value.duplicate orelse false,
    });

    // Query stream info to see the message count
    var info = try js.streamInfo("DEMO_PUBLISH");
    defer info.deinit();

    if (info.value.state) |state| {
        std.debug.print(
            "\nStream has {d} messages" ++
                " ({d} bytes)\n",
            .{ state.messages, state.bytes },
        );
    }

    // Clean up: delete the stream and all its data
    var del = try js.deleteStream("DEMO_PUBLISH");
    del.deinit();

    std.debug.print(
        "\nStream deleted. Done!\n",
        .{},
    );
}
