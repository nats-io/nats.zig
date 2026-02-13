//! Batch Receiving Patterns
//!
//! Demonstrates efficient batch message retrieval:
//! - nextMsgBatch(): blocking batch receive (waits for at least 1 message)
//! - tryNextMsgBatch(): non-blocking batch receive for polling
//! - Stats monitoring: track messages and detect drops
//!
//! Run with: zig build run-batch-receiving
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    // Connect with larger subscription queue for batch receiving
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{
            .name = "batch-receiving",
            .sub_queue_size = 512, // Larger queue for batch demos
        },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    const sub = try client.subscribeSync("bench.>");
    defer sub.deinit();

    std.debug.print("Subscribed to 'bench.>'\n\n", .{});

    // Publish test messages
    const message_count: u32 = 100;
    std.debug.print("Publishing {d} messages...\n", .{message_count});

    for (0..message_count) |i| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "Message {d}", .{i + 1}) catch "Msg";
        try client.publish("bench.test", payload);
    }

    // BATCH RECEIVING: Receive multiple messages at once
    std.debug.print("Receiving messages (batch mode)...\n", .{});

    var batch_buf: [32]nats.Message = undefined;
    var total_received: u32 = 0;
    var batch_count: u32 = 0;

    const recv_start = Io.Timestamp.now(io, .awake);

    while (total_received < message_count) {
        // nextBatch waits for at least 1 message, returns up to 32
        const count = sub.nextMsgBatch(io, &batch_buf) catch break;
        batch_count += 1;

        for (batch_buf[0..count]) |*msg| {
            defer msg.deinit();
            total_received += 1;
        }

        // Check for dropped messages
        const dropped = sub.dropped();
        if (dropped > 0) {
            std.debug.print(
                "  Warning: {d} messages dropped (consumer too slow)\n",
                .{dropped},
            );
        }
    }

    const recv_end = Io.Timestamp.now(io, .awake);
    const elapsed = recv_start.durationTo(recv_end);
    const recv_ns: u64 = @intCast(elapsed.nanoseconds);
    const recv_ms = @as(f64, @floatFromInt(recv_ns)) /
        1_000_000.0;

    std.debug.print(
        "Received {d} messages in {d} batches ({d:.2}ms)\n",
        .{ total_received, batch_count, recv_ms },
    );
    std.debug.print(
        "Throughput: {d:.0} msgs/sec\n\n",
        .{@as(f64, @floatFromInt(total_received)) / (recv_ms / 1000.0)},
    );

    // NON-BLOCKING BATCH: tryNextBatch for polling
    std.debug.print("Demonstrating tryNextBatch (non-blocking)...\n", .{});

    // Publish a few more messages
    for (0..5) |i| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "Extra {d}", .{i + 1}) catch "Msg";
        try client.publish("bench.extra", payload);
    }

    // Small delay to let messages arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Non-blocking batch receive
    const available = sub.tryNextMsgBatch(&batch_buf);
    std.debug.print("  tryNextBatch returned {d} messages immediately\n", .{available});

    for (batch_buf[0..available]) |*msg| {
        defer msg.deinit();
        std.debug.print("    {s}\n", .{msg.data});
    }

    // STATS SUMMARY
    std.debug.print("\nStats summary:\n", .{});
    std.debug.print("  Messages received: {d}\n", .{sub.received_msgs});
    std.debug.print("  Messages dropped: {d}\n", .{sub.dropped()});

    const stats = client.stats();
    std.debug.print("  Total bytes out: {d}\n", .{stats.bytes_out});
    std.debug.print("  Total bytes in: {d}\n", .{stats.bytes_in});

    std.debug.print("\nDone!\n", .{});
}
