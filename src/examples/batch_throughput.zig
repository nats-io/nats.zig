//! High-Throughput Batch Patterns
//!
//! Demonstrates techniques for maximizing throughput:
//! - Batch publishing: multiple publishes before single flush
//! - Batch receiving: nextBatch() for efficient message retrieval
//! - Stats monitoring: track messages and detect drops
//!
//! Run with: zig build run-batch-throughput
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Connect with larger buffers for high throughput
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{
            .name = "batch-throughput",
            .buffer_size = 512 * 1024, // 512KB buffer
            .sub_queue_size = 512, // Larger subscription queue
        },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    const sub = try client.subscribe(allocator, "bench.>");
    defer sub.deinit(allocator);
    try client.flush(allocator);

    std.debug.print("Subscribed to 'bench.>'\n\n", .{});

    // BATCH PUBLISHING: Write many messages, flush once
    const message_count: u32 = 100;
    std.debug.print("Publishing {d} messages (batch mode)...\n", .{message_count});

    const start = std.time.Instant.now() catch unreachable;

    for (0..message_count) |i| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(&buf, "Message {d}", .{i + 1}) catch "Msg";
        try client.publish("bench.test", payload);
        // No flush here - messages accumulate in buffer
    }

    // Single flush sends all messages at once
    try client.flush(allocator);

    const elapsed = (std.time.Instant.now() catch unreachable).since(start);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    std.debug.print(
        "Published {d} messages in {d:.2}ms ({d:.0} msgs/sec)\n\n",
        .{
            message_count,
            elapsed_ms,
            @as(f64, @floatFromInt(message_count)) / (elapsed_ms / 1000.0),
        },
    );

    // BATCH RECEIVING: Receive multiple messages at once
    std.debug.print("Receiving messages (batch mode)...\n", .{});

    var batch_buf: [32]nats.Message = undefined;
    var total_received: u32 = 0;
    var batch_count: u32 = 0;

    const recv_start = std.time.Instant.now() catch unreachable;

    while (total_received < message_count) {
        // nextBatch waits for at least 1 message, returns up to 32
        const count = sub.nextBatch(io, &batch_buf) catch break;
        batch_count += 1;

        for (batch_buf[0..count]) |*msg| {
            defer msg.deinit(allocator);
            total_received += 1;
        }

        // Check for dropped messages
        const dropped = sub.getDroppedCount();
        if (dropped > 0) {
            std.debug.print(
                "  Warning: {d} messages dropped (consumer too slow)\n",
                .{dropped},
            );
        }
    }

    const recv_elapsed = (std.time.Instant.now() catch unreachable).since(recv_start);
    const recv_ms = @as(f64, @floatFromInt(recv_elapsed)) / 1_000_000.0;

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
    try client.flush(allocator);

    // Small delay to let messages arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Non-blocking batch receive
    const available = sub.tryNextBatch(&batch_buf);
    std.debug.print("  tryNextBatch returned {d} messages immediately\n", .{available});

    for (batch_buf[0..available]) |*msg| {
        defer msg.deinit(allocator);
        std.debug.print("    {s}\n", .{msg.data});
    }

    // STATS SUMMARY
    std.debug.print("\nStats summary:\n", .{});
    std.debug.print("  Messages received: {d}\n", .{sub.received_msgs});
    std.debug.print("  Messages dropped: {d}\n", .{sub.getDroppedCount()});

    const stats = client.getStats();
    std.debug.print("  Total bytes out: {d}\n", .{stats.bytes_out});
    std.debug.print("  Total bytes in: {d}\n", .{stats.bytes_in});

    std.debug.print("\nDone!\n", .{});
}
