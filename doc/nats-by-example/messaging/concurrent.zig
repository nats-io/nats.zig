//! Concurrent Message Processing
//!
//! By default, messages from a subscription are processed
//! sequentially. This example shows how to process messages
//! concurrently using multiple worker threads.
//!
//! The pattern: receive messages on the main thread, dispatch
//! them to concurrent workers via an Io.Queue, and collect
//! results. Each worker simulates variable processing time
//! with a random delay, causing messages to complete out of
//! their original order.
//!
//! Based on: https://natsbyexample.com/examples/messaging/concurrent/rust
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server
//!
//! Run with: zig build run-nbe-messaging-concurrent

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

const NUM_MSGS = 10;
const NUM_WORKERS = 3;

/// Work item passed from main thread to workers.
/// Contains a copy of the message data (the original
/// Message is freed after copying).
const WorkItem = struct {
    data: [64]u8 = undefined,
    len: usize = 0,
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

    const sub = try client.subscribe(allocator, "greet.*");
    defer sub.deinit(allocator);

    // Publish 10 messages
    for (0..NUM_MSGS) |i| {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "hello {d}",
            .{i},
        ) catch continue;
        try client.publish("greet.joe", payload);
    }

    // Wait for messages to arrive
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Receive all messages and copy data into work items.
    // We copy because the Message backing buffer is freed
    // on deinit, but workers need the data later.
    var items: [NUM_MSGS]WorkItem = @splat(WorkItem{});
    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (try sub.nextWithTimeout(
            allocator,
            1000,
        )) |msg| {
            defer msg.deinit(allocator);
            const len = @min(msg.data.len, 64);
            @memcpy(
                items[received].data[0..len],
                msg.data[0..len],
            );
            items[received].len = len;
            received += 1;
        }
    }

    // Dispatch work to 3 concurrent workers. Each worker
    // gets a slice of the items array to process.
    // io.concurrent() ensures true parallel execution.
    const slice1_end = received / 3;
    const slice2_end = (received * 2) / 3;

    var w1 = try io.concurrent(processWorker, .{
        io,
        items[0..slice1_end],
    });
    defer w1.cancel(io);

    var w2 = try io.concurrent(processWorker, .{
        io,
        items[slice1_end..slice2_end],
    });
    defer w2.cancel(io);

    // Third worker runs on this thread (no extra thread needed)
    processWorker(io, items[slice2_end..received]);

    // Wait for concurrent workers to finish
    w1.await(io);
    w2.await(io);

    try stdout.print(
        "\nprocessed {d} messages concurrently\n",
        .{received},
    );
    try stdout.flush();
}

/// Worker function that processes a slice of work items.
/// Each item is "processed" with a random delay to simulate
/// variable work, then printed. The random delays cause
/// messages to complete out of their original order.
fn processWorker(io: Io, items: []WorkItem) void {
    const file_stdout = Io.File.stdout();
    for (items) |item| {
        // Random delay 0-100ms to simulate processing
        var rnd: [1]u8 = undefined;
        io.random(&rnd);
        const delay_ms: i64 = @intCast(rnd[0] % 100);
        io.sleep(
            .fromMilliseconds(delay_ms),
            .awake,
        ) catch {};

        // Write directly to stdout (single write syscall
        // per line avoids interleaved output)
        var buf: [80]u8 = undefined;
        const line = std.fmt.bufPrint(
            &buf,
            "received message: \"{s}\"\n",
            .{item.data[0..item.len]},
        ) catch continue;
        file_stdout.writeStreamingAll(io, line) catch {};
    }
}
