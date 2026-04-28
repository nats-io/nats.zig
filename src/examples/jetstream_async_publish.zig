//! JetStream Async Publish -- non-blocking publish with
//! futures.
//!
//! AsyncPublisher decouples publishing from ack waiting.
//! Messages are sent immediately and acks are correlated
//! in the background via a shared reply subscription.
//! Use this when throughput matters more than per-message
//! confirmation.
//!
//! Run with: zig build run-jetstream-async-publish
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
        .{ .name = "js-async-pub-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    var js = try js_mod.JetStream.init(client, .{});

    var stream_resp = try js.createStream(.{
        .name = "DEMO_ASYNC",
        .subjects = &.{"perf.>"},
        .storage = .memory,
    });
    stream_resp.deinit();

    // AsyncPublisher manages a shared reply inbox and
    // correlates incoming acks to pending futures.
    // max_pending=64 means backpressure kicks in after
    // 64 unacknowledged publishes (the caller blocks
    // until acks drain below the threshold).
    var ap = try js_mod.AsyncPublisher.init(
        &js,
        .{ .max_pending = 64 },
    );
    defer ap.deinit();

    const msg_count: u32 = 100;

    // Fire-and-forget: publish all messages without
    // waiting for individual acks. The futures
    // accumulate and resolve as acks arrive from the
    // server in the background.
    var futures: [100]*js_mod.PubAckFuture = undefined;
    for (0..msg_count) |i| {
        var buf: [64]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "measurement #{d}",
            .{i + 1},
        ) catch "data";
        futures[i] = try ap.publish(
            "perf.metrics",
            payload,
        );
    }

    std.debug.print(
        "Published {d} messages.\n",
        .{msg_count},
    );
    std.debug.print(
        "Pending acks: {d}\n\n",
        .{ap.publishAsyncPending()},
    );

    // waitComplete blocks until all pending futures
    // resolve (acks received) or the timeout expires.
    // This is the batch-level sync point.
    try ap.waitComplete(10000);

    std.debug.print(
        "All acks received (pending={d}).\n\n",
        .{ap.publishAsyncPending()},
    );

    // Verify a few individual futures to show the
    // per-message API. Each future can be checked
    // independently with wait() or result().
    for (0..3) |i| {
        const fut = futures[i];
        defer fut.deinit();
        if (fut.result()) |ack| {
            std.debug.print(
                "  Future[{d}]: seq={d}\n",
                .{ i, ack.seq },
            );
        }
    }
    // Deinit remaining futures
    for (3..msg_count) |i| futures[i].deinit();

    // Check stream info for final message count
    var info = try js.streamInfo("DEMO_ASYNC");
    defer info.deinit();

    if (info.value.state) |state| {
        std.debug.print(
            "\nStream has {d} messages.\n",
            .{state.messages},
        );
    }

    var del = try js.deleteStream("DEMO_ASYNC");
    del.deinit();

    std.debug.print("Done!\n", .{});
}
