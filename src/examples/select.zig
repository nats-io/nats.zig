//! io.select() Pattern - Subscription with Timeout
//!
//! Demonstrates io.select() to race a subscription receive against a timeout.
//! This is the correct use case for io.select() with NATS - racing ONE
//! subscription against a non-resource operation like sleep.
//!
//! NOTE: Do NOT use io.select() to race multiple subscriptions - cancelling
//! a subscription future discards any message it received. Use polling or
//! io.concurrent() + Io.Queue instead (see multi_sub.zig, multi_sub_async.zig).
//!
//! Run with: zig build example-select
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Sub = nats.Client.Sub;

/// Sleep function compatible with io.async()
fn sleepMs(io: Io, ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

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
        .{ .name = "select-example" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n", .{});

    const sub = try client.subscribe(allocator, "demo.select");
    defer sub.deinit(allocator);
    try client.flush(allocator);

    std.debug.print("Subscribed to 'demo.select'\n", .{});
    std.debug.print("\nPublishing 3 messages with 200ms gaps...\n", .{});
    std.debug.print("Using 500ms timeout - should receive all 3.\n\n", .{});

    // Spawn publisher in background
    var publisher = io.async(publishMessages, .{ client, io, allocator });
    defer publisher.cancel(io);

    // Receive with timeout using io.select()
    var received: u32 = 0;
    const max_attempts = 5;

    for (0..max_attempts) |attempt| {
        // Create futures for receive and timeout
        var recv_future = io.async(Sub.next, .{ sub, allocator, io });
        var timeout_future = io.async(sleepMs, .{ io, 500 });

        // Track winner to avoid double-free
        var winner: enum { none, message, timeout } = .none;

        // Defer cancel for non-winners
        defer if (winner != .message) {
            if (recv_future.cancel(io)) |m| m.deinit(allocator) else |_| {}
        };
        defer if (winner != .timeout) {
            timeout_future.cancel(io);
        };

        // Wait for EITHER message OR timeout
        const result = io.select(.{
            .message = &recv_future,
            .timeout = &timeout_future,
        }) catch break; // Defers handle cleanup

        switch (result) {
            .message => |msg_result| {
                winner = .message;
                const msg = msg_result catch continue;
                defer msg.deinit(allocator);
                received += 1;
                std.debug.print(
                    "  [{d}] Received: {s}\n",
                    .{ attempt + 1, msg.data },
                );
            },
            .timeout => {
                winner = .timeout;
                std.debug.print("  [{d}] Timeout - no message\n", .{attempt + 1});
            },
        }
    }

    std.debug.print("\nReceived {d} messages in {d} attempts.\n", .{
        received,
        max_attempts,
    });
    std.debug.print("Done!\n", .{});
}

fn publishMessages(
    client: *nats.Client,
    io: Io,
    alloc: std.mem.Allocator,
) void {
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    for (1..4) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Message {d}", .{i}) catch "Msg";
        client.publish("demo.select", msg) catch return;
        client.flush(alloc) catch return;
        io.sleep(.fromMilliseconds(200), .awake) catch {};
    }
}
