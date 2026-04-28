//! Io.Select Pattern - Subscription with Timeout
//!
//! Demonstrates Io.Select to race a subscription receive against a
//! timeout. This is the correct use case for Io.Select with NATS -
//! racing ONE subscription against a non-resource operation like sleep.
//!
//! NOTE: Do NOT use Io.Select to race multiple subscriptions -
//! cancelling a subscription task discards any message it received.
//! Use polling or io.concurrent() + Io.Queue instead (see
//! polling_loop.zig and queue_groups.zig).
//!
//! Run with: zig build run-select
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const Sub = nats.Client.Sub;
const Message = nats.Message;

/// Sleep function compatible with Io.Select.async()
fn sleepMs(io: Io, ms: i64) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "select-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n", .{});

    const sub = try client.subscribeSync("demo.select");
    defer sub.deinit();

    std.debug.print("Subscribed to 'demo.select'\n", .{});
    std.debug.print(
        "\nPublishing 3 messages with 200ms gaps...\n",
        .{},
    );
    std.debug.print(
        "Using 500ms timeout - should receive all 3.\n\n",
        .{},
    );

    // Spawn publisher in background
    var publisher = io.async(publishMessages, .{ client, io });
    defer publisher.cancel(io);

    // Receive with timeout using Io.Select
    var received: u32 = 0;
    const max_attempts = 5;

    const Sel = Io.Select(union(enum) {
        message: anyerror!Message,
        timeout: void,
    });

    for (0..max_attempts) |attempt| {
        var buf: [2]Sel.Union = undefined;
        var sel = Sel.init(io, &buf);
        sel.async(.message, Sub.nextMsg, .{sub});
        sel.async(.timeout, sleepMs, .{ io, 500 });

        // Wait for EITHER message OR timeout
        const result = sel.await() catch {
            // Cancel remaining tasks, deinit any messages
            while (sel.cancel()) |remaining| {
                switch (remaining) {
                    .message => |r| {
                        if (r) |m| m.deinit() else |_| {}
                    },
                    .timeout => {},
                }
            }
            break;
        };
        // Cancel the loser task
        while (sel.cancel()) |remaining| {
            switch (remaining) {
                .message => |r| {
                    if (r) |m| m.deinit() else |_| {}
                },
                .timeout => {},
            }
        }

        switch (result) {
            .message => |msg_result| {
                const msg = msg_result catch continue;
                defer msg.deinit();
                received += 1;
                std.debug.print(
                    "  [{d}] Received: {s}\n",
                    .{ attempt + 1, msg.data },
                );
            },
            .timeout => {
                std.debug.print(
                    "  [{d}] Timeout - no message\n",
                    .{attempt + 1},
                );
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
) void {
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    for (1..4) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "Message {d}",
            .{i},
        ) catch "Msg";
        client.publish("demo.select", msg) catch return;
        io.sleep(.fromMilliseconds(200), .awake) catch {};
    }
}
