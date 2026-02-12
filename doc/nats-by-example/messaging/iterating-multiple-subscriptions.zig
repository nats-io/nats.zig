//! Iterating Over Multiple Subscriptions
//!
//! NATS wildcards cover many routing cases, but sometimes you
//! need separate subscriptions - for example, you want
//! "transport.cars", "transport.planes", and "transport.ships"
//! but NOT "transport.spaceships".
//!
//! This example shows how to poll multiple subscriptions in
//! a unified loop using tryNext() - the Zig equivalent of
//! merging multiple async streams.
//!
//! Based on: https://natsbyexample.com/examples/messaging/iterating-multiple-subscriptions/rust
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server
//!
//! Run with: zig build run-nbe-messaging-iterating-multiple-subscriptions

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;
const NUM_MSGS_PER_CATEGORY = 10;
const TOTAL_MSGS = NUM_MSGS_PER_CATEGORY * 3;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const io = init.io;

    var stdout_buf: [8192]u8 = undefined;
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
    defer client.deinit();

    // Create three separate subscriptions. We use ">"
    // (multi-level wildcard) to match all sub-subjects.
    const sub_cars = try client.subscribeSync("cars.>");
    defer sub_cars.deinit();

    const sub_planes = try client.subscribeSync("planes.>");
    defer sub_planes.deinit();

    const sub_ships = try client.subscribeSync("ships.>");
    defer sub_ships.deinit();

    // Publish 10 messages to each category
    for (0..NUM_MSGS_PER_CATEGORY) |i| {
        var buf: [64]u8 = undefined;

        const cars_subj = std.fmt.bufPrint(
            &buf,
            "cars.{d}",
            .{i},
        ) catch continue;
        var payload_buf: [64]u8 = undefined;
        const cars_payload = std.fmt.bufPrint(
            &payload_buf,
            "car number {d}",
            .{i},
        ) catch continue;
        try client.publish(cars_subj, cars_payload);

        const planes_subj = std.fmt.bufPrint(
            &buf,
            "planes.{d}",
            .{i},
        ) catch continue;
        const planes_payload = std.fmt.bufPrint(
            &payload_buf,
            "plane number {d}",
            .{i},
        ) catch continue;
        try client.publish(planes_subj, planes_payload);

        const ships_subj = std.fmt.bufPrint(
            &buf,
            "ships.{d}",
            .{i},
        ) catch continue;
        const ships_payload = std.fmt.bufPrint(
            &payload_buf,
            "ship number {d}",
            .{i},
        ) catch continue;
        try client.publish(ships_subj, ships_payload);
    }

    // Wait for messages to arrive
    io.sleep(.fromMilliseconds(100), .awake) catch {};

    // Poll all 3 subscriptions in round-robin fashion.
    // tryNext() is non-blocking - returns null instantly if
    // no message is available, letting us cycle to the next
    // subscription without waiting.
    const subs = [_]*nats.Client.Sub{
        sub_cars,
        sub_planes,
        sub_ships,
    };
    var total: u32 = 0;
    var idx: usize = 0;
    var empty_cycles: u32 = 0;

    while (total < TOTAL_MSGS) {
        if (subs[idx].tryNext()) |msg| {
            defer msg.deinit();
            total += 1;
            empty_cycles = 0;
            try stdout.print(
                "received on {s}: {s}\n",
                .{ msg.subject, msg.data },
            );
        }
        idx = (idx + 1) % subs.len;

        // Avoid busy-spinning when no messages are ready
        if (idx == 0) {
            empty_cycles += 1;
            if (empty_cycles > 10) {
                io.sleep(
                    .fromMilliseconds(10),
                    .awake,
                ) catch {};
            }
        }

        // Safety: don't spin forever if messages are lost
        if (empty_cycles > 100) break;
    }

    try stdout.print(
        "\nreceived {d} messages from 3 subscriptions\n",
        .{total},
    );
    try stdout.flush();
}
