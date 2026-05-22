//! Request Many (Scatter/Gather)
//!
//! Implements the ADR-47 "Request Many" pattern: publish one
//! request and gather multiple replies through a dedicated inbox.
//! Useful for fan-out RPC, leader-election style probes, and any
//! workflow where several services answer the same question.
//!
//! This example uses a single service connection with three
//! subscribers on the same subject to act as three "workers".
//! Each worker replies with its own ID, and the requester
//! collects all three answers.
//!
//! Two forms are demonstrated:
//!   1. Iterator form  -- pull each reply with `iter.next()`.
//!   2. Callback form  -- replies dispatched via `MsgHandler`.
//!
//! Stop conditions (configured via `RequestManyOptions`):
//!   - `max_wait_ms`   : total deadline for the call (required)
//!   - `max_messages`  : terminate after N replies
//!   - `stall_ms`      : terminate after a gap between replies
//!   - `sentinel`      : terminate on a predicate match. Use
//!                       `nats.emptyPayloadSentinel()` for the
//!                       ADR-47 standard end-of-stream marker.
//!
//! Run with: zig build run-request-many
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");
const io_backend = @import("io_backend");

/// Worker handler: replies with its own ID for each request.
const Worker = struct {
    client: *nats.Client,
    id: u32,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        var buf: [32]u8 = undefined;
        const reply = std.fmt.bufPrint(
            &buf,
            "worker-{d}",
            .{self.id},
        ) catch return;
        msg.respond(self.client, reply) catch {};
    }
};

/// Collector for the callback form: counts and prints each reply.
const Collector = struct {
    count: u32 = 0,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        self.count += 1;
        std.debug.print(
            "  reply {d}: {s}\n",
            .{ self.count, msg.data },
        );
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // -------- Service: one connection, three workers --------
    var svc_backend: io_backend.Backend = undefined;
    try io_backend.init(&svc_backend, allocator);
    defer svc_backend.deinit();

    const svc = try nats.Client.connect(
        allocator,
        svc_backend.io(),
        "nats://localhost:4222",
        .{ .name = "scatter-service" },
    );
    defer svc.deinit();

    var w1 = Worker{ .client = svc, .id = 1 };
    var w2 = Worker{ .client = svc, .id = 2 };
    var w3 = Worker{ .client = svc, .id = 3 };

    // Three plain (non-queue) subscriptions to the same subject.
    // The server delivers each request to ALL three -- this is
    // what makes the pattern "scatter" rather than load-balanced.
    const sub1 = try svc.subscribe(
        "rpc.scatter",
        nats.MsgHandler.init(Worker, &w1),
    );
    defer sub1.deinit();

    const sub2 = try svc.subscribe(
        "rpc.scatter",
        nats.MsgHandler.init(Worker, &w2),
    );
    defer sub2.deinit();

    const sub3 = try svc.subscribe(
        "rpc.scatter",
        nats.MsgHandler.init(Worker, &w3),
    );
    defer sub3.deinit();

    // Make sure all subscriptions are registered before the
    // first request lands on the server.
    try svc.flush(1_000_000_000);

    // -------- Requester: separate connection --------
    var req_backend: io_backend.Backend = undefined;
    try io_backend.init(&req_backend, allocator);
    defer req_backend.deinit();

    const requester = try nats.Client.connect(
        allocator,
        req_backend.io(),
        "nats://localhost:4222",
        .{ .name = "scatter-requester" },
    );
    defer requester.deinit();

    std.debug.print(
        "Connected. 3 workers listening on 'rpc.scatter'.\n",
        .{},
    );

    // -------- Iterator form --------
    //
    // Publishes one request, then yields each reply through
    // `iter.next()`. Caller owns each yielded `Message` and must
    // call `deinit()`. Caller must also call `iter.deinit()` once
    // the loop ends, regardless of the termination reason.
    std.debug.print("\n[iterator]\n", .{});
    var iter = try requester.requestMany(
        "rpc.scatter",
        "ping",
        .{
            .max_wait_ms = 2000,
            .max_messages = 3,
        },
    );
    defer iter.deinit();

    var got: u32 = 0;
    while (try iter.next()) |msg| {
        defer msg.deinit();
        got += 1;
        std.debug.print(
            "  reply {d}: {s}\n",
            .{ got, msg.data },
        );
    }
    std.debug.print(
        "  ended: received={d} termination={s}\n",
        .{ got, @tagName(iter.termination) },
    );

    // -------- Callback form --------
    //
    // Same options, but replies are dispatched to a `MsgHandler`
    // and freed automatically after `onMessage` returns. Returns
    // a summary with the final count and termination reason.
    std.debug.print("\n[callback]\n", .{});
    var collector = Collector{};
    const result = try requester.requestManyCallback(
        "rpc.scatter",
        "ping",
        .{
            .max_wait_ms = 2000,
            .max_messages = 3,
        },
        nats.MsgHandler.init(Collector, &collector),
    );
    std.debug.print(
        "  ended: received={d} termination={s}\n",
        .{ result.received, @tagName(result.termination) },
    );

    std.debug.print("\nDone!\n", .{});
}
