//! JetStream Push Consumer -- server-side message delivery.
//!
//! Push consumers have the server send messages to a
//! deliver_subject. The client subscribes to that subject
//! and processes messages via a callback handler.
//!
//! When to use push vs pull:
//! - Pull: client controls pace, best for batch/worker
//!   patterns, recommended for most use cases.
//! - Push: server controls pace, good for real-time
//!   fan-out, simpler for "firehose" scenarios.
//!
//! Run with: zig-out/bin/example-jetstream-push
//!
//! Prerequisites: nats-server -js

const std = @import("std");
const nats = @import("nats");
const js_mod = nats.jetstream;

// Counter tracks how many messages the handler has
// processed. Must implement onMessage for the
// JsMsgHandler vtable interface. The JsMsg has
// owned=false -- its slice fields are valid only
// during this callback; do not save pointers past
// the function return.
const Counter = struct {
    received: u32 = 0,
    target: u32 = 0,

    pub fn onMessage(
        self: *Counter,
        msg: *js_mod.JsMsg,
    ) void {
        self.received += 1;
        std.debug.print(
            "  [{d}/{d}] {s}: {s}\n",
            .{
                self.received,
                self.target,
                msg.subject(),
                msg.data(),
            },
        );
        // Push consumers with ack_policy=none don't
        // need explicit acks. For explicit ack policy,
        // call msg.ack() here.
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://127.0.0.1:4222",
        .{ .name = "js-push-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    var js = js_mod.JetStream.init(client, .{});

    var stream_resp = try js.createStream(.{
        .name = "DEMO_PUSH",
        .subjects = &.{"events.>"},
        .storage = .memory,
    });
    stream_resp.deinit();

    const msg_count: u32 = 5;

    // Set up the push subscription BEFORE creating
    // the consumer. The subscription must be active
    // so it catches messages the server starts
    // pushing immediately after consumer creation.
    var push_sub = js_mod.PushSubscription{
        .js = &js,
        .stream = "DEMO_PUSH",
    };
    push_sub.setConsumer("push-worker");
    push_sub.setDeliverSubject(
        "_DELIVER.push-example",
    );

    var counter = Counter{
        .target = msg_count,
    };

    // consume() subscribes to the deliver subject
    // and dispatches messages to our Counter handler
    // on the IO thread.
    var ctx = try push_sub.consume(
        js_mod.JsMsgHandler.init(
            Counter,
            &counter,
        ),
        .{},
    );
    defer ctx.deinit();

    // Now create the push consumer on the server.
    // ack_policy=none means no ack required -- good
    // for monitoring/logging where loss is acceptable.
    var cons_resp = try js.createPushConsumer(
        "DEMO_PUSH",
        .{
            .name = "push-worker",
            .deliver_subject = "_DELIVER.push-example",
            .ack_policy = .none,
        },
    );
    cons_resp.deinit();

    // Publish messages -- server pushes them to our
    // deliver subject automatically.
    for (0..msg_count) |i| {
        var buf: [32]u8 = undefined;
        const payload = std.fmt.bufPrint(
            &buf,
            "event {d}",
            .{i + 1},
        ) catch "event";
        var ack = try js.publish(
            "events.clicks",
            payload,
        );
        ack.deinit();
    }

    // Flush to ensure all publishes reach the server
    try client.flush(2_000_000_000);

    // Wait briefly for delivery to complete
    var waited: u32 = 0;
    while (counter.received < msg_count and
        waited < 3000)
    {
        var ts: std.posix.timespec = .{
            .sec = 0,
            .nsec = 1_000_000,
        };
        _ = std.posix.system.nanosleep(
            &ts,
            &ts,
        );
        waited += 1;
    }

    std.debug.print(
        "\nReceived {d}/{d} messages.\n",
        .{ counter.received, msg_count },
    );

    var del = try js.deleteStream("DEMO_PUSH");
    del.deinit();

    std.debug.print("Done!\n", .{});
}
