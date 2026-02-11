//! Callback Subscriptions
//!
//! Demonstrates callback-style message handling using MsgHandler
//! (vtable pattern) and plain function pointers. Messages are
//! dispatched automatically -- no manual next() loop needed.
//!
//! Run with: zig build run-callback
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const nats = @import("nats");

// -- MsgHandler pattern: handler struct with state --

/// Application state shared with the handler.
const AppState = struct {
    count: u32 = 0,
    last_subject: [64]u8 = undefined,
    last_subject_len: usize = 0,
};

/// Handler struct -- implements onMessage to receive callbacks.
const MyHandler = struct {
    app: *AppState,

    pub fn onMessage(self: *@This(), msg: *const nats.Message) void {
        self.app.count += 1;
        const len = @min(msg.subject.len, 64);
        @memcpy(self.app.last_subject[0..len], msg.subject[0..len]);
        self.app.last_subject_len = len;

        std.debug.print(
            "  [handler] #{d} {s}: {s}\n",
            .{
                self.app.count,
                msg.subject,
                msg.data,
            },
        );
    }
};

// -- Plain fn pattern: no struct needed --

/// Simple alert function -- stateless callback.
fn alertFn(msg: *const nats.Message) void {
    std.debug.print(
        "  [alert] {s}: {s}\n",
        .{ msg.subject, msg.data },
    );
}

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "callback-example" },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected to NATS!\n\n", .{});

    // 1. MsgHandler callback subscription
    var app = AppState{};
    var handler = MyHandler{ .app = &app };

    const sub1 = try client.subscribeWithCallback(
        allocator,
        "demo.handler",
        nats.MsgHandler.init(MyHandler, &handler),
    );
    defer sub1.deinit(allocator);

    // 2. Plain fn callback subscription
    const sub2 = try client.subscribeWithCallbackFn(
        allocator,
        "demo.alert",
        alertFn,
    );
    defer sub2.deinit(allocator);

    std.debug.print("Subscribed with callbacks.\n", .{});
    std.debug.print(
        "Publishing messages...\n\n",
        .{},
    );

    // Publish messages
    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "hello {d}",
            .{i + 1},
        ) catch "hello";
        try client.publish("demo.handler", msg);
    }
    try client.publish("demo.alert", "fire!");
    try client.publish("demo.alert", "smoke!");

    // Wait for callbacks to fire
    io.sleep(.fromMilliseconds(200), .awake) catch {};

    std.debug.print(
        "\nHandler count: {d}\n",
        .{app.count},
    );
    std.debug.print(
        "Last subject: {s}\n",
        .{app.last_subject[0..app.last_subject_len]},
    );

    // Verify all messages delivered
    std.debug.assert(app.count == 5);

    std.debug.print("Done!\n", .{});
}
