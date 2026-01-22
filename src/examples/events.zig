//! Event Callbacks Example
//!
//! Demonstrates how to handle connection lifecycle events using the
//! EventHandler pattern. Shows how handlers can reference external state
//! without closures.
//!
//! Run with: zig build run-events
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV
//!
//! Try stopping/starting nats-server to see disconnect/reconnect events.

const std = @import("std");
const nats = @import("nats");

/// Application state that callbacks will modify.
/// This pattern allows event handlers to update shared state
/// without closures.
const AppState = struct {
    is_online: bool = false,
    reconnect_count: u32 = 0,
    last_error: ?anyerror = null,
    should_shutdown: bool = false,
};

/// Event handler that references external AppState.
/// All callback methods are optional - only implement what you need.
const MyEventHandler = struct {
    app: *AppState,

    pub fn onConnect(self: *@This()) void {
        self.app.is_online = true;
        std.debug.print("[EVENT] Connected to NATS server\n", .{});
    }

    pub fn onDisconnect(self: *@This(), err: ?anyerror) void {
        self.app.is_online = false;
        self.app.last_error = err;
        if (err) |e| {
            std.debug.print("[EVENT] Disconnected: {s}\n", .{@errorName(e)});
        } else {
            std.debug.print("[EVENT] Disconnected (clean)\n", .{});
        }
    }

    pub fn onReconnect(self: *@This()) void {
        self.app.is_online = true;
        self.app.reconnect_count += 1;
        std.debug.print(
            "[EVENT] Reconnected! (total reconnects: {})\n",
            .{self.app.reconnect_count},
        );
    }

    pub fn onClose(self: *@This()) void {
        self.app.is_online = false;
        self.app.should_shutdown = true;
        std.debug.print("[EVENT] Connection closed permanently\n", .{});
    }

    pub fn onError(self: *@This(), err: anyerror) void {
        self.app.last_error = err;
        std.debug.print("[EVENT] Async error: {s}\n", .{@errorName(err)});
    }

    pub fn onLameDuck(_: *@This()) void {
        std.debug.print(
            "[EVENT] Server entering lame duck mode - prepare for shutdown!\n",
            .{},
        );
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create async I/O runtime
    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // External state that callbacks will modify
    var app_state = AppState{};

    // Handler with reference to external state
    var handler = MyEventHandler{ .app = &app_state };

    std.debug.print("Connecting to NATS with event callbacks...\n", .{});

    // Connect with event handler
    const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
        .event_handler = nats.EventHandler.init(MyEventHandler, &handler),
        .reconnect = true,
    });
    defer client.deinit(allocator);

    // Subscribe to test subject
    const sub = try client.subscribe(allocator, "test.>");
    defer sub.deinit(allocator);

    std.debug.print("\nSubscribed to test.>\n", .{});
    std.debug.print("Try: nats pub test.hello 'world'\n", .{});
    std.debug.print("Try stopping/starting nats-server to see events!\n", .{});
    std.debug.print("Press Ctrl+C to exit.\n\n", .{});

    // Main loop - processes messages and checks app_state
    var msg_count: u32 = 0;
    const max_msgs: u32 = 100;

    while (!app_state.should_shutdown and msg_count < max_msgs) {
        // Non-blocking message check with timeout
        if (try sub.nextWithTimeout(allocator, 1000)) |msg| {
            defer msg.deinit(allocator);
            std.debug.print("Received: {s}\n", .{msg.data});
            msg_count += 1;
        }

        // React to state changes from callbacks
        if (!app_state.is_online) {
            std.debug.print("(offline - waiting for reconnect...)\n", .{});
            io.sleep(.fromMilliseconds(1000), .awake) catch {};
        }
    }

    std.debug.print("\n=== Final App State ===\n", .{});
    std.debug.print("  Online: {}\n", .{app_state.is_online});
    std.debug.print("  Reconnects: {}\n", .{app_state.reconnect_count});
    if (app_state.last_error) |err| {
        std.debug.print("  Last error: {s}\n", .{@errorName(err)});
    } else {
        std.debug.print("  Last error: none\n", .{});
    }
    std.debug.print("  Messages received: {}\n", .{msg_count});
}
