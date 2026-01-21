//! Event Callbacks for NATS Client
//!
//! Type-erased event handler using comptime vtable pattern (like std.mem.Allocator).
//! Enables callbacks without closures, maintaining Zig's no-hidden-allocation guarantee.
//!
//! ## Architecture
//!
//! ```
//! ┌─────────────────┐     SPSC Queue        ┌─────────────────┐
//! │   io_task       │ ────────────────────→ │ callback_task   │
//! │   (reader)      │   Event{...}          │ (calls handler) │
//! │   never blocks  │                       │   user code     │
//! └─────────────────┘                       └─────────────────┘
//! ```
//!
//! io_task pushes events to SPSC queue (non-blocking).
//! callback_task drains queue and dispatches to user handlers.
//!
//! ## Usage
//!
//! ```zig
//! const MyHandler = struct {
//!     counter: *u32,
//!
//!     pub fn onConnect(self: *@This()) void {
//!         self.counter.* += 1;
//!     }
//! };
//!
//! var counter: u32 = 0;
//! var handler = MyHandler{ .counter = &counter };
//! const client = try nats.connect(allocator, io, url, .{
//!     .event_handler = nats.EventHandler.init(MyHandler, &handler),
//! });
//! ```

const std = @import("std");
const assert = std.debug.assert;

/// NATS-specific errors for event callbacks.
pub const Error = error{
    /// Subscription queue full - messages being dropped (slow consumer).
    SlowConsumer,
    /// Server permission violation (publish/subscribe rejected).
    PermissionViolation,
    /// Connection is stale (ping timeout).
    StaleConnection,
    /// Server sent -ERR response.
    ServerError,
    /// Authorization failed (invalid credentials).
    AuthorizationViolation,
    /// Server connection limit reached.
    MaxConnectionsExceeded,
};

/// Events pushed from io_task to callback_task.
/// These represent connection lifecycle changes and async errors.
pub const Event = union(enum) {
    /// Initial connection established (INFO/CONNECT handshake complete).
    /// Fired once on first connect, NOT on reconnect.
    connected: void,

    /// Connection lost. err is the I/O error that caused disconnect,
    /// or null if clean close.
    disconnected: struct { err: ?anyerror },

    /// Successfully reconnected after disconnect.
    /// Fired each time reconnection succeeds.
    reconnected: void,

    /// Connection permanently closed. No more events after this.
    /// Fired exactly once when client becomes unusable.
    closed: void,

    /// Slow consumer - subscription queue full, message dropped.
    /// sid identifies the affected subscription.
    slow_consumer: struct { sid: u64 },

    /// Async error that doesn't close connection.
    /// Includes permission violations, server errors, stale connection.
    err: struct { err: anyerror, msg: ?[]const u8 },

    /// Server entering lame duck mode (graceful shutdown).
    /// Client should prepare for eventual disconnect.
    lame_duck: void,
};

/// Type-erased event handler using std.mem.Allocator vtable pattern.
/// All callbacks are optional - only implement what you need.
///
/// Handler struct can contain references to external state:
/// ```zig
/// const MyHandler = struct {
///     app_state: *AppState,  // Reference to your state
///
///     pub fn onConnect(self: *@This()) void {
///         self.app_state.is_connected = true;
///     }
/// };
/// ```
pub const EventHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        onConnect: ?*const fn (*anyopaque) void = null,
        onDisconnect: ?*const fn (*anyopaque, ?anyerror) void = null,
        onReconnect: ?*const fn (*anyopaque) void = null,
        onClose: ?*const fn (*anyopaque) void = null,
        onError: ?*const fn (*anyopaque, anyerror) void = null,
        onLameDuck: ?*const fn (*anyopaque) void = null,
    };

    /// Create handler from concrete type using comptime.
    /// Only generates vtable entries for methods that exist on T.
    pub fn init(comptime T: type, ptr: *T) EventHandler {
        const gen = struct {
            fn onConnect(p: *anyopaque) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onConnect();
            }
            fn onDisconnect(p: *anyopaque, err: ?anyerror) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onDisconnect(err);
            }
            fn onReconnect(p: *anyopaque) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onReconnect();
            }
            fn onClose(p: *anyopaque) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onClose();
            }
            fn onError(p: *anyopaque, err: anyerror) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onError(err);
            }
            fn onLameDuck(p: *anyopaque) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onLameDuck();
            }
        };

        const vtable = comptime blk: {
            break :blk VTable{
                .onConnect = if (@hasDecl(T, "onConnect"))
                    gen.onConnect
                else
                    null,
                .onDisconnect = if (@hasDecl(T, "onDisconnect"))
                    gen.onDisconnect
                else
                    null,
                .onReconnect = if (@hasDecl(T, "onReconnect"))
                    gen.onReconnect
                else
                    null,
                .onClose = if (@hasDecl(T, "onClose"))
                    gen.onClose
                else
                    null,
                .onError = if (@hasDecl(T, "onError"))
                    gen.onError
                else
                    null,
                .onLameDuck = if (@hasDecl(T, "onLameDuck"))
                    gen.onLameDuck
                else
                    null,
            };
        };

        return .{
            .ptr = ptr,
            .vtable = &vtable,
        };
    }

    /// Dispatch connected event to handler.
    pub fn dispatchConnect(self: EventHandler) void {
        if (self.vtable.onConnect) |f| f(self.ptr);
    }

    /// Dispatch disconnected event to handler.
    pub fn dispatchDisconnect(self: EventHandler, err: ?anyerror) void {
        if (self.vtable.onDisconnect) |f| f(self.ptr, err);
    }

    /// Dispatch reconnected event to handler.
    pub fn dispatchReconnect(self: EventHandler) void {
        if (self.vtable.onReconnect) |f| f(self.ptr);
    }

    /// Dispatch closed event to handler.
    pub fn dispatchClose(self: EventHandler) void {
        if (self.vtable.onClose) |f| f(self.ptr);
    }

    /// Dispatch error event to handler.
    pub fn dispatchError(self: EventHandler, err: anyerror) void {
        if (self.vtable.onError) |f| f(self.ptr, err);
    }

    /// Dispatch lame duck event to handler.
    pub fn dispatchLameDuck(self: EventHandler) void {
        if (self.vtable.onLameDuck) |f| f(self.ptr);
    }
};

test "EventHandler vtable generation" {
    const FullHandler = struct {
        connect_count: u32 = 0,
        disconnect_count: u32 = 0,
        last_error: ?anyerror = null,

        pub fn onConnect(self: *@This()) void {
            self.connect_count += 1;
        }
        pub fn onDisconnect(self: *@This(), err: ?anyerror) void {
            self.disconnect_count += 1;
            self.last_error = err;
        }
        pub fn onReconnect(self: *@This()) void {
            self.connect_count += 1;
        }
        pub fn onClose(_: *@This()) void {}
        pub fn onError(self: *@This(), err: anyerror) void {
            self.last_error = err;
        }
        pub fn onLameDuck(_: *@This()) void {}
    };

    var handler = FullHandler{};
    const eh = EventHandler.init(FullHandler, &handler);

    try std.testing.expect(eh.vtable.onConnect != null);
    try std.testing.expect(eh.vtable.onDisconnect != null);
    try std.testing.expect(eh.vtable.onReconnect != null);
    try std.testing.expect(eh.vtable.onClose != null);
    try std.testing.expect(eh.vtable.onError != null);
    try std.testing.expect(eh.vtable.onLameDuck != null);

    // Test dispatch
    eh.dispatchConnect();
    try std.testing.expectEqual(@as(u32, 1), handler.connect_count);

    eh.dispatchDisconnect(error.OutOfMemory);
    try std.testing.expectEqual(@as(u32, 1), handler.disconnect_count);
    try std.testing.expectEqual(error.OutOfMemory, handler.last_error.?);
}

test "EventHandler partial implementation" {
    // Handler with only onConnect
    const MinimalHandler = struct {
        called: bool = false,

        pub fn onConnect(self: *@This()) void {
            self.called = true;
        }
    };

    var handler = MinimalHandler{};
    const eh = EventHandler.init(MinimalHandler, &handler);

    try std.testing.expect(eh.vtable.onConnect != null);
    try std.testing.expect(eh.vtable.onDisconnect == null);
    try std.testing.expect(eh.vtable.onReconnect == null);
    try std.testing.expect(eh.vtable.onClose == null);
    try std.testing.expect(eh.vtable.onError == null);
    try std.testing.expect(eh.vtable.onLameDuck == null);

    // Dispatch should work (and not crash for null callbacks)
    eh.dispatchConnect();
    try std.testing.expect(handler.called);

    eh.dispatchDisconnect(null); // Should be no-op
    eh.dispatchReconnect(); // Should be no-op
    eh.dispatchClose(); // Should be no-op
}

test "EventHandler with external state" {
    const AppState = struct {
        is_online: bool = false,
        reconnect_count: u32 = 0,
    };

    const MyHandler = struct {
        app: *AppState,

        pub fn onConnect(self: *@This()) void {
            self.app.is_online = true;
        }

        pub fn onDisconnect(self: *@This(), _: ?anyerror) void {
            self.app.is_online = false;
        }

        pub fn onReconnect(self: *@This()) void {
            self.app.is_online = true;
            self.app.reconnect_count += 1;
        }
    };

    var app_state = AppState{};
    var handler = MyHandler{ .app = &app_state };
    const eh = EventHandler.init(MyHandler, &handler);

    try std.testing.expect(!app_state.is_online);
    try std.testing.expectEqual(@as(u32, 0), app_state.reconnect_count);

    eh.dispatchConnect();
    try std.testing.expect(app_state.is_online);

    eh.dispatchDisconnect(error.BrokenPipe);
    try std.testing.expect(!app_state.is_online);

    eh.dispatchReconnect();
    try std.testing.expect(app_state.is_online);
    try std.testing.expectEqual(@as(u32, 1), app_state.reconnect_count);
}

test "Event union" {
    // Test all event variants
    const events = [_]Event{
        .{ .connected = {} },
        .{ .disconnected = .{ .err = error.BrokenPipe } },
        .{ .disconnected = .{ .err = null } },
        .{ .reconnected = {} },
        .{ .closed = {} },
        .{ .slow_consumer = .{ .sid = 42 } },
        .{ .err = .{ .err = Error.SlowConsumer, .msg = null } },
        .{ .err = .{ .err = Error.PermissionViolation, .msg = "test" } },
        .{ .lame_duck = {} },
    };

    for (events) |event| {
        switch (event) {
            .connected => {},
            .disconnected => |d| {
                // Test that err field exists and is optional
                if (d.err) |err| {
                    _ = @errorName(err);
                }
            },
            .reconnected => {},
            .closed => {},
            .slow_consumer => |sc| try std.testing.expect(sc.sid >= 0),
            .err => |e| {
                // Test that err is an anyerror
                _ = @errorName(e.err);
            },
            .lame_duck => {},
        }
    }
}
