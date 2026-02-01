//! Event Callbacks for NATS Client
//!
//! Type-erased event handler using comptime vtable pattern (like std.mem.Allocator).
//! Enables callbacks without closures, maintaining Zig's no-hidden-allocation guarantee.
//!
//! ## Architecture
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
    /// Failed to restore subscriptions after reconnect.
    /// User may need to re-subscribe manually.
    SubscriptionRestoreFailed,
    /// Message allocation failed (slab exhausted).
    AllocationFailed,
    /// Protocol parse error (malformed data skipped).
    ProtocolParseError,
    /// Subject too long for backup buffer (>256 bytes).
    SubjectTooLong,
    /// Queue group too long for backup buffer (>64 bytes).
    QueueGroupTooLong,
    /// Drain completed with failures (UNSUB or flush failed).
    DrainIncomplete,
    /// TCP_NODELAY socket option failed (performance impact).
    TcpNoDelayFailed,
    /// TCP receive buffer option failed (performance impact).
    TcpRcvBufFailed,
    /// URL too long (>256 bytes, would be truncated).
    UrlTooLong,
};

/// Events pushed from io_task to callback_task.
/// These represent connection lifecycle changes and async errors.
pub const Event = union(enum) {
    /// Initial connection established. Fired once, not on reconnect.
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
    lame_duck: void,

    /// Message allocation failed (slab exhausted). Rate-limited.
    alloc_failed: struct { sid: u64, count: u64 },

    /// Protocol parse error (malformed data recovered via CRLF skip).
    /// Rate-limited: fires on first error, then every 100k messages.
    protocol_error: struct { bytes_skipped: usize, count: u64 },

    /// New servers discovered via cluster INFO (connect_urls).
    /// count is the number of new servers added to the pool.
    discovered_servers: struct { count: u8 },

    /// Connection entering drain mode.
    /// Fired when drain() is called on the client.
    draining: void,

    /// Subscription auto-unsubscribe limit reached.
    /// Fired when a subscription hits its max messages limit.
    subscription_complete: struct { sid: u64 },
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
        onDiscoveredServers: ?*const fn (*anyopaque, u8) void = null,
        onDraining: ?*const fn (*anyopaque) void = null,
        onSubscriptionComplete: ?*const fn (*anyopaque, u64) void = null,
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
            fn onDiscoveredServers(p: *anyopaque, count: u8) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onDiscoveredServers(count);
            }
            fn onDraining(p: *anyopaque) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onDraining();
            }
            fn onSubscriptionComplete(p: *anyopaque, sid: u64) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onSubscriptionComplete(sid);
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
                .onDiscoveredServers = if (@hasDecl(T, "onDiscoveredServers"))
                    gen.onDiscoveredServers
                else
                    null,
                .onDraining = if (@hasDecl(T, "onDraining"))
                    gen.onDraining
                else
                    null,
                .onSubscriptionComplete = if (@hasDecl(T, "onSubscriptionComplete"))
                    gen.onSubscriptionComplete
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

    /// Dispatch discovered servers event to handler.
    pub fn dispatchDiscoveredServers(self: EventHandler, count: u8) void {
        if (self.vtable.onDiscoveredServers) |f| f(self.ptr, count);
    }

    /// Dispatch draining event to handler.
    pub fn dispatchDraining(self: EventHandler) void {
        if (self.vtable.onDraining) |f| f(self.ptr);
    }

    /// Dispatch subscription complete event to handler.
    pub fn dispatchSubscriptionComplete(self: EventHandler, sid: u64) void {
        if (self.vtable.onSubscriptionComplete) |f| f(self.ptr, sid);
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

    eh.dispatchConnect();
    try std.testing.expectEqual(@as(u32, 1), handler.connect_count);

    eh.dispatchDisconnect(error.OutOfMemory);
    try std.testing.expectEqual(@as(u32, 1), handler.disconnect_count);
    try std.testing.expectEqual(error.OutOfMemory, handler.last_error.?);
}

test "EventHandler partial implementation" {
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
        .{ .alloc_failed = .{ .sid = 1, .count = 5 } },
        .{ .protocol_error = .{ .bytes_skipped = 128, .count = 3 } },
        .{ .discovered_servers = .{ .count = 3 } },
        .{ .draining = {} },
        .{ .subscription_complete = .{ .sid = 42 } },
    };

    for (events) |event| {
        switch (event) {
            .connected => {},
            .disconnected => |d| {
                if (d.err) |err| {
                    _ = @errorName(err);
                }
            },
            .reconnected => {},
            .closed => {},
            .slow_consumer => |sc| try std.testing.expect(sc.sid >= 0),
            .err => |e| {
                _ = @errorName(e.err);
            },
            .lame_duck => {},
            .alloc_failed => |af| {
                try std.testing.expect(af.sid > 0);
                try std.testing.expect(af.count > 0);
            },
            .protocol_error => |pe| {
                try std.testing.expect(pe.bytes_skipped > 0);
                try std.testing.expect(pe.count > 0);
            },
            .discovered_servers => |ds| {
                try std.testing.expect(ds.count > 0);
            },
            .draining => {},
            .subscription_complete => |sc| {
                try std.testing.expect(sc.sid > 0);
            },
        }
    }
}
