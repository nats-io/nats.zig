//! Shared consumer abstractions for JetStream pull and push.
//!
//! Types here are designed for reuse across consumption modes.
//! Pull consumers (pull.zig) use them now; push consumers
//! (push.zig) will import them in v1.1 without changes.

const std = @import("std");
const JsMsg = @import("message.zig").JsMsg;

/// Callback handler for JetStream message consumption.
/// Comptime vtable pattern matching Client.MsgHandler but
/// taking `*JsMsg` instead of `*const Message`.
pub const JsMsgHandler = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        onMessage: *const fn (
            *anyopaque,
            *JsMsg,
        ) void,
    };

    /// Creates a handler from a concrete type via comptime.
    /// The type must have `onMessage(self, *JsMsg) void`.
    pub fn init(
        comptime T: type,
        ptr: *T,
    ) JsMsgHandler {
        const gen = struct {
            fn onMessage(
                p: *anyopaque,
                msg: *JsMsg,
            ) void {
                const self: *T = @ptrCast(
                    @alignCast(p),
                );
                self.onMessage(msg);
            }
        };
        return .{
            .ptr = ptr,
            .vtable = &.{
                .onMessage = gen.onMessage,
            },
        };
    }

    /// Creates a handler from a plain function pointer.
    pub fn initFn(
        func: *const fn (*JsMsg) void,
    ) JsMsgHandler {
        const gen = struct {
            fn onMessage(
                p: *anyopaque,
                msg: *JsMsg,
            ) void {
                const f: *const fn (*JsMsg) void =
                    @ptrCast(@alignCast(p));
                f(msg);
            }
        };
        return .{
            .ptr = @ptrCast(@constCast(func)),
            .vtable = &.{
                .onMessage = gen.onMessage,
            },
        };
    }

    /// Dispatches a message to the handler.
    pub fn dispatch(
        self: JsMsgHandler,
        msg: *JsMsg,
    ) void {
        std.debug.assert(self.ptr != undefined_ptr);
        self.vtable.onMessage(self.ptr, msg);
    }

    const undefined_ptr: *anyopaque = @ptrFromInt(
        std.math.maxInt(usize),
    );
};

/// Options for continuous message consumption.
/// Shared by pull and (future) push consumers.
pub const ConsumeOpts = struct {
    max_messages: u32 = 500,
    max_bytes: ?u32 = null,
    /// Idle heartbeat interval in ms. 0 = disabled.
    /// When enabled, must be less than expires_ms.
    /// Server sends status 100 at this interval when
    /// idle. Client detects stale connection after 2
    /// consecutive misses.
    heartbeat_ms: u32 = 0,
    threshold_pct: u8 = 50,
    expires_ms: u32 = 30000,
    err_handler: ?ErrHandler = null,
};

/// Error callback for consume operations.
pub const ErrHandler = *const fn (anyerror) void;

/// Controls an active consume() operation.
/// Standalone struct -- NOT coupled to pull or push
/// specifics. Both modes reuse the same context type.
pub const ConsumeContext = struct {
    _state: std.atomic.Value(State) =
        std.atomic.Value(State).init(.running),
    _task: ?std.Io.Future(void) = null,
    _io: std.Io = undefined,
    _thread: ?std.Thread = null,

    pub const State = enum(u8) {
        running = 0,
        draining = 1,
        stopped = 2,
    };

    /// Reads the current state atomically.
    pub fn state(self: *const ConsumeContext) State {
        return self._state.load(.acquire);
    }

    /// Stops consumption immediately. Buffered messages
    /// that have not been dispatched are discarded.
    pub fn stop(self: *ConsumeContext) void {
        std.debug.assert(self.state() != .stopped);
        self._state.store(.stopped, .release);
    }

    /// Signals the consumer to process remaining buffered
    /// messages and then stop.
    pub fn drain(self: *ConsumeContext) void {
        std.debug.assert(self.state() == .running);
        self._state.store(.draining, .release);
    }

    /// Returns true if consumption has fully stopped.
    pub fn closed(self: *const ConsumeContext) bool {
        return self.state() == .stopped;
    }

    /// Stops the background task and cleans up. The
    /// background task handles its own sub cleanup.
    pub fn deinit(self: *ConsumeContext) void {
        self._state.store(.stopped, .release);
        if (self._thread) |t| {
            t.join();
            self._thread = null;
        } else if (self._task) |*t| {
            t.cancel(self._io);
            self._task = null;
        }
    }
};

/// Tracks idle heartbeat health for pull/push consumers.
/// The server sends status 100 heartbeats at the configured
/// interval. If we miss `max_misses` consecutive heartbeats
/// (each window = 2x interval), the connection is stale.
///
/// Usage: set receive timeout to `timeoutMs()`, call
/// `recordActivity()` on any message, `recordTimeout()`
/// on receive timeout. Reusable by pull, push, and
/// ordered consumers.
pub const HeartbeatMonitor = struct {
    heartbeat_ms: u32,
    misses: u32 = 0,
    max_misses: u32 = 2,

    /// Creates a monitor for the given heartbeat interval.
    pub fn init(heartbeat_ms: u32) HeartbeatMonitor {
        std.debug.assert(heartbeat_ms > 0);
        return .{ .heartbeat_ms = heartbeat_ms };
    }

    /// Returns the timeout (ms) to use for receive ops.
    /// Set to 2x heartbeat interval so one missed heartbeat
    /// doesn't immediately trigger an error.
    pub fn timeoutMs(
        self: *const HeartbeatMonitor,
    ) u32 {
        std.debug.assert(self.heartbeat_ms > 0);
        return self.heartbeat_ms *| 2;
    }

    /// Call when any message or heartbeat is received.
    /// Resets the miss counter.
    pub fn recordActivity(
        self: *HeartbeatMonitor,
    ) void {
        self.misses = 0;
    }

    /// Call when a receive times out with no data.
    /// Returns true if heartbeat is considered lost.
    pub fn recordTimeout(
        self: *HeartbeatMonitor,
    ) bool {
        self.misses += 1;
        return self.misses >= self.max_misses;
    }

    /// Returns true if heartbeat is currently healthy.
    pub fn isHealthy(
        self: *const HeartbeatMonitor,
    ) bool {
        return self.misses < self.max_misses;
    }
};

// -- Tests --

test "JsMsgHandler dispatch with struct" {
    const Counter = struct {
        count: u32 = 0,
        pub fn onMessage(self: *@This(), _: *JsMsg) void {
            self.count += 1;
        }
    };

    var counter = Counter{};
    const handler = JsMsgHandler.init(Counter, &counter);

    // Create a minimal JsMsg for testing dispatch
    var msg = JsMsg{
        .msg = undefined,
        .client = undefined,
    };
    handler.dispatch(&msg);
    handler.dispatch(&msg);

    try std.testing.expectEqual(@as(u32, 2), counter.count);
}

test "ConsumeContext state transitions" {
    var ctx = ConsumeContext{};
    try std.testing.expect(!ctx.closed());
    try std.testing.expectEqual(
        ConsumeContext.State.running,
        ctx.state(),
    );

    ctx.drain();
    try std.testing.expectEqual(
        ConsumeContext.State.draining,
        ctx.state(),
    );
    try std.testing.expect(!ctx.closed());

    ctx._state.store(.running, .release);
    ctx.stop();
    try std.testing.expect(ctx.closed());
    try std.testing.expectEqual(
        ConsumeContext.State.stopped,
        ctx.state(),
    );
}

test "HeartbeatMonitor timeout detection" {
    var hb = HeartbeatMonitor.init(5000);
    try std.testing.expectEqual(
        @as(u32, 10000),
        hb.timeoutMs(),
    );
    try std.testing.expect(hb.isHealthy());

    // First timeout: not yet lost
    try std.testing.expect(!hb.recordTimeout());
    try std.testing.expect(hb.isHealthy());

    // Second timeout: heartbeat lost
    try std.testing.expect(hb.recordTimeout());
    try std.testing.expect(!hb.isHealthy());

    // Activity resets
    hb.recordActivity();
    try std.testing.expect(hb.isHealthy());
    try std.testing.expectEqual(@as(u32, 0), hb.misses);
}
