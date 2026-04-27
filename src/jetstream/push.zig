//! JetStream push-based consumer subscription.
//!
//! Push consumers have a deliver_subject configured and the
//! server pushes messages to that subject. The client
//! subscribes using a callback and processes messages as
//! they arrive on the IO thread.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;

const types = @import("types.zig");
const errors = @import("errors.zig");
const consumer_mod = @import("consumer.zig");
const JsMsg = @import("message.zig").JsMsg;
const JsMsgHandler = consumer_mod.JsMsgHandler;
const JetStream = @import("JetStream.zig");

/// Push-based consumer subscription. Created after a
/// push consumer exists on the server. Subscribe to
/// the deliver_subject and process messages via
/// consume().
pub const PushSubscription = struct {
    js: *JetStream,
    stream: []const u8,
    consumer_buf: [48]u8 = undefined,
    consumer_len: u8 = 0,
    deliver_buf: [256]u8 = undefined,
    deliver_len: u16 = 0,
    deliver_group_buf: [64]u8 = undefined,
    deliver_group_len: u8 = 0,

    /// Returns consumer name.
    pub fn consumerName(
        self: *const PushSubscription,
    ) []const u8 {
        std.debug.assert(self.consumer_len > 0);
        return self.consumer_buf[0..self.consumer_len];
    }

    /// Returns the deliver subject.
    pub fn deliverSubject(
        self: *const PushSubscription,
    ) []const u8 {
        std.debug.assert(self.deliver_len > 0);
        return self.deliver_buf[0..self.deliver_len];
    }

    /// Sets consumer name.
    pub fn setConsumer(
        self: *PushSubscription,
        name: []const u8,
    ) void {
        std.debug.assert(name.len > 0);
        std.debug.assert(
            name.len <= self.consumer_buf.len,
        );
        @memcpy(
            self.consumer_buf[0..name.len],
            name,
        );
        self.consumer_len = @intCast(name.len);
    }

    /// Sets the deliver subject.
    pub fn setDeliverSubject(
        self: *PushSubscription,
        subj: []const u8,
    ) void {
        std.debug.assert(subj.len > 0);
        std.debug.assert(
            subj.len <= self.deliver_buf.len,
        );
        @memcpy(
            self.deliver_buf[0..subj.len],
            subj,
        );
        self.deliver_len = @intCast(subj.len);
    }

    /// Sets the deliver group (queue group).
    pub fn setDeliverGroup(
        self: *PushSubscription,
        group: []const u8,
    ) void {
        std.debug.assert(group.len > 0);
        std.debug.assert(
            group.len <= self.deliver_group_buf.len,
        );
        @memcpy(
            self.deliver_group_buf[0..group.len],
            group,
        );
        self.deliver_group_len = @intCast(group.len);
    }

    /// Options for push consumption.
    pub const ConsumeOpts = struct {
        /// Expected idle-heartbeat interval in ms.
        /// If no message or heartbeat arrives within
        /// 2x this interval, err_handler is called
        /// with error.NoHeartbeat. In normal use this
        /// should match the consumer's server-side
        /// idle_heartbeat configuration to avoid false
        /// positives during idle periods.
        heartbeat_ms: u32 = 0,
        err_handler: ?consumer_mod.ErrHandler = null,
    };

    /// Starts callback-based consumption on the
    /// deliver subject. Uses the client's native
    /// callback subscription (runs on IO thread).
    /// To stop: call the returned context's deinit().
    ///
    /// The handler receives `*JsMsg` with `owned = false`.
    /// Slice fields (data, subject, headers, reply_to)
    /// are valid ONLY during the callback; do not copy
    /// the struct out or save pointers past return.
    pub fn consume(
        self: *PushSubscription,
        handler: JsMsgHandler,
        opts: ConsumeOpts,
    ) !PushConsumeContext {
        std.debug.assert(self.deliver_len > 0);
        std.debug.assert(self.consumer_len > 0);

        const client = self.js.client;
        const subj = self.deliverSubject();

        // Allocate wrapper on heap so it outlives
        // this function. Stores the JsMsgHandler
        // and a pointer back to the client.
        const wrapper = try client.allocator.create(
            PushCallbackWrapper,
        );
        wrapper.* = .{
            .handler = handler,
            .client = client,
            .allocator = client.allocator,
            .err_handler = opts.err_handler,
            .last_activity_ns = std.atomic.Value(u64).init(
                getNowNs(client.io),
            ),
        };

        const qg: ?[]const u8 =
            if (self.deliver_group_len > 0)
                self.deliver_group_buf[0..self.deliver_group_len]
            else
                null;

        // Use client.subscribe (callback mode).
        // This runs callbackDrainFn on the IO thread.
        // Messages are dispatched via the wrapper.
        const sub = try client.queueSubscribe(
            subj,
            qg,
            Client.MsgHandler.init(
                PushCallbackWrapper,
                wrapper,
            ),
        );

        // Flush to ensure SUB reaches the server
        // before the caller creates the push consumer.
        try client.flush(5_000_000_000);

        var ctx = PushConsumeContext{
            .sub = sub,
            .wrapper = wrapper,
            .io = client.io,
        };
        if (opts.heartbeat_ms > 0) {
            ctx.monitor_future = client.io.async(
                pushHeartbeatMonitorTask,
                .{ wrapper, opts.heartbeat_ms },
            );
        }
        return ctx;
    }
};

/// Heap-allocated wrapper that bridges Client.MsgHandler
/// (receives *const Message) to JsMsgHandler (receives
/// *JsMsg with owned=false). Lives on the heap because the
/// callback subscription outlives the consume() call.
const PushCallbackWrapper = struct {
    handler: JsMsgHandler,
    client: *Client,
    allocator: Allocator,
    err_handler: ?consumer_mod.ErrHandler = null,
    last_activity_ns: std.atomic.Value(u64) =
        std.atomic.Value(u64).init(0),

    pub fn onMessage(
        self: *PushCallbackWrapper,
        msg: *const Client.Message,
    ) void {
        self.last_activity_ns.store(
            getNowNs(self.client.io),
            .release,
        );

        if (msg.status()) |code| {
            if (code == 100) {
                if (msg.reply_to) |reply| {
                    self.client.publish(reply, "") catch |err| {
                        if (self.err_handler) |eh| eh(err);
                    };
                }
                return;
            }
        }

        // Borrowed message. The underlying Client.Message
        // backing buffer is reclaimed by callbackDrainFn
        // after handler.dispatch() returns. owned=false
        // makes JsMsg.deinit() a no-op.
        var js_msg = JsMsg{
            .msg = msg.*,
            .client = self.client,
            .owned = false,
        };
        self.handler.dispatch(&js_msg);
    }
};

/// Context for controlling an active push consume.
/// Simpler than ConsumeContext -- just wraps the
/// subscription. Stopping = unsubscribing.
pub const PushConsumeContext = struct {
    sub: ?*Client.Sub,
    wrapper: *PushCallbackWrapper,
    monitor_future: ?std.Io.Future(void) = null,
    io: std.Io = undefined,

    /// Stops consumption. Safe to call before deinit.
    pub fn stop(self: *PushConsumeContext) void {
        if (self.monitor_future) |*future| {
            _ = future.cancel(self.io);
            self.monitor_future = null;
        }
        if (self.sub) |s| {
            s.deinit();
            self.sub = null;
        }
    }

    /// Stops (if not already) and frees resources.
    pub fn deinit(self: *PushConsumeContext) void {
        self.stop();
        self.wrapper.allocator.destroy(
            self.wrapper,
        );
    }
};

fn getNowNs(io: std.Io) u64 {
    const ts = std.Io.Timestamp.now(io, .awake);
    return @intCast(ts.nanoseconds);
}

fn pushHeartbeatMonitorTask(
    wrapper: *PushCallbackWrapper,
    heartbeat_ms: u32,
) void {
    std.debug.assert(heartbeat_ms > 0);
    const io = wrapper.client.io;
    const timeout_ns =
        @as(u64, heartbeat_ms) * 2 * std.time.ns_per_ms;
    var notified = false;

    while (true) {
        io.sleep(
            .fromMilliseconds(heartbeat_ms),
            .awake,
        ) catch |err| {
            if (err == error.Canceled) return;
            return;
        };

        const last =
            wrapper.last_activity_ns.load(.acquire);
        const now = getNowNs(io);
        if (now -| last >= timeout_ns) {
            if (!notified) {
                if (wrapper.err_handler) |eh| {
                    eh(errors.Error.NoHeartbeat);
                }
                notified = true;
            }
        } else {
            notified = false;
        }
    }
}
