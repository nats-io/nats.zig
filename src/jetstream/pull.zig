//! JetStream pull-based subscription.
//!
//! Implements fetch-based message consumption: subscribe to a
//! temporary inbox, publish a pull request, collect messages
//! until batch complete or timeout/status signals.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;

const types = @import("types.zig");
const errors = @import("errors.zig");
const consumer_mod = @import("consumer.zig");
const JsMsg = @import("message.zig").JsMsg;
const JetStream = @import("JetStream.zig");

const JsMsgHandler = consumer_mod.JsMsgHandler;
const ConsumeContext = consumer_mod.ConsumeContext;
const ConsumeOpts = consumer_mod.ConsumeOpts;
const HeartbeatMonitor = consumer_mod.HeartbeatMonitor;

fn returnsErrorUnion(comptime f: anytype) bool {
    const ret = @typeInfo(@TypeOf(f)).@"fn".return_type orelse return false;
    return switch (@typeInfo(ret)) {
        .error_union => true,
        else => false,
    };
}

/// Pull-based consumer subscription.
pub const PullSubscription = struct {
    js: *JetStream,
    stream: []const u8,
    /// Inline consumer name buffer (avoids dangling
    /// slices from external buffers).
    consumer_buf: [48]u8 = undefined,
    consumer_len: u8 = 0,

    /// Returns consumer name as a slice into the
    /// inline buffer. Safe after move/copy.
    pub fn consumerName(
        self: *const PullSubscription,
    ) []const u8 {
        std.debug.assert(self.consumer_len > 0);
        return self.consumer_buf[0..self.consumer_len];
    }

    /// Sets the consumer name from a source slice.
    pub fn setConsumer(
        self: *PullSubscription,
        name: []const u8,
    ) errors.Error!void {
        try JetStream.validateName(name);
        if (name.len > self.consumer_buf.len) {
            return errors.Error.NameTooLong;
        }
        @memcpy(
            self.consumer_buf[0..name.len],
            name,
        );
        self.consumer_len = @intCast(name.len);
    }

    /// Options for pull-based message fetching.
    pub const FetchOpts = struct {
        max_messages: u32 = 1,
        timeout_ms: u32 = 5000,
    };

    /// Fetches messages from the consumer. Returns a
    /// FetchResult that owns the messages. Caller must
    /// call `deinit()` on the result when done.
    /// Auto-configures 5s heartbeat for requests > 10s
    /// (matching Go client behavior).
    pub fn fetch(
        self: *PullSubscription,
        opts: FetchOpts,
    ) !FetchResult {
        std.debug.assert(opts.max_messages > 0);
        std.debug.assert(self.stream.len > 0);
        std.debug.assert(self.consumer_len > 0);
        // Auto-heartbeat for long requests (Go default)
        const hb: ?i64 = if (opts.timeout_ms > 10000)
            5_000_000_000
        else
            null;
        return self.fetchInternal(.{
            .batch = @intCast(opts.max_messages),
            .expires = msToNs(opts.timeout_ms),
            .idle_heartbeat = hb,
        }, opts.timeout_ms);
    }

    /// Fetches with no_wait: returns immediately with
    /// whatever is available (may be 0 messages).
    pub fn fetchNoWait(
        self: *PullSubscription,
        max_messages: u32,
    ) !FetchResult {
        std.debug.assert(max_messages > 0);
        std.debug.assert(self.stream.len > 0);
        std.debug.assert(self.consumer_len > 0);
        return self.fetchInternal(.{
            .batch = @intCast(max_messages),
            .no_wait = true,
        }, 2000);
    }

    /// Fetches up to max_bytes worth of messages.
    pub fn fetchBytes(
        self: *PullSubscription,
        max_bytes: u32,
        opts: FetchOpts,
    ) !FetchResult {
        std.debug.assert(max_bytes > 0);
        std.debug.assert(self.stream.len > 0);
        return self.fetchInternal(.{
            .batch = @intCast(opts.max_messages),
            .max_bytes = @intCast(max_bytes),
            .expires = msToNs(opts.timeout_ms),
        }, opts.timeout_ms);
    }

    /// Fetches a single message. Returns null on timeout.
    pub fn next(
        self: *PullSubscription,
        timeout_ms: u32,
    ) !?JsMsg {
        std.debug.assert(timeout_ms > 0);
        std.debug.assert(self.stream.len > 0);
        var result = try self.fetchInternal(.{
            .batch = 1,
            .expires = msToNs(timeout_ms),
        }, timeout_ms);
        if (result.messages.len == 0) {
            result.deinit();
            return null;
        }
        std.debug.assert(result.messages.len == 1);
        const msg = result.messages[0];
        result.allocator.free(result.messages);
        return msg;
    }

    /// Internal fetch with arbitrary PullRequest params.
    fn fetchInternal(
        self: *PullSubscription,
        pull_req: types.PullRequest,
        timeout_ms: u32,
    ) !FetchResult {
        const client = self.js.client;
        const allocator = self.js.allocator;

        const inbox = try client.newInbox();
        defer allocator.free(inbox);

        var sub = try client.subscribeSync(inbox);
        defer sub.deinit();

        var subj_buf: [512]u8 = undefined;
        const prefix = self.js.apiPrefix();
        const pull_subj = std.fmt.bufPrint(
            &subj_buf,
            "{s}CONSUMER.MSG.NEXT.{s}.{s}",
            .{ prefix, self.stream, self.consumerName() },
        ) catch return errors.Error.SubjectTooLong;

        const payload = try types.jsonStringify(
            allocator,
            pull_req,
        );
        defer allocator.free(payload);

        try client.publishRequest(
            pull_subj,
            inbox,
            payload,
        );
        try client.flush(5_000_000_000);

        const batch: u32 = if (pull_req.batch) |b|
            @intCast(b)
        else
            1;

        var msgs: std.ArrayList(JsMsg) = .empty;
        errdefer {
            for (msgs.items) |*m| m.deinit();
            msgs.deinit(allocator);
        }

        var collected: u32 = 0;
        while (collected < batch) {
            const maybe_msg = sub.nextMsgTimeout(
                timeout_ms,
            ) catch |err| {
                if (collected > 0) break;
                return err;
            };
            const msg = maybe_msg orelse break;

            if (msg.status()) |code| {
                msg.deinit();
                switch (code) {
                    404, 408, 409 => break,
                    100 => continue,
                    else => break,
                }
            }

            try msgs.append(allocator, JsMsg{
                .msg = msg,
                .client = client,
            });
            collected += 1;
        }

        return FetchResult{
            .messages = try msgs.toOwnedSlice(
                allocator,
            ),
            .allocator = allocator,
        };
    }

    fn msToNs(ms: u32) i64 {
        return @as(i64, @intCast(ms)) * 1_000_000;
    }

    /// Creates a message iterator for continuous pull
    /// consumption. Returns a MessagesContext whose
    /// `next()` method yields one JsMsg at a time.
    /// Caller must call `deinit()` when done.
    pub fn messages(
        self: *PullSubscription,
        opts: ConsumeOpts,
    ) !MessagesContext {
        std.debug.assert(self.stream.len > 0);
        std.debug.assert(self.consumer_len > 0);
        std.debug.assert(opts.max_messages > 0);
        std.debug.assert(opts.expires_ms > 0);
        // heartbeat must be less than expires
        std.debug.assert(
            opts.heartbeat_ms == 0 or
                opts.heartbeat_ms < opts.expires_ms,
        );

        const client = self.js.client;
        const inbox = try client.newInbox();
        defer client.allocator.free(inbox);

        const sub = try client.subscribeSync(inbox);

        return MessagesContext{
            .pull = self,
            .sub = sub,
            .opts = opts,
            .hb = if (opts.heartbeat_ms > 0)
                HeartbeatMonitor.init(opts.heartbeat_ms)
            else
                null,
        };
    }

    /// Starts continuous callback-based consumption.
    /// Messages are dispatched to the handler in a
    /// background task. Returns a ConsumeContext for
    /// stop/drain control. Caller must call `deinit()`
    /// on the returned context when done.
    pub fn consume(
        self: *PullSubscription,
        handler: JsMsgHandler,
        opts: ConsumeOpts,
    ) !ConsumeContext {
        std.debug.assert(self.stream.len > 0);
        std.debug.assert(self.consumer_len > 0);
        std.debug.assert(opts.max_messages > 0);
        std.debug.assert(opts.expires_ms > 0);
        std.debug.assert(
            opts.heartbeat_ms == 0 or
                opts.heartbeat_ms < opts.expires_ms,
        );

        const client = self.js.client;
        const inbox = try client.newInbox();
        defer client.allocator.free(inbox);

        const sub = try client.subscribeSync(inbox);
        errdefer sub.deinit();

        // Issue initial pull request
        try issuePull(
            self.js,
            client,
            sub.subject,
            self.stream,
            self.consumerName(),
            opts,
        );

        const io = client.io;
        const state = try client.allocator.create(
            std.atomic.Value(ConsumeContext.State),
        );
        errdefer client.allocator.destroy(state);
        state.* = std.atomic.Value(ConsumeContext.State).init(.running);

        var ctx = ConsumeContext{
            ._io = io,
            ._shared_state = state,
            ._allocator = client.allocator,
        };

        ctx._task = io.async(
            consumeDrainTask,
            .{
                self.js,
                client,
                sub,
                handler,
                opts,
                state,
                self.stream,
                self.consumerName(),
            },
        );

        return ctx;
    }

    /// Result of a fetch operation.
    pub const FetchResult = struct {
        messages: []JsMsg,
        allocator: Allocator,

        /// Returns the number of messages fetched.
        pub fn count(self: *const FetchResult) usize {
            return self.messages.len;
        }

        /// Frees all messages and the backing slice.
        pub fn deinit(self: *FetchResult) void {
            for (self.messages) |*m| m.deinit();
            self.allocator.free(self.messages);
        }
    };
};

test "PullSubscription setConsumer reports invalid input at runtime" {
    try std.testing.expect(returnsErrorUnion(PullSubscription.setConsumer));
}

/// Iterator for continuous pull-based consumption.
/// Each call to `next()` returns a single JsMsg.
/// Automatically issues new pull requests when the
/// current batch is exhausted. Monitors heartbeats
/// when configured (heartbeat_ms > 0 in ConsumeOpts).
pub const MessagesContext = struct {
    pull: *PullSubscription,
    sub: *Client.Sub,
    opts: ConsumeOpts,
    hb: ?HeartbeatMonitor = null,
    active: bool = true,
    delivered: u32 = 0,
    batch_pending: bool = false,

    /// Returns the next message, or null on timeout.
    /// Issues pull requests automatically. Caller owns
    /// the returned JsMsg and must call ack + deinit.
    /// Returns error.NoHeartbeat if heartbeats stop.
    pub fn next(self: *MessagesContext) !?JsMsg {
        std.debug.assert(self.active);
        const client = self.pull.js.client;
        const recv_ms = if (self.hb) |hb|
            hb.timeoutMs()
        else
            self.opts.expires_ms;

        // Issue pull if needed
        if (!self.batch_pending) {
            try issuePull(
                self.pull.js,
                client,
                self.sub.subject,
                self.pull.stream,
                self.pull.consumerName(),
                self.opts,
            );
            self.batch_pending = true;
            self.delivered = 0;
        }

        while (self.active) {
            const maybe = self.sub.nextMsgTimeout(
                recv_ms,
            ) catch |err| {
                self.batch_pending = false;
                return err;
            };
            const msg = maybe orelse {
                // Receive timed out -- check heartbeat
                if (self.hb) |*hb| {
                    if (hb.recordTimeout())
                        return errors.Error.NoHeartbeat;
                }
                self.batch_pending = false;
                return null;
            };

            // Any message resets heartbeat monitor
            if (self.hb) |*hb| hb.recordActivity();

            if (msg.status()) |code| {
                if (code == 100) {
                    if (msg.reply_to) |reply| {
                        client.publish(
                            reply,
                            "",
                        ) catch {};
                    }
                    msg.deinit();
                    continue;
                }
                msg.deinit();
                switch (code) {
                    404, 408 => {
                        self.batch_pending = false;
                        return null;
                    },
                    409 => {
                        self.batch_pending = false;
                        return null;
                    },
                    else => {
                        self.batch_pending = false;
                        return null;
                    },
                }
            }

            self.delivered += 1;

            const threshold = self.opts.max_messages *
                self.opts.threshold_pct / 100;
            if (self.delivered >= threshold) {
                issuePull(
                    self.pull.js,
                    client,
                    self.sub.subject,
                    self.pull.stream,
                    self.pull.consumerName(),
                    self.opts,
                ) catch {};
                self.delivered = 0;
            }

            return JsMsg{
                .msg = msg,
                .client = client,
            };
        }

        return null;
    }

    /// Stops the iterator. No more messages after this.
    pub fn stop(self: *MessagesContext) void {
        std.debug.assert(self.active);
        self.active = false;
    }

    /// Frees the underlying subscription.
    pub fn deinit(self: *MessagesContext) void {
        self.active = false;
        self.sub.deinit();
    }
};

/// Issues a pull request to the server.
fn issuePull(
    js: *JetStream,
    client: *Client,
    inbox: []const u8,
    stream: []const u8,
    consumer_name: []const u8,
    opts: ConsumeOpts,
) !void {
    std.debug.assert(inbox.len > 0);
    std.debug.assert(stream.len > 0);
    std.debug.assert(
        opts.heartbeat_ms == 0 or
            opts.heartbeat_ms < opts.expires_ms,
    );

    var subj_buf: [512]u8 = undefined;
    const prefix = js.apiPrefix();
    const pull_subj = std.fmt.bufPrint(
        &subj_buf,
        "{s}CONSUMER.MSG.NEXT.{s}.{s}",
        .{ prefix, stream, consumer_name },
    ) catch return errors.Error.SubjectTooLong;

    const hb_ns: ?i64 = if (opts.heartbeat_ms > 0)
        PullSubscription.msToNs(opts.heartbeat_ms)
    else
        null;
    const pull_req = types.PullRequest{
        .batch = @intCast(opts.max_messages),
        .expires = PullSubscription.msToNs(
            opts.expires_ms,
        ),
        .idle_heartbeat = hb_ns,
    };
    const payload = try types.jsonStringify(
        js.allocator,
        pull_req,
    );
    defer js.allocator.free(payload);

    try client.publishRequest(
        pull_subj,
        inbox,
        payload,
    );
    try client.flush(5_000_000_000);
}

/// Background task for callback-based consume().
fn consumeDrainTask(
    js: *JetStream,
    client: *Client,
    sub: *Client.Sub,
    handler: JsMsgHandler,
    opts: ConsumeOpts,
    state: *std.atomic.Value(ConsumeContext.State),
    stream: []const u8,
    consumer_name: []const u8,
) void {
    defer {
        sub.deinit();
        state.store(.stopped, .release);
    }

    var hb: ?HeartbeatMonitor = if (opts.heartbeat_ms > 0)
        HeartbeatMonitor.init(opts.heartbeat_ms)
    else
        null;
    const recv_ms = if (hb) |h|
        h.timeoutMs()
    else
        opts.expires_ms;

    var delivered: u32 = 0;

    while (state.load(.acquire) == .running or
        state.load(.acquire) == .draining)
    {
        const maybe = sub.nextMsgTimeout(
            recv_ms,
        ) catch |err| {
            if (opts.err_handler) |eh| eh(err);
            if (state.load(.acquire) == .draining) break;
            issuePull(
                js,
                client,
                sub.subject,
                stream,
                consumer_name,
                opts,
            ) catch break;
            continue;
        };
        const msg = maybe orelse {
            if (state.load(.acquire) == .draining) break;
            // Check heartbeat
            if (hb) |*h| {
                if (h.recordTimeout()) {
                    if (opts.err_handler) |eh|
                        eh(errors.Error.NoHeartbeat);
                    break;
                }
            }
            issuePull(
                js,
                client,
                sub.subject,
                stream,
                consumer_name,
                opts,
            ) catch break;
            delivered = 0;
            continue;
        };

        if (hb) |*h| h.recordActivity();

        if (msg.status()) |code| {
            if (code == 100) {
                if (msg.reply_to) |reply| {
                    client.publish(
                        reply,
                        "",
                    ) catch {};
                }
                msg.deinit();
                continue;
            }
            msg.deinit();
            switch (code) {
                404, 408 => {
                    if (state.load(.acquire) == .draining) break;
                    // Re-issue pull (batch expired)
                    issuePull(
                        js,
                        client,
                        sub.subject,
                        stream,
                        consumer_name,
                        opts,
                    ) catch break;
                    delivered = 0;
                    continue;
                },
                409 => break,
                else => continue,
            }
        }

        // REVIEWED(2025-03): Stack-local JsMsg is intentional.
        // handler.dispatch() is synchronous — handler must
        // process before return. Avoids allocation per msg.
        var js_msg = JsMsg{
            .msg = msg,
            .client = client,
        };
        handler.dispatch(&js_msg);
        delivered += 1;

        const threshold = opts.max_messages *
            opts.threshold_pct / 100;
        if (delivered >= threshold) {
            issuePull(
                js,
                client,
                sub.subject,
                stream,
                consumer_name,
                opts,
            ) catch {};
            delivered = 0;
        }
    }
}
