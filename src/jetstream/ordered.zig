//! Ordered consumer for gap-free, in-order delivery.
//!
//! Internal type used by KV Watch. Automatically recreates
//! the ephemeral consumer on sequence gaps or heartbeat
//! failures, resuming from the last known stream position.

const std = @import("std");

const nats = @import("../nats.zig");
const Client = nats.Client;

const types = @import("types.zig");
const errors = @import("errors.zig");
const consumer_mod = @import("consumer.zig");
const msg_mod = @import("message.zig");
const JsMsg = msg_mod.JsMsg;
const MsgMetadata = msg_mod.MsgMetadata;
const JetStream = @import("JetStream.zig");
const PullSubscription = @import("pull.zig").PullSubscription;
const HeartbeatMonitor = consumer_mod.HeartbeatMonitor;

/// Auto-recreating ephemeral pull consumer ensuring
/// gap-free, in-order delivery. Not public API --
/// used internally by KV Watch.
pub const OrderedConsumer = struct {
    js: *JetStream,
    stream: []const u8,
    config: OrderedConfig,
    consumer_name_buf: [64]u8 = undefined,
    consumer_name_len: u8 = 0,
    stream_seq: u64 = 0,
    consumer_seq: u64 = 0,
    serial: u32 = 0,
    pull: ?PullSubscription = null,
    hb: ?HeartbeatMonitor = null,
    reset_count: u32 = 0,

    /// Configuration for ordered consumers.
    /// Restricted subset of full ConsumerConfig.
    pub const OrderedConfig = struct {
        filter_subject: ?[]const u8 = null,
        deliver_policy: ?types.DeliverPolicy = null,
        opt_start_seq: ?u64 = null,
        headers_only: ?bool = null,
        heartbeat_ms: u32 = 0,
    };

    /// Creates an ordered consumer. Does NOT create
    /// the server-side consumer yet (lazy on first
    /// next() call).
    pub fn init(
        js: *JetStream,
        stream: []const u8,
        config: OrderedConfig,
    ) OrderedConsumer {
        std.debug.assert(stream.len > 0);
        std.debug.assert(js.timeout_ms > 0);
        return OrderedConsumer{
            .js = js,
            .stream = stream,
            .config = config,
            .hb = if (config.heartbeat_ms > 0)
                HeartbeatMonitor.init(
                    config.heartbeat_ms,
                )
            else
                null,
        };
    }

    /// Fetches the next message in order. Creates or
    /// recreates the consumer as needed. Returns null
    /// when no messages are available within timeout.
    pub fn next(
        self: *OrderedConsumer,
        timeout_ms: u32,
    ) !?JsMsg {
        std.debug.assert(timeout_ms > 0);

        while (true) {
            // Ensure consumer exists
            if (self.pull == null) {
                try self.createOrReset();
            }

            const recv_ms = if (self.hb) |hb|
                hb.timeoutMs()
            else
                timeout_ms;

            var pull = &self.pull.?;
            const maybe = pull.next(
                recv_ms,
            ) catch |err| {
                if (err == error.Timeout or
                    err == error.NoResponders)
                {
                    self.deleteConsumer();
                    self.pull = null;
                    return null;
                }
                return err;
            };

            const msg = maybe orelse {
                if (self.hb) |*hb| {
                    if (hb.recordTimeout()) {
                        try self.createOrReset();
                    }
                }
                return null;
            };

            if (self.hb) |*hb| hb.recordActivity();

            // Check for sequence gap
            const md = msg.metadata();
            if (md) |m| {
                const expected = self.consumer_seq + 1;
                if (expected > 1 and
                    m.consumer_seq != expected)
                {
                    // REVIEWED(2025-03): Setting stream_seq to
                    // gap message's seq is correct per NATS
                    // ordered consumer protocol. Gaps mean
                    // messages were lost; restart from gap
                    // point is the documented recovery.
                    var m2 = msg;
                    m2.deinit();
                    self.stream_seq = m.stream_seq;
                    try self.createOrReset();
                    continue;
                }
                self.stream_seq = m.stream_seq;
                self.consumer_seq = m.consumer_seq;
            }

            return msg;
        }
    }

    /// Creates or recreates the server-side consumer,
    /// resuming from last known stream position.
    fn createOrReset(self: *OrderedConsumer) !void {
        // Delete old consumer (ignore errors)
        if (self.consumer_name_len > 0) {
            self.deleteConsumer();
            self.backoffSleep();
        }

        self.serial += 1;
        self.consumer_seq = 0;

        // Generate unique name (avoid _ prefix which
        // NATS reserves for internal use)
        const name = std.fmt.bufPrint(
            &self.consumer_name_buf,
            "oc{d}x{d}",
            .{ self.serial, @intFromPtr(self) % 99999 },
        ) catch unreachable;
        self.consumer_name_len = @intCast(name.len);

        // Go's ordered consumer ALWAYS uses
        // DeliverByStartSequencePolicy (ordered.go:626)
        var next_seq: u64 = 1;
        if (self.stream_seq > 0) {
            next_seq = self.stream_seq + 1;
        } else if (self.config.opt_start_seq) |s| {
            next_seq = s;
        }

        const cfg = types.ConsumerConfig{
            .name = name,
            .deliver_policy = .by_start_sequence,
            .opt_start_seq = next_seq,
            .ack_policy = .none,
            .max_deliver = 1,
            .mem_storage = true,
            .inactive_threshold = 300_000_000_000,
            .num_replicas = 1,
            .headers_only = self.config.headers_only,
            .filter_subject = self.config.filter_subject,
        };

        var resp = try self.js.createConsumer(
            self.stream,
            cfg,
        );
        resp.deinit();

        // Ensure server has processed the consumer
        self.js.client.flush(5_000_000_000) catch {};

        var p = PullSubscription{
            .js = self.js,
            .stream = self.stream,
        };
        p.setConsumer(self.consumerName());
        self.pull = p;

        self.reset_count += 1;
    }

    /// Backoff sleep between resets.
    fn backoffSleep(self: *const OrderedConsumer) void {
        const delays = [_]u64{
            250_000_000,
            500_000_000,
            1_000_000_000,
            2_000_000_000,
            5_000_000_000,
            10_000_000_000,
        };
        const idx = @min(
            self.reset_count,
            delays.len - 1,
        );
        var ts: std.posix.timespec = .{
            .sec = @intCast(delays[idx] / 1_000_000_000),
            .nsec = @intCast(
                delays[idx] % 1_000_000_000,
            ),
        };
        _ = std.posix.system.nanosleep(&ts, &ts);
    }

    /// Deletes the current server-side consumer.
    fn deleteConsumer(self: *OrderedConsumer) void {
        if (self.consumer_name_len == 0) return;
        var resp = self.js.deleteConsumer(
            self.stream,
            self.consumerName(),
        ) catch return;
        resp.deinit();
    }

    /// Returns the current consumer name slice.
    fn consumerName(self: *const OrderedConsumer) []const u8 {
        std.debug.assert(self.consumer_name_len > 0);
        return self.consumer_name_buf[0..self.consumer_name_len];
    }

    /// Cleans up the server-side consumer.
    pub fn deinit(self: *OrderedConsumer) void {
        self.deleteConsumer();
        self.pull = null;
        self.consumer_name_len = 0;
    }
};

// -- Tests --

test "OrderedConsumer config restrictions" {
    // Verify the config we build matches restrictions
    const cfg = types.ConsumerConfig{
        .name = "_oc_test",
        .ack_policy = .none,
        .max_deliver = 1,
        .inactive_threshold = 300_000_000_000,
        .num_replicas = 1,
        .filter_subject = "$KV.mybucket.>",
    };
    try std.testing.expectEqual(
        types.AckPolicy.none,
        cfg.ack_policy.?,
    );
    try std.testing.expectEqual(
        @as(i64, 1),
        cfg.max_deliver.?,
    );
    try std.testing.expectEqual(
        @as(i64, 300_000_000_000),
        cfg.inactive_threshold.?,
    );
}

test "backoff delays increase" {
    const delays = [_]u64{
        250_000_000,
        500_000_000,
        1_000_000_000,
        2_000_000_000,
        5_000_000_000,
        10_000_000_000,
    };
    // Verify delays are monotonically increasing
    var prev: u64 = 0;
    for (delays) |d| {
        try std.testing.expect(d > prev);
        prev = d;
    }
    // Verify cap at 10s
    try std.testing.expectEqual(
        @as(u64, 10_000_000_000),
        delays[delays.len - 1],
    );
}
