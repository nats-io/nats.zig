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
const JsMsg = @import("message.zig").JsMsg;
const JetStream = @import("JetStream.zig");

/// Pull-based consumer subscription.
pub const PullSubscription = struct {
    js: *JetStream,
    stream: []const u8,
    consumer: []const u8,

    pub const FetchOpts = struct {
        max_messages: u32 = 1,
        timeout_ms: u32 = 5000,
    };

    /// Fetches messages from the consumer. Returns a
    /// FetchResult that owns the messages. Caller must
    /// call `deinit()` on the result when done.
    pub fn fetch(
        self: *PullSubscription,
        opts: FetchOpts,
    ) !FetchResult {
        std.debug.assert(opts.max_messages > 0);
        std.debug.assert(self.stream.len > 0);
        std.debug.assert(self.consumer.len > 0);

        const client = self.js.client;
        const allocator = self.js.allocator;

        var sub = try client.subscribeSync(
            "_INBOX_JS.>",
        );
        defer sub.deinit();

        const inbox = sub.subject;

        var subj_buf: [512]u8 = undefined;
        const prefix = self.js.apiPrefix();
        const pull_subj = std.fmt.bufPrint(
            &subj_buf,
            "{s}CONSUMER.MSG.NEXT.{s}.{s}",
            .{ prefix, self.stream, self.consumer },
        ) catch return errors.Error.SubjectTooLong;

        const expires_ns: i64 = @as(i64, @intCast(
            opts.timeout_ms,
        )) * 1_000_000;
        const pull_req = types.PullRequest{
            .batch = @intCast(opts.max_messages),
            .expires = expires_ns,
        };
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

        var msgs: std.ArrayList(JsMsg) = .empty;
        errdefer {
            for (msgs.items) |*m| m.deinit();
            msgs.deinit(allocator);
        }

        var collected: u32 = 0;
        while (collected < opts.max_messages) {
            const maybe_msg = sub.nextMsgTimeout(
                opts.timeout_ms,
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
