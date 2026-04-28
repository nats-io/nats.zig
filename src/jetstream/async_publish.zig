//! Async JetStream publish with PubAckFuture.
//!
//! Non-blocking publish that returns a future for the ack.
//! Uses a shared reply subscription and correlation map to
//! match incoming acks to pending futures. Supports max
//! in-flight backpressure and per-ack timeouts.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;

const types = @import("types.zig");
const errors = @import("errors.zig");
const JetStream = @import("JetStream.zig");
const Io = std.Io;
const publish_headers = @import("publish_headers.zig");

/// Published ack future. Returned by publishAsync().
/// Call wait() to block until the ack arrives.
pub const PubAckFuture = struct {
    _done: std.atomic.Value(bool) =
        std.atomic.Value(bool).init(false),
    _ack: ?types.PubAck = null,
    _err: ?anyerror = null,
    _id_buf: [token_size]u8 = undefined,
    _allocator: Allocator,
    _stream_owned: ?[]u8 = null,
    _domain_owned: ?[]u8 = null,

    /// Blocks until the ack is received or timeout.
    pub fn wait(
        self: *PubAckFuture,
        timeout_ms: u32,
    ) !types.PubAck {
        std.debug.assert(timeout_ms > 0);
        var elapsed: u32 = 0;
        while (!self._done.load(.acquire)) {
            if (elapsed >= timeout_ms)
                return errors.Error.Timeout;
            sleepNs(1_000_000); // 1ms poll
            elapsed += 1;
        }

        if (self._err) |e| return e;
        return self._ack orelse
            errors.Error.Timeout;
    }

    /// Non-blocking check. Returns ack if ready.
    pub fn result(
        self: *const PubAckFuture,
    ) ?types.PubAck {
        if (!self._done.load(.acquire)) return null;
        return self._ack;
    }

    /// Returns the error if the ack failed.
    pub fn err(self: *const PubAckFuture) ?anyerror {
        if (!self._done.load(.acquire)) return null;
        return self._err;
    }

    /// Frees the future.
    pub fn deinit(self: *PubAckFuture) void {
        if (self._stream_owned) |stream| {
            self._allocator.free(stream);
            self._stream_owned = null;
        }
        if (self._domain_owned) |domain| {
            self._allocator.free(domain);
            self._domain_owned = null;
        }
        self._allocator.destroy(self);
    }
};

const token_size = 6;
const alphabet =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" ++
    "abcdefghijklmnopqrstuvwxyz";

/// Async publisher for JetStream. Created from a
/// JetStream context. Manages a shared reply
/// subscription and correlates incoming acks to
/// pending publish futures.
pub const AsyncPublisher = struct {
    js: *JetStream,
    client: *Client,
    allocator: Allocator,

    // Reply inbox: "<inbox_prefix>.<unique>."
    reply_prefix_buf: [128]u8 = undefined,
    reply_prefix_len: u8 = 0,

    // Callback subscription on reply_prefix + "*"
    sub: ?*Client.Sub = null,

    // Pending acks: id_str → *PubAckFuture
    mu: Io.Mutex = .init,
    pending: PendingMap = .empty,
    pending_count: std.atomic.Value(u32) =
        std.atomic.Value(u32).init(0),

    // Config
    max_pending: u32 = 256,
    ack_timeout_ms: u32 = 5000,

    const PendingMap = std.StringHashMapUnmanaged(
        *PubAckFuture,
    );

    /// Options for the async publisher.
    pub const Options = struct {
        max_pending: u32 = 256,
        ack_timeout_ms: u32 = 5000,
    };

    /// Creates an async publisher bound to the given
    /// JetStream context. The reply subscription is
    /// created lazily on first publish (the struct
    /// must have a stable address by then).
    pub fn init(
        js: *JetStream,
        opts: Options,
    ) !AsyncPublisher {
        std.debug.assert(js.timeout_ms > 0);
        const client = js.client;
        const allocator = js.allocator;

        var ap = AsyncPublisher{
            .js = js,
            .client = client,
            .allocator = allocator,
            .max_pending = opts.max_pending,
            .ack_timeout_ms = opts.ack_timeout_ms,
        };

        // Build reply prefix: _INBOX.<random>.
        const inbox = try client.newInbox();
        defer allocator.free(inbox);
        std.debug.assert(inbox.len > 0);
        const plen = @min(
            inbox.len,
            ap.reply_prefix_buf.len - 1,
        );
        @memcpy(
            ap.reply_prefix_buf[0..plen],
            inbox[0..plen],
        );
        ap.reply_prefix_buf[plen] = '.';
        ap.reply_prefix_len = @intCast(plen + 1);

        return ap;
    }

    /// Ensures the reply subscription exists. Called
    /// lazily on first publish when the struct has a
    /// stable address.
    fn ensureSubscribed(
        self: *AsyncPublisher,
    ) !void {
        if (self.sub != null) return;
        var sub_buf: [130]u8 = undefined;
        const sub_subj = std.fmt.bufPrint(
            &sub_buf,
            "{s}*",
            .{self.replyPrefix()},
        ) catch return errors.Error.SubjectTooLong;

        self.sub = try self.client.subscribe(
            sub_subj,
            Client.MsgHandler.init(
                AsyncPublisher,
                self,
            ),
        );
    }

    /// Returns the reply prefix slice.
    fn replyPrefix(
        self: *const AsyncPublisher,
    ) []const u8 {
        std.debug.assert(self.reply_prefix_len > 0);
        return self.reply_prefix_buf[0..self.reply_prefix_len];
    }

    /// Publishes a message asynchronously. Returns a
    /// future that resolves when the server acks.
    /// Blocks if max_pending is reached (backpressure).
    pub fn publish(
        self: *AsyncPublisher,
        subject: []const u8,
        payload: []const u8,
    ) !*PubAckFuture {
        std.debug.assert(subject.len > 0);
        return self.publishWithOpts(
            subject,
            payload,
            .{},
        );
    }

    /// Publishes with header options (msg-id, etc).
    pub fn publishWithOpts(
        self: *AsyncPublisher,
        subject: []const u8,
        payload: []const u8,
        opts: types.PublishOpts,
    ) !*PubAckFuture {
        std.debug.assert(subject.len > 0);

        try self.ensureSubscribed();

        // Backpressure: wait if too many pending
        var bp_elapsed: u32 = 0;
        while (self.pending_count.load(.acquire) >=
            self.max_pending)
        {
            if (bp_elapsed >= self.ack_timeout_ms)
                return errors.Error.Timeout;
            sleepNs(1_000_000);
            bp_elapsed += 1;
        }

        // Generate unique token
        var id: [token_size]u8 = undefined;
        self.client.io.random(&id);
        for (&id) |*b| {
            b.* = alphabet[@mod(b.*, alphabet.len)];
        }

        // Build reply subject: prefix + token
        var reply_buf: [140]u8 = undefined;
        const prefix = self.replyPrefix();
        @memcpy(
            reply_buf[0..prefix.len],
            prefix,
        );
        @memcpy(
            reply_buf[prefix.len..][0..token_size],
            &id,
        );
        const reply = reply_buf[0 .. prefix.len +
            token_size];

        // Create future
        const fut = try self.allocator.create(
            PubAckFuture,
        );
        fut.* = .{ ._allocator = self.allocator };
        @memcpy(&fut._id_buf, &id);

        // Register in pending map
        const io = self.client.io;
        const id_key = fut._id_buf[0..token_size];
        {
            try self.mu.lock(io);
            defer self.mu.unlock(io);
            self.pending.put(
                self.allocator,
                id_key,
                fut,
            ) catch {
                self.allocator.destroy(fut);
                return error.OutOfMemory;
            };
        }
        _ = self.pending_count.fetchAdd(1, .release);
        errdefer {
            self.mu.lock(io) catch {};
            const removed = self.pending.fetchRemove(id_key);
            self.mu.unlock(io);
            if (removed) |entry| {
                _ = self.pending_count.fetchSub(1, .release);
                self.allocator.destroy(entry.value);
            }
        }

        var hdrs: publish_headers.PublishHeaderSet = undefined;
        hdrs.populate(opts);

        // Publish with reply-to
        if (hdrs.count > 0) {
            try self.client
                .publishRequestWithHeaders(
                subject,
                reply,
                hdrs.slice(),
                payload,
            );
        } else {
            try self.client.publishRequest(
                subject,
                reply,
                payload,
            );
        }

        return fut;
    }

    /// Returns the number of pending acks.
    pub fn publishAsyncPending(
        self: *const AsyncPublisher,
    ) u32 {
        return self.pending_count.load(.acquire);
    }

    /// Blocks until all pending acks are resolved
    /// or timeout.
    pub fn waitComplete(
        self: *const AsyncPublisher,
        timeout_ms: u32,
    ) !void {
        std.debug.assert(timeout_ms > 0);
        var elapsed: u32 = 0;
        while (self.pending_count.load(.acquire) > 0) {
            if (elapsed >= timeout_ms)
                return errors.Error.Timeout;
            sleepNs(1_000_000);
            elapsed += 1;
        }
    }

    /// Callback handler for incoming ack messages.
    /// Routes acks to the correct PubAckFuture by
    /// extracting the token from the reply subject.
    pub fn onMessage(
        self: *AsyncPublisher,
        msg: *const Client.Message,
    ) void {
        const subj = msg.subject;
        const plen = self.reply_prefix_len;
        if (subj.len <= plen) return;
        const id = subj[plen..];

        const io = self.client.io;
        self.mu.lock(io) catch return;
        const entry = self.pending.fetchRemove(id);
        self.mu.unlock(io);

        const fut = if (entry) |e| e.value else return;
        defer _ = self.pending_count.fetchSub(1, .release);

        // Parse PubAck from message data
        if (msg.data.len == 0) {
            fut._err = errors.Error.NoResponders;
            fut._done.store(true, .release);
            return;
        }

        var parsed = types.jsonParse(
            types.PubAck,
            self.allocator,
            msg.data,
        ) catch {
            fut._err = errors.Error.JsonParseError;
            fut._done.store(true, .release);
            return;
        };
        defer parsed.deinit();

        if (parsed.value.@"error") |api_err| {
            if (api_err.code > 0) {
                fut._err = errors.Error.ApiError;
                fut._done.store(true, .release);
                return;
            }
        }

        const stream_owned: ?[]u8 = if (parsed.value.stream) |stream|
            self.allocator.dupe(u8, stream) catch {
                fut._err = error.OutOfMemory;
                fut._done.store(true, .release);
                return;
            }
        else
            null;

        const domain_owned: ?[]u8 = if (parsed.value.domain) |domain|
            self.allocator.dupe(u8, domain) catch {
                if (stream_owned) |stream| self.allocator.free(stream);
                fut._err = error.OutOfMemory;
                fut._done.store(true, .release);
                return;
            }
        else
            null;

        fut._stream_owned = stream_owned;
        fut._domain_owned = domain_owned;
        fut._ack = .{
            .stream = if (stream_owned) |stream| stream else null,
            .seq = parsed.value.seq,
            .duplicate = parsed.value.duplicate,
            .domain = if (domain_owned) |domain| domain else null,
        };
        fut._done.store(true, .release);
    }

    /// Cleans up the publisher. Unsubscribes and
    /// fails all pending futures.
    pub fn cleanup(self: *AsyncPublisher) void {
        if (self.sub) |s| {
            s.deinit();
            self.sub = null;
        }

        // Fail all pending futures
        const io = self.client.io;
        self.mu.lock(io) catch {};
        var it = self.pending.iterator();
        while (it.next()) |entry| {
            const fut = entry.value_ptr.*;
            fut._err = errors.Error.Timeout;
            fut._done.store(true, .release);
        }
        self.pending.clearAndFree(self.allocator);
        self.mu.unlock(io);
        self.pending_count.store(0, .release);
    }

    /// Alias for cleanup.
    pub fn deinit(self: *AsyncPublisher) void {
        self.cleanup();
    }
};

fn sleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}
