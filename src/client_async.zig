//! Async NATS Client for Concurrent Subscriptions
//!
//! Use ClientAsync when you need multiple subscriptions receiving messages
//! concurrently on a single connection. For simple sync pub/sub, use Client.
//!
//! Key differences from Client:
//! - Dedicated reader task routes messages to per-subscription Io.Queue
//! - Multiple subscriptions can call next() concurrently
//! - Reader task starts automatically on connect

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

const protocol = @import("protocol.zig");
const Parser = protocol.Parser;
const OwnedServerInfo = protocol.OwnedServerInfo;

const connection = @import("connection.zig");
const State = connection.State;

const pubsub = @import("pubsub.zig");
const subscription_mod = @import("pubsub/subscription.zig");
const memory = @import("memory.zig");
const SidMap = memory.SidMap;
const client_mod = @import("client.zig");

/// Message type (re-exported for convenience).
pub const Message = subscription_mod.Message;

/// Fixed subscription limits.
pub const MAX_SUBSCRIPTIONS: u16 = 256;
pub const SIDMAP_CAPACITY: u32 = 512;

/// Default async queue size per subscription.
pub const DEFAULT_QUEUE_SIZE: u16 = 256;

/// Async client connection options.
pub const Options = struct {
    /// Client name for identification.
    name: ?[]const u8 = null,
    /// Enable verbose mode.
    verbose: bool = false,
    /// Enable pedantic mode.
    pedantic: bool = false,
    /// Username for auth.
    user: ?[]const u8 = null,
    /// Password for auth.
    pass: ?[]const u8 = null,
    /// Auth token.
    auth_token: ?[]const u8 = null,
    /// Connection timeout in nanoseconds.
    connect_timeout_ns: u64 = 5_000_000_000,
    /// Per-subscription async queue size.
    async_queue_size: u16 = DEFAULT_QUEUE_SIZE,
};

/// Connection statistics.
pub const Stats = client_mod.Stats;

/// Async NATS Client for concurrent subscriptions.
///
/// Uses a dedicated reader task to route incoming messages to per-subscription
/// queues, enabling multiple subscriptions to receive concurrently.
pub const ClientAsync = struct {
    /// Async subscription type.
    pub const Sub = AsyncSubscription;

    // Connection state
    io: Io,
    stream: net.Stream,
    reader: net.Stream.Reader,
    writer: net.Stream.Writer,
    parser: Parser,
    server_info: ?OwnedServerInfo,
    state: State,
    options: Options,

    // Buffers
    read_buffer: [32768]u8,
    write_buffer: [32768]u8,
    pending_toss: usize,

    // Subscription routing (O(1) via SidMap)
    sidmap: SidMap,
    sidmap_keys: [SIDMAP_CAPACITY]u64,
    sidmap_vals: [SIDMAP_CAPACITY]u16,
    sub_ptrs: [MAX_SUBSCRIPTIONS]?*Sub,
    free_slots: [MAX_SUBSCRIPTIONS]u16,
    free_count: u16,
    next_sid: u64,

    // Reader task state
    reader_running: bool,
    shutdown_requested: bool,
    reader_future: ?ReaderFuture,
    reader_allocator: Allocator,

    // Statistics
    stats: Stats,

    /// Reader future type (derived from readerTaskFn return type).
    const ReaderFuture = Io.Future(
        @typeInfo(@TypeOf(readerTaskFn)).@"fn".return_type.?,
    );

    /// Connects to a NATS server and starts the reader task.
    pub fn connect(
        allocator: Allocator,
        io: Io,
        url: []const u8,
        opts: Options,
    ) !*ClientAsync {
        assert(url.len > 0);
        const parsed = try client_mod.parseUrl(url);

        const client = try allocator.create(ClientAsync);
        client.server_info = null;
        errdefer {
            if (client.server_info) |*info| {
                info.deinit(allocator);
            }
            allocator.destroy(client);
        }

        // Parse address
        const host = if (std.mem.eql(u8, parsed.host, "localhost"))
            "127.0.0.1"
        else
            parsed.host;

        const address = net.IpAddress.parse(host, parsed.port) catch {
            return error.InvalidAddress;
        };

        // Connect
        client.stream = net.IpAddress.connect(address, io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch {
            return error.ConnectionFailed;
        };

        // Set TCP_NODELAY
        const enable: u32 = 1;
        std.posix.setsockopt(
            client.stream.socket.handle,
            std.posix.IPPROTO.TCP,
            std.os.linux.TCP.NODELAY,
            std.mem.asBytes(&enable),
        ) catch {};

        // Initialize buffers and state
        client.read_buffer = undefined;
        client.write_buffer = undefined;
        client.io = io;
        client.reader = client.stream.reader(io, &client.read_buffer);
        client.writer = client.stream.writer(io, &client.write_buffer);
        client.parser = .{};
        client.state = .connecting;
        client.pending_toss = 0;
        client.options = opts;
        client.next_sid = 1;
        client.stats = .{};
        client.reader_running = false;
        client.shutdown_requested = false;
        client.reader_future = null;
        client.reader_allocator = allocator;

        // Initialize SidMap and free slot stack
        client.sidmap_keys = undefined;
        client.sidmap_vals = undefined;
        client.sidmap = SidMap.init(&client.sidmap_keys, &client.sidmap_vals);
        client.sub_ptrs = [_]?*Sub{null} ** MAX_SUBSCRIPTIONS;
        for (0..MAX_SUBSCRIPTIONS) |i| {
            client.free_slots[i] = @intCast(MAX_SUBSCRIPTIONS - 1 - i);
        }
        client.free_count = MAX_SUBSCRIPTIONS;

        // Perform handshake
        try client.handshake(allocator, opts, parsed);

        // Start reader task (producer for subscription queues)
        client.reader_future = io.concurrent(
            readerTaskFn,
            .{ client, allocator },
        ) catch {
            client.stream.close(io);
            return error.ConcurrencyUnavailable;
        };
        client.reader_running = true;

        assert(client.next_sid >= 1);
        assert(client.state == .connected);
        return client;
    }

    /// Background reader task - runs via io.concurrent().
    /// Reads from socket and routes messages to subscription queues.
    /// Uses fillMore() which is async-aware and respects cancellation.
    fn readerTaskFn(client: *ClientAsync, allocator: Allocator) !void {
        defer client.reader_running = false;

        while (!client.shutdown_requested) {
            // routeNextMessage blocks on fillMore() until data arrives
            // fillMore() respects cancellation via future.cancel(io)
            _ = client.routeNextMessage(allocator) catch |err| {
                if (client.shutdown_requested) return;

                // Connection error - close all queues to wake waiters
                client.closeAllQueues();
                return err;
            };
        }
    }

    /// Performs NATS handshake (INFO/CONNECT exchange).
    fn handshake(
        self: *ClientAsync,
        allocator: Allocator,
        opts: Options,
        parsed: client_mod.ParsedUrl,
    ) !void {
        assert(self.state == .connecting);
        assert(parsed.host.len > 0);

        // Read INFO from server
        const info_data = self.reader.interface.peekGreedy(1) catch {
            return error.ConnectionFailed;
        };

        var consumed: usize = 0;
        const cmd = self.parser.parse(allocator, info_data, &consumed) catch {
            return error.ProtocolError;
        };

        assert(consumed <= info_data.len);
        self.reader.interface.toss(consumed);

        if (cmd) |c| {
            switch (c) {
                .info => |parsed_info| {
                    self.server_info = parsed_info;
                    self.state = .connected;
                },
                else => return error.UnexpectedCommand,
            }
        } else {
            return error.NoInfoReceived;
        }

        // Send CONNECT
        const pass = opts.pass orelse parsed.pass;
        var user = opts.user orelse parsed.user;
        var auth_token = opts.auth_token;

        if (parsed.user != null and parsed.pass == null and opts.user == null) {
            auth_token = parsed.user;
            user = null;
        }

        const connect_opts = protocol.ConnectOptions{
            .verbose = opts.verbose,
            .pedantic = opts.pedantic,
            .name = opts.name,
            .user = user,
            .pass = pass,
            .auth_token = auth_token,
            .lang = "zig",
            .version = "0.1.0",
            .protocol = 1,
        };

        protocol.Encoder.encodeConnect(
            &self.writer.interface,
            connect_opts,
        ) catch {
            return error.EncodingFailed;
        };

        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };

        // Check for auth rejection
        if (self.server_info.?.auth_required) {
            try self.checkAuthRejection();
        }
    }

    /// Checks for -ERR auth rejection after CONNECT.
    /// Uses io.sleep instead of raw posix.poll for async compatibility.
    fn checkAuthRejection(self: *ClientAsync) !void {
        assert(self.state == .connected);

        // Brief sleep to allow server to respond with -ERR if auth fails
        // Uses io.sleep which is async-aware
        self.io.sleep(.fromMilliseconds(100), .awake) catch {};

        // Check if any data is buffered (non-blocking peek)
        const buffered = self.reader.interface.buffered();
        if (buffered.len > 0) {
            if (std.mem.startsWith(u8, buffered, "-ERR")) {
                self.state = .closed;
                return error.AuthorizationViolation;
            }
        }

        // Try a non-blocking peek to see if more data arrived
        const response = self.reader.interface.peekGreedy(1) catch {
            // No data or error - assume auth is OK
            return;
        };

        if (std.mem.startsWith(u8, response, "-ERR")) {
            self.state = .closed;
            return error.AuthorizationViolation;
        }
    }

    /// Subscribes to a subject. Returns async subscription.
    pub fn subscribe(
        self: *ClientAsync,
        allocator: Allocator,
        subject: []const u8,
    ) !*Sub {
        return self.subscribeQueue(allocator, subject, null);
    }

    /// Subscribes with queue group.
    pub fn subscribeQueue(
        self: *ClientAsync,
        allocator: Allocator,
        subject: []const u8,
        queue_group: ?[]const u8,
    ) !*Sub {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validateSubscribe(subject);
        assert(self.next_sid >= 1);

        // Allocate slot
        if (self.free_count == 0) {
            return error.TooManySubscriptions;
        }
        self.free_count -= 1;
        const slot_idx = self.free_slots[self.free_count];

        const sid = self.next_sid;
        self.next_sid += 1;

        // Create subscription
        const sub = try allocator.create(Sub);
        errdefer {
            allocator.destroy(sub);
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
        }

        const owned_subject = try allocator.dupe(u8, subject);
        errdefer allocator.free(owned_subject);

        const owned_queue = if (queue_group) |qg|
            try allocator.dupe(u8, qg)
        else
            null;
        errdefer if (owned_queue) |qg| allocator.free(qg);

        // Allocate Io.Queue buffer
        const queue_size = self.options.async_queue_size;
        const queue_buf = try allocator.alloc(Message, queue_size);
        errdefer allocator.free(queue_buf);

        sub.* = .{
            .client = self,
            .sid = sid,
            .subject = owned_subject,
            .queue_group = owned_queue,
            .queue_buf = queue_buf,
            .queue = Io.Queue(Message).init(queue_buf),
            .state = .active,
            .received_msgs = 0,
        };

        // Store in SidMap
        self.sidmap.put(sid, slot_idx) catch {
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
            return error.TooManySubscriptions;
        };
        self.sub_ptrs[slot_idx] = sub;

        // Send SUB command
        protocol.Encoder.encodeSub(&self.writer.interface, .{
            .subject = subject,
            .queue_group = queue_group,
            .sid = sid,
        }) catch {
            return error.EncodingFailed;
        };

        return sub;
    }

    /// Publishes a message to a subject.
    pub fn publish(
        self: *ClientAsync,
        subject: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = null,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };

        self.stats.msgs_out += 1;
        self.stats.bytes_out += payload.len;
    }

    /// Publishes with a reply-to subject.
    pub fn publishRequest(
        self: *ClientAsync,
        subject: []const u8,
        reply_to: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        assert(reply_to.len > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = reply_to,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };

        self.stats.msgs_out += 1;
        self.stats.bytes_out += payload.len;
    }

    /// Flushes pending writes to the server.
    pub fn flush(self: *ClientAsync) !void {
        if (!self.state.canSend()) {
            return error.NotConnected;
        }
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }

    /// Sends a request and waits for a reply with timeout.
    /// Creates a temporary inbox subscription internally.
    /// Uses io.select() for proper async timeout handling.
    pub fn request(
        self: *ClientAsync,
        allocator: Allocator,
        subject: []const u8,
        payload: []const u8,
        timeout_ms: u32,
    ) !?Message {
        assert(subject.len > 0);
        assert(timeout_ms > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }

        // Generate unique inbox for reply
        const inbox = try pubsub.newInbox(allocator);
        defer allocator.free(inbox);

        // Subscribe to inbox (temporary subscription)
        const sub = try self.subscribe(allocator, inbox);
        defer sub.deinit(allocator);

        // Flush subscription registration before publishing
        try self.flush();

        // Brief delay to ensure server has registered subscription
        // Uses io.sleep which is async-aware
        self.io.sleep(.fromMilliseconds(5), .awake) catch {};

        // Publish request with reply-to
        try self.publishRequest(subject, inbox, payload);
        try self.flush();

        // Wait for reply using io.select() - proper async pattern!
        // Create response future (waits on subscription queue)
        var response_future = self.io.async(
            AsyncSubscription.next,
            .{ sub, self.io },
        );

        // Create timeout future
        var timeout_future = self.io.async(
            sleepForRequest,
            .{ self.io, timeout_ms },
        );

        // Use io.select() to wait for first to complete
        // Note: we handle cleanup manually to avoid double-free
        const select_result = self.io.select(.{
            .response = &response_future,
            .timeout = &timeout_future,
        }) catch |err| {
            // On error, cancel both futures
            _ = timeout_future.cancel(self.io);
            if (response_future.cancel(self.io)) |msg| {
                msg.deinit(allocator);
            } else |_| {}
            if (err == error.Canceled) return null;
            return err;
        };

        switch (select_result) {
            .response => |msg_result| {
                // Got response - cancel timeout and return
                _ = timeout_future.cancel(self.io);
                // Response future already consumed via select
                return msg_result catch |err| {
                    if (err == error.Canceled or err == error.Closed) {
                        return null;
                    }
                    return err;
                };
            },
            .timeout => |_| {
                // Timeout - cancel response future and clean up
                if (response_future.cancel(self.io)) |msg| {
                    msg.deinit(allocator);
                } else |_| {}
                return null;
            },
        }
    }

    /// Helper for request timeout - sleeps for specified duration.
    fn sleepForRequest(io: Io, timeout_ms: u32) void {
        io.sleep(.fromMilliseconds(timeout_ms), .awake) catch {};
    }

    /// Gracefully drains subscriptions and closes the connection.
    /// Unsubscribes all subscriptions and closes connection.
    pub fn drain(self: *ClientAsync, allocator: Allocator) !void {
        if (self.state != .connected) {
            return error.NotConnected;
        }
        assert(self.next_sid >= 1);

        // Signal shutdown to reader task
        self.shutdown_requested = true;

        // Unsubscribe all active subscriptions
        for (self.sub_ptrs, 0..) |maybe_sub, slot_idx| {
            if (maybe_sub) |sub| {
                // Send UNSUB to server
                protocol.Encoder.encodeUnsub(&self.writer.interface, .{
                    .sid = sub.sid,
                    .max_msgs = null,
                }) catch {};

                // Close the subscription's queue to wake any waiters
                sub.queue.close(self.io);

                // Remove from tracking
                _ = self.sidmap.remove(sub.sid);
                self.sub_ptrs[slot_idx] = null;
                self.free_slots[self.free_count] = @intCast(slot_idx);
                self.free_count += 1;

                // Drain remaining messages
                var drain_buf: [1]Message = undefined;
                while (true) {
                    const n = sub.queue.get(self.io, &drain_buf, 0) catch break;
                    if (n == 0) break;
                    drain_buf[0].deinit(allocator);
                }

                // Clean up subscription memory
                allocator.free(sub.queue_buf);
                allocator.free(sub.subject);
                if (sub.queue_group) |qg| allocator.free(qg);
                allocator.destroy(sub);
            }
        }

        // Flush UNSUB commands
        self.writer.interface.flush() catch {};

        // Update state
        self.state = .draining;

        // Cancel reader task
        if (self.reader_future) |*future| {
            _ = future.cancel(self.io) catch {};
            self.reader_future = null;
        }

        // Close connection
        self.stream.close(self.io);
        self.state = .closed;

        // Free server_info
        if (self.server_info) |*info| {
            info.deinit(allocator);
            self.server_info = null;
        }
    }

    /// Returns true if connected.
    pub fn isConnected(self: *const ClientAsync) bool {
        assert(self.next_sid >= 1);
        return self.state == .connected;
    }

    /// Returns connection statistics.
    pub fn getStats(self: *const ClientAsync) Stats {
        assert(self.next_sid >= 1);
        return self.stats;
    }

    /// Returns server info.
    pub fn getServerInfo(self: *const ClientAsync) ?*const OwnedServerInfo {
        assert(self.next_sid >= 1);
        if (self.server_info) |*info| {
            return info;
        }
        return null;
    }

    /// Get subscription by SID.
    pub fn getSubscriptionBySid(self: *ClientAsync, sid: u64) ?*Sub {
        assert(sid > 0);
        if (self.sidmap.get(sid)) |slot_idx| {
            return self.sub_ptrs[slot_idx];
        }
        return null;
    }

    /// Unsubscribes by SID.
    pub fn unsubscribeSid(self: *ClientAsync, sid: u64) !void {
        assert(sid > 0);
        if (!self.state.canSend()) {
            return error.NotConnected;
        }

        protocol.Encoder.encodeUnsub(&self.writer.interface, .{
            .sid = sid,
            .max_msgs = null,
        }) catch {
            return error.EncodingFailed;
        };

        if (self.sidmap.get(sid)) |slot_idx| {
            self.sub_ptrs[slot_idx] = null;
            _ = self.sidmap.remove(sid);
            self.free_slots[self.free_count] = slot_idx;
            self.free_count += 1;
        }
    }

    /// Toss bytes consumed from previous read.
    pub fn tossPending(self: *ClientAsync) void {
        assert(self.pending_toss <= self.read_buffer.len);
        if (self.pending_toss > 0) {
            self.reader.interface.toss(self.pending_toss);
            self.pending_toss = 0;
        }
    }

    /// Routes a single message from socket to subscription queues.
    /// Used by reader task internally. Uses fillMore() which respects
    /// cancellation via std.Io.
    pub fn routeNextMessage(
        self: *ClientAsync,
        allocator: Allocator,
    ) !bool {
        assert(self.state.canReceive());

        self.tossPending();

        while (true) {
            var data = self.reader.interface.buffered();

            if (data.len == 0) {
                // fillMore() blocks until data available or connection closes
                // This is async-aware and respects cancellation
                self.reader.interface.fillMore() catch |err| {
                    switch (err) {
                        error.EndOfStream, error.ReadFailed => {
                            self.state = .disconnected;
                            return false;
                        },
                    }
                };

                data = self.reader.interface.buffered();
                if (data.len == 0) return false;
            }

            var consumed: usize = 0;
            const cmd = self.parser.parse(allocator, data, &consumed) catch {
                return error.ProtocolError;
            };

            if (cmd) |c| {
                switch (c) {
                    .ping => {
                        self.reader.interface.toss(consumed);
                        try self.sendPong();
                    },
                    .pong, .ok => {
                        self.reader.interface.toss(consumed);
                    },
                    .err => |_| {
                        self.reader.interface.toss(consumed);
                    },
                    .info => |new_info| {
                        self.reader.interface.toss(consumed);
                        if (self.server_info) |*info| {
                            info.deinit(std.heap.page_allocator);
                        }
                        self.server_info = new_info;
                    },
                    .msg => |args| {
                        self.stats.msgs_in += 1;
                        self.stats.bytes_in += args.payload.len;

                        // Route to subscription
                        if (self.getSubscriptionBySid(args.sid)) |sub| {
                            // Single allocation for all message data
                            const reply_len = if (args.reply_to) |rt|
                                rt.len
                            else
                                0;
                            const total = args.subject.len +
                                args.payload.len + reply_len;

                            const buf = allocator.alloc(u8, total) catch {
                                self.reader.interface.toss(consumed);
                                continue;
                            };

                            // Pack: [subject][payload][reply_to]
                            @memcpy(buf[0..args.subject.len], args.subject);
                            const subj = buf[0..args.subject.len];

                            const data_start = args.subject.len;
                            const data_end = data_start + args.payload.len;
                            @memcpy(buf[data_start..data_end], args.payload);
                            const payload = buf[data_start..data_end];

                            const reply = if (args.reply_to) |rt| blk: {
                                @memcpy(buf[data_end..], rt);
                                break :blk buf[data_end..][0..rt.len];
                            } else null;

                            const msg = Message{
                                .subject = subj,
                                .sid = args.sid,
                                .reply_to = reply,
                                .data = payload,
                                .headers = null,
                                .owned = true,
                                .backing_buf = buf,
                            };

                            // Push to subscription queue (non-blocking)
                            sub.pushMessage(msg) catch {
                                allocator.free(buf);
                            };

                            sub.received_msgs += 1;
                        }
                        self.reader.interface.toss(consumed);
                        return true;
                    },
                    .hmsg => |args| {
                        self.stats.msgs_in += 1;
                        self.stats.bytes_in +=
                            args.payload.len + args.headers.len;

                        if (self.getSubscriptionBySid(args.sid)) |sub| {
                            // Single allocation for all message data
                            const reply_len = if (args.reply_to) |rt|
                                rt.len
                            else
                                0;
                            const hdrs_len = args.headers.len;
                            const total = args.subject.len +
                                args.payload.len + reply_len + hdrs_len;

                            const buf = allocator.alloc(u8, total) catch {
                                self.reader.interface.toss(consumed);
                                continue;
                            };

                            // Pack: [subject][payload][reply_to][headers]
                            var offset: usize = 0;

                            @memcpy(buf[offset..][0..args.subject.len], args.subject);
                            const subj = buf[offset..][0..args.subject.len];
                            offset += args.subject.len;

                            @memcpy(buf[offset..][0..args.payload.len], args.payload);
                            const payload = buf[offset..][0..args.payload.len];
                            offset += args.payload.len;

                            const reply = if (args.reply_to) |rt| blk: {
                                @memcpy(buf[offset..][0..rt.len], rt);
                                const slice = buf[offset..][0..rt.len];
                                offset += rt.len;
                                break :blk slice;
                            } else null;

                            const hdrs = if (hdrs_len > 0) blk: {
                                @memcpy(buf[offset..][0..hdrs_len], args.headers);
                                break :blk buf[offset..][0..hdrs_len];
                            } else null;

                            const msg = Message{
                                .subject = subj,
                                .sid = args.sid,
                                .reply_to = reply,
                                .data = payload,
                                .headers = hdrs,
                                .owned = true,
                                .backing_buf = buf,
                            };

                            sub.pushMessage(msg) catch {
                                allocator.free(buf);
                            };

                            sub.received_msgs += 1;
                        }
                        self.reader.interface.toss(consumed);
                        return true;
                    },
                }
            } else {
                self.reader.interface.fillMore() catch |err| {
                    switch (err) {
                        error.EndOfStream, error.ReadFailed => {
                            self.state = .disconnected;
                            return false;
                        },
                    }
                };
            }
        }
    }

    /// Sends PONG response.
    fn sendPong(self: *ClientAsync) !void {
        assert(self.state.canSend());
        self.writer.interface.writeAll("PONG\r\n") catch {
            return error.WriteFailed;
        };
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }

    /// Closes all subscription queues (wakes waiters with error).
    pub fn closeAllQueues(self: *ClientAsync) void {
        for (self.sub_ptrs) |maybe_sub| {
            if (maybe_sub) |sub| {
                sub.queue.close(self.io);
            }
        }
    }

    /// Closes the connection and frees resources.
    pub fn deinit(self: *ClientAsync, allocator: Allocator) void {
        assert(self.next_sid >= 1);

        // Request shutdown and close queues to wake waiters
        self.shutdown_requested = true;
        self.closeAllQueues();

        // Cancel reader task (interrupts pollAndRoute)
        if (self.reader_future) |*future| {
            _ = future.cancel(self.io) catch {};
        }

        // Close connection
        if (self.state != .closed) {
            self.stream.close(self.io);
            self.state = .closed;
        }

        // Clean up subscriptions
        for (self.sub_ptrs) |maybe_sub| {
            if (maybe_sub) |sub| {
                // Drain any remaining messages (non-blocking with target=0)
                var drain_buf: [1]Message = undefined;
                while (true) {
                    const n = sub.queue.get(
                        self.io,
                        &drain_buf,
                        0,
                    ) catch break;
                    if (n == 0) break;
                    drain_buf[0].deinit(allocator);
                }
                allocator.free(sub.queue_buf);
                allocator.free(sub.subject);
                if (sub.queue_group) |qg| {
                    allocator.free(qg);
                }
                allocator.destroy(sub);
            }
        }

        if (self.server_info) |*info| {
            info.deinit(allocator);
        }
        allocator.destroy(self);
    }
};

/// Async subscription with Io.Queue for message delivery.
pub const AsyncSubscription = struct {
    client: *ClientAsync,
    sid: u64,
    subject: []const u8,
    queue_group: ?[]const u8,
    queue_buf: []Message,
    queue: Io.Queue(Message),
    state: subscription_mod.State,
    received_msgs: u64,

    /// Async receive - waits on queue. Can be called concurrently!
    /// Blocks until a message is available or queue is closed.
    pub fn next(self: *AsyncSubscription, io: Io) !Message {
        assert(self.state == .active or self.state == .draining);
        return self.queue.getOne(io);
    }

    /// Try receive without blocking. Returns null if no message available.
    /// Uses get() with target=0 for non-blocking behavior.
    pub fn tryNext(self: *AsyncSubscription) ?Message {
        var buf: [1]Message = undefined;
        // Use target=0 for non-blocking get
        const n = self.queue.get(self.client.io, &buf, 0) catch return null;
        if (n == 0) return null;
        return buf[0];
    }

    /// Batch receive - gets up to buf.len messages, waiting for at least 1.
    /// Returns number of messages received. More efficient than repeated next().
    pub fn nextBatch(self: *AsyncSubscription, io: Io, buf: []Message) !usize {
        assert(self.state == .active or self.state == .draining);
        assert(buf.len > 0);
        // Wait for at least 1 message, return up to buf.len
        return self.queue.get(io, buf, 1);
    }

    /// Non-blocking batch receive - gets available messages up to buf.len.
    /// Returns 0 if queue is empty.
    pub fn tryNextBatch(self: *AsyncSubscription, buf: []Message) usize {
        return self.queue.get(self.client.io, buf, 0) catch 0;
    }

    /// Async receive with timeout. Returns null on timeout.
    /// Uses io.select() for proper async timeout handling.
    pub fn nextWithTimeout(
        self: *AsyncSubscription,
        allocator: Allocator,
        timeout_ms: u32,
    ) !?Message {
        assert(self.state == .active or self.state == .draining);
        assert(timeout_ms > 0);

        const io = self.client.io;

        // Create response future (waits on queue)
        var response_future = io.async(next, .{ self, io });

        // Create timeout future
        var timeout_future = io.async(
            ClientAsync.sleepForRequest,
            .{ io, timeout_ms },
        );

        // Use io.select() to wait for first to complete
        // Note: handle cleanup manually to avoid double-free
        const select_result = io.select(.{
            .response = &response_future,
            .timeout = &timeout_future,
        }) catch |err| {
            // On error, cancel both futures
            _ = timeout_future.cancel(io);
            if (response_future.cancel(io)) |msg| {
                msg.deinit(allocator);
            } else |_| {}
            if (err == error.Canceled) return null;
            return err;
        };

        switch (select_result) {
            .response => |msg_result| {
                // Got response - cancel timeout and return
                _ = timeout_future.cancel(io);
                return msg_result catch |err| {
                    if (err == error.Canceled or err == error.Closed) {
                        return null;
                    }
                    return err;
                };
            },
            .timeout => |_| {
                // Timeout - cancel response and clean up
                if (response_future.cancel(io)) |msg| {
                    msg.deinit(allocator);
                } else |_| {}
                return null;
            },
        }
    }

    /// Returns queue capacity (not current count).
    pub fn capacity(self: *AsyncSubscription) usize {
        return self.queue.capacity();
    }

    /// Push message to queue (called by reader task).
    /// Uses put() with target=0 for non-blocking behavior.
    pub fn pushMessage(self: *AsyncSubscription, msg: Message) !void {
        const n = self.queue.put(
            self.client.io,
            &.{msg},
            0,
        ) catch return error.QueueFull;
        if (n == 0) return error.QueueFull;
    }

    /// Unsubscribe from the subject.
    pub fn unsubscribe(self: *AsyncSubscription) !void {
        if (self.state == .unsubscribed) return;
        self.state = .unsubscribed;
        self.queue.close(self.client.io);
        try self.client.unsubscribeSid(self.sid);
    }

    /// Closes the subscription and frees resources.
    pub fn deinit(self: *AsyncSubscription, allocator: Allocator) void {
        // Close queue to wake any waiters
        self.queue.close(self.client.io);

        // Unsubscribe if still active
        if (self.state != .unsubscribed) {
            self.client.unsubscribeSid(self.sid) catch {};
        }

        // Remove from client
        if (self.client.sidmap.get(self.sid)) |slot_idx| {
            self.client.sub_ptrs[slot_idx] = null;
            _ = self.client.sidmap.remove(self.sid);
            self.client.free_slots[self.client.free_count] = slot_idx;
            self.client.free_count += 1;
        }

        // Drain remaining messages (non-blocking with target=0)
        var drain_buf: [1]Message = undefined;
        while (true) {
            const n = self.queue.get(self.client.io, &drain_buf, 0) catch break;
            if (n == 0) break;
            drain_buf[0].deinit(allocator);
        }

        // Free resources
        allocator.free(self.queue_buf);
        allocator.free(self.subject);
        if (self.queue_group) |qg| {
            allocator.free(qg);
        }
        allocator.destroy(self);
    }
};

test "async options defaults" {
    const opts: Options = .{};
    try std.testing.expect(opts.name == null);
    try std.testing.expect(!opts.verbose);
    try std.testing.expectEqual(DEFAULT_QUEUE_SIZE, opts.async_queue_size);
}
