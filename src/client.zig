//! NATS Client
//!
//! High-level client API for connecting to NATS servers.
//! Provides publish, subscribe, and request/reply functionality.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const net = Io.net;

const protocol = @import("protocol.zig");
const Parser = protocol.Parser;
const ServerInfo = protocol.ServerInfo;
const OwnedServerInfo = protocol.OwnedServerInfo;

const connection = @import("connection.zig");
const State = connection.State;
const Event = connection.Event;
const EventQueue = connection.EventQueue;

const pubsub = @import("pubsub.zig");
const Subscription = pubsub.Subscription;
const SubscriptionMap = pubsub.SubscriptionMap;

/// Client connection options.
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
};

/// Parse result for NATS URL.
pub const ParsedUrl = struct {
    host: []const u8,
    port: u16,
    user: ?[]const u8,
    pass: ?[]const u8,
};

/// Parses a NATS URL like nats://user:pass@host:port
pub fn parseUrl(url: []const u8) error{InvalidUrl}!ParsedUrl {
    assert(url.len > 0);
    var remaining = url;

    // Strip nats:// prefix
    if (std.mem.startsWith(u8, remaining, "nats://")) {
        remaining = remaining[7..];
    }

    var user: ?[]const u8 = null;
    var pass: ?[]const u8 = null;

    // Check for user:pass@
    if (std.mem.indexOf(u8, remaining, "@")) |at_pos| {
        const auth = remaining[0..at_pos];
        remaining = remaining[at_pos + 1 ..];

        if (std.mem.indexOf(u8, auth, ":")) |colon_pos| {
            user = auth[0..colon_pos];
            pass = auth[colon_pos + 1 ..];
        } else {
            user = auth;
        }
    }

    // Parse host:port
    var host: []const u8 = undefined;
    var port: u16 = 4222;

    if (std.mem.indexOf(u8, remaining, ":")) |colon_pos| {
        host = remaining[0..colon_pos];
        port = std.fmt.parseInt(u16, remaining[colon_pos + 1 ..], 10) catch {
            return error.InvalidUrl;
        };
    } else {
        host = remaining;
    }

    if (host.len == 0) return error.InvalidUrl;

    assert(host.len > 0);
    assert(port > 0);
    return .{
        .host = host,
        .port = port,
        .user = user,
        .pass = pass,
    };
}

/// NATS Client for pub/sub messaging.
pub const Client = struct {
    stream: net.Stream,
    io: Io,
    reader: net.Stream.Reader,
    writer: net.Stream.Writer,
    parser: Parser,
    server_info: ?OwnedServerInfo,
    subscriptions: SubscriptionMap,
    events: EventQueue,
    next_sid: u64,
    state: State,
    read_buffer: [32768]u8,
    write_buffer: [32768]u8,

    /// Connects to a NATS server.
    /// URL format: nats://[user:pass@]host[:port]
    /// The caller owns the Io instance and must keep it alive.
    pub fn connect(
        allocator: Allocator,
        io: Io,
        url: []const u8,
        opts: Options,
    ) !*Client {
        assert(url.len > 0);
        const parsed = try parseUrl(url);

        // Allocate client
        const client = try allocator.create(Client);
        errdefer allocator.destroy(client);

        client.io = io;

        // Parse address (handle localhost specially)
        const host = if (std.mem.eql(u8, parsed.host, "localhost"))
            "127.0.0.1"
        else
            parsed.host;

        const address = net.IpAddress.parse(host, parsed.port) catch {
            return error.InvalidAddress;
        };

        // Connect
        client.stream = net.IpAddress.connect(address, client.io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch {
            return error.ConnectionFailed;
        };

        // Initialize buffers and reader/writer
        client.read_buffer = undefined;
        client.write_buffer = undefined;
        client.reader = client.stream.reader(client.io, &client.read_buffer);
        client.writer = client.stream.writer(client.io, &client.write_buffer);

        // Initialize state
        client.parser = .{};
        client.server_info = null;
        client.subscriptions = .{};
        client.events = .{};
        client.next_sid = 1;
        client.state = .connecting;

        // Perform handshake
        try client.handshake(allocator, opts, parsed);

        assert(client.next_sid >= 1);
        assert(client.state == .connected);
        return client;
    }

    fn handshake(
        self: *Client,
        allocator: Allocator,
        opts: Options,
        parsed: ParsedUrl,
    ) !void {
        assert(self.state == .connecting);
        assert(parsed.host.len > 0);

        // Read INFO from server
        const info_data = self.reader.interface.peekGreedy(1) catch {
            return error.ConnectionFailed;
        };

        // Parse INFO command
        var consumed: usize = 0;
        const cmd = self.parser.parse(allocator, info_data, &consumed) catch {
            return error.ProtocolError;
        };

        // Consume parsed data from buffer
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
        // Token auth: nats://token@host uses token, not user
        // User auth: nats://user:pass@host uses user/pass
        const pass = opts.pass orelse parsed.pass;
        var user = opts.user orelse parsed.user;
        var auth_token = opts.auth_token;

        // If URL has user but no pass, treat as token auth
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
    }

    /// Closes the connection and frees resources.
    /// Note: The Io instance is owned by the caller and not freed here.
    pub fn deinit(self: *Client, allocator: Allocator) void {
        assert(self.next_sid >= 1);
        self.stream.close(self.io);
        self.subscriptions.deinit(allocator);
        if (self.server_info) |*info| {
            info.deinit(allocator);
        }
        allocator.destroy(self);
    }

    /// Publishes a message to a subject.
    pub fn publish(
        self: *Client,
        subject: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        assert(self.state.canSend());
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = null,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };
    }

    /// Publishes with a reply-to subject.
    pub fn publishRequest(
        self: *Client,
        subject: []const u8,
        reply_to: []const u8,
        payload: []const u8,
    ) !void {
        assert(subject.len > 0);
        assert(reply_to.len > 0);
        assert(self.state.canSend());
        try pubsub.validatePublish(subject);

        protocol.Encoder.encodePub(&self.writer.interface, .{
            .subject = subject,
            .reply_to = reply_to,
            .payload = payload,
        }) catch {
            return error.EncodingFailed;
        };
    }

    /// Subscribes to a subject.
    pub fn subscribe(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
    ) !u64 {
        assert(subject.len > 0);
        return self.subscribeQueue(allocator, subject, null);
    }

    /// Subscribes to a subject with queue group.
    pub fn subscribeQueue(
        self: *Client,
        allocator: Allocator,
        subject: []const u8,
        queue_group: ?[]const u8,
    ) !u64 {
        assert(subject.len > 0);
        assert(self.state.canSend());
        assert(self.next_sid >= 1);
        try pubsub.validateSubscribe(subject);

        const sid = self.next_sid;
        self.next_sid += 1;

        // Store subscription
        const sub = try Subscription.initOwned(
            allocator,
            sid,
            subject,
            queue_group,
        );
        try self.subscriptions.put(allocator, sub);

        // Send SUB command
        protocol.Encoder.encodeSub(&self.writer.interface, .{
            .subject = subject,
            .queue_group = queue_group,
            .sid = sid,
        }) catch {
            return error.EncodingFailed;
        };

        return sid;
    }

    /// Unsubscribes from a subscription.
    pub fn unsubscribe(self: *Client, allocator: Allocator, sid: u64) !void {
        assert(sid > 0);
        assert(self.state.canSend());
        protocol.Encoder.encodeUnsub(&self.writer.interface, .{
            .sid = sid,
            .max_msgs = null,
        }) catch {
            return error.EncodingFailed;
        };

        self.subscriptions.remove(allocator, sid);
    }

    /// Flushes pending writes to the server.
    pub fn flush(self: *Client) !void {
        assert(self.state.canSend());
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }

    /// Sends PING to server.
    pub fn ping(self: *Client) !void {
        assert(self.state.canSend());
        self.writer.interface.writeAll("PING\r\n") catch {
            return error.WriteFailed;
        };
    }

    /// Returns true if connected.
    pub fn isConnected(self: *const Client) bool {
        assert(self.next_sid >= 1);
        return self.state == .connected;
    }

    /// Returns server info received during handshake.
    pub fn getServerInfo(self: *const Client) ?*const OwnedServerInfo {
        assert(self.next_sid >= 1);
        if (self.server_info) |*info| {
            return info;
        }
        return null;
    }

    /// Polls for incoming data and processes commands.
    /// Call regularly to receive messages and handle server pings.
    /// Returns true if any data was processed, false if no data available.
    pub fn poll(self: *Client, allocator: Allocator) !bool {
        assert(self.state.canReceive());

        // Read available data - peekGreedy fills buffer and returns all data
        const data = self.reader.interface.peekGreedy(1) catch |err| {
            // EndOfStream means connection closed
            // ReadFailed can mean no data yet in async context
            return switch (err) {
                error.EndOfStream => false,
                error.ReadFailed => false,
            };
        };

        if (data.len == 0) return false;

        // Parse and handle all complete commands
        var total_consumed: usize = 0;
        while (total_consumed < data.len) {
            var consumed: usize = 0;
            const remaining = data[total_consumed..];

            const cmd = self.parser.parse(
                allocator,
                remaining,
                &consumed,
            ) catch {
                return error.ProtocolError;
            };

            // Always track consumed bytes, even when waiting for more data
            total_consumed += consumed;

            if (cmd) |c| {
                try self.handleCommand(c);
            } else {
                // Parser needs more data
                break;
            }
        }

        // Consume processed data from buffer
        if (total_consumed > 0) {
            self.reader.interface.toss(total_consumed);
        }

        return total_consumed > 0;
    }

    /// Returns the next event from the event queue.
    /// Returns null if no events are pending.
    pub fn nextEvent(self: *Client) ?Event {
        assert(self.next_sid >= 1);
        return self.events.pop();
    }

    /// Handles a parsed server command.
    fn handleCommand(self: *Client, cmd: protocol.ServerCommand) !void {
        switch (cmd) {
            .ping => try self.sendPong(),
            .pong => {},
            .ok => {},
            .err => |msg| {
                _ = self.events.push(.{ .server_error = msg });
            },
            .msg => |args| self.handleMessage(args),
            .hmsg => |args| self.handleHMessage(args),
            .info => |new_info| {
                // Server may send updated INFO (e.g., cluster changes)
                if (self.server_info) |*info| {
                    info.deinit(std.heap.page_allocator);
                }
                self.server_info = new_info;
            },
        }
    }

    /// Handles MSG command - pushes message event to queue.
    fn handleMessage(self: *Client, args: protocol.MsgArgs) void {
        assert(args.subject.len > 0);
        assert(args.sid > 0);

        _ = self.events.push(.{ .message = .{
            .subject = args.subject,
            .sid = args.sid,
            .reply_to = args.reply_to,
            .data = args.payload,
            .headers = null,
        } });
    }

    /// Handles HMSG command - pushes message event with headers.
    fn handleHMessage(self: *Client, args: protocol.HMsgArgs) void {
        assert(args.subject.len > 0);
        assert(args.sid > 0);

        _ = self.events.push(.{ .message = .{
            .subject = args.subject,
            .sid = args.sid,
            .reply_to = args.reply_to,
            .data = args.payload,
            .headers = args.headers,
        } });
    }

    /// Sends PONG response to server PING.
    fn sendPong(self: *Client) !void {
        assert(self.state.canSend());
        self.writer.interface.writeAll("PONG\r\n") catch {
            return error.WriteFailed;
        };
        self.writer.interface.flush() catch {
            return error.WriteFailed;
        };
    }
};

test "parse url" {
    {
        const parsed = try parseUrl("nats://localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expect(parsed.user == null);
    }

    {
        const parsed = try parseUrl("nats://user:pass@localhost:4222");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
        try std.testing.expectEqualSlices(u8, "user", parsed.user.?);
        try std.testing.expectEqualSlices(u8, "pass", parsed.pass.?);
    }

    {
        const parsed = try parseUrl("localhost");
        try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
        try std.testing.expectEqual(@as(u16, 4222), parsed.port);
    }

    {
        const parsed = try parseUrl("127.0.0.1:4223");
        try std.testing.expectEqualSlices(u8, "127.0.0.1", parsed.host);
        try std.testing.expectEqual(@as(u16, 4223), parsed.port);
    }
}

test "parse url with user only" {
    const parsed = try parseUrl("nats://admin@localhost:4222");
    try std.testing.expectEqualSlices(u8, "localhost", parsed.host);
    try std.testing.expectEqualSlices(u8, "admin", parsed.user.?);
    try std.testing.expect(parsed.pass == null);
}

test "parse url invalid" {
    // Empty URL triggers assert (programming error), not tested here
    try std.testing.expectError(error.InvalidUrl, parseUrl("nats://"));
    try std.testing.expectError(error.InvalidUrl, parseUrl("nats://:4222"));
}

test "parse url default port" {
    const parsed = try parseUrl("nats://myserver");
    try std.testing.expectEqualSlices(u8, "myserver", parsed.host);
    try std.testing.expectEqual(@as(u16, 4222), parsed.port);
}

test "options defaults" {
    const opts: Options = .{};
    try std.testing.expect(opts.name == null);
    try std.testing.expect(!opts.verbose);
    try std.testing.expect(!opts.pedantic);
    try std.testing.expect(opts.user == null);
    try std.testing.expect(opts.pass == null);
    try std.testing.expectEqual(@as(u64, 5_000_000_000), opts.connect_timeout_ns);
}
