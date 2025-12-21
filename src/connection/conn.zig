//! NATS Connection
//!
//! The main Connection type that manages the NATS protocol handshake,
//! message parsing, and I/O operations. Uses comptime generics for
//! zero-overhead transport abstraction.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

const protocol = @import("../protocol.zig");
const Parser = protocol.Parser;
const ServerInfo = protocol.ServerInfo;
const ConnectOptions = protocol.ConnectOptions;
const ServerCommand = protocol.commands.ServerCommand;

const state_mod = @import("state.zig");
const State = state_mod.State;
const StateMachine = state_mod.StateMachine;

const events_mod = @import("events.zig");
const Event = events_mod.Event;
const EventQueue = events_mod.EventQueue;

const transport_mod = @import("transport.zig");
const ReadError = transport_mod.ReadError;
const WriteError = transport_mod.WriteError;

/// Connection configuration options.
pub const Options = struct {
    /// Client name for identification.
    name: ?[]const u8 = null,
    /// Enable verbose mode (receive +OK).
    verbose: bool = false,
    /// Enable pedantic mode (strict parsing).
    pedantic: bool = false,
    /// Enable TLS.
    tls_required: bool = false,
    /// Username for auth.
    user: ?[]const u8 = null,
    /// Password for auth.
    pass: ?[]const u8 = null,
    /// Auth token.
    auth_token: ?[]const u8 = null,
    /// Read buffer size.
    read_buffer_size: usize = 32768,
    /// Write buffer size.
    write_buffer_size: usize = 32768,
};

/// NATS Connection using comptime generic transport.
/// Transport must implement read, write, and close methods.
pub fn Connection(comptime Transport: type) type {
    // Validate transport at compile time
    _ = transport_mod.Transport(Transport);

    return struct {
        const Self = @This();

        transport: Transport,
        state_machine: StateMachine = .{},
        events: EventQueue = .{},
        parser: Parser = .{},
        server_info: ?ServerInfo = null,
        next_sid: u64 = 1,

        /// Perform the NATS handshake after transport connection.
        /// Reads INFO from server and sends CONNECT.
        pub fn handshake(
            self: *Self,
            allocator: Allocator,
            opts: Options,
        ) !void {
            try self.state_machine.startConnect();

            // Read and parse INFO
            var buf: [4096]u8 = undefined;
            const n = try self.transport.read(&buf);
            if (n == 0) return error.ConnectionClosed;

            try self.parser.feed(buf[0..n]);

            const cmd = self.parser.next(allocator) catch |err| {
                return switch (err) {
                    error.NeedMoreData => error.IncompleteInfo,
                    else => error.ProtocolError,
                };
            };

            if (cmd) |c| {
                switch (c) {
                    .info => |info| {
                        self.server_info = info;
                        try self.state_machine.receivedInfo();

                        // Push connected event
                        _ = self.events.push(.{ .connected = .{
                            .server_id = info.server_id orelse "",
                            .server_name = info.server_name orelse "",
                            .version = info.version orelse "",
                            .is_reconnect = false,
                        } });
                    },
                    else => return error.UnexpectedCommand,
                }
            } else {
                return error.NoInfoReceived;
            }

            // Send CONNECT
            try self.sendConnect(opts);
            try self.state_machine.connectAcknowledged();
        }

        fn sendConnect(self: *Self, opts: Options) !void {
            var buf: [1024]u8 = undefined;
            var writer = Io.Writer.fixed(&buf);

            const connect_opts = ConnectOptions{
                .verbose = opts.verbose,
                .pedantic = opts.pedantic,
                .tls_required = opts.tls_required,
                .name = opts.name,
                .user = opts.user,
                .pass = opts.pass,
                .auth_token = opts.auth_token,
                .lang = "zig",
                .version = "0.1.0",
                .protocol = 1,
            };

            protocol.encoder.encodeConnect(&writer, connect_opts) catch {
                return error.EncodingFailed;
            };

            _ = self.transport.write(writer.buffered()) catch {
                return error.WriteFailed;
            };
        }

        /// Publish a message to a subject.
        pub fn publish(
            self: *Self,
            subject: []const u8,
            payload: []const u8,
        ) !void {
            if (!self.state_machine.state.canSend()) {
                return error.NotConnected;
            }

            var buf: [256]u8 = undefined;
            var writer = Io.Writer.fixed(&buf);

            protocol.encoder.encodePub(&writer, .{
                .subject = subject,
                .reply_to = null,
                .payload_len = payload.len,
            }) catch {
                return error.EncodingFailed;
            };

            _ = self.transport.write(writer.buffered()) catch {
                return error.WriteFailed;
            };

            _ = self.transport.write(payload) catch {
                return error.WriteFailed;
            };

            _ = self.transport.write("\r\n") catch {
                return error.WriteFailed;
            };
        }

        /// Subscribe to a subject.
        pub fn subscribe(self: *Self, subject: []const u8) !u64 {
            if (!self.state_machine.state.canSend()) {
                return error.NotConnected;
            }

            const sid = self.next_sid;
            self.next_sid += 1;

            var buf: [256]u8 = undefined;
            var writer = Io.Writer.fixed(&buf);

            protocol.encoder.encodeSub(&writer, .{
                .subject = subject,
                .queue_group = null,
                .sid = sid,
            }) catch {
                return error.EncodingFailed;
            };

            _ = self.transport.write(writer.buffered()) catch {
                return error.WriteFailed;
            };

            return sid;
        }

        /// Send PING to server.
        pub fn ping(self: *Self) !void {
            _ = self.transport.write("PING\r\n") catch {
                return error.WriteFailed;
            };
        }

        /// Send PONG to server.
        pub fn pong(self: *Self) !void {
            _ = self.transport.write("PONG\r\n") catch {
                return error.WriteFailed;
            };
        }

        /// Poll for next event.
        pub fn nextEvent(self: *Self) ?Event {
            return self.events.pop();
        }

        /// Get current connection state.
        pub fn getState(self: *const Self) State {
            return self.state_machine.state;
        }

        /// Check if connected.
        pub fn isConnected(self: *const Self) bool {
            return self.state_machine.state == .connected;
        }

        /// Close the connection.
        pub fn close(self: *Self) void {
            self.state_machine.close();
            self.transport.close();
            _ = self.events.push(.{ .disconnected = .{
                .reason = .user_close,
                .error_msg = null,
            } });
        }
    };
}

test "connection with mock transport" {
    const MockTransport = transport_mod.MockTransport;

    // Simulate server INFO response
    const info_response =
        \\INFO {"server_id":"test","version":"2.10.0","max_payload":1048576}
        \\
    ;

    var mock = MockTransport.init(info_response);
    defer mock.deinit(std.testing.allocator);

    var conn: Connection(MockTransport) = .{ .transport = mock };

    try std.testing.expectEqual(State.disconnected, conn.getState());
}
