//! TCP Transport
//!
//! Implements TCP transport for NATS connections using std.Io.net.
//! This module provides the TcpTransport type that satisfies the
//! Transport interface for use with the Connection type.

const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;
const net = Io.net;

const transport = @import("transport.zig");
const ReadError = transport.ReadError;
const WriteError = transport.WriteError;

/// TCP transport for NATS connections.
/// Wraps std.Io.net.Stream with buffered I/O.
pub const TcpTransport = struct {
    stream: net.Stream,
    io: Io,
    reader: net.Stream.Reader,
    writer: net.Stream.Writer,
    read_buffer: [8192]u8,
    write_buffer: [8192]u8,
    connected: bool,

    /// Connect to a NATS server at the given host and port.
    pub fn connect(
        io: Io,
        host: []const u8,
        port: u16,
    ) ConnectError!TcpTransport {
        assert(host.len > 0);
        assert(port > 0);
        const address = net.IpAddress.parse(host, port) catch {
            return error.InvalidAddress;
        };

        const stream = net.IpAddress.connect(address, io, .{
            .mode = .stream,
            .protocol = .tcp,
        }) catch {
            return error.ConnectionFailed;
        };

        // Set TCP_NODELAY to disable Nagle's algorithm for low latency
        const enable: u32 = 1;
        std.posix.setsockopt(
            stream.socket.handle,
            std.posix.IPPROTO.TCP,
            std.os.linux.TCP.NODELAY,
            std.mem.asBytes(&enable),
        ) catch {};

        var result = TcpTransport{
            .stream = stream,
            .io = io,
            .reader = undefined,
            .writer = undefined,
            .read_buffer = undefined,
            .write_buffer = undefined,
            .connected = true,
        };

        result.reader = stream.reader(io, &result.read_buffer);
        result.writer = stream.writer(io, &result.write_buffer);

        return result;
    }

    pub const ConnectError = error{
        InvalidAddress,
        ConnectionFailed,
        ConnectionRefused,
        Timeout,
    };

    /// Reads data from the TCP connection.
    pub fn read(self: *TcpTransport, buf: []u8) ReadError!usize {
        assert(buf.len > 0);
        if (!self.connected) return ReadError.ConnectionClosed;

        const n = self.reader.interface.readAll(buf) catch |err| {
            return switch (err) {
                error.EndOfStream => ReadError.ConnectionClosed,
                error.ReadFailed => blk: {
                    if (self.reader.err) |e| {
                        _ = e;
                        break :blk ReadError.ConnectionReset;
                    }
                    break :blk ReadError.Unexpected;
                },
                else => ReadError.Unexpected,
            };
        };
        return n;
    }

    /// Writes data to the TCP connection.
    pub fn write(self: *TcpTransport, data: []const u8) WriteError!usize {
        assert(data.len > 0);
        if (!self.connected) return WriteError.ConnectionClosed;

        self.writer.interface.writeAll(data) catch |err| {
            return switch (err) {
                error.WriteFailed => blk: {
                    if (self.writer.err) |e| {
                        _ = e;
                        break :blk WriteError.ConnectionReset;
                    }
                    break :blk WriteError.BrokenPipe;
                },
                else => WriteError.Unexpected,
            };
        };
        return data.len;
    }

    /// Flushes the write buffer.
    pub fn flush(self: *TcpTransport) WriteError!void {
        assert(self.connected);
        if (!self.connected) return WriteError.ConnectionClosed;

        self.writer.interface.flush() catch {
            return WriteError.Unexpected;
        };
    }

    /// Closes the TCP connection.
    pub fn close(self: *TcpTransport) void {
        if (self.connected) {
            self.stream.close(self.io);
            self.connected = false;
        }
    }

    /// Returns true if the connection is open.
    pub fn isConnected(self: *const TcpTransport) bool {
        return self.connected;
    }
};

// Compile-time verification that TcpTransport satisfies Transport interface
comptime {
    _ = transport.Transport(TcpTransport);
}
