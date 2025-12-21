//! NATS Connection Example
//!
//! Demonstrates connecting to a NATS server, receiving INFO,
//! and sending CONNECT. Run with: zig build run-connect
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();

    // Create threaded I/O
    var threaded = Io.Threaded.init(gpa.allocator());
    defer threaded.deinit();

    const io = threaded.io();

    // Parse address
    const address = try net.IpAddress.parse("127.0.0.1", 4222);

    std.debug.print("Connecting to NATS at 127.0.0.1:4222...\n", .{});

    // Connect
    const stream = net.IpAddress.connect(address, io, .{
        .mode = .stream,
        .protocol = .tcp,
    }) catch |err| {
        std.debug.print("Connection failed: {}\n", .{err});
        return err;
    };
    defer stream.close(io);

    std.debug.print("Connected!\n", .{});

    // Create reader/writer buffers
    var read_buf: [4096]u8 = undefined;
    var write_buf: [1024]u8 = undefined;

    var reader = stream.reader(io, &read_buf);
    var writer = stream.writer(io, &write_buf);

    // Read INFO from server (greedily fill buffer)
    const info_data = reader.interface.peekGreedy(1) catch |err| {
        std.debug.print("Read error: {}\n", .{err});
        return err;
    };

    const len = info_data.len;
    std.debug.print("Received ({d} bytes):\n{s}\n", .{ len, info_data });
    reader.interface.tossBuffered();

    // Send CONNECT
    const connect_cmd = "CONNECT {\"verbose\":false,\"pedantic\":false," ++
        "\"lang\":\"zig\",\"version\":\"0.1.0\"}\r\n";

    writer.interface.writeAll(connect_cmd) catch |err| {
        std.debug.print("Write error: {}\n", .{err});
        return err;
    };

    writer.interface.flush() catch |err| {
        std.debug.print("Flush error: {}\n", .{err});
        return err;
    };

    std.debug.print("Sent CONNECT command\n", .{});

    // Send PING
    writer.interface.writeAll("PING\r\n") catch |err| {
        std.debug.print("Write error: {}\n", .{err});
        return err;
    };

    writer.interface.flush() catch |err| {
        std.debug.print("Flush error: {}\n", .{err});
        return err;
    };

    std.debug.print("Sent PING\n", .{});

    // Read PONG (greedily fill buffer)
    const pong_data = reader.interface.peekGreedy(1) catch |err| {
        std.debug.print("Read error: {}\n", .{err});
        return err;
    };

    std.debug.print("Received: {s}\n", .{pong_data});
    reader.interface.tossBuffered();
    std.debug.print("Successfully connected to NATS!\n", .{});
}
