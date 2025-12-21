//! Transport Interface
//!
//! Defines the interface that all transports (TCP, TLS) must implement.
//! Uses comptime duck typing for zero-overhead abstraction.

const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

/// Transport capabilities and requirements.
/// Any type used as Transport must have these methods.
pub fn Transport(comptime T: type) type {
    // Validate at comptime that T has required methods
    comptime {
        if (!@hasDecl(T, "read")) {
            @compileError("Transport must have read method");
        }
        if (!@hasDecl(T, "write")) {
            @compileError("Transport must have write method");
        }
        if (!@hasDecl(T, "close")) {
            @compileError("Transport must have close method");
        }
    }
    return T;
}

/// Read/write errors common to all transports.
pub const ReadError = error{
    ConnectionClosed,
    ConnectionReset,
    Timeout,
    Unexpected,
};

pub const WriteError = error{
    ConnectionClosed,
    ConnectionReset,
    BrokenPipe,
    Unexpected,
};

/// Mock transport for testing without network.
/// Uses unmanaged ArrayList pattern - allocator passed to methods.
pub const MockTransport = struct {
    read_data: []const u8,
    read_pos: usize = 0,
    write_buf: std.ArrayListUnmanaged(u8) = .empty,
    closed: bool = false,

    /// Initialize mock transport with data to return on reads.
    pub fn init(read_data: []const u8) MockTransport {
        return .{
            .read_data = read_data,
        };
    }

    /// Free resources. Allocator must match the one used for writes.
    pub fn deinit(self: *MockTransport, allocator: std.mem.Allocator) void {
        self.write_buf.deinit(allocator);
    }

    /// Reads data from mock transport buffer.
    pub fn read(self: *MockTransport, buf: []u8) ReadError!usize {
        assert(buf.len > 0);
        if (self.closed) return ReadError.ConnectionClosed;
        if (self.read_pos >= self.read_data.len) return 0;

        const available = self.read_data.len - self.read_pos;
        const to_read = @min(buf.len, available);
        @memcpy(buf[0..to_read], self.read_data[self.read_pos..][0..to_read]);
        self.read_pos += to_read;
        return to_read;
    }

    /// Write data. Allocator used for buffer growth.
    pub fn writeWithAllocator(
        self: *MockTransport,
        allocator: std.mem.Allocator,
        data: []const u8,
    ) WriteError!usize {
        if (self.closed) return WriteError.ConnectionClosed;
        self.write_buf.appendSlice(allocator, data) catch {
            return WriteError.Unexpected;
        };
        return data.len;
    }

    /// Write without allocator - for Transport interface compatibility.
    /// Note: This variant cannot grow buffer and will fail.
    pub fn write(self: *MockTransport, data: []const u8) WriteError!usize {
        _ = self;
        _ = data;
        return WriteError.Unexpected;
    }

    /// Closes the mock transport.
    pub fn close(self: *MockTransport) void {
        self.closed = true;
    }

    /// Returns all data written to mock transport.
    pub fn written(self: *const MockTransport) []const u8 {
        return self.write_buf.items;
    }

    /// Resets mock transport with new read data.
    pub fn reset(self: *MockTransport, new_read_data: []const u8) void {
        self.read_data = new_read_data;
        self.read_pos = 0;
        self.write_buf.clearRetainingCapacity();
        self.closed = false;
    }
};

test "mock transport read" {
    var mock = MockTransport.init("hello world");
    defer mock.deinit(std.testing.allocator);

    var buf: [5]u8 = undefined;
    const n1 = try mock.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n1);
    try std.testing.expectEqualSlices(u8, "hello", buf[0..n1]);

    const n2 = try mock.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualSlices(u8, " worl", buf[0..n2]);
}

test "mock transport write" {
    const allocator = std.testing.allocator;
    var mock = MockTransport.init("");
    defer mock.deinit(allocator);

    _ = try mock.writeWithAllocator(allocator, "hello");
    _ = try mock.writeWithAllocator(allocator, " world");

    try std.testing.expectEqualSlices(u8, "hello world", mock.written());
}

test "mock transport close" {
    const allocator = std.testing.allocator;
    var mock = MockTransport.init("data");
    defer mock.deinit(allocator);

    mock.close();

    var buf: [10]u8 = undefined;
    try std.testing.expectError(ReadError.ConnectionClosed, mock.read(&buf));
    try std.testing.expectError(
        WriteError.ConnectionClosed,
        mock.writeWithAllocator(allocator, "test"),
    );
}
