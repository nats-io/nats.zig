//! Connection Events
//!
//! Event queue for connection lifecycle and message events.
//! Uses event queues over callbacks for composability and testability.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

const commands = @import("../protocol/commands.zig");
const ServerInfo = commands.ServerInfo;

/// Connection events that can be polled by the user.
pub const Event = union(enum) {
    /// Successfully connected to server.
    connected: ConnectedInfo,

    /// Disconnected from server.
    disconnected: DisconnectedInfo,

    /// Received a message.
    message: MessageInfo,

    /// Received an error from server.
    server_error: []const u8,

    /// Reconnecting to server.
    reconnecting: ReconnectingInfo,

    /// Lamport drain started.
    drain_started,

    /// Lamport drain completed.
    drain_completed,
};

/// Information about successful connection.
pub const ConnectedInfo = struct {
    /// Server information received during handshake.
    server_id: []const u8,
    server_name: []const u8,
    version: []const u8,
    /// True if this is a reconnection.
    is_reconnect: bool,
};

/// Information about disconnection.
pub const DisconnectedInfo = struct {
    /// Reason for disconnection.
    reason: DisconnectReason,
    /// Error message if applicable.
    error_msg: ?[]const u8,
};

/// Reasons for disconnection.
pub const DisconnectReason = enum {
    /// Normal close requested by user.
    user_close,
    /// Server closed connection.
    server_close,
    /// Network error occurred.
    network_error,
    /// Authentication failed.
    auth_failed,
    /// Protocol error.
    protocol_error,
    /// Connection timeout.
    timeout,
};

/// Information about received message.
pub const MessageInfo = struct {
    /// Message subject.
    subject: []const u8,
    /// Subscription ID that matched.
    sid: u64,
    /// Optional reply-to subject.
    reply_to: ?[]const u8,
    /// Message payload.
    data: []const u8,
    /// Header data if present (HMSG).
    headers: ?[]const u8,
};

/// Information about reconnection attempt.
pub const ReconnectingInfo = struct {
    /// Current attempt number.
    attempt: u32,
    /// Maximum attempts configured.
    max_attempts: u32,
    /// Server being connected to.
    server: []const u8,
};

/// Thread-safe event queue using a ring buffer.
/// Events are stored inline to avoid allocations during normal operation.
pub const EventQueue = struct {
    const CAPACITY = 256;

    events: [CAPACITY]Event = undefined,
    head: usize = 0,
    tail: usize = 0,
    count: usize = 0,

    /// Pushes an event to the queue.
    /// Returns false if queue is full.
    pub fn push(self: *EventQueue, event: Event) bool {
        if (self.count >= CAPACITY) return false;
        assert(self.count < CAPACITY);

        self.events[self.tail] = event;
        self.tail = (self.tail + 1) % CAPACITY;
        self.count += 1;
        assert(self.count <= CAPACITY);
        return true;
    }

    /// Pops an event from the queue.
    /// Returns null if queue is empty.
    pub fn pop(self: *EventQueue) ?Event {
        if (self.count == 0) return null;
        assert(self.count > 0);

        const event = self.events[self.head];
        self.head = (self.head + 1) % CAPACITY;
        self.count -= 1;
        assert(self.count <= CAPACITY);
        return event;
    }

    /// Returns number of events in queue.
    pub fn len(self: *const EventQueue) usize {
        return self.count;
    }

    /// Returns true if queue is empty.
    pub fn isEmpty(self: *const EventQueue) bool {
        return self.count == 0;
    }

    /// Returns true if queue is full.
    pub fn isFull(self: *const EventQueue) bool {
        return self.count >= CAPACITY;
    }

    /// Clears all events from queue.
    pub fn clear(self: *EventQueue) void {
        self.head = 0;
        self.tail = 0;
        self.count = 0;
        assert(self.isEmpty());
    }
};

test "event queue push and pop" {
    var queue: EventQueue = .{};

    try std.testing.expect(queue.isEmpty());
    try std.testing.expectEqual(@as(usize, 0), queue.len());

    const event1: Event = .{ .connected = .{
        .server_id = "test",
        .server_name = "test-server",
        .version = "2.10.0",
        .is_reconnect = false,
    } };

    try std.testing.expect(queue.push(event1));
    try std.testing.expectEqual(@as(usize, 1), queue.len());
    try std.testing.expect(!queue.isEmpty());

    const popped = queue.pop();
    try std.testing.expect(popped != null);
    try std.testing.expectEqual(@as(usize, 0), queue.len());
    try std.testing.expect(queue.isEmpty());
}

test "event queue full" {
    var queue: EventQueue = .{};

    const event: Event = .drain_started;

    var i: usize = 0;
    while (i < EventQueue.CAPACITY) : (i += 1) {
        try std.testing.expect(queue.push(event));
    }

    try std.testing.expect(queue.isFull());
    try std.testing.expect(!queue.push(event));

    // Pop one and push should work again
    _ = queue.pop();
    try std.testing.expect(!queue.isFull());
    try std.testing.expect(queue.push(event));
}

test "event queue clear" {
    var queue: EventQueue = .{};

    _ = queue.push(.drain_started);
    _ = queue.push(.drain_completed);

    try std.testing.expectEqual(@as(usize, 2), queue.len());

    queue.clear();

    try std.testing.expectEqual(@as(usize, 0), queue.len());
    try std.testing.expect(queue.isEmpty());
}

test "event queue wraparound" {
    var queue: EventQueue = .{};

    const event: Event = .drain_started;

    // Push and pop to move head/tail forward
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        try std.testing.expect(queue.push(event));
        _ = queue.pop();
    }

    // Now fill queue - should handle wraparound correctly
    i = 0;
    while (i < EventQueue.CAPACITY) : (i += 1) {
        try std.testing.expect(queue.push(event));
    }

    try std.testing.expect(queue.isFull());
    try std.testing.expectEqual(@as(usize, EventQueue.CAPACITY), queue.len());
}

test "message event" {
    var queue: EventQueue = .{};

    const msg_event: Event = .{ .message = .{
        .subject = "test.subject",
        .sid = 42,
        .reply_to = "_INBOX.reply",
        .data = "hello",
        .headers = null,
    } };

    try std.testing.expect(queue.push(msg_event));

    const popped = queue.pop().?;
    switch (popped) {
        .message => |msg| {
            try std.testing.expectEqualSlices(u8, "test.subject", msg.subject);
            try std.testing.expectEqual(@as(u64, 42), msg.sid);
            const reply = "_INBOX.reply";
            try std.testing.expectEqualSlices(u8, reply, msg.reply_to.?);
            try std.testing.expectEqualSlices(u8, "hello", msg.data);
        },
        else => return error.UnexpectedEvent,
    }
}

test "disconnected event" {
    var queue: EventQueue = .{};

    const event: Event = .{ .disconnected = .{
        .reason = .network_error,
        .error_msg = "connection reset",
    } };

    try std.testing.expect(queue.push(event));

    const popped = queue.pop().?;
    switch (popped) {
        .disconnected => |info| {
            const expected_reason = DisconnectReason.network_error;
            try std.testing.expectEqual(expected_reason, info.reason);
            const expected_msg = "connection reset";
            const err_msg = info.error_msg.?;
            try std.testing.expectEqualSlices(u8, expected_msg, err_msg);
        },
        else => return error.UnexpectedEvent,
    }
}
