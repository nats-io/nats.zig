//! Connection State Machine
//!
//! Manages the connection state transitions for NATS protocol.

const std = @import("std");
const assert = std.debug.assert;

/// Connection states.
pub const State = enum {
    /// Initial state, not connected.
    disconnected,

    /// TCP connection established, waiting for INFO.
    connecting,

    /// Received INFO, sent CONNECT, waiting for response.
    authenticating,

    /// Fully connected and ready.
    connected,

    /// Connection lost, attempting reconnect.
    reconnecting,

    /// Gracefully draining before close.
    draining,

    /// Permanently closed.
    closed,

    /// Thread-safe state read (use from io_task/callback_task).
    pub inline fn atomicLoad(state_ptr: *const State) State {
        return @atomicLoad(State, state_ptr, .acquire);
    }

    /// Thread-safe state write (use from io_task).
    pub inline fn atomicStore(state_ptr: *State, new_state: State) void {
        @atomicStore(State, state_ptr, new_state, .release);
    }

    /// Returns true if the connection can send messages.
    pub fn canSend(self: State) bool {
        return self == .connected or self == .draining;
    }

    /// Returns true if the connection can receive messages.
    pub fn canReceive(self: State) bool {
        return self == .connected or self == .draining;
    }

    /// Returns true if the connection is in a terminal state.
    pub fn isTerminal(self: State) bool {
        return self == .closed;
    }

    /// Returns true if the connection should attempt reconnection.
    pub fn shouldReconnect(self: State) bool {
        return self == .reconnecting;
    }
};

/// State machine for connection lifecycle.
pub const StateMachine = struct {
    state: State = .disconnected,
    last_error: ?[]const u8 = null,

    /// Transitions to connecting state.
    pub fn startConnect(self: *StateMachine) !void {
        switch (self.state) {
            .disconnected, .reconnecting => {
                self.state = .connecting;
                self.last_error = null;
                assert(self.state == .connecting);
            },
            .closed => return error.ConnectionClosed,
            else => return error.InvalidState,
        }
    }

    /// Called when INFO is received.
    pub fn receivedInfo(self: *StateMachine) !void {
        if (self.state != .connecting) return error.InvalidState;
        self.state = .authenticating;
        assert(self.state == .authenticating);
    }

    /// Called when CONNECT is acknowledged.
    pub fn connectAcknowledged(self: *StateMachine) !void {
        if (self.state != .authenticating) return error.InvalidState;
        self.state = .connected;
        assert(self.state == .connected);
    }

    /// Called when connection is lost.
    pub fn connectionLost(self: *StateMachine, err: ?[]const u8) void {
        self.last_error = err;
        switch (self.state) {
            .connected, .authenticating, .connecting => {
                self.state = .reconnecting;
            },
            .draining => {
                self.state = .closed;
            },
            else => {},
        }
    }

    /// Starts graceful drain.
    pub fn startDrain(self: *StateMachine) !void {
        if (self.state != .connected) return error.InvalidState;
        self.state = .draining;
        assert(self.state == .draining);
    }

    /// Closes the connection permanently.
    pub fn close(self: *StateMachine) void {
        self.state = .closed;
        assert(self.state.isTerminal());
    }

    /// Resets to disconnected for reconnection attempt.
    pub fn resetForReconnect(self: *StateMachine) !void {
        if (self.state != .reconnecting) return error.InvalidState;
        self.state = .disconnected;
        assert(self.state == .disconnected);
    }
};

test "state machine happy path" {
    var sm: StateMachine = .{};

    try std.testing.expectEqual(State.disconnected, sm.state);

    try sm.startConnect();
    try std.testing.expectEqual(State.connecting, sm.state);

    try sm.receivedInfo();
    try std.testing.expectEqual(State.authenticating, sm.state);

    try sm.connectAcknowledged();
    try std.testing.expectEqual(State.connected, sm.state);

    try std.testing.expect(sm.state.canSend());
    try std.testing.expect(sm.state.canReceive());
}

test "state machine reconnect" {
    var sm: StateMachine = .{};

    try sm.startConnect();
    try sm.receivedInfo();
    try sm.connectAcknowledged();

    sm.connectionLost("test error");
    try std.testing.expectEqual(State.reconnecting, sm.state);
    try std.testing.expect(sm.state.shouldReconnect());

    try sm.resetForReconnect();
    try std.testing.expectEqual(State.disconnected, sm.state);
}

test "state machine drain" {
    var sm: StateMachine = .{};

    try sm.startConnect();
    try sm.receivedInfo();
    try sm.connectAcknowledged();

    try sm.startDrain();
    try std.testing.expectEqual(State.draining, sm.state);
    try std.testing.expect(sm.state.canSend());

    sm.connectionLost(null);
    try std.testing.expectEqual(State.closed, sm.state);
    try std.testing.expect(sm.state.isTerminal());
}

test "state machine close" {
    var sm: StateMachine = .{};

    try sm.startConnect();
    sm.close();

    try std.testing.expectEqual(State.closed, sm.state);
    try std.testing.expectError(error.ConnectionClosed, sm.startConnect());
}
