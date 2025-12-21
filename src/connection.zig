//! Connection Module
//!
//! Provides connection management for NATS including TCP transport,
//! state machine, and event handling.

const std = @import("std");

pub const transport = @import("connection/transport.zig");
pub const state = @import("connection/state.zig");
pub const events = @import("connection/events.zig");
pub const tcp = @import("connection/tcp.zig");
pub const conn = @import("connection/conn.zig");

// Re-export common types
pub const Transport = transport.Transport;
pub const MockTransport = transport.MockTransport;
pub const ReadError = transport.ReadError;
pub const WriteError = transport.WriteError;

pub const State = state.State;
pub const StateMachine = state.StateMachine;

pub const Event = events.Event;
pub const EventQueue = events.EventQueue;
pub const ConnectedInfo = events.ConnectedInfo;
pub const DisconnectedInfo = events.DisconnectedInfo;
pub const DisconnectReason = events.DisconnectReason;
pub const MessageInfo = events.MessageInfo;

pub const TcpTransport = tcp.TcpTransport;
pub const Connection = conn.Connection;
pub const Options = conn.Options;

test {
    std.testing.refAllDecls(@This());
}
