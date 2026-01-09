//! Connection Module
//!
//! Provides connection state management and events for NATS.

const std = @import("std");

pub const state = @import("connection/state.zig");
pub const events = @import("connection/events.zig");
pub const errors = @import("connection/errors.zig");

// State types
pub const State = state.State;
pub const StateMachine = state.StateMachine;

// Event types
pub const Event = events.Event;
pub const EventQueue = events.EventQueue;
pub const ConnectedInfo = events.ConnectedInfo;
pub const DisconnectedInfo = events.DisconnectedInfo;
pub const DisconnectReason = events.DisconnectReason;
pub const MessageInfo = events.MessageInfo;
pub const ReconnectingInfo = events.ReconnectingInfo;

// Connection errors
pub const Error = errors.Error;
pub const parseAuthError = errors.parseAuthError;
pub const isRetryable = errors.isRetryable;

test {
    std.testing.refAllDecls(@This());
}
