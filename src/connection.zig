//! Connection Module
//!
//! Provides connection state management and events for NATS.

const std = @import("std");

pub const state = @import("connection/state.zig");
pub const events = @import("connection/events.zig");
pub const errors = @import("connection/errors.zig");
pub const server_pool = @import("connection/server_pool.zig");
pub const io_task = @import("connection/io_task.zig");

pub const State = state.State;
pub const StateMachine = state.StateMachine;

pub const Event = events.Event;
pub const ConnectedInfo = events.ConnectedInfo;
pub const DisconnectedInfo = events.DisconnectedInfo;
pub const DisconnectReason = events.DisconnectReason;
pub const MessageInfo = events.MessageInfo;
pub const ReconnectingInfo = events.ReconnectingInfo;

pub const Error = errors.Error;
pub const parseAuthError = errors.parseAuthError;
pub const isRetryable = errors.isRetryable;

pub const ServerPool = server_pool.ServerPool;

test {
    std.testing.refAllDecls(@This());
}
