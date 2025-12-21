//! Core NATS Types
//!
//! This module defines the core types used throughout the NATS client:
//! - Message: Received messages with subject, data, and optional headers
//! - Options: Connection configuration options
//! - Error: NATS-specific error types
//! - Subscription: Subscription handle for receiving messages

const std = @import("std");

pub const errors = @import("types/errors.zig");
pub const options = @import("types/options.zig");
pub const message = @import("types/message.zig");
pub const subscription = @import("types/subscription.zig");

// Re-export common types
pub const Error = errors.Error;
pub const Options = options.Options;
pub const Message = message.Message;
pub const Subscription = subscription.Subscription;

test {
    std.testing.refAllDecls(@This());
}
