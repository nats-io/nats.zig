//! Pub/Sub Module
//!
//! Provides publish/subscribe functionality including inbox generation,
//! subject validation, and subscription management.

const std = @import("std");

pub const inbox = @import("pubsub/inbox.zig");
pub const subject = @import("pubsub/subject.zig");
pub const subscription = @import("pubsub/subscription.zig");

// Re-export subscription types
pub const Message = subscription.Message;
pub const ReceiveOptions = subscription.ReceiveOptions;
pub const MessageQueue = subscription.MessageQueue;
pub const SubscriptionState = subscription.State;

// Comptime generic subscription - instantiate with client type
pub const Subscription = subscription.Subscription;

// Inbox functions
pub const newInbox = inbox.newInbox;
pub const newInboxBuf = inbox.newInboxBuf;
pub const isInbox = inbox.isInbox;

// Subject functions
pub const validatePublish = subject.validatePublish;
pub const validateSubscribe = subject.validateSubscribe;
pub const subjectMatches = subject.matches;

test {
    std.testing.refAllDecls(@This());
}
