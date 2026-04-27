//! Pub/Sub Module
//!
//! Provides publish/subscribe utilities including inbox generation
//! and subject validation. For Message and Subscription types, see client.zig.

const std = @import("std");

pub const inbox = @import("pubsub/inbox.zig");
pub const subject = @import("pubsub/subject.zig");
pub const subscription = @import("pubsub/subscription.zig");

// Subscription state enum (for embedded/fixed use)
pub const SubscriptionState = subscription.State;
pub const SubscriptionError = subscription.Error;

// Fixed types for embedded use (no allocations)
pub const FixedQueue = subscription.FixedQueue;
pub const FixedSubscription = subscription.FixedSubscription;
pub const FixedSubConfig = subscription.FixedSubConfig;

pub const newInbox = inbox.newInbox;
pub const newInboxBuf = inbox.newInboxBuf;
pub const isInbox = inbox.isInbox;

pub const validatePublish = subject.validatePublish;
pub const validateSubscribe = subject.validateSubscribe;
pub const validateQueueGroup = subject.validateQueueGroup;
pub const validateReplyTo = subject.validateReplyTo;
pub const subjectMatches = subject.matches;

test {
    std.testing.refAllDecls(@This());
}
