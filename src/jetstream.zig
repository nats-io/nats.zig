//! JetStream -- NATS persistence and streaming layer.
//!
//! Provides stream/consumer CRUD, publish with ack, pull-based
//! message consumption, and message acknowledgment protocol over
//! core NATS request/reply.

const std = @import("std");

pub const JetStream = @import("jetstream/JetStream.zig");
pub const types = @import("jetstream/types.zig");
pub const errors = @import("jetstream/errors.zig");
pub const JsMsg = @import("jetstream/message.zig").JsMsg;
pub const PullSubscription = @import(
    "jetstream/pull.zig",
).PullSubscription;

// Convenience re-exports
pub const StreamConfig = types.StreamConfig;
pub const ConsumerConfig = types.ConsumerConfig;
pub const StreamInfo = types.StreamInfo;
pub const ConsumerInfo = types.ConsumerInfo;
pub const PubAck = types.PubAck;
pub const ApiError = errors.ApiError;
pub const Response = types.Response;

test {
    std.testing.refAllDecls(@This());
}
