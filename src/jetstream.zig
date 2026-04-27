//! JetStream -- NATS persistence and streaming layer.
//!
//! Provides stream/consumer CRUD, publish with ack, pull-based
//! message consumption, and message acknowledgment protocol over
//! core NATS request/reply.

const std = @import("std");

pub const JetStream = @import("jetstream/JetStream.zig");
pub const types = @import("jetstream/types.zig");
pub const errors = @import("jetstream/errors.zig");
pub const consumer = @import("jetstream/consumer.zig");
const message = @import("jetstream/message.zig");
pub const JsMsg = message.JsMsg;
pub const MsgMetadata = message.MsgMetadata;
const pull_mod = @import("jetstream/pull.zig");
pub const PullSubscription = pull_mod.PullSubscription;
pub const MessagesContext = pull_mod.MessagesContext;
const push_mod = @import("jetstream/push.zig");
pub const PushSubscription = push_mod.PushSubscription;
const ordered_mod = @import("jetstream/ordered.zig");
pub const OrderedConsumer = ordered_mod.OrderedConsumer;
const kv_mod = @import("jetstream/kv.zig");
pub const KeyValue = kv_mod.KeyValue;
pub const KvWatcher = kv_mod.KvWatcher;
pub const KeyLister = kv_mod.KeyValue.KeyLister;
pub const KeyValueConfig = types.KeyValueConfig;
pub const KeyValueEntry = types.KeyValueEntry;
pub const KeyValueOp = types.KeyValueOp;
pub const WatchOpts = types.WatchOpts;

// Consumer abstractions
pub const JsMsgHandler = consumer.JsMsgHandler;
pub const ConsumeContext = consumer.ConsumeContext;
pub const ConsumeOpts = consumer.ConsumeOpts;
pub const HeartbeatMonitor = consumer.HeartbeatMonitor;

// Convenience re-exports
pub const StreamConfig = types.StreamConfig;
pub const ConsumerConfig = types.ConsumerConfig;
pub const StreamInfo = types.StreamInfo;
pub const ConsumerInfo = types.ConsumerInfo;
pub const PubAck = types.PubAck;
pub const ApiError = errors.ApiError;
pub const Response = types.Response;
pub const AccountInfo = types.AccountInfo;
pub const ConsumerPauseResponse = types.ConsumerPauseResponse;
pub const PublishOpts = types.PublishOpts;
pub const MsgGetResponse = types.MsgGetResponse;
pub const KeyValueStatus = types.KeyValueStatus;
const async_pub = @import("jetstream/async_publish.zig");
pub const AsyncPublisher = async_pub.AsyncPublisher;
pub const PubAckFuture = async_pub.PubAckFuture;

test {
    std.testing.refAllDecls(@This());
}
