//! JetStream type definitions for stream/consumer configuration,
//! API responses, and request payloads.
//!
//! All structs use optional fields with null defaults for
//! forward-compatible JSON serialization (omit nulls) and
//! parsing (ignore unknown fields).

const std = @import("std");
const errors = @import("errors.zig");

pub const ApiErrorJson = errors.ApiErrorJson;

// -- Enums (lowercase tags match NATS JSON wire format) --

pub const RetentionPolicy = enum {
    limits,
    interest,
    workqueue,
};

pub const StorageType = enum { file, memory };
pub const DiscardPolicy = enum { old, new };
pub const StoreCompression = enum { none, s2 };

pub const DeliverPolicy = enum {
    all,
    last,
    new,
    by_start_sequence,
    by_start_time,
    last_per_subject,
};

pub const AckPolicy = enum { none, all, explicit };
pub const ReplayPolicy = enum { instant, original };

// -- Stream types --

pub const StreamConfig = struct {
    name: []const u8,
    description: ?[]const u8 = null,
    subjects: ?[]const []const u8 = null,
    retention: ?RetentionPolicy = null,
    max_consumers: ?i64 = null,
    max_msgs: ?i64 = null,
    max_bytes: ?i64 = null,
    max_age: ?i64 = null,
    max_msgs_per_subject: ?i64 = null,
    max_msg_size: ?i32 = null,
    storage: ?StorageType = null,
    num_replicas: ?i32 = null,
    no_ack: ?bool = null,
    duplicate_window: ?i64 = null,
    discard: ?DiscardPolicy = null,
    discard_new_per_subject: ?bool = null,
    sealed: ?bool = null,
    deny_delete: ?bool = null,
    deny_purge: ?bool = null,
    allow_rollup_hdrs: ?bool = null,
    allow_direct: ?bool = null,
    mirror_direct: ?bool = null,
    compression: ?StoreCompression = null,
    first_seq: ?u64 = null,
};

pub const StreamState = struct {
    messages: u64 = 0,
    bytes: u64 = 0,
    first_seq: u64 = 0,
    last_seq: u64 = 0,
    consumer_count: i64 = 0,
    num_deleted: i64 = 0,
    num_subjects: u64 = 0,
};

pub const StreamInfo = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    config: ?StreamConfig = null,
    state: ?StreamState = null,
    created: ?[]const u8 = null,
    ts: ?[]const u8 = null,
};

// -- Consumer types --

pub const ConsumerConfig = struct {
    name: ?[]const u8 = null,
    durable_name: ?[]const u8 = null,
    description: ?[]const u8 = null,
    deliver_policy: ?DeliverPolicy = null,
    opt_start_seq: ?u64 = null,
    ack_policy: ?AckPolicy = null,
    ack_wait: ?i64 = null,
    max_deliver: ?i64 = null,
    filter_subject: ?[]const u8 = null,
    filter_subjects: ?[]const []const u8 = null,
    replay_policy: ?ReplayPolicy = null,
    max_waiting: ?i64 = null,
    max_ack_pending: ?i64 = null,
    inactive_threshold: ?i64 = null,
    num_replicas: ?i32 = null,
    headers_only: ?bool = null,
};

pub const SequenceInfo = struct {
    consumer_seq: u64 = 0,
    stream_seq: u64 = 0,
};

pub const ConsumerInfo = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    stream_name: ?[]const u8 = null,
    name: ?[]const u8 = null,
    config: ?ConsumerConfig = null,
    delivered: ?SequenceInfo = null,
    ack_floor: ?SequenceInfo = null,
    num_ack_pending: i64 = 0,
    num_redelivered: i64 = 0,
    num_waiting: i64 = 0,
    num_pending: u64 = 0,
    created: ?[]const u8 = null,
    ts: ?[]const u8 = null,
};

pub const CreateConsumerRequest = struct {
    stream_name: []const u8,
    config: ConsumerConfig,
    action: ?[]const u8 = null,
};

// -- Publish types --

pub const PubAck = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    stream: ?[]const u8 = null,
    seq: u64 = 0,
    duplicate: ?bool = null,
    domain: ?[]const u8 = null,
};

pub const PublishOpts = struct {
    msg_id: ?[]const u8 = null,
    expected_stream: ?[]const u8 = null,
    expected_last_seq: ?u64 = null,
    expected_last_msg_id: ?[]const u8 = null,
    expected_last_subj_seq: ?u64 = null,
};

// -- Pull types --

pub const PullRequest = struct {
    batch: ?i64 = null,
    expires: ?i64 = null,
    no_wait: ?bool = null,
    max_bytes: ?i64 = null,
};

// -- Delete response --

pub const DeleteResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    success: bool = false,
};

// -- Purge response --

pub const PurgeResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    success: bool = false,
    purged: u64 = 0,
};

// -- Generic response wrapper --

/// Wraps a parsed JSON response. All string slices in
/// `value` point into the parsed arena -- they become
/// invalid after `deinit()`. Copy any strings you need
/// to keep: `const s = try alloc.dupe(u8, val.name);`
/// Caller MUST call `deinit()`.
pub fn Response(comptime T: type) type {
    return struct {
        const Self = @This();
        value: T,
        _parsed: std.json.Parsed(T),

        pub fn deinit(self: *Self) void {
            self._parsed.deinit();
        }
    };
}

// -- JSON helpers --

const json_stringify_opts: std.json.Stringify.Options = .{
    .emit_null_optional_fields = false,
};

const json_parse_opts: std.json.ParseOptions = .{
    .ignore_unknown_fields = true,
};

/// Serializes a value to JSON, omitting null optional fields.
pub fn jsonStringify(
    allocator: std.mem.Allocator,
    value: anytype,
) error{OutOfMemory}![]u8 {
    return std.json.Stringify.valueAlloc(
        allocator,
        value,
        json_stringify_opts,
    );
}

/// Parses JSON into type T, ignoring unknown fields.
pub fn jsonParse(
    comptime T: type,
    allocator: std.mem.Allocator,
    data: []const u8,
) std.json.ParseError(std.json.Scanner)!std.json.Parsed(T) {
    return std.json.parseFromSlice(
        T,
        allocator,
        data,
        json_parse_opts,
    );
}

// -- Tests --

test "StreamConfig JSON round-trip" {
    const alloc = std.testing.allocator;
    const config = StreamConfig{
        .name = "TEST",
        .subjects = &.{"test.>"},
        .retention = .limits,
        .storage = .file,
        .max_msgs = 1000,
    };

    const json = try jsonStringify(alloc, config);
    defer alloc.free(json);

    var parsed = try jsonParse(StreamConfig, alloc, json);
    defer parsed.deinit();

    const v = parsed.value;
    try std.testing.expectEqualStrings("TEST", v.name);
    try std.testing.expectEqual(RetentionPolicy.limits, v.retention.?);
    try std.testing.expectEqual(StorageType.file, v.storage.?);
    try std.testing.expectEqual(@as(i64, 1000), v.max_msgs.?);
    try std.testing.expect(v.subjects != null);
    try std.testing.expectEqual(@as(usize, 1), v.subjects.?.len);
}

test "ConsumerConfig JSON round-trip" {
    const alloc = std.testing.allocator;
    const config = ConsumerConfig{
        .name = "my-consumer",
        .durable_name = "my-consumer",
        .ack_policy = .explicit,
        .deliver_policy = .all,
        .max_ack_pending = 1000,
    };

    const json = try jsonStringify(alloc, config);
    defer alloc.free(json);

    var parsed = try jsonParse(ConsumerConfig, alloc, json);
    defer parsed.deinit();

    const v = parsed.value;
    try std.testing.expectEqualStrings("my-consumer", v.name.?);
    try std.testing.expectEqual(AckPolicy.explicit, v.ack_policy.?);
    try std.testing.expectEqual(
        DeliverPolicy.all,
        v.deliver_policy.?,
    );
}

test "PubAck parse with error" {
    const alloc = std.testing.allocator;
    const json =
        \\{"type":"io.nats.jetstream.api.v1.pub_ack",
        \\"error":{"code":503,"err_code":10076,
        \\"description":"jetstream not enabled"}}
    ;

    var parsed = try jsonParse(PubAck, alloc, json);
    defer parsed.deinit();

    const v = parsed.value;
    try std.testing.expect(v.@"error" != null);
    try std.testing.expectEqual(@as(u16, 503), v.@"error".?.code);
    try std.testing.expectEqual(
        @as(u16, 10076),
        v.@"error".?.err_code,
    );
}

test "DeleteResponse parse success" {
    const alloc = std.testing.allocator;
    const json = "{\"success\":true}";

    var parsed = try jsonParse(DeleteResponse, alloc, json);
    defer parsed.deinit();

    try std.testing.expect(parsed.value.success);
}

test "null optional fields omitted in JSON" {
    const alloc = std.testing.allocator;
    const config = StreamConfig{ .name = "TEST" };

    const json = try jsonStringify(alloc, config);
    defer alloc.free(json);

    // Should not contain "retention" since it's null
    try std.testing.expect(
        std.mem.indexOf(u8, json, "retention") == null,
    );
    // Should contain "name"
    try std.testing.expect(
        std.mem.indexOf(u8, json, "name") != null,
    );
}
