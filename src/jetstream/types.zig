//! JetStream type definitions for stream/consumer configuration,
//! API responses, and request payloads.
//!
//! All structs use optional fields with null defaults for
//! forward-compatible JSON serialization (omit nulls) and
//! parsing (ignore unknown fields).

const std = @import("std");
const errors = @import("errors.zig");
const headers = @import("../protocol/headers.zig");

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

pub const SubjectTransform = struct {
    src: ?[]const u8 = null,
    dest: ?[]const u8 = null,
};

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
    allow_msg_ttl: ?bool = null,
    metadata: ?std.json.Value = null,
    subject_transform: ?SubjectTransform = null,
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
    mem_storage: ?bool = null,
    // Push consumer fields (v1.1 ready, harmless as null)
    deliver_subject: ?[]const u8 = null,
    deliver_group: ?[]const u8 = null,
    flow_control: ?bool = null,
    idle_heartbeat: ?i64 = null,
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
    ttl: ?[]const u8 = null,
};

/// Pre-built JetStream publish message with user headers.
///
/// Passed to `JetStream.publishMsg()` for publishing messages
/// with arbitrary user-supplied headers alongside JetStream-
/// specific headers from `opts`. On header-key collision
/// (case-insensitive per NATS convention), JetStream headers
/// from `opts` override the user-supplied value -- matching
/// Go client `PublishMsg` semantics.
pub const JsPublishMsg = struct {
    subject: []const u8,
    payload: []const u8,
    headers: ?[]const headers.Entry = null,
    opts: PublishOpts = .{},
};

// -- Pull types --

pub const PullRequest = struct {
    batch: ?i64 = null,
    expires: ?i64 = null,
    no_wait: ?bool = null,
    max_bytes: ?i64 = null,
    idle_heartbeat: ?i64 = null,
};

// -- Delete response --

pub const DeleteResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    success: bool = false,
};

// -- Purge response --

pub const PurgeRequest = struct {
    filter: ?[]const u8 = null,
    seq: ?u64 = null,
    keep: ?u64 = null,
};

pub const PurgeResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    success: bool = false,
    purged: u64 = 0,
};

// -- Listing responses (paginated) --

pub const StreamNamesResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    total: u64 = 0,
    offset: u64 = 0,
    limit: u64 = 0,
    streams: ?[]const []const u8 = null,
};

pub const StreamListResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    total: u64 = 0,
    offset: u64 = 0,
    limit: u64 = 0,
    streams: ?[]const StreamInfo = null,
};

pub const ConsumerNamesResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    total: u64 = 0,
    offset: u64 = 0,
    limit: u64 = 0,
    consumers: ?[]const []const u8 = null,
};

pub const ConsumerListResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    total: u64 = 0,
    offset: u64 = 0,
    limit: u64 = 0,
    consumers: ?[]const ConsumerInfo = null,
};

/// Request body for paginated listing APIs.
pub const ListRequest = struct {
    offset: u64 = 0,
    subject: ?[]const u8 = null,
};

// -- Key-Value types --

pub const KeyValueConfig = struct {
    bucket: []const u8,
    description: ?[]const u8 = null,
    max_value_size: ?i32 = null,
    history: ?u8 = null,
    ttl: ?i64 = null,
    max_bytes: ?i64 = null,
    storage: ?StorageType = null,
    replicas: ?i32 = null,
};

pub const KeyValueOp = enum { put, delete, purge };

/// Options for KV watch operations.
pub const WatchOpts = struct {
    /// Deliver all historical values, not just latest.
    include_history: bool = false,
    /// Skip entries with delete/purge markers.
    ignore_deletes: bool = false,
    /// Only deliver new updates, skip initial values.
    updates_only: bool = false,
    /// Only deliver metadata, not values.
    meta_only: bool = false,
    /// Resume watching from a specific revision.
    resume_from_revision: ?u64 = null,
};

pub const KeyValueEntry = struct {
    bucket: []const u8,
    key: []const u8,
    value: []const u8,
    revision: u64,
    operation: KeyValueOp,
    /// Allocator used for owned key (null = not owned).
    key_allocator: ?std.mem.Allocator = null,
    /// Allocator used for owned value (null = not owned).
    value_allocator: ?std.mem.Allocator = null,

    /// Frees owned key/value buffers if allocated.
    pub fn deinit(self: *KeyValueEntry) void {
        if (self.key_allocator) |a| {
            if (self.key.len > 0) a.free(self.key);
            self.key_allocator = null;
        }
        if (self.value_allocator) |a| {
            if (self.value.len > 0) a.free(self.value);
            self.value_allocator = null;
        }
    }
};

// -- Stream MSG.GET types --

pub const MsgGetRequest = struct {
    last_by_subj: ?[]const u8 = null,
    seq: ?u64 = null,
};

pub const MsgGetResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    message: ?StoredMsg = null,
};

pub const StoredMsg = struct {
    subject: ?[]const u8 = null,
    seq: u64 = 0,
    data: ?[]const u8 = null,
    hdrs: ?[]const u8 = null,
    time: ?[]const u8 = null,
};

// -- Key-Value status --

pub const KeyValueStatus = struct {
    bucket: []const u8 = "",
    values: u64 = 0,
    history: i64 = 1,
    ttl: i64 = 0,
    bytes: u64 = 0,
    backing_store: StorageType = .file,
    is_compressed: bool = false,
};

// -- Stream MSG.DELETE types --

pub const MsgDeleteRequest = struct {
    seq: u64,
    no_erase: ?bool = null,
};

// -- Consumer pause types --

pub const ConsumerPauseRequest = struct {
    pause_until: ?[]const u8 = null,
};

pub const ConsumerPauseResponse = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    paused: bool = false,
    pause_until: ?[]const u8 = null,
    pause_remaining: ?i64 = null,
};

// -- Consumer unpin types --

pub const ConsumerUnpinRequest = struct {
    group: []const u8,
};

// -- Account info --

pub const AccountInfo = struct {
    type: ?[]const u8 = null,
    @"error": ?ApiErrorJson = null,
    memory: u64 = 0,
    storage: u64 = 0,
    streams: u64 = 0,
    consumers: u64 = 0,
    limits: ?AccountLimits = null,
    api: ?APIStats = null,
    domain: ?[]const u8 = null,
};

pub const AccountLimits = struct {
    max_memory: i64 = 0,
    max_storage: i64 = 0,
    max_streams: i64 = 0,
    max_consumers: i64 = 0,
};

pub const APIStats = struct {
    total: u64 = 0,
    errors: u64 = 0,
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
    .allocate = .alloc_always,
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

test "StreamNamesResponse JSON round-trip" {
    const alloc = std.testing.allocator;
    const json =
        \\{"total":3,"offset":0,"limit":1024,
        \\"streams":["S1","S2","S3"]}
    ;
    var parsed = try jsonParse(
        StreamNamesResponse,
        alloc,
        json,
    );
    defer parsed.deinit();

    const v = parsed.value;
    try std.testing.expectEqual(@as(u64, 3), v.total);
    try std.testing.expectEqual(@as(u64, 0), v.offset);
    try std.testing.expect(v.streams != null);
    try std.testing.expectEqual(
        @as(usize, 3),
        v.streams.?.len,
    );
    try std.testing.expectEqualStrings(
        "S1",
        v.streams.?[0],
    );
}

test "AccountInfo JSON round-trip" {
    const alloc = std.testing.allocator;
    const json =
        \\{"memory":1024,"storage":4096,"streams":2,
        \\"consumers":5,"limits":{"max_memory":-1,
        \\"max_storage":-1,"max_streams":-1,
        \\"max_consumers":-1},"api":{"total":42,
        \\"errors":1}}
    ;
    var parsed = try jsonParse(AccountInfo, alloc, json);
    defer parsed.deinit();

    const v = parsed.value;
    try std.testing.expectEqual(@as(u64, 1024), v.memory);
    try std.testing.expectEqual(@as(u64, 4096), v.storage);
    try std.testing.expectEqual(@as(u64, 2), v.streams);
    try std.testing.expectEqual(@as(u64, 5), v.consumers);
    try std.testing.expect(v.limits != null);
    try std.testing.expect(v.api != null);
    try std.testing.expectEqual(
        @as(u64, 42),
        v.api.?.total,
    );
}

test "ConsumerConfig with push fields serializes" {
    const alloc = std.testing.allocator;
    const config = ConsumerConfig{
        .name = "push-test",
        .deliver_subject = "deliver.test",
        .deliver_group = "grp",
    };

    const json = try jsonStringify(alloc, config);
    defer alloc.free(json);

    try std.testing.expect(
        std.mem.indexOf(u8, json, "deliver_subject") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, json, "deliver_group") !=
            null,
    );

    var parsed = try jsonParse(
        ConsumerConfig,
        alloc,
        json,
    );
    defer parsed.deinit();
    try std.testing.expectEqualStrings(
        "deliver.test",
        parsed.value.deliver_subject.?,
    );
}

test "MsgDeleteRequest serialization" {
    const alloc = std.testing.allocator;
    // With no_erase
    const json1 = try jsonStringify(alloc, MsgDeleteRequest{
        .seq = 42,
        .no_erase = true,
    });
    defer alloc.free(json1);
    try std.testing.expect(
        std.mem.indexOf(u8, json1, "\"seq\":42") != null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, json1, "no_erase") != null,
    );

    // Without no_erase (null omitted)
    const json2 = try jsonStringify(
        alloc,
        MsgDeleteRequest{ .seq = 7 },
    );
    defer alloc.free(json2);
    try std.testing.expect(
        std.mem.indexOf(u8, json2, "no_erase") == null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, json2, "\"seq\":7") != null,
    );
}

test "ConsumerPauseRequest serialization" {
    const alloc = std.testing.allocator;
    const json1 = try jsonStringify(
        alloc,
        ConsumerPauseRequest{
            .pause_until = "2026-04-01T00:00:00Z",
        },
    );
    defer alloc.free(json1);
    try std.testing.expect(
        std.mem.indexOf(u8, json1, "pause_until") !=
            null,
    );

    const json2 = try jsonStringify(
        alloc,
        ConsumerPauseRequest{},
    );
    defer alloc.free(json2);
    try std.testing.expect(
        std.mem.indexOf(u8, json2, "pause_until") ==
            null,
    );
}

test "ConsumerPauseResponse parsing" {
    const alloc = std.testing.allocator;
    const json =
        \\{"paused":true,
        \\"pause_until":"2026-04-01T00:00:00Z",
        \\"pause_remaining":3600000000000}
    ;
    var parsed = try jsonParse(
        ConsumerPauseResponse,
        alloc,
        json,
    );
    defer parsed.deinit();
    try std.testing.expect(parsed.value.paused);
    try std.testing.expect(
        parsed.value.pause_until != null,
    );
    try std.testing.expectEqual(
        @as(i64, 3600000000000),
        parsed.value.pause_remaining.?,
    );
}

test "PublishOpts TTL field" {
    const alloc = std.testing.allocator;
    const json = try jsonStringify(
        alloc,
        PublishOpts{ .ttl = "5s" },
    );
    defer alloc.free(json);
    try std.testing.expect(
        std.mem.indexOf(u8, json, "\"ttl\":\"5s\"") !=
            null,
    );
}

test "StreamConfig allow_msg_ttl" {
    const alloc = std.testing.allocator;
    const json = try jsonStringify(alloc, StreamConfig{
        .name = "TTL_TEST",
        .allow_msg_ttl = true,
    });
    defer alloc.free(json);
    try std.testing.expect(
        std.mem.indexOf(u8, json, "allow_msg_ttl") !=
            null,
    );
}

test "CreateConsumerRequest action field" {
    const alloc = std.testing.allocator;
    // With action = "create"
    const json1 = try jsonStringify(
        alloc,
        CreateConsumerRequest{
            .stream_name = "S",
            .config = .{ .name = "C" },
            .action = "create",
        },
    );
    defer alloc.free(json1);
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            json1,
            "\"action\":\"create\"",
        ) != null,
    );

    // With action = null (omitted for createOrUpdate)
    const json2 = try jsonStringify(
        alloc,
        CreateConsumerRequest{
            .stream_name = "S",
            .config = .{ .name = "C" },
        },
    );
    defer alloc.free(json2);
    try std.testing.expect(
        std.mem.indexOf(u8, json2, "action") == null,
    );
}
