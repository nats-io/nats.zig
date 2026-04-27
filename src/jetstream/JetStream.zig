//! JetStream context providing stream/consumer CRUD, publish,
//! and pull subscription operations over core NATS request/reply.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const errors = @import("errors.zig");
const publish_headers = @import("publish_headers.zig");

const nats = @import("../nats.zig");
const Client = nats.Client;
const headers = nats.protocol.headers;

pub const Response = types.Response;
pub const StreamConfig = types.StreamConfig;
pub const StreamInfo = types.StreamInfo;
pub const ConsumerConfig = types.ConsumerConfig;
pub const ConsumerInfo = types.ConsumerInfo;
pub const CreateConsumerRequest = types.CreateConsumerRequest;
pub const DeleteResponse = types.DeleteResponse;
pub const PurgeResponse = types.PurgeResponse;
pub const ConsumerPauseResponse = types.ConsumerPauseResponse;
pub const PubAck = types.PubAck;
pub const PublishOpts = types.PublishOpts;
pub const StreamNamesResponse = types.StreamNamesResponse;
pub const StreamListResponse = types.StreamListResponse;
pub const ConsumerNamesResponse = types.ConsumerNamesResponse;
pub const ConsumerListResponse = types.ConsumerListResponse;
pub const ListRequest = types.ListRequest;
pub const AccountInfo = types.AccountInfo;
const StorageType = types.StorageType;
const PushSubscription = @import(
    "push.zig",
).PushSubscription;
pub const ApiError = errors.ApiError;
pub const ApiErrorJson = errors.ApiErrorJson;

const JetStream = @This();

client: *Client,
allocator: Allocator,
api_prefix_buf: [128]u8 = undefined,
api_prefix_len: u8 = 0,
timeout_ms: u32 = 5000,
last_api_err: ?ApiError = null,

/// JetStream context options for API prefix, timeout, and
/// multi-tenant domain configuration.
pub const Options = struct {
    api_prefix: []const u8 = "$JS.API.",
    timeout_ms: u32 = 5000,
    domain: ?[]const u8 = null,
};

/// Initializes a JetStream context bound to the given client.
pub fn init(client: *Client, opts: Options) JetStream {
    std.debug.assert(client.isConnected());
    var js = JetStream{
        .client = client,
        .allocator = client.allocator,
        .timeout_ms = opts.timeout_ms,
    };
    if (opts.domain) |d| {
        std.debug.assert(d.len > 0);
        // "$JS." + domain + ".API." = 9 overhead
        std.debug.assert(d.len <= 119);
        var buf: [128]u8 = undefined;
        const p = std.fmt.bufPrint(
            &buf,
            "$JS.{s}.API.",
            .{d},
        ) catch unreachable;
        @memcpy(
            js.api_prefix_buf[0..p.len],
            p,
        );
        js.api_prefix_len = @intCast(p.len);
    } else {
        const p = opts.api_prefix;
        std.debug.assert(p.len > 0);
        std.debug.assert(p.len <= js.api_prefix_buf.len);
        @memcpy(js.api_prefix_buf[0..p.len], p);
        js.api_prefix_len = @intCast(p.len);
    }
    return js;
}

/// Returns the last API error from the server, if any.
pub fn lastApiError(self: *const JetStream) ?ApiError {
    return self.last_api_err;
}

// -- Stream CRUD --

/// Creates a stream with the given configuration.
pub fn createStream(
    self: *JetStream,
    config: StreamConfig,
) !Response(StreamInfo) {
    std.debug.assert(config.name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.CREATE.{s}",
        .{config.name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(StreamInfo, subj, config);
}

/// Updates a stream with the given configuration.
pub fn updateStream(
    self: *JetStream,
    config: StreamConfig,
) !Response(StreamInfo) {
    std.debug.assert(config.name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.UPDATE.{s}",
        .{config.name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(StreamInfo, subj, config);
}

/// Deletes a stream by name.
pub fn deleteStream(
    self: *JetStream,
    name: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.DELETE.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        DeleteResponse,
        subj,
    );
}

/// Creates a stream or updates it if it already exists.
/// Tries update first; falls back to create on
/// stream_not_found (matches Go client behavior).
pub fn createOrUpdateStream(
    self: *JetStream,
    config: StreamConfig,
) !Response(StreamInfo) {
    std.debug.assert(config.name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    return self.updateStream(config) catch |err| {
        if (err == error.ApiError) {
            if (self.lastApiError()) |ae| {
                if (ae.err_code ==
                    errors.ErrCode.stream_not_found)
                    return self.createStream(config);
            }
        }
        return err;
    };
}

/// Gets stream info by name.
pub fn streamInfo(
    self: *JetStream,
    name: []const u8,
) !Response(StreamInfo) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.INFO.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(StreamInfo, subj);
}

/// Purges a stream by name. Optionally filter by
/// subject to only purge matching messages.
pub fn purgeStream(
    self: *JetStream,
    name: []const u8,
) !Response(PurgeResponse) {
    return self.purgeStreamFiltered(name, null);
}

/// Purges messages matching a specific subject.
pub fn purgeStreamSubject(
    self: *JetStream,
    name: []const u8,
    subject: []const u8,
) !Response(PurgeResponse) {
    std.debug.assert(subject.len > 0);
    return self.purgeStreamFiltered(name, subject);
}

fn purgeStreamFiltered(
    self: *JetStream,
    name: []const u8,
    subject: ?[]const u8,
) !Response(PurgeResponse) {
    std.debug.assert(name.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.PURGE.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    if (subject) |s| {
        return self.apiRequest(
            PurgeResponse,
            subj,
            types.PurgeRequest{ .filter = s },
        );
    }
    return self.apiRequestNoPayload(
        PurgeResponse,
        subj,
    );
}

// -- Stream message operations --

/// Gets a raw message from a stream by sequence number.
pub fn getMsg(
    self: *JetStream,
    stream: []const u8,
    seq: u64,
) !Response(types.MsgGetResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(seq > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.MSG.GET.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        types.MsgGetResponse,
        subj,
        types.MsgGetRequest{ .seq = seq },
    );
}

/// Gets the last message on a specific subject.
pub fn getLastMsgForSubject(
    self: *JetStream,
    stream: []const u8,
    subject: []const u8,
) !Response(types.MsgGetResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(subject.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.MSG.GET.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        types.MsgGetResponse,
        subj,
        types.MsgGetRequest{ .last_by_subj = subject },
    );
}

/// Deletes a message from a stream by sequence.
/// The message is marked as erased but not overwritten.
pub fn deleteMsg(
    self: *JetStream,
    stream: []const u8,
    seq: u64,
) !Response(DeleteResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(seq > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.MSG.DELETE.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        DeleteResponse,
        subj,
        types.MsgDeleteRequest{
            .seq = seq,
            .no_erase = true,
        },
    );
}

/// Securely deletes a message by overwriting it with
/// random data. Slower than deleteMsg.
pub fn secureDeleteMsg(
    self: *JetStream,
    stream: []const u8,
    seq: u64,
) !Response(DeleteResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(seq > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.MSG.DELETE.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        DeleteResponse,
        subj,
        types.MsgDeleteRequest{ .seq = seq },
    );
}

// -- Consumer CRUD --

/// Creates a consumer on the given stream. Returns
/// error if consumer already exists with different
/// config. The filter_subject (if any) is in the body.
pub fn createConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const name = config.name orelse
        config.durable_name orelse "";
    std.debug.assert(name.len > 0);
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.CREATE.{s}.{s}",
        .{ stream, name },
    ) catch return errors.Error.SubjectTooLong;
    const req = CreateConsumerRequest{
        .stream_name = stream,
        .config = config,
        .action = "create",
    };
    return self.apiRequest(ConsumerInfo, subj, req);
}

/// Creates or updates a consumer on the given stream.
/// If the consumer exists, it will be updated if the
/// config change is compatible.
pub fn createOrUpdateConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const name = config.name orelse
        config.durable_name orelse "";
    std.debug.assert(name.len > 0);
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.CREATE.{s}.{s}",
        .{ stream, name },
    ) catch return errors.Error.SubjectTooLong;
    const req = CreateConsumerRequest{
        .stream_name = stream,
        .config = config,
    };
    return self.apiRequest(ConsumerInfo, subj, req);
}

/// Updates a consumer on the given stream.
pub fn updateConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const name = config.name orelse
        config.durable_name orelse "";
    std.debug.assert(name.len > 0);
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.CREATE.{s}.{s}",
        .{ stream, name },
    ) catch return errors.Error.SubjectTooLong;
    const req = CreateConsumerRequest{
        .stream_name = stream,
        .config = config,
        .action = "update",
    };
    return self.apiRequest(ConsumerInfo, subj, req);
}

/// Deletes a consumer from a stream.
pub fn deleteConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.DELETE.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        DeleteResponse,
        subj,
    );
}

/// Gets consumer info.
pub fn consumerInfo(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.INFO.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        ConsumerInfo,
        subj,
    );
}

/// Creates a push consumer on the given stream.
/// The config must have deliver_subject set.
pub fn createPushConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(config.deliver_subject != null);
    std.debug.assert(self.timeout_ms > 0);
    return self.createConsumer(stream, config);
}

/// Creates or updates a push consumer.
/// The config must have deliver_subject set.
pub fn createOrUpdatePushConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(config.deliver_subject != null);
    std.debug.assert(self.timeout_ms > 0);
    return self.createOrUpdateConsumer(stream, config);
}

/// Pauses a consumer until the given time (RFC 3339).
/// The consumer will not deliver messages until resumed
/// or the pause_until time is reached.
pub fn pauseConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
    pause_until: []const u8,
) !Response(ConsumerPauseResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    std.debug.assert(pause_until.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.PAUSE.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerPauseResponse,
        subj,
        types.ConsumerPauseRequest{
            .pause_until = pause_until,
        },
    );
}

/// Resumes a paused consumer immediately.
pub fn resumeConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
) !Response(ConsumerPauseResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.PAUSE.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerPauseResponse,
        subj,
        types.ConsumerPauseRequest{},
    );
}

/// Updates an existing push consumer.
/// Config must have deliver_subject set.
pub fn updatePushConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(config.deliver_subject != null);
    std.debug.assert(self.timeout_ms > 0);
    return self.updateConsumer(stream, config);
}

/// Binds to an existing push consumer by name.
/// Returns a PushSubscription with deliver_subject
/// populated from the server-side config.
pub fn pushConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer_name: []const u8,
) !PushSubscription {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer_name.len > 0);
    var resp = try self.consumerInfo(
        stream,
        consumer_name,
    );
    defer resp.deinit();

    const cfg = resp.value.config orelse
        return errors.Error.ApiError;
    const ds = cfg.deliver_subject orelse
        return errors.Error.ApiError;

    var ps = PushSubscription{
        .js = self,
        .stream = stream,
    };
    ps.setConsumer(consumer_name);
    ps.setDeliverSubject(ds);
    if (cfg.deliver_group) |dg| {
        ps.setDeliverGroup(dg);
    }
    return ps;
}

/// Unpins the currently pinned client for a consumer
/// in the given delivery group.
pub fn unpinConsumer(
    self: *JetStream,
    stream: []const u8,
    consumer: []const u8,
    group: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(consumer.len > 0);
    std.debug.assert(group.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [512]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.UNPIN.{s}.{s}",
        .{ stream, consumer },
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        DeleteResponse,
        subj,
        types.ConsumerUnpinRequest{
            .group = group,
        },
    );
}

/// Returns the underlying client connection.
pub fn conn(self: *const JetStream) *Client {
    return self.client;
}

/// Returns the JetStream context options.
pub fn options(self: *const JetStream) Options {
    return .{
        .api_prefix = self.apiPrefix(),
        .timeout_ms = self.timeout_ms,
    };
}

// -- Listing & Account Info --

/// Returns stream names. Pass offset=0 for first
/// page. Check response total/offset/limit for
/// pagination. Returns one page per call.
pub fn streamNames(
    self: *JetStream,
) !Response(StreamNamesResponse) {
    return self.streamNamesOffset(0);
}

/// Returns stream names starting at offset.
pub fn streamNamesOffset(
    self: *JetStream,
    offset: u64,
) !Response(StreamNamesResponse) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamNamesResponse,
        "STREAM.NAMES",
        ListRequest{ .offset = offset },
    );
}

/// Returns all stream names across all pages.
/// Caller owns the returned slice; free each
/// string and the slice with allocator.
pub fn allStreamNames(
    self: *JetStream,
    allocator: Allocator,
) ![][]const u8 {
    std.debug.assert(self.timeout_ms > 0);
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |n|
            allocator.free(n);
        result.deinit(allocator);
    }
    var offset: u64 = 0;
    while (true) {
        var resp = try self.streamNamesOffset(
            offset,
        );
        defer resp.deinit();
        const names = resp.value.streams orelse
            break;
        for (names) |n| {
            const owned = try allocator.dupe(u8, n);
            result.append(allocator, owned) catch |e| {
                allocator.free(owned);
                return e;
            };
        }
        offset += names.len;
        if (offset >= resp.value.total) break;
    }
    return result.toOwnedSlice(allocator);
}

/// Returns stream info list (one page).
pub fn streams(
    self: *JetStream,
) !Response(StreamListResponse) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamListResponse,
        "STREAM.LIST",
        ListRequest{},
    );
}

/// Finds the stream name that captures a subject.
pub fn streamNameBySubject(
    self: *JetStream,
    subject: []const u8,
) !Response(StreamNamesResponse) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequest(
        StreamNamesResponse,
        "STREAM.NAMES",
        ListRequest{ .subject = subject },
    );
}

/// Returns consumer names (one page).
pub fn consumerNames(
    self: *JetStream,
    stream: []const u8,
) !Response(ConsumerNamesResponse) {
    return self.consumerNamesOffset(stream, 0);
}

/// Returns consumer names at offset.
pub fn consumerNamesOffset(
    self: *JetStream,
    stream: []const u8,
    offset: u64,
) !Response(ConsumerNamesResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.NAMES.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerNamesResponse,
        subj,
        ListRequest{ .offset = offset },
    );
}

/// Returns consumer info list (one page).
pub fn consumers(
    self: *JetStream,
    stream: []const u8,
) !Response(ConsumerListResponse) {
    std.debug.assert(stream.len > 0);
    std.debug.assert(self.timeout_ms > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "CONSUMER.LIST.{s}",
        .{stream},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequest(
        ConsumerListResponse,
        subj,
        ListRequest{},
    );
}

/// Returns JetStream account information including
/// usage stats and limits.
pub fn accountInfo(
    self: *JetStream,
) !Response(AccountInfo) {
    std.debug.assert(self.timeout_ms > 0);
    return self.apiRequestNoPayload(
        AccountInfo,
        "INFO",
    );
}

// -- Key-Value Store --

const kv_mod = @import("kv.zig");
pub const KeyValue = kv_mod.KeyValue;
pub const KvWatcher = kv_mod.KvWatcher;
const KeyValueConfig = types.KeyValueConfig;

/// Creates a new key-value bucket backed by a
/// JetStream stream. Returns a KeyValue handle.
pub fn createKeyValue(
    self: *JetStream,
    cfg: KeyValueConfig,
) !KeyValue {
    std.debug.assert(cfg.bucket.len > 0);
    std.debug.assert(cfg.bucket.len <= 64);
    std.debug.assert(self.timeout_ms > 0);
    const sc = self.kvStreamConfig(cfg);
    var resp = try self.createStream(sc.config(
        sc.stream_name(),
        &sc.subjects,
    ));
    resp.deinit();
    return self.initKeyValue(cfg.bucket);
}

/// Updates an existing key-value bucket config.
/// Returns error if the bucket doesn't exist.
pub fn updateKeyValue(
    self: *JetStream,
    cfg: KeyValueConfig,
) !KeyValue {
    std.debug.assert(cfg.bucket.len > 0);
    std.debug.assert(cfg.bucket.len <= 64);
    std.debug.assert(self.timeout_ms > 0);
    const sc = self.kvStreamConfig(cfg);
    var resp = try self.updateStream(sc.config(
        sc.stream_name(),
        &sc.subjects,
    ));
    resp.deinit();
    return self.initKeyValue(cfg.bucket);
}

/// Creates or updates a key-value bucket.
pub fn createOrUpdateKeyValue(
    self: *JetStream,
    cfg: KeyValueConfig,
) !KeyValue {
    std.debug.assert(cfg.bucket.len > 0);
    std.debug.assert(cfg.bucket.len <= 64);
    std.debug.assert(self.timeout_ms > 0);
    const sc = self.kvStreamConfig(cfg);
    var resp = try self.createOrUpdateStream(
        sc.config(sc.stream_name(), &sc.subjects),
    );
    resp.deinit();
    return self.initKeyValue(cfg.bucket);
}

/// Binds to an existing key-value bucket.
/// Returns error if the stream doesn't exist.
pub fn keyValue(
    self: *JetStream,
    bucket_name: []const u8,
) !KeyValue {
    std.debug.assert(bucket_name.len > 0);
    std.debug.assert(bucket_name.len <= 64);

    var stream_buf: [68]u8 = undefined;
    const stream_name = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch return errors.Error.SubjectTooLong;

    // Verify stream exists
    var resp = try self.streamInfo(stream_name);
    resp.deinit();

    return self.initKeyValue(bucket_name);
}

/// Deletes a key-value bucket and its backing stream.
pub fn deleteKeyValue(
    self: *JetStream,
    bucket_name: []const u8,
) !Response(DeleteResponse) {
    std.debug.assert(bucket_name.len > 0);
    var stream_buf: [68]u8 = undefined;
    const stream_name = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch return errors.Error.SubjectTooLong;
    return self.deleteStream(stream_name);
}

fn initKeyValue(
    self: *JetStream,
    bucket_name: []const u8,
) KeyValue {
    std.debug.assert(bucket_name.len > 0);
    var kv = KeyValue{ .js = self };

    @memcpy(
        kv.bucket_buf[0..bucket_name.len],
        bucket_name,
    );
    kv.bucket_len = @intCast(bucket_name.len);

    var stream_buf: [68]u8 = undefined;
    const sn = std.fmt.bufPrint(
        &stream_buf,
        "KV_{s}",
        .{bucket_name},
    ) catch unreachable;
    @memcpy(kv.stream_buf[0..sn.len], sn);
    kv.stream_len = @intCast(sn.len);

    return kv;
}

/// Builds the stream config for a KV bucket without
/// executing the API call. Shared by create/update/
/// createOrUpdate.
const KvStreamCfg = struct {
    stream_name_buf: [68]u8 = undefined,
    stream_name_len: u8 = 0,
    subj_buf: [128]u8 = undefined,
    subj_len: u8 = 0,
    subjects: [1][]const u8 = undefined,
    hist: i64 = 1,
    dup_window: ?i64 = null,
    max_bytes: ?i64 = null,
    max_age: ?i64 = null,
    max_msg_size: ?i32 = null,
    storage: StorageType = .file,
    replicas: ?i32 = null,
    desc: ?[]const u8 = null,

    fn stream_name(
        self: *const KvStreamCfg,
    ) []const u8 {
        std.debug.assert(self.stream_name_len > 0);
        return self.stream_name_buf[0..self.stream_name_len];
    }

    fn config(
        self: *const KvStreamCfg,
        name: []const u8,
        subjects: *const [1][]const u8,
    ) StreamConfig {
        return .{
            .name = name,
            .subjects = subjects,
            .max_msgs_per_subject = self.hist,
            .max_bytes = self.max_bytes,
            .max_age = self.max_age,
            .max_msg_size = self.max_msg_size,
            .storage = self.storage,
            .num_replicas = self.replicas,
            .discard = .new,
            .duplicate_window = self.dup_window,
            .max_msgs = -1,
            .max_consumers = -1,
            .allow_rollup_hdrs = true,
            .deny_delete = true,
            .deny_purge = false,
            .allow_direct = true,
            .mirror_direct = false,
            .description = self.desc,
        };
    }
};

fn kvStreamConfig(
    _: *const JetStream,
    cfg: KeyValueConfig,
) KvStreamCfg {
    std.debug.assert(cfg.bucket.len > 0);
    var sc = KvStreamCfg{};

    const sn = std.fmt.bufPrint(
        &sc.stream_name_buf,
        "KV_{s}",
        .{cfg.bucket},
    ) catch unreachable;
    sc.stream_name_len = @intCast(sn.len);

    const sp = std.fmt.bufPrint(
        &sc.subj_buf,
        "$KV.{s}.>",
        .{cfg.bucket},
    ) catch unreachable;
    sc.subj_len = @intCast(sp.len);
    sc.subjects = .{
        sc.subj_buf[0..sc.subj_len],
    };

    sc.hist = if (cfg.history) |h|
        @intCast(h)
    else
        1;

    const two_min: i64 = 120_000_000_000;
    sc.dup_window = if (cfg.ttl) |ttl|
        @min(ttl, two_min)
    else
        two_min;

    sc.max_bytes = cfg.max_bytes;
    sc.max_age = cfg.ttl;
    sc.max_msg_size = cfg.max_value_size;
    sc.storage = cfg.storage orelse .file;
    sc.replicas = cfg.replicas;
    sc.desc = cfg.description;

    return sc;
}

/// Returns names of all key-value buckets. Queries
/// stream names with $KV subject filter, strips the
/// KV_ prefix. Caller owns the returned slice.
pub fn keyValueStoreNames(
    self: *JetStream,
    alloc: std.mem.Allocator,
) ![][]const u8 {
    std.debug.assert(self.timeout_ms > 0);
    var result: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (result.items) |n| alloc.free(n);
        result.deinit(alloc);
    }
    var offset: u64 = 0;
    while (true) {
        var resp = try self.apiRequest(
            StreamNamesResponse,
            "STREAM.NAMES",
            ListRequest{
                .offset = offset,
                .subject = "$KV.*.>",
            },
        );
        defer resp.deinit();
        const names = resp.value.streams orelse break;
        for (names) |n| {
            // Strip "KV_" prefix
            const bucket = if (n.len > 3 and
                std.mem.startsWith(u8, n, "KV_"))
                n[3..]
            else
                n;
            const owned = try alloc.dupe(u8, bucket);
            result.append(alloc, owned) catch |e| {
                alloc.free(owned);
                return e;
            };
        }
        offset += names.len;
        if (offset >= resp.value.total) break;
    }
    return result.toOwnedSlice(alloc);
}

/// Returns all KV buckets with status info. Caller
/// owns the returned slice.
pub fn keyValueStores(
    self: *JetStream,
    alloc: std.mem.Allocator,
) ![]types.KeyValueStatus {
    std.debug.assert(self.timeout_ms > 0);
    const names = try self.keyValueStoreNames(alloc);
    defer {
        for (names) |n| alloc.free(n);
        alloc.free(names);
    }

    var result: std.ArrayList(
        types.KeyValueStatus,
    ) = .empty;
    errdefer result.deinit(alloc);

    for (names) |bucket| {
        var stream_buf: [68]u8 = undefined;
        const sn = std.fmt.bufPrint(
            &stream_buf,
            "KV_{s}",
            .{bucket},
        ) catch continue;
        var resp = self.streamInfo(sn) catch continue;
        defer resp.deinit();

        const cfg = resp.value.config orelse continue;
        const state = resp.value.state orelse
            types.StreamState{};

        try result.append(alloc, .{
            .bucket = bucket,
            .values = state.messages,
            .history = cfg.max_msgs_per_subject orelse
                1,
            .ttl = cfg.max_age orelse 0,
            .bytes = state.bytes,
            .backing_store = cfg.storage orelse .file,
            .is_compressed = if (cfg.compression) |c|
                c == .s2
            else
                false,
        });
    }
    return result.toOwnedSlice(alloc);
}

// -- JetStream Publish --

/// Publishes a message to a JetStream stream subject
/// and waits for a PubAck. Retries up to 2 times on
/// NoResponders (matching Go client behavior for
/// transient leadership changes).
pub fn publish(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
) !Response(PubAck) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(payload.len <= 1048576);
    return self.publishRetry(subject, payload, null);
}

pub fn publishRetry(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
    hdrs: ?[]const headers.Entry,
) !Response(PubAck) {
    const max_retries: u32 = 2;
    const retry_wait_ns: u64 = 250_000_000;
    var attempt: u32 = 0;

    while (true) {
        const resp = if (hdrs) |h|
            self.client.requestWithHeaders(
                subject,
                h,
                payload,
                self.timeout_ms,
            ) catch |err| return err
        else
            self.client.request(
                subject,
                payload,
                self.timeout_ms,
            ) catch |err| return err;

        var msg = resp orelse
            return errors.Error.Timeout;

        if (msg.isNoResponders()) {
            msg.deinit();
            attempt += 1;
            if (attempt > max_retries)
                return errors.Error.NoResponders;
            sleepNs(retry_wait_ns);
            continue;
        }

        defer msg.deinit();
        return self.parsePubAckResponse(&msg);
    }
}

fn sleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / 1_000_000_000),
        .nsec = @intCast(ns % 1_000_000_000),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

/// Publishes with header-based options (msg-id, expected
/// stream/seq) and waits for a PubAck.
pub fn publishWithOpts(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
    opts: PublishOpts,
) !Response(PubAck) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(payload.len <= 1048576);

    var hdrs: publish_headers.PublishHeaderSet = undefined;
    hdrs.populate(opts);

    // Pass null when opts produced no headers -- the protocol
    // layer asserts entries.len > 0, and publishRetry uses the
    // null/non-null distinction to select the correct publish
    // path.
    const slice = hdrs.slice();
    const hdr_slice: ?[]const headers.Entry =
        if (slice.len == 0) null else slice;
    return self.publishRetry(
        subject,
        payload,
        hdr_slice,
    );
}

/// Publishes a pre-built JetStream message with user-supplied
/// headers and optional PublishOpts. User headers are merged
/// with JetStream-generated headers (msg_id, expected_stream,
/// etc.); on key collision, JetStream headers from `msg.opts`
/// override the user-supplied value (matches Go client
/// PublishMsg semantics).
///
/// Header key comparison is case-insensitive per NATS header
/// conventions.
///
/// The allocator is used only for a temporary merge buffer
/// that is freed before return. No allocations escape.
pub fn publishMsg(
    self: *JetStream,
    allocator: Allocator,
    msg: types.JsPublishMsg,
) !Response(PubAck) {
    std.debug.assert(msg.subject.len > 0);
    std.debug.assert(msg.payload.len <= 1048576);

    var js_hdrs: publish_headers.PublishHeaderSet = undefined;
    js_hdrs.populate(msg.opts);

    // Fast path: no user headers. Pass null to publishRetry
    // when opts also produced no JS headers -- the protocol
    // layer asserts entries.len > 0 in encodedSize() and the
    // null/non-null distinction is how publishRetry chooses
    // between the header and no-header publish paths.
    if (msg.headers == null or msg.headers.?.len == 0) {
        const js_slice = js_hdrs.slice();
        const hdr_slice: ?[]const headers.Entry =
            if (js_slice.len == 0) null else js_slice;
        return self.publishRetry(
            msg.subject,
            msg.payload,
            hdr_slice,
        );
    }

    // Merge: user headers first, JS headers override on
    // case-insensitive key collision.
    var merged: std.ArrayList(headers.Entry) = .empty;
    defer merged.deinit(allocator);

    try merged.appendSlice(allocator, msg.headers.?);

    for (js_hdrs.slice()) |js_entry| {
        var replaced = false;
        for (merged.items) |*existing| {
            if (std.ascii.eqlIgnoreCase(
                existing.key,
                js_entry.key,
            )) {
                existing.value = js_entry.value;
                replaced = true;
                break;
            }
        }
        if (!replaced) try merged.append(
            allocator,
            js_entry,
        );
    }

    const hdr_slice: ?[]const headers.Entry =
        if (merged.items.len == 0) null else merged.items;
    return self.publishRetry(
        msg.subject,
        msg.payload,
        hdr_slice,
    );
}

// -- Internal helpers --

/// Builds the full API subject and sends a request with
/// JSON payload, parsing the response.
pub fn apiRequest(
    self: *JetStream,
    comptime T: type,
    api_subject: []const u8,
    payload: anytype,
) !Response(T) {
    std.debug.assert(api_subject.len > 0);
    const prefix = self.apiPrefix();
    std.debug.assert(prefix.len > 0);

    var full_buf: [512]u8 = undefined;
    const full_subj = std.fmt.bufPrint(
        &full_buf,
        "{s}{s}",
        .{ prefix, api_subject },
    ) catch return errors.Error.SubjectTooLong;

    const json_payload = try types.jsonStringify(
        self.allocator,
        payload,
    );
    defer self.allocator.free(json_payload);

    const resp = self.client.request(
        full_subj,
        json_payload,
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();

    if (msg.isNoResponders())
        return errors.Error.NoResponders;

    return self.parseResponse(T, msg.data);
}

/// Sends a request with no payload body.
fn apiRequestNoPayload(
    self: *JetStream,
    comptime T: type,
    api_subject: []const u8,
) !Response(T) {
    std.debug.assert(api_subject.len > 0);
    const prefix = self.apiPrefix();
    std.debug.assert(prefix.len > 0);

    var full_buf: [512]u8 = undefined;
    const full_subj = std.fmt.bufPrint(
        &full_buf,
        "{s}{s}",
        .{ prefix, api_subject },
    ) catch return errors.Error.SubjectTooLong;

    const resp = self.client.request(
        full_subj,
        "",
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();

    if (msg.isNoResponders())
        return errors.Error.NoResponders;

    return self.parseResponse(T, msg.data);
}

/// Parses JSON response, checks for API error envelope.
fn parseResponse(
    self: *JetStream,
    comptime T: type,
    data: []const u8,
) !Response(T) {
    std.debug.assert(data.len > 0);
    var parsed = types.jsonParse(
        T,
        self.allocator,
        data,
    ) catch return errors.Error.JsonParseError;

    if (checkApiError(T, &parsed.value)) |api_err| {
        self.last_api_err = ApiError.fromJson(api_err);
        parsed.deinit();
        return errors.Error.ApiError;
    }

    return Response(T){
        .value = parsed.value,
        ._parsed = parsed,
    };
}

/// Parses PubAck from a message response (publish goes
/// directly to stream subject, not through $JS.API).
fn parsePubAckResponse(
    self: *JetStream,
    msg: *Client.Message,
) !Response(PubAck) {
    if (msg.isNoResponders())
        return errors.Error.NoResponders;
    std.debug.assert(msg.data.len > 0);
    return self.parseResponse(PubAck, msg.data);
}

/// Checks if a parsed response contains an API error.
fn checkApiError(
    comptime T: type,
    value: *const T,
) ?ApiErrorJson {
    if (@hasField(T, "error")) {
        if (value.@"error") |err| {
            if (err.code > 0) return err;
        }
    }
    return null;
}

/// Returns the API prefix slice.
pub fn apiPrefix(self: *const JetStream) []const u8 {
    std.debug.assert(self.api_prefix_len > 0);
    return self.api_prefix_buf[0..self.api_prefix_len];
}

// -- Tests --

test "subject building" {
    // Test apiPrefix format for default
    var js = JetStream{
        .client = undefined,
        .allocator = std.testing.allocator,
    };
    const default_prefix = "$JS.API.";
    @memcpy(
        js.api_prefix_buf[0..default_prefix.len],
        default_prefix,
    );
    js.api_prefix_len = @intCast(default_prefix.len);
    try std.testing.expectEqualStrings(
        "$JS.API.",
        js.apiPrefix(),
    );
}
