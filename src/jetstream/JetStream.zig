//! JetStream context providing stream/consumer CRUD, publish,
//! and pull subscription operations over core NATS request/reply.

const std = @import("std");
const Allocator = std.mem.Allocator;

const types = @import("types.zig");
const errors = @import("errors.zig");

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
pub const PubAck = types.PubAck;
pub const PublishOpts = types.PublishOpts;
pub const ApiError = errors.ApiError;
pub const ApiErrorJson = errors.ApiErrorJson;

const JetStream = @This();

client: *Client,
allocator: Allocator,
api_prefix_buf: [128]u8 = undefined,
api_prefix_len: u8 = 0,
timeout_ms: u32 = 5000,
last_api_err: ?ApiError = null,

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
    const prefix = if (opts.domain) |d| blk: {
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
        break :blk js.api_prefix_buf[0..p.len];
    } else blk: {
        const p = opts.api_prefix;
        std.debug.assert(p.len > 0);
        std.debug.assert(p.len <= js.api_prefix_buf.len);
        @memcpy(js.api_prefix_buf[0..p.len], p);
        js.api_prefix_len = @intCast(p.len);
        break :blk js.api_prefix_buf[0..p.len];
    };
    _ = prefix;
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

/// Gets stream info by name.
pub fn streamInfo(
    self: *JetStream,
    name: []const u8,
) !Response(StreamInfo) {
    std.debug.assert(name.len > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.INFO.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(StreamInfo, subj);
}

/// Purges a stream by name.
pub fn purgeStream(
    self: *JetStream,
    name: []const u8,
) !Response(PurgeResponse) {
    std.debug.assert(name.len > 0);
    var buf: [256]u8 = undefined;
    const subj = std.fmt.bufPrint(
        &buf,
        "STREAM.PURGE.{s}",
        .{name},
    ) catch return errors.Error.SubjectTooLong;
    return self.apiRequestNoPayload(
        PurgeResponse,
        subj,
    );
}

// -- Consumer CRUD --

/// Creates a consumer on the given stream.
pub fn createConsumer(
    self: *JetStream,
    stream: []const u8,
    config: ConsumerConfig,
) !Response(ConsumerInfo) {
    std.debug.assert(stream.len > 0);
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

// -- JetStream Publish --

/// Publishes a message to a JetStream stream subject
/// and waits for a PubAck.
pub fn publish(
    self: *JetStream,
    subject: []const u8,
    payload: []const u8,
) !Response(PubAck) {
    std.debug.assert(subject.len > 0);
    std.debug.assert(payload.len <= 1048576);
    const resp = self.client.request(
        subject,
        payload,
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();
    return self.parsePubAckResponse(&msg);
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

    var hdr_entries: [5]headers.Entry = undefined;
    var hdr_count: usize = 0;

    if (opts.msg_id) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.msg_id,
            .value = v,
        };
        hdr_count += 1;
    }
    if (opts.expected_stream) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_stream,
            .value = v,
        };
        hdr_count += 1;
    }
    if (opts.expected_last_msg_id) |v| {
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_msg_id,
            .value = v,
        };
        hdr_count += 1;
    }

    // Numeric headers need formatting
    var seq_buf: [20]u8 = undefined;
    if (opts.expected_last_seq) |v| {
        const s = std.fmt.bufPrint(
            &seq_buf,
            "{d}",
            .{v},
        ) catch unreachable;
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_seq,
            .value = s,
        };
        hdr_count += 1;
    }
    var subj_seq_buf: [20]u8 = undefined;
    if (opts.expected_last_subj_seq) |v| {
        const s = std.fmt.bufPrint(
            &subj_seq_buf,
            "{d}",
            .{v},
        ) catch unreachable;
        hdr_entries[hdr_count] = .{
            .key = headers.HeaderName.expected_last_subj_seq,
            .value = s,
        };
        hdr_count += 1;
    }

    const resp = self.client.requestWithHeaders(
        subject,
        hdr_entries[0..hdr_count],
        payload,
        self.timeout_ms,
    ) catch |err| return err;
    var msg = resp orelse
        return errors.Error.Timeout;
    defer msg.deinit();
    return self.parsePubAckResponse(&msg);
}

// -- Internal helpers --

/// Builds the full API subject and sends a request with
/// JSON payload, parsing the response.
fn apiRequest(
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
