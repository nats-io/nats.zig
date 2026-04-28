//! JetStream Key-Value Store.
//!
//! A key-value store backed by a JetStream stream. Keys are
//! NATS subjects under `$KV.{bucket}.{key}`, values are
//! message payloads. Supports history, delete markers, watch,
//! and optimistic concurrency via revision numbers.

const std = @import("std");
const Allocator = std.mem.Allocator;

const nats = @import("../nats.zig");
const Client = nats.Client;
const headers_mod = nats.protocol.headers;

const types = @import("types.zig");
const errors = @import("errors.zig");
const JetStream = @import("JetStream.zig");
const PullSubscription = @import(
    "pull.zig",
).PullSubscription;
const JsMsg = @import("message.zig").JsMsg;

var ephemeral_counter: std.atomic.Value(u32) =
    std.atomic.Value(u32).init(0);

fn validateKeyToken(token: []const u8, allow_wildcards: bool, last: bool) !void {
    if (token.len == 0) return errors.Error.InvalidKey;
    if (std.mem.eql(u8, token, "*")) {
        if (!allow_wildcards) return errors.Error.InvalidKey;
        return;
    }
    if (std.mem.eql(u8, token, ">")) {
        if (!allow_wildcards or !last) return errors.Error.InvalidKey;
        return;
    }
    for (token) |c| {
        if (c <= 0x20 or c == 0x7f or c == '*' or c == '>') {
            return errors.Error.InvalidKey;
        }
    }
}

fn validateKeyLike(value: []const u8, allow_wildcards: bool) !void {
    if (value.len == 0) return errors.Error.InvalidKey;
    var start: usize = 0;
    var i: usize = 0;
    while (i <= value.len) : (i += 1) {
        if (i == value.len or value[i] == '.') {
            try validateKeyToken(
                value[start..i],
                allow_wildcards,
                i == value.len,
            );
            start = i + 1;
        }
    }
}

fn validateKey(key: []const u8) !void {
    try validateKeyLike(key, false);
}

fn validateKeyPattern(pattern: []const u8) !void {
    try validateKeyLike(pattern, true);
}

/// Key-value store bound to a specific bucket.
/// Created via `JetStream.createKeyValue()` or
/// `JetStream.keyValue()`.
pub const KeyValue = struct {
    js: *JetStream,
    bucket_buf: [64]u8 = undefined,
    bucket_len: u8 = 0,
    stream_buf: [68]u8 = undefined,
    stream_len: u8 = 0,
    // Stable storage for ephemeral consumer names
    _eph_name_buf: [48]u8 = undefined,
    _eph_name_len: u8 = 0,

    /// Returns the bucket name.
    pub fn bucket(self: *const KeyValue) []const u8 {
        std.debug.assert(self.bucket_len > 0);
        return self.bucket_buf[0..self.bucket_len];
    }

    /// Returns the underlying stream name.
    fn streamName(
        self: *const KeyValue,
    ) []const u8 {
        std.debug.assert(self.stream_len > 0);
        return self.stream_buf[0..self.stream_len];
    }

    /// Builds the KV subject for a key. Validates key
    /// contains no wildcards or control characters.
    fn kvSubject(
        self: *const KeyValue,
        key: []const u8,
        buf: []u8,
    ) ![]const u8 {
        try validateKey(key);
        return std.fmt.bufPrint(
            buf,
            "$KV.{s}.{s}",
            .{ self.bucket(), key },
        ) catch return errors.Error.SubjectTooLong;
    }

    // -- Get --

    /// Gets the latest value for a key. Returns null
    /// if the key does not exist. Returns the entry
    /// even if it's a delete/purge marker (check
    /// entry.operation).
    pub fn get(
        self: *KeyValue,
        key: []const u8,
    ) !?types.KeyValueEntry {
        std.debug.assert(key.len > 0);
        std.debug.assert(self.bucket_len > 0);
        return self.getBySubject(key);
    }

    /// Gets a specific revision of a key.
    pub fn getRevision(
        self: *KeyValue,
        key: []const u8,
        revision: u64,
    ) !?types.KeyValueEntry {
        std.debug.assert(key.len > 0);
        std.debug.assert(revision > 0);
        return self.getBySeq(key, revision);
    }

    fn getBySubject(
        self: *KeyValue,
        key: []const u8,
    ) !?types.KeyValueEntry {
        var subj_buf: [256]u8 = undefined;
        const kv_subj = try self.kvSubject(
            key,
            &subj_buf,
        );

        var api_buf: [512]u8 = undefined;
        const api_subj = std.fmt.bufPrint(
            &api_buf,
            "STREAM.MSG.GET.{s}",
            .{self.streamName()},
        ) catch return errors.Error.SubjectTooLong;

        const req = types.MsgGetRequest{
            .last_by_subj = kv_subj,
        };
        return self.fetchAndParse(api_subj, req, key);
    }

    fn getBySeq(
        self: *KeyValue,
        key: []const u8,
        seq: u64,
    ) !?types.KeyValueEntry {
        var api_buf: [512]u8 = undefined;
        const api_subj = std.fmt.bufPrint(
            &api_buf,
            "STREAM.MSG.GET.{s}",
            .{self.streamName()},
        ) catch return errors.Error.SubjectTooLong;

        const req = types.MsgGetRequest{ .seq = seq };
        return self.fetchAndParse(api_subj, req, key);
    }

    fn fetchAndParse(
        self: *KeyValue,
        api_subj: []const u8,
        req: types.MsgGetRequest,
        key: []const u8,
    ) !?types.KeyValueEntry {
        var resp = self.js.apiRequest(
            types.MsgGetResponse,
            api_subj,
            req,
        ) catch |err| {
            if (err == error.ApiError) {
                if (self.js.lastApiError()) |ae| {
                    if (ae.err_code == 10037)
                        return null;
                }
            }
            return err;
        };
        defer resp.deinit();

        const msg = resp.value.message orelse
            return null;
        const seq = msg.seq;

        // Validate subject matches expected key
        if (msg.subject) |subj| {
            var exp_buf: [256]u8 = undefined;
            const expected = std.fmt.bufPrint(
                &exp_buf,
                "$KV.{s}.{s}",
                .{ self.bucket(), key },
            ) catch return errors.Error.SubjectTooLong;
            if (!std.mem.eql(u8, subj, expected))
                return null;
        }

        // Determine operation from stored headers
        var op: types.KeyValueOp = .put;
        if (msg.hdrs) |hdr_b64| {
            // hdrs is base64-encoded
            var decode_buf: [1024]u8 = undefined;
            const decoded = decodeBase64(
                hdr_b64,
                &decode_buf,
            ) orelse return types.KeyValueEntry{
                .bucket = self.bucket(),
                .key = key,
                .value = "",
                .revision = seq,
                .operation = .put,
            };
            op = parseKvOp(decoded);
        }

        // Data is base64-encoded in JSON response —
        // decode before returning to caller
        const allocator = self.js.allocator;
        var val: []const u8 = "";
        var val_alloc: ?Allocator = null;
        if (msg.data) |data_b64| {
            if (data_b64.len > 0 and op == .put) {
                // Decode base64 into allocated buffer
                const decoder = std.base64.standard
                    .Decoder;
                const dec_len = decoder
                    .calcSizeForSlice(data_b64) catch {
                    return error.InvalidData;
                };
                const decoded_val = try allocator.alloc(
                    u8,
                    dec_len,
                );
                decoder.decode(
                    decoded_val[0..dec_len],
                    data_b64,
                ) catch {
                    allocator.free(decoded_val);
                    return error.InvalidData;
                };
                val = decoded_val[0..dec_len];
                val_alloc = allocator;
            }
        }

        return types.KeyValueEntry{
            .bucket = self.bucket(),
            .key = key,
            .value = val,
            .revision = seq,
            .operation = op,
            .value_allocator = val_alloc,
        };
    }

    // -- Put --

    /// Puts a value for a key. Returns the revision
    /// (sequence number).
    pub fn put(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        std.debug.assert(self.bucket_len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publish(subj, value);
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Puts a string value for a key. Convenience
    /// wrapper around put() -- in Zig, strings are
    /// already []const u8.
    pub fn putString(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        return self.put(key, value);
    }

    /// Creates a key only if it does not already exist.
    /// Returns the revision, or error.ApiError if the
    /// key already exists (check lastApiError for
    /// stream_wrong_last_seq).
    pub fn create(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publishWithOpts(
            subj,
            value,
            .{ .expected_last_subj_seq = 0 },
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Updates a key only if the current revision matches.
    /// Returns the new revision, or error.ApiError on
    /// revision mismatch.
    pub fn update(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
        revision: u64,
    ) !u64 {
        std.debug.assert(key.len > 0);
        std.debug.assert(revision > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publishWithOpts(
            subj,
            value,
            .{ .expected_last_subj_seq = revision },
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Options for KV create operations.
    pub const CreateOpts = struct {
        /// Per-key TTL (e.g., "5s", "1m"). Requires
        /// the bucket to have allow_msg_ttl enabled.
        ttl: ?[]const u8 = null,
    };

    /// Creates a key with options (e.g., per-key TTL).
    pub fn createWithOpts(
        self: *KeyValue,
        key: []const u8,
        value: []const u8,
        opts: CreateOpts,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var resp = try self.js.publishWithOpts(
            subj,
            value,
            .{
                .expected_last_subj_seq = 0,
                .ttl = opts.ttl,
            },
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Options for conditional delete/purge.
    pub const KvDeleteOpts = struct {
        /// Only delete if latest revision matches.
        last_revision: ?u64 = null,
    };

    // -- Delete / Purge --

    /// Soft-deletes a key by publishing a delete marker.
    /// Returns the revision number. The key can still
    /// appear in history.
    pub fn delete(
        self: *KeyValue,
        key: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        const hdrs = [_]nats.protocol.headers.Entry{
            .{
                .key = "KV-Operation",
                .value = "DEL",
            },
        };
        var resp = try self.js.publishRetry(
            subj,
            "",
            &hdrs,
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Purges a key and all its history.
    /// Returns the revision number.
    pub fn purge(
        self: *KeyValue,
        key: []const u8,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        const hdrs = [_]nats.protocol.headers.Entry{
            .{
                .key = "KV-Operation",
                .value = "PURGE",
            },
            .{
                .key = "Nats-Rollup",
                .value = "sub",
            },
        };
        var resp = try self.js.publishRetry(
            subj,
            "",
            &hdrs,
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Deletes a key only if latest revision matches.
    pub fn deleteWithOpts(
        self: *KeyValue,
        key: []const u8,
        opts: KvDeleteOpts,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var hdr_entries: [2]nats.protocol.headers.Entry =
            undefined;
        hdr_entries[0] = .{
            .key = "KV-Operation",
            .value = "DEL",
        };
        var hdr_count: usize = 1;
        var rev_buf: [20]u8 = undefined;
        if (opts.last_revision) |rev| {
            const s = std.fmt.bufPrint(
                &rev_buf,
                "{d}",
                .{rev},
            ) catch unreachable;
            hdr_entries[1] = .{
                .key = headers_mod.HeaderName
                    .expected_last_subj_seq,
                .value = s,
            };
            hdr_count = 2;
        }
        var resp = try self.js.publishRetry(
            subj,
            "",
            hdr_entries[0..hdr_count],
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    /// Purges a key only if latest revision matches.
    pub fn purgeWithOpts(
        self: *KeyValue,
        key: []const u8,
        opts: KvDeleteOpts,
    ) !u64 {
        std.debug.assert(key.len > 0);
        var subj_buf: [256]u8 = undefined;
        const subj = try self.kvSubject(
            key,
            &subj_buf,
        );
        var hdr_entries: [3]nats.protocol.headers.Entry =
            undefined;
        hdr_entries[0] = .{
            .key = "KV-Operation",
            .value = "PURGE",
        };
        hdr_entries[1] = .{
            .key = "Nats-Rollup",
            .value = "sub",
        };
        var hdr_count: usize = 2;
        var rev_buf: [20]u8 = undefined;
        if (opts.last_revision) |rev| {
            const s = std.fmt.bufPrint(
                &rev_buf,
                "{d}",
                .{rev},
            ) catch unreachable;
            hdr_entries[2] = .{
                .key = headers_mod.HeaderName
                    .expected_last_subj_seq,
                .value = s,
            };
            hdr_count = 3;
        }
        var resp = try self.js.publishRetry(
            subj,
            "",
            hdr_entries[0..hdr_count],
        );
        defer resp.deinit();
        return resp.value.seq;
    }

    // -- Keys --

    /// Returns all current (non-deleted) keys in the
    /// bucket. Creates an ephemeral consumer with
    /// last_per_subject deliver policy. Caller owns
    /// the slice; free each key + slice with allocator.
    pub fn keys(
        self: *KeyValue,
        allocator: Allocator,
    ) ![][]const u8 {
        std.debug.assert(self.bucket_len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        var pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );
        defer self.deleteEphemeralPull(&pull);

        var result: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (result.items) |k| allocator.free(k);
            result.deinit(allocator);
        }

        while (true) {
            var msg = (try pull.next(3000)) orelse break;
            defer msg.deinit();

            const subj = msg.subject();
            const plen: usize = 4 +
                @as(usize, self.bucket_len) + 1;
            if (subj.len > plen) {
                const k = subj[plen..];
                if (msg.headers()) |h| {
                    if (isDeleteOp(h)) continue;
                }
                const owned = try allocator.dupe(
                    u8,
                    k,
                );
                result.append(allocator, owned) catch |e| {
                    allocator.free(owned);
                    return e;
                };
            }
        }

        return result.toOwnedSlice(allocator);
    }

    // -- History --

    /// Returns all revisions for a key (up to max
    /// history). Caller owns the returned slice.
    pub fn history(
        self: *KeyValue,
        allocator: Allocator,
        key: []const u8,
    ) ![]types.KeyValueEntry {
        std.debug.assert(key.len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = try self.kvSubject(
            key,
            &subj_buf,
        );

        var pull = try self.createEphemeralPull(
            filter,
            .all,
            null,
        );
        defer self.deleteEphemeralPull(&pull);

        var result: std.ArrayList(
            types.KeyValueEntry,
        ) = .empty;
        errdefer {
            for (result.items) |*entry| entry.deinit();
            result.deinit(allocator);
        }

        while (true) {
            var msg = (try pull.next(3000)) orelse break;
            defer msg.deinit();

            var op: types.KeyValueOp = .put;
            if (msg.headers()) |h| {
                op = parseKvOp(h);
            }

            const md = msg.metadata();
            const seq = if (md) |m|
                m.stream_seq
            else
                0;

            // Extract value before defer deinit frees msg
            const data = msg.data();
            var val: []const u8 = "";
            var val_alloc: ?Allocator = null;
            if (data.len > 0 and op == .put) {
                val = try allocator.dupe(
                    u8,
                    data,
                );
                val_alloc = allocator;
            }

            result.append(allocator, .{
                .bucket = self.bucket(),
                .key = key,
                .value = val,
                .revision = seq,
                .operation = op,
                .value_allocator = val_alloc,
            }) catch |err| {
                if (val_alloc) |a| a.free(val);
                return err;
            };
        }

        return result.toOwnedSlice(allocator);
    }

    /// Returns all revisions with watch options.
    pub fn historyWithOpts(
        self: *KeyValue,
        allocator: Allocator,
        key: []const u8,
        opts: types.WatchOpts,
    ) ![]types.KeyValueEntry {
        std.debug.assert(key.len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = try self.kvSubject(
            key,
            &subj_buf,
        );

        const dp: types.DeliverPolicy = if (opts
            .include_history) .all else .all;
        var pull = try self.createEphemeralPull(
            filter,
            dp,
            if (opts.resume_from_revision) |r| r else null,
        );
        defer self.deleteEphemeralPull(&pull);

        var result: std.ArrayList(
            types.KeyValueEntry,
        ) = .empty;
        errdefer {
            for (result.items) |*entry| entry.deinit();
            result.deinit(allocator);
        }

        while (true) {
            var msg = (try pull.next(3000)) orelse break;
            defer msg.deinit();

            var op: types.KeyValueOp = .put;
            if (msg.headers()) |h| {
                op = parseKvOp(h);
            }

            if (opts.ignore_deletes and
                (op == .delete or op == .purge))
                continue;

            const md = msg.metadata();
            const seq = if (md) |m|
                m.stream_seq
            else
                0;

            if (opts.meta_only) {
                try result.append(allocator, .{
                    .bucket = self.bucket(),
                    .key = key,
                    .value = "",
                    .revision = seq,
                    .operation = op,
                });
                continue;
            }

            const data = msg.data();
            var val: []const u8 = "";
            var val_alloc: ?Allocator = null;
            if (data.len > 0 and op == .put) {
                val = try allocator.dupe(u8, data);
                val_alloc = allocator;
            }

            result.append(allocator, .{
                .bucket = self.bucket(),
                .key = key,
                .value = val,
                .revision = seq,
                .operation = op,
                .value_allocator = val_alloc,
            }) catch |err| {
                if (val_alloc) |a| a.free(val);
                return err;
            };
        }

        return result.toOwnedSlice(allocator);
    }

    /// Returns a streaming key lister with filters.
    /// Only returns keys matching the given patterns.
    pub fn listKeysFiltered(
        self: *KeyValue,
        patterns: []const []const u8,
    ) !KeyLister {
        std.debug.assert(self.bucket_len > 0);
        std.debug.assert(patterns.len > 0);
        std.debug.assert(patterns.len <= 16);

        var filters: [16][]const u8 = undefined;
        var filter_bufs: [16][256]u8 = undefined;

        for (patterns, 0..) |p, i| {
            try validateKeyPattern(p);
            const f = std.fmt.bufPrint(
                &filter_bufs[i],
                "$KV.{s}.{s}",
                .{ self.bucket(), p },
            ) catch return errors.Error.SubjectTooLong;
            filters[i] = filter_bufs[i][0..f.len];
        }

        const seq = ephemeral_counter.fetchAdd(
            1,
            .monotonic,
        );
        const name = std.fmt.bufPrint(
            &self._eph_name_buf,
            "kv{d}x{d}",
            .{ seq, @intFromPtr(self) % 99999 },
        ) catch unreachable;
        self._eph_name_len = @intCast(name.len);

        const fs = filters[0..patterns.len];
        var resp = try self.js.createConsumer(
            self.streamName(),
            .{
                .name = name,
                .ack_policy = .none,
                .deliver_policy = .last_per_subject,
                .filter_subjects = fs,
                .mem_storage = true,
                .inactive_threshold = 60_000_000_000,
            },
        );
        resp.deinit();

        var pull = PullSubscription{
            .js = self.js,
            .stream = self.streamName(),
        };
        try pull.setConsumer(name);

        return KeyLister{ .kv = self, .pull = pull };
    }

    // -- Watch --

    /// Watches a key pattern for real-time updates.
    /// Delivers current values (last per subject)
    /// first, then continues with live updates.
    pub fn watch(
        self: *KeyValue,
        key_pattern: []const u8,
    ) !KvWatcher {
        return self.watchWithOpts(key_pattern, .{});
    }

    /// Watches with configurable options.
    pub fn watchWithOpts(
        self: *KeyValue,
        key_pattern: []const u8,
        opts: types.WatchOpts,
    ) !KvWatcher {
        try validateKeyPattern(key_pattern);
        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.{s}",
            .{ self.bucket(), key_pattern },
        ) catch return errors.Error.SubjectTooLong;

        const dp = watchDeliverPolicy(opts);
        const start = if (opts.resume_from_revision) |r|
            r
        else
            null;

        const pull = try self.createEphemeralPull(
            filter,
            dp,
            start,
        );

        return KvWatcher{
            .kv = self,
            .pull = pull,
            .opts = opts,
        };
    }

    /// Watches all keys in the bucket.
    pub fn watchAll(self: *KeyValue) !KvWatcher {
        return self.watchAllWithOpts(.{});
    }

    /// Watches all keys with configurable options.
    pub fn watchAllWithOpts(
        self: *KeyValue,
        opts: types.WatchOpts,
    ) !KvWatcher {
        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        const dp = watchDeliverPolicy(opts);
        const start = if (opts.resume_from_revision) |r|
            r
        else
            null;

        const pull = try self.createEphemeralPull(
            filter,
            dp,
            start,
        );

        return KvWatcher{
            .kv = self,
            .pull = pull,
            .opts = opts,
        };
    }

    /// Watches multiple key patterns simultaneously.
    /// Uses filter_subjects on the consumer config.
    pub fn watchFiltered(
        self: *KeyValue,
        patterns: []const []const u8,
        opts: types.WatchOpts,
    ) !KvWatcher {
        std.debug.assert(patterns.len > 0);
        std.debug.assert(patterns.len <= 16);

        var filters: [16][]const u8 = undefined;
        var filter_bufs: [16][256]u8 = undefined;
        var filter_lens: [16]u8 = undefined;

        for (patterns, 0..) |p, i| {
            try validateKeyPattern(p);
            const f = std.fmt.bufPrint(
                &filter_bufs[i],
                "$KV.{s}.{s}",
                .{ self.bucket(), p },
            ) catch return errors.Error.SubjectTooLong;
            filter_lens[i] = @intCast(f.len);
            filters[i] = filter_bufs[i][0..f.len];
        }

        const dp = watchDeliverPolicy(opts);
        const start = opts.resume_from_revision;

        const seq = ephemeral_counter.fetchAdd(
            1,
            .monotonic,
        );
        const name = std.fmt.bufPrint(
            &self._eph_name_buf,
            "kv{d}x{d}",
            .{ seq, @intFromPtr(self) % 99999 },
        ) catch unreachable;
        self._eph_name_len = @intCast(name.len);

        const fs = filters[0..patterns.len];
        var resp = try self.js.createConsumer(
            self.streamName(),
            .{
                .name = name,
                .ack_policy = .none,
                .deliver_policy = dp,
                .opt_start_seq = start,
                .filter_subjects = fs,
                .mem_storage = true,
                .inactive_threshold = 60_000_000_000,
                .headers_only = if (opts.meta_only)
                    true
                else
                    null,
            },
        );
        resp.deinit();

        var pull = PullSubscription{
            .js = self.js,
            .stream = self.streamName(),
        };
        try pull.setConsumer(name);

        return KvWatcher{
            .kv = self,
            .pull = pull,
            .opts = opts,
        };
    }

    // -- Key listing (streaming) --

    /// Streaming key lister. More memory-efficient
    /// than keys() for large buckets.
    pub const KeyLister = struct {
        kv: *KeyValue,
        pull: PullSubscription,
        done: bool = false,
        current_msg: ?JsMsg = null,

        fn clearCurrent(self: *KeyLister) void {
            if (self.current_msg) |*msg| {
                msg.deinit();
                self.current_msg = null;
            }
        }

        /// Returns the next key, or null when done.
        /// Caller does NOT own the returned slice --
        /// it points into the message buffer and is
        /// valid until the next call to next().
        pub fn next(
            self: *KeyLister,
        ) !?[]const u8 {
            if (self.done) return null;
            self.clearCurrent();
            while (true) {
                var msg = (try self.pull.next(3000)) orelse {
                    self.done = true;
                    return null;
                };

                const subj = msg.subject();
                const plen: usize = 4 +
                    @as(usize, self.kv.bucket_len) + 1;
                if (subj.len <= plen) {
                    msg.deinit();
                    continue;
                }

                if (msg.headers()) |h| {
                    if (KeyValue.isDeleteOp(h)) {
                        msg.deinit();
                        continue;
                    }
                }

                self.current_msg = msg;
                const stored = self.current_msg.?;
                return stored.subject()[plen..];
            }
        }

        /// Cleans up the lister and its consumer.
        pub fn deinit(self: *KeyLister) void {
            self.clearCurrent();
            self.kv.deleteEphemeralPull(&self.pull);
        }
    };

    /// Returns a streaming key lister. Caller must
    /// call deinit() when done.
    pub fn listKeys(self: *KeyValue) !KeyLister {
        std.debug.assert(self.bucket_len > 0);

        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        const pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );

        return KeyLister{ .kv = self, .pull = pull };
    }

    fn watchDeliverPolicy(
        opts: types.WatchOpts,
    ) types.DeliverPolicy {
        if (opts.resume_from_revision != null)
            return .by_start_sequence;
        if (opts.updates_only)
            return .new;
        if (opts.include_history)
            return .all;
        return .last_per_subject;
    }

    // -- Purge Deletes --

    /// Options for purging delete markers.
    pub const PurgeDeletesOpts = struct {
        /// Only purge markers older than this (ns).
        /// Default 0 = purge all markers.
        older_than_ns: i64 = 0,
    };

    /// Removes all delete/purge markers from the
    /// bucket. Optionally filters by marker age.
    pub fn purgeDeletes(
        self: *KeyValue,
        opts: PurgeDeletesOpts,
    ) !u64 {
        std.debug.assert(self.bucket_len > 0);
        const cutoff_ns: i64 = if (opts.older_than_ns > 0) blk: {
            const now = std.Io.Clock.real.now(self.js.client.io);
            const now_ns: i64 = @intCast(now.nanoseconds);
            break :blk if (now_ns > opts.older_than_ns)
                now_ns - opts.older_than_ns
            else
                0;
        } else 0;
        var subj_buf: [256]u8 = undefined;
        const filter = std.fmt.bufPrint(
            &subj_buf,
            "$KV.{s}.>",
            .{self.bucket()},
        ) catch return errors.Error.SubjectTooLong;

        var pull = try self.createEphemeralPull(
            filter,
            .last_per_subject,
            null,
        );
        defer self.deleteEphemeralPull(&pull);

        var purged: u64 = 0;
        while (true) {
            var msg = (try pull.next(3000)) orelse break;
            defer msg.deinit();

            if (msg.headers()) |h| {
                if (isDeleteOp(h)) {
                    if (opts.older_than_ns > 0) {
                        const md = msg.metadata() orelse
                            continue;
                        if (md.timestamp <= 0 or
                            md.timestamp > cutoff_ns)
                            continue;
                    }
                    const subj = msg.subject();
                    var pr = try self.js.purgeStreamSubject(
                        self.streamName(),
                        subj,
                    );
                    pr.deinit();
                    purged += 1;
                }
            }
        }
        return purged;
    }

    /// Creates an ephemeral consumer with the given
    /// deliver policy and returns a PullSubscription.
    fn createEphemeralPull(
        self: *KeyValue,
        filter: []const u8,
        deliver_policy: types.DeliverPolicy,
        opt_start_seq: ?u64,
    ) !PullSubscription {
        std.debug.assert(filter.len > 0);
        // Generate unique name into stable storage
        const seq = ephemeral_counter.fetchAdd(
            1,
            .monotonic,
        );
        const name = std.fmt.bufPrint(
            &self._eph_name_buf,
            "kv{d}x{d}",
            .{ seq, @intFromPtr(self) % 99999 },
        ) catch unreachable;
        self._eph_name_len = @intCast(name.len);

        var resp = try self.js.createConsumer(
            self.streamName(),
            .{
                .name = name,
                .ack_policy = .none,
                .deliver_policy = deliver_policy,
                .opt_start_seq = opt_start_seq,
                .filter_subject = filter,
                .mem_storage = true,
                .inactive_threshold = 60_000_000_000,
            },
        );
        resp.deinit();

        var pull = PullSubscription{
            .js = self.js,
            .stream = self.streamName(),
        };
        try pull.setConsumer(name);
        return pull;
    }

    fn ephName(self: *const KeyValue) []const u8 {
        std.debug.assert(self._eph_name_len > 0);
        return self._eph_name_buf[0..self._eph_name_len];
    }

    fn deleteEphemeralPull(
        self: *KeyValue,
        pull: *PullSubscription,
    ) void {
        var resp = self.js.deleteConsumer(
            self.streamName(),
            pull.consumerName(),
        ) catch return;
        resp.deinit();
    }

    // -- Status --

    /// Returns bucket status information.
    pub fn status(
        self: *KeyValue,
    ) !types.Response(types.StreamInfo) {
        return self.js.streamInfo(self.streamName());
    }

    // -- Helpers --

    fn isDeleteOp(raw_headers: []const u8) bool {
        const op = parseKvOp(raw_headers);
        return op == .delete or op == .purge;
    }

    /// Matches exact KV-Operation header values to
    /// avoid substring false positives.
    fn parseKvOp(raw_headers: []const u8) types.KeyValueOp {
        if (std.mem.indexOf(
            u8,
            raw_headers,
            "KV-Operation: PURGE\r\n",
        ) != null) return .purge;
        if (std.mem.indexOf(
            u8,
            raw_headers,
            "KV-Operation: DEL\r\n",
        ) != null) return .delete;
        return .put;
    }

    fn decodeBase64(
        encoded: []const u8,
        buf: []u8,
    ) ?[]const u8 {
        if (encoded.len == 0) return null;
        const decoder = std.base64.standard.Decoder;
        const len = decoder.calcSizeForSlice(
            encoded,
        ) catch return null;
        if (len > buf.len) return null;
        decoder.decode(
            buf[0..len],
            encoded,
        ) catch return null;
        return buf[0..len];
    }
};

/// Watcher for real-time KV updates using an ephemeral
/// consumer with last_per_subject. Call `next()` to
/// receive entries.
pub const KvWatcher = struct {
    kv: *KeyValue,
    pull: PullSubscription,
    opts: types.WatchOpts = .{},
    initial_done: bool = false,

    /// Returns the next entry update. Returns null
    /// when no more updates within timeout. First
    /// null after creation means all existing keys
    /// have been delivered.
    pub fn next(
        self: *KvWatcher,
        timeout_ms: u32,
    ) !?types.KeyValueEntry {
        std.debug.assert(timeout_ms > 0);
        while (true) {
            var msg = (try self.pull.next(
                timeout_ms,
            )) orelse {
                if (!self.initial_done) {
                    self.initial_done = true;
                }
                return null;
            };
            defer msg.deinit();

            var op: types.KeyValueOp = .put;
            if (msg.headers()) |h| {
                op = KeyValue.parseKvOp(h);
            }

            // Skip deletes if configured
            if (self.opts.ignore_deletes and
                (op == .delete or op == .purge))
                continue;

            const subj = msg.subject();
            const plen: usize = 4 +
                @as(usize, self.kv.bucket_len) + 1;
            const key = if (subj.len > plen)
                subj[plen..]
            else
                "";
            const allocator = self.kv.js.allocator;
            const owned_key = try allocator.dupe(u8, key);
            errdefer allocator.free(owned_key);

            const md = msg.metadata();
            const seq = if (md) |m|
                m.stream_seq
            else
                0;

            // meta_only: skip value extraction
            if (self.opts.meta_only) {
                return types.KeyValueEntry{
                    .bucket = self.kv.bucket(),
                    .key = owned_key,
                    .value = "",
                    .revision = seq,
                    .operation = op,
                    .key_allocator = allocator,
                };
            }

            const data = msg.data();
            var val: []const u8 = "";
            var val_alloc: ?Allocator = null;
            if (data.len > 0 and op == .put) {
                val = try allocator.dupe(u8, data);
                val_alloc = allocator;
            }

            return types.KeyValueEntry{
                .bucket = self.kv.bucket(),
                .key = owned_key,
                .value = val,
                .revision = seq,
                .operation = op,
                .key_allocator = allocator,
                .value_allocator = val_alloc,
            };
        }
    }

    /// Cleans up the watcher and its consumer.
    pub fn deinit(self: *KvWatcher) void {
        self.kv.deleteEphemeralPull(&self.pull);
    }
};

test "KV keys reject spaces and DEL" {
    var kv = KeyValue{ .js = undefined };
    @memcpy(kv.bucket_buf[0..1], "B");
    kv.bucket_len = 1;

    var buf: [256]u8 = undefined;
    try std.testing.expectError(
        errors.Error.InvalidKey,
        kv.kvSubject("bad key", &buf),
    );
    try std.testing.expectError(
        errors.Error.InvalidKey,
        kv.kvSubject("bad\x7fkey", &buf),
    );
}
