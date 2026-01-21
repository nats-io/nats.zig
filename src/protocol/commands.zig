//! NATS Protocol Command Definitions
//!
//! Defines the structure of all NATS protocol commands for both
//! server-to-client and client-to-server communication.

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const defaults = @import("../defaults.zig");

/// Commands sent from server to client.
pub const ServerCommand = union(enum) {
    info: ServerInfo,
    msg: MsgArgs,
    hmsg: HMsgArgs,
    ping,
    pong,
    ok,
    err: []const u8,
};

/// Commands sent from client to server.
pub const ClientCommand = union(enum) {
    connect: ConnectOptions,
    pub_cmd: PubArgs,
    hpub: HPubArgs,
    sub: SubArgs,
    unsub: UnsubArgs,
    ping,
    pong,
};

/// Server info with owned string copies.
/// All strings are allocated and owned by this struct.
pub const ServerInfo = struct {
    /// Maximum allowed max_payload value (1GB).
    pub const MAX_PAYLOAD_LIMIT = 1024 * 1024 * 1024;

    /// Validation errors for ServerInfo.
    pub const ValidationError = error{InvalidServerInfo};

    server_id: []const u8,
    server_name: []const u8,
    version: []const u8,
    host: []const u8,
    proto: u32,
    port: u16,
    headers: bool,
    max_payload: u32,
    jetstream: bool,
    tls_required: bool,
    tls_available: bool,
    auth_required: bool,
    nonce: ?[]const u8,
    client_id: ?u64,
    client_ip: ?[]const u8,
    cluster: ?[]const u8,

    /// Discovered server URLs from cluster (inline storage, no allocation).
    connect_urls: [16][256]u8 = undefined,
    connect_urls_lens: [16]u8 = [_]u8{0} ** 16,
    connect_urls_count: u8 = 0,

    /// Creates an owned copy from parsed JSON RawServerInfo.
    /// Copies all strings so they outlive the JSON arena.
    /// Returns InvalidServerInfo if required fields are missing or invalid.
    pub fn fromParsed(
        allocator: Allocator,
        parsed: std.json.Parsed(RawServerInfo),
    ) (ValidationError || Allocator.Error)!ServerInfo {
        const info = parsed.value;

        // Must have at least server_id or version for identification
        if (info.server_id.len == 0 and info.version.len == 0) {
            return error.InvalidServerInfo;
        }

        // max_payload must be > 0 and <= 1GB
        if (info.max_payload == 0 or info.max_payload > MAX_PAYLOAD_LIMIT) {
            return error.InvalidServerInfo;
        }

        var owned = ServerInfo{
            .server_id = try allocator.dupe(u8, info.server_id),
            .server_name = try allocator.dupe(u8, info.server_name),
            .version = try allocator.dupe(u8, info.version),
            .host = try allocator.dupe(u8, info.host),
            .proto = info.proto,
            .port = info.port,
            .headers = info.headers,
            .max_payload = info.max_payload,
            .jetstream = info.jetstream,
            .tls_required = info.tls_required,
            .tls_available = info.tls_available,
            .auth_required = info.auth_required,
            .nonce = if (info.nonce) |n| try allocator.dupe(u8, n) else null,
            .client_id = info.client_id,
            .client_ip = if (info.client_ip) |ip|
                try allocator.dupe(u8, ip)
            else
                null,
            .cluster = if (info.cluster) |c|
                try allocator.dupe(u8, c)
            else
                null,
        };

        // Copy connect_urls (inline, no allocation)
        // Skip URLs > max_url_len (truncated URL would be invalid anyway)
        if (info.connect_urls) |urls| {
            for (urls) |url| {
                if (owned.connect_urls_count >= 16) break;
                if (url.len > defaults.Server.max_url_len) continue;
                const len: u8 = @intCast(url.len);
                const idx = owned.connect_urls_count;
                @memcpy(owned.connect_urls[idx][0..len], url);
                owned.connect_urls_lens[idx] = len;
                owned.connect_urls_count += 1;
            }
        }

        return owned;
    }

    /// Frees all owned strings.
    pub fn deinit(self: *ServerInfo, allocator: Allocator) void {
        assert(self.port > 0);
        allocator.free(self.server_id);
        allocator.free(self.server_name);
        allocator.free(self.version);
        allocator.free(self.host);
        if (self.nonce) |n| allocator.free(n);
        if (self.client_ip) |ip| allocator.free(ip);
        if (self.cluster) |c| allocator.free(c);
        self.* = undefined;
    }

    /// Get connect URL at index. Returns null if index out of bounds.
    pub fn getConnectUrl(self: *const ServerInfo, idx: u8) ?[]const u8 {
        if (idx >= self.connect_urls_count) return null;
        const len = self.connect_urls_lens[idx];
        if (len == 0) return null;
        return self.connect_urls[idx][0..len];
    }
};

/// Raw server INFO payload parsed from JSON.
/// Internal use only - strings borrow from JSON arena.
pub const RawServerInfo = struct {
    server_id: []const u8 = "",
    server_name: []const u8 = "",
    version: []const u8 = "",
    proto: u32 = 1,
    host: []const u8 = "",
    port: u16 = 4222,
    headers: bool = false,
    max_payload: u32 = 1048576,
    jetstream: bool = false,
    tls_required: bool = false,
    tls_available: bool = false,
    auth_required: bool = false,
    connect_urls: ?[]const []const u8 = null,
    nonce: ?[]const u8 = null,
    client_id: ?u64 = null,
    client_ip: ?[]const u8 = null,
    cluster: ?[]const u8 = null,

    /// Parses RawServerInfo from JSON data.
    pub fn parse(
        allocator: Allocator,
        json_data: []const u8,
    ) std.json.ParseError(std.json.Scanner)!std.json.Parsed(RawServerInfo) {
        return std.json.parseFromSlice(
            RawServerInfo,
            allocator,
            json_data,
            .{ .ignore_unknown_fields = true },
        );
    }

    /// Frees a parsed RawServerInfo.
    pub fn deinit(parsed: *std.json.Parsed(RawServerInfo)) void {
        parsed.deinit();
    }
};

/// Client CONNECT command options.
pub const ConnectOptions = struct {
    verbose: bool = false,
    pedantic: bool = false,
    tls_required: bool = false,
    auth_token: ?[]const u8 = null,
    user: ?[]const u8 = null,
    pass: ?[]const u8 = null,
    name: ?[]const u8 = null,
    lang: []const u8 = "zig",
    version: []const u8 = "0.1.0",
    protocol: u32 = 1,
    echo: bool = true,
    sig: ?[]const u8 = null,
    jwt: ?[]const u8 = null,
    nkey: ?[]const u8 = null,
    headers: bool = true,
    no_responders: bool = true,
};

/// Arguments for PUB command.
pub const PubArgs = struct {
    subject: []const u8,
    reply_to: ?[]const u8 = null,
    payload: []const u8,
};

/// Arguments for HPUB command (publish with headers).
pub const HPubArgs = struct {
    subject: []const u8,
    reply_to: ?[]const u8 = null,
    headers: []const u8,
    payload: []const u8,
};

/// Arguments for HPUB command with structured header entries.
/// Preferred over HPubArgs for type-safe header construction.
pub const HPubWithEntriesArgs = struct {
    subject: []const u8,
    reply_to: ?[]const u8 = null,
    headers: []const headers_mod.Entry,
    payload: []const u8,
};

const headers_mod = @import("headers.zig");

/// Arguments for SUB command.
pub const SubArgs = struct {
    subject: []const u8,
    queue_group: ?[]const u8 = null,
    sid: u64,
};

/// Arguments for UNSUB command.
pub const UnsubArgs = struct {
    sid: u64,
    max_msgs: ?u64 = null,
};

/// Arguments parsed from MSG command.
pub const MsgArgs = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8 = null,
    payload_len: usize,
    payload: []const u8 = "",
    /// Length of header line including \r\n (for partial message parsing).
    header_line_len: usize = 0,
};

/// Arguments parsed from HMSG command (message with headers).
pub const HMsgArgs = struct {
    subject: []const u8,
    sid: u64,
    reply_to: ?[]const u8 = null,
    header_len: usize,
    total_len: usize,
    headers: []const u8 = "",
    payload: []const u8 = "",
    /// Length of header line including \r\n (for partial message parsing).
    header_line_len: usize = 0,
};

test "server info parse" {
    const allocator = std.testing.allocator;
    const json = "{\"server_id\":\"test\",\"version\":\"2.10.0\"," ++
        "\"proto\":1,\"max_payload\":1048576}";

    var parsed = try RawServerInfo.parse(allocator, json);
    defer parsed.deinit();

    try std.testing.expectEqualSlices(u8, "test", parsed.value.server_id);
    try std.testing.expectEqualSlices(u8, "2.10.0", parsed.value.version);
    try std.testing.expectEqual(@as(u32, 1), parsed.value.proto);
    try std.testing.expectEqual(@as(u32, 1048576), parsed.value.max_payload);
}

test "server info parse with unknown fields" {
    const allocator = std.testing.allocator;
    const json =
        \\{"server_id":"x","unknown_field":"ignored","version":"1.0"}
    ;

    var parsed = try RawServerInfo.parse(allocator, json);
    defer parsed.deinit();

    try std.testing.expectEqualSlices(u8, "x", parsed.value.server_id);
}

test "connect options defaults" {
    const opts: ConnectOptions = .{};
    try std.testing.expect(!opts.verbose);
    try std.testing.expect(opts.echo);
    try std.testing.expect(opts.headers);
    try std.testing.expectEqualSlices(u8, "zig", opts.lang);
}

test "pub args" {
    const args: PubArgs = .{
        .subject = "test.subject",
        .payload = "hello",
    };
    try std.testing.expectEqualSlices(u8, "test.subject", args.subject);
    try std.testing.expectEqual(@as(?[]const u8, null), args.reply_to);
}

test "sub args with queue" {
    const args: SubArgs = .{
        .subject = "orders.>",
        .queue_group = "workers",
        .sid = 42,
    };
    try std.testing.expectEqualSlices(u8, "workers", args.queue_group.?);
}
