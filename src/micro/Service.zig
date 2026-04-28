const std = @import("std");
const Client = @import("../Client.zig");
const pubsub = @import("../pubsub.zig");
const endpoint_mod = @import("endpoint.zig");
const protocol = @import("protocol.zig");
const validation = @import("validation.zig");
const json_util = @import("json_util.zig");
const timeutil = @import("timeutil.zig");

pub const Error = anyerror;

pub const Config = struct {
    name: []const u8,
    version: []const u8,
    description: ?[]const u8 = null,
    metadata: []const protocol.MetadataPair = &.{},
    service_prefix: []const u8 = "$SRV",
    queue_policy: endpoint_mod.QueuePolicy = .{ .queue = "q" },
    endpoint: ?endpoint_mod.EndpointConfig = null,
};

pub const Service = @This();

client: *Client,
allocator: std.mem.Allocator,
name: []const u8,
version: []const u8,
description: ?[]const u8,
id: []const u8,
service_prefix: []const u8,
started: []const u8,
metadata: []protocol.MetadataPair,
queue_policy: endpoint_mod.QueuePolicy,
endpoints: std.ArrayList(*endpoint_mod.Endpoint) = .empty,
group_prefixes: std.ArrayList([]u8) = .empty,
group_queues: std.ArrayList([]u8) = .empty,
monitor_subs: std.ArrayList(*Client.Subscription) = .empty,
mutex: std.Io.Mutex = .init,
in_flight: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
stopping: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
stopped_flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
stop_error: ?anyerror = null,

pub fn addService(client: *Client, config: Config) !*Service {
    try validation.validateName(config.name);
    try validation.validateVersion(config.version);
    try validation.validatePrefix(config.service_prefix);

    const service = try client.allocator.create(Service);
    errdefer client.allocator.destroy(service);

    const name = try client.allocator.dupe(u8, config.name);
    errdefer client.allocator.free(name);
    const version = try client.allocator.dupe(u8, config.version);
    errdefer client.allocator.free(version);
    const description = if (config.description) |d|
        try client.allocator.dupe(u8, d)
    else
        null;
    errdefer if (description) |d| client.allocator.free(d);
    const id = try generateId(client.allocator, client.io);
    errdefer client.allocator.free(id);
    const service_prefix = try client.allocator.dupe(u8, config.service_prefix);
    errdefer client.allocator.free(service_prefix);
    const metadata = try endpoint_mod.dupMetadata(client.allocator, config.metadata);
    errdefer endpoint_mod.freeMetadata(client.allocator, metadata);
    const queue_policy = try dupQueuePolicy(client.allocator, config.queue_policy);
    errdefer freeQueuePolicy(client.allocator, queue_policy);

    service.* = .{
        .client = client,
        .allocator = client.allocator,
        .name = name,
        .version = version,
        .description = description,
        .id = id,
        .service_prefix = service_prefix,
        .started = undefined,
        .metadata = metadata,
        .queue_policy = queue_policy,
    };

    var started_buf: [32]u8 = undefined;
    const started = try timeutil.nowRfc3339(client.io, &started_buf);
    service.started = try client.allocator.dupe(u8, started);
    errdefer client.allocator.free(service.started);
    errdefer service.cleanupRuntimeResources();

    try service.initMonitorSubs();

    if (config.endpoint) |ep_cfg| {
        _ = try service.addEndpoint(ep_cfg);
    }

    // Make service discovery and endpoint subscriptions visible to the
    // server before returning so immediate requests do not race setup.
    try client.flush(5 * std.time.ns_per_s);

    return service;
}

pub fn addEndpoint(self: *Service, cfg: endpoint_mod.EndpointConfig) !*endpoint_mod.Endpoint {
    return self.addEndpointWithPrefix("", .inherit, cfg);
}

pub fn addGroup(self: *Service, prefix: []const u8) !endpoint_mod.Group {
    try validation.validateGroup(prefix);
    const full = try self.allocGroupPrefix("", prefix);
    return .{
        .service = self,
        .prefix = full,
        .queue_policy = .inherit,
    };
}

pub fn addGroupWithQueue(
    self: *Service,
    prefix: []const u8,
    queue: []const u8,
) !endpoint_mod.Group {
    try validation.validateGroup(prefix);
    try pubsub.validateQueueGroup(queue);
    const full = try self.allocGroupPrefix("", prefix);
    const owned_queue = try self.allocGroupQueue(queue);
    return .{
        .service = self,
        .prefix = full,
        .queue_policy = .{ .queue = owned_queue },
    };
}

pub fn info(self: *Service, allocator: std.mem.Allocator) !protocol.Info {
    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);

    const endpoints = try allocator.alloc(protocol.EndpointInfo, self.endpoints.items.len);
    for (self.endpoints.items, 0..) |ep, i| {
        endpoints[i] = .{
            .name = ep.name,
            .subject = ep.subject,
            .queue_group = ep.queue_group,
            .metadata = if (ep.metadata.len == 0) null else ep.metadata,
        };
    }
    return .{
        .name = self.name,
        .id = self.id,
        .version = self.version,
        .description = self.description,
        .metadata = if (self.metadata.len == 0) null else self.metadata,
        .endpoints = endpoints,
    };
}

pub fn stats(self: *Service, allocator: std.mem.Allocator) !protocol.StatsResponse {
    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);

    const endpoints = try allocator.alloc(protocol.EndpointStatsJson, self.endpoints.items.len);
    for (self.endpoints.items, 0..) |ep, i| {
        const snap = ep.stats.snapshot();
        endpoints[i] = .{
            .name = ep.name,
            .subject = ep.subject,
            .queue_group = ep.queue_group,
            .metadata = if (ep.metadata.len == 0) null else ep.metadata,
            .num_requests = snap.num_requests,
            .num_errors = snap.num_errors,
            .last_error = snap.last_error,
            .processing_time = snap.processing_time,
            .average_processing_time = snap.average_processing_time,
        };
    }
    return .{
        .name = self.name,
        .id = self.id,
        .version = self.version,
        .started = self.started,
        .metadata = if (self.metadata.len == 0) null else self.metadata,
        .endpoints = endpoints,
    };
}

pub fn reset(self: *Service) void {
    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    for (self.endpoints.items) |ep| {
        ep.stats.reset();
    }
}

pub fn stop(self: *Service, stop_error: ?anyerror) !void {
    if (self.stopped_flag.load(.acquire)) return;
    if (self.stopping.swap(true, .acq_rel)) {
        return self.waitStopped();
    }
    self.stop_error = stop_error;

    self.mutex.lockUncancelable(self.client.io);
    for (self.monitor_subs.items) |sub| {
        sub.deinit();
    }
    self.monitor_subs.clearRetainingCapacity();

    for (self.endpoints.items) |ep| {
        ep.sub.drain() catch {};
    }
    self.mutex.unlock(self.client.io);

    // Ensure UNSUB frames reach the server before stop() returns so no
    // new requests are accepted after shutdown completes.
    self.client.flush(5 * std.time.ns_per_s) catch {};

    const drain_timeout_ms = self.client.options.drain_timeout_ms;
    var drain_err: ?anyerror = null;
    for (self.endpoints.items) |ep| {
        ep.sub.waitDrained(drain_timeout_ms) catch |err| {
            if (drain_err == null) drain_err = err;
        };
    }

    const start = std.Io.Timestamp.now(self.client.io, .awake);
    const timeout_ns =
        @as(i128, drain_timeout_ms) * std.time.ns_per_ms;
    var spins: u32 = 0;
    while (self.in_flight.load(.acquire) != 0) {
        const now = std.Io.Timestamp.now(self.client.io, .awake);
        if (now.nanoseconds - start.nanoseconds >= timeout_ns) {
            drain_err = drain_err orelse error.Timeout;
            break;
        }
        spins += 1;
        if (spins < 100) {
            std.atomic.spinLoopHint();
        } else {
            self.client.io.sleep(.fromNanoseconds(0), .awake) catch {};
            spins = 0;
        }
    }

    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    for (self.endpoints.items) |ep| {
        ep.sub.deinit();
    }

    // `deinit()` sends the final unsubscribe/cancel path for callback
    // subscriptions. Confirm it has reached the server before reporting
    // the service as stopped.
    self.client.flush(5 * std.time.ns_per_s) catch {};

    self.stopped_flag.store(true, .release);
    if (drain_err) |err| return err;
}

pub fn waitStopped(self: *Service) !void {
    while (!self.stopped_flag.load(.acquire)) {
        self.client.io.sleep(.fromNanoseconds(0), .awake) catch {};
    }
    if (self.stop_error) |err| return err;
}

pub fn stopped(self: *const Service) bool {
    return self.stopped_flag.load(.acquire);
}

fn cleanupRuntimeResources(self: *Service) void {
    for (self.monitor_subs.items) |sub| {
        sub.deinit();
    }
    self.monitor_subs.deinit(self.allocator);

    for (self.endpoints.items) |ep| {
        ep.sub.deinit();
        ep.deinit(self.allocator);
    }
    self.endpoints.deinit(self.allocator);

    for (self.group_prefixes.items) |prefix| self.allocator.free(prefix);
    self.group_prefixes.deinit(self.allocator);

    for (self.group_queues.items) |queue| self.allocator.free(queue);
    self.group_queues.deinit(self.allocator);
}

pub fn deinit(self: *Service) void {
    self.stop(null) catch {};

    for (self.endpoints.items) |ep| ep.deinit(self.allocator);
    self.endpoints.deinit(self.allocator);

    for (self.group_prefixes.items) |prefix| self.allocator.free(prefix);
    self.group_prefixes.deinit(self.allocator);
    for (self.group_queues.items) |queue| self.allocator.free(queue);
    self.group_queues.deinit(self.allocator);
    self.monitor_subs.deinit(self.allocator);

    endpoint_mod.freeMetadata(self.allocator, self.metadata);
    self.allocator.free(self.name);
    self.allocator.free(self.version);
    if (self.description) |d| self.allocator.free(d);
    self.allocator.free(self.id);
    self.allocator.free(self.service_prefix);
    self.allocator.free(self.started);
    freeQueuePolicy(self.allocator, self.queue_policy);
    self.allocator.destroy(self);
}

pub fn addEndpointWithPrefix(
    self: *Service,
    prefix: []const u8,
    inherited_policy: endpoint_mod.QueuePolicy,
    cfg: endpoint_mod.EndpointConfig,
) !*endpoint_mod.Endpoint {
    if (self.stopping.load(.acquire)) return error.InvalidState;

    const full_subject = try joinSubject(self.allocator, prefix, cfg.subject);
    errdefer self.allocator.free(full_subject);
    try pubsub.validatePublish(full_subject);

    const name = try self.allocator.dupe(u8, cfg.name orelse cfg.subject);
    errdefer self.allocator.free(name);
    const queue_group = try resolveQueuePolicy(self.allocator, cfg.queue_policy, inherited_policy, self.queue_policy);
    errdefer if (queue_group) |q| self.allocator.free(q);
    const metadata = try endpoint_mod.dupMetadata(self.allocator, cfg.metadata);
    errdefer endpoint_mod.freeMetadata(self.allocator, metadata);

    const ep = try self.allocator.create(endpoint_mod.Endpoint);
    errdefer self.allocator.destroy(ep);

    ep.* = .{
        .service = self,
        .sub = undefined,
        .name = name,
        .subject = full_subject,
        .queue_group = queue_group,
        .metadata = metadata,
        .handler = cfg.handler,
    };
    ep.callback = .{ .endpoint = ep };

    ep.sub = if (queue_group) |q|
        try self.client.queueSubscribe(full_subject, q, Client.MsgHandler.init(endpoint_mod.EndpointCallback, &ep.callback))
    else
        try self.client.subscribe(full_subject, Client.MsgHandler.init(endpoint_mod.EndpointCallback, &ep.callback));
    errdefer ep.sub.deinit();

    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    try self.endpoints.append(self.allocator, ep);

    // Make the new endpoint visible to the server before returning.
    self.client.flush(5 * std.time.ns_per_s) catch {};
    return ep;
}

pub fn allocGroupPrefix(self: *Service, base: []const u8, next: []const u8) ![]const u8 {
    const full = try joinSubject(self.allocator, base, next);
    errdefer self.allocator.free(full);
    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    try self.group_prefixes.append(self.allocator, full);
    return full;
}

pub fn allocGroupQueue(self: *Service, queue: []const u8) ![]const u8 {
    try pubsub.validateQueueGroup(queue);
    const owned = try self.allocator.dupe(u8, queue);
    errdefer self.allocator.free(owned);
    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    try self.group_queues.append(self.allocator, owned);
    return owned;
}

pub fn onMessage(self: *Service, msg: *const Client.Message) void {
    const reply_to = msg.reply_to orelse return;
    if (self.stopped_flag.load(.acquire)) return;

    var payload: ?[]u8 = null;
    defer if (payload) |buf| self.allocator.free(buf);

    const subject = msg.subject;
    const prefix = self.service_prefix;
    if (!std.mem.startsWith(u8, subject, prefix)) return;
    if (subject.len <= prefix.len or subject[prefix.len] != '.') return;
    const rest = subject[prefix.len + 1 ..];

    if (matchVerb(rest, "PING")) {
        const ping = protocol.Ping{
            .name = self.name,
            .id = self.id,
            .version = self.version,
            .metadata = if (self.metadata.len == 0) null else self.metadata,
        };
        payload = json_util.jsonStringify(self.allocator, ping) catch return;
    } else if (matchVerb(rest, "INFO")) {
        var info_resp = self.info(self.allocator) catch return;
        defer info_resp.deinit(self.allocator);
        payload = json_util.jsonStringify(self.allocator, info_resp) catch return;
    } else if (matchVerb(rest, "STATS")) {
        var stats_resp = self.stats(self.allocator) catch return;
        defer stats_resp.deinit(self.allocator);
        payload = json_util.jsonStringify(self.allocator, stats_resp) catch return;
    } else return;

    self.client.publish(reply_to, payload.?) catch {};
}

fn initMonitorSubs(self: *Service) !void {
    const prefixes = [_][]const u8{
        "PING", "INFO", "STATS",
    };
    for (prefixes) |verb| {
        try self.subscribeMonitor(verb);
        const name_subject = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}",
            .{ verb, self.name },
        );
        defer self.allocator.free(name_subject);
        try self.subscribeMonitor(name_subject);

        const id_subject = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}.{s}",
            .{ verb, self.name, self.id },
        );
        defer self.allocator.free(id_subject);
        try self.subscribeMonitor(id_subject);
    }
}

fn subscribeMonitor(self: *Service, suffix: []const u8) !void {
    const subject = try std.fmt.allocPrint(
        self.allocator,
        "{s}.{s}",
        .{ self.service_prefix, suffix },
    );
    defer self.allocator.free(subject);
    const sub = try self.client.subscribe(subject, Client.MsgHandler.init(Service, self));
    errdefer sub.deinit();

    self.mutex.lockUncancelable(self.client.io);
    defer self.mutex.unlock(self.client.io);
    try self.monitor_subs.append(self.allocator, sub);
}

fn joinSubject(
    allocator: std.mem.Allocator,
    prefix: []const u8,
    leaf: []const u8,
) ![]u8 {
    if (prefix.len == 0) return allocator.dupe(u8, leaf);
    return std.fmt.allocPrint(allocator, "{s}.{s}", .{ prefix, leaf });
}

fn generateId(allocator: std.mem.Allocator, io: std.Io) ![]u8 {
    const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    const out = try allocator.alloc(u8, 16);
    var random: [16]u8 = undefined;
    io.random(&random);
    for (out, random) |*dst, src| {
        dst.* = alphabet[@mod(src, alphabet.len)];
    }
    return out;
}

fn dupQueuePolicy(
    allocator: std.mem.Allocator,
    policy: endpoint_mod.QueuePolicy,
) !endpoint_mod.QueuePolicy {
    return switch (policy) {
        .inherit => .inherit,
        .no_queue => .no_queue,
        .queue => |q| .{ .queue = try allocator.dupe(u8, q) },
    };
}

fn freeQueuePolicy(
    allocator: std.mem.Allocator,
    policy: endpoint_mod.QueuePolicy,
) void {
    switch (policy) {
        .queue => |q| allocator.free(q),
        else => {},
    }
}

fn resolveQueuePolicy(
    allocator: std.mem.Allocator,
    endpoint_policy: endpoint_mod.QueuePolicy,
    group_policy: endpoint_mod.QueuePolicy,
    service_policy: endpoint_mod.QueuePolicy,
) !?[]u8 {
    const resolved = switch (endpoint_policy) {
        .inherit => switch (group_policy) {
            .inherit => service_policy,
            else => group_policy,
        },
        else => endpoint_policy,
    };

    return switch (resolved) {
        .inherit => try allocator.dupe(u8, "q"),
        .no_queue => null,
        .queue => |q| try allocator.dupe(u8, q),
    };
}

fn matchVerb(rest: []const u8, verb: []const u8) bool {
    if (!std.mem.startsWith(u8, rest, verb)) return false;
    return rest.len == verb.len or rest[verb.len] == '.';
}
