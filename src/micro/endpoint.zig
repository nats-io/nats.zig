const std = @import("std");
const Client = @import("../Client.zig");
const protocol = @import("protocol.zig");
const request_mod = @import("request.zig");
const stats_mod = @import("stats.zig");
const validation = @import("validation.zig");
const pubsub = @import("../pubsub.zig");

pub const QueuePolicy = union(enum) {
    inherit,
    queue: []const u8,
    no_queue,
};

pub const EndpointConfig = struct {
    subject: []const u8,
    name: ?[]const u8 = null,
    handler: request_mod.Handler,
    metadata: []const protocol.MetadataPair = &.{},
    queue_policy: QueuePolicy = .inherit,
};

pub const Endpoint = struct {
    service: *anyopaque,
    sub: *Client.Subscription,
    name: []const u8,
    subject: []const u8,
    queue_group: ?[]const u8,
    metadata: []protocol.MetadataPair,
    handler: request_mod.Handler,
    stats: stats_mod.EndpointStats = .{},
    callback: EndpointCallback = undefined,

    pub fn deinit(self: *Endpoint, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.subject);
        freeMetadata(allocator, self.metadata);
        if (self.queue_group) |q| allocator.free(q);
        allocator.destroy(self);
    }
};

pub const Group = struct {
    service: *anyopaque,
    prefix: []const u8,
    queue_policy: QueuePolicy,

    pub fn addEndpoint(self: *Group, cfg: EndpointConfig) !*Endpoint {
        const service = servicePtr(self.service);
        return service.addEndpointWithPrefix(self.prefix, self.queue_policy, cfg);
    }

    pub fn group(self: *Group, prefix: []const u8) !Group {
        const service = servicePtr(self.service);
        try validation.validateGroup(prefix);
        const full = try service.allocGroupPrefix(self.prefix, prefix);
        return .{
            .service = self.service,
            .prefix = full,
            .queue_policy = self.queue_policy,
        };
    }

    pub fn groupWithQueue(self: *Group, prefix: []const u8, queue: []const u8) !Group {
        const service = servicePtr(self.service);
        try validation.validateGroup(prefix);
        try pubsub.validateQueueGroup(queue);
        const full = try service.allocGroupPrefix(self.prefix, prefix);
        const owned_queue = try service.allocGroupQueue(queue);
        return .{
            .service = self.service,
            .prefix = full,
            .queue_policy = .{ .queue = owned_queue },
        };
    }
};

pub const EndpointCallback = struct {
    endpoint: *Endpoint,

    pub fn onMessage(self: *@This(), msg: *const Client.Message) void {
        const service = servicePtr(self.endpoint.service);

        // Belt-and-suspenders: skip dispatch once stop() has begun.
        // sub.drain() + flush already prevent new messages from reaching
        // here, but this guard ensures any racing in-flight delivery does
        // not re-enter the user handler after stop() observed in_flight==0.
        if (service.stopping.load(.acquire)) return;

        const start = std.Io.Timestamp.now(service.client.io, .awake);
        _ = service.in_flight.fetchAdd(1, .acq_rel);
        defer _ = service.in_flight.fetchSub(1, .acq_rel);

        var req = request_mod.Request{
            .client = service.client,
            .msg = msg,
        };
        self.endpoint.handler.dispatch(&req);

        const end = std.Io.Timestamp.now(service.client.io, .awake);
        const elapsed: u64 = @intCast(end.nanoseconds - start.nanoseconds);
        if (req.errored) {
            self.endpoint.stats.recordError(
                elapsed,
                req.error_code,
                req.errorDescription(),
            );
        } else {
            self.endpoint.stats.recordSuccess(elapsed);
        }
    }
};

pub fn dupMetadata(
    allocator: std.mem.Allocator,
    metadata: []const protocol.MetadataPair,
) ![]protocol.MetadataPair {
    const out = try allocator.alloc(protocol.MetadataPair, metadata.len);
    errdefer allocator.free(out);
    for (metadata, 0..) |pair, i| {
        out[i].key = try allocator.dupe(u8, pair.key);
        errdefer allocator.free(out[i].key);
        errdefer {
            for (out[0..i]) |prev| {
                allocator.free(prev.key);
                allocator.free(prev.value);
            }
        }
        out[i].value = try allocator.dupe(u8, pair.value);
    }
    return out;
}

pub fn freeMetadata(
    allocator: std.mem.Allocator,
    metadata: []const protocol.MetadataPair,
) void {
    for (metadata) |pair| {
        allocator.free(pair.key);
        allocator.free(pair.value);
    }
    if (metadata.len > 0) allocator.free(metadata);
}

fn servicePtr(ptr: *anyopaque) *@import("Service.zig").Service {
    return @ptrCast(@alignCast(ptr));
}

test "dupMetadata frees current key if value allocation fails" {
    const pairs = [_]protocol.MetadataPair{
        .{ .key = "role", .value = "primary" },
    };

    var failing = std.testing.FailingAllocator.init(
        std.testing.allocator,
        .{ .fail_index = 2 },
    );

    try std.testing.expectError(
        error.OutOfMemory,
        dupMetadata(failing.allocator(), &pairs),
    );
}
