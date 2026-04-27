const std = @import("std");

pub const Type = struct {
    pub const ping = "io.nats.micro.v1.ping_response";
    pub const info = "io.nats.micro.v1.info_response";
    pub const stats = "io.nats.micro.v1.stats_response";
};

pub const MetadataPair = struct {
    key: []const u8,
    value: []const u8,
};

pub const Error = struct {
    code: u16,
    description: []const u8,
};

pub const Ping = struct {
    type: []const u8 = Type.ping,
    name: []const u8,
    id: []const u8,
    version: []const u8,
    metadata: ?[]const MetadataPair = null,
};

pub const EndpointInfo = struct {
    name: []const u8,
    subject: []const u8,
    queue_group: ?[]const u8 = null,
    metadata: ?[]const MetadataPair = null,
};

pub const Info = struct {
    type: []const u8 = Type.info,
    name: []const u8,
    id: []const u8,
    version: []const u8,
    description: ?[]const u8 = null,
    metadata: ?[]const MetadataPair = null,
    endpoints: []const EndpointInfo = &.{},

    pub fn deinit(self: *Info, allocator: std.mem.Allocator) void {
        if (self.endpoints.len > 0) allocator.free(self.endpoints);
        self.endpoints = &.{};
    }
};

pub const EndpointStatsJson = struct {
    name: []const u8,
    subject: []const u8,
    queue_group: ?[]const u8 = null,
    metadata: ?[]const MetadataPair = null,
    num_requests: u64,
    num_errors: u64,
    last_error: ?Error = null,
    processing_time: u64,
    average_processing_time: u64,
};

pub const StatsResponse = struct {
    type: []const u8 = Type.stats,
    name: []const u8,
    id: []const u8,
    version: []const u8,
    started: []const u8,
    metadata: ?[]const MetadataPair = null,
    endpoints: []const EndpointStatsJson = &.{},

    pub fn deinit(self: *StatsResponse, allocator: std.mem.Allocator) void {
        if (self.endpoints.len > 0) allocator.free(self.endpoints);
        self.endpoints = &.{};
    }
};
