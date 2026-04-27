const std = @import("std");

pub const Service = @import("micro/Service.zig");
pub const Config = Service.Config;
pub const Error = Service.Error;
pub const addService = Service.addService;

const request_mod = @import("micro/request.zig");
pub const Request = request_mod.Request;
pub const Handler = request_mod.Handler;
pub const HandlerFn = request_mod.HandlerFn;

pub const endpoint = @import("micro/endpoint.zig");
pub const Endpoint = endpoint.Endpoint;
pub const EndpointConfig = endpoint.EndpointConfig;
pub const Group = endpoint.Group;
pub const QueuePolicy = endpoint.QueuePolicy;

pub const protocol = @import("micro/protocol.zig");
pub const Info = protocol.Info;
pub const Ping = protocol.Ping;
pub const StatsResponse = protocol.StatsResponse;

test {
    std.testing.refAllDecls(@This());
}
