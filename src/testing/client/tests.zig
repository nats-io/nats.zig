//! Client Test Suite
//!
//! Re-exports all synchronous client test modules.

const std = @import("std");
const utils = @import("../test_utils.zig");
const ServerManager = utils.ServerManager;

pub const connection = @import("connection.zig");
pub const publish = @import("publish.zig");
pub const subscribe = @import("subscribe.zig");
pub const request_reply = @import("request_reply.zig");
pub const wildcard = @import("wildcard.zig");
pub const queue = @import("queue.zig");
pub const auth = @import("auth.zig");
pub const stats = @import("stats.zig");
pub const stress = @import("stress.zig");
pub const multi_client = @import("multi_client.zig");
pub const edge_cases = @import("edge_cases.zig");
pub const server = @import("server.zig");
pub const drain = @import("drain.zig");
pub const async_helpers = @import("async_helpers.zig");

/// Runs all client tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    connection.runAll(allocator, manager);
    publish.runAll(allocator);
    subscribe.runAll(allocator);
    request_reply.runAll(allocator);
    wildcard.runAll(allocator);
    queue.runAll(allocator);
    auth.runAll(allocator);
    stats.runAll(allocator);
    stress.runAll(allocator);
    multi_client.runAll(allocator);
    edge_cases.runAll(allocator);
    server.runAll(allocator);
    drain.runAll(allocator);
    async_helpers.runAll(allocator);
}
