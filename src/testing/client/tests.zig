//! Async Client Test Suite
//!
//! Re-exports all async client test modules.

const std = @import("std");
const utils = @import("../test_utils.zig");
const ServerManager = utils.ServerManager;

pub const basic = @import("basic.zig");
pub const publish = @import("publish.zig");
pub const subscribe = @import("subscribe.zig");
pub const multi_client = @import("multi_client.zig");
pub const stats = @import("stats.zig");
pub const stress = @import("stress.zig");
pub const auth = @import("auth.zig");
pub const connection = @import("connection.zig");
pub const request_reply = @import("request_reply.zig");
pub const drain = @import("drain.zig");
pub const edge_cases = @import("edge_cases.zig");
pub const wildcard = @import("wildcard.zig");
pub const queue = @import("queue.zig");
pub const server = @import("server.zig");
pub const protocol = @import("protocol.zig");
pub const concurrency = @import("concurrency.zig");
pub const reconnect = @import("reconnect.zig");

/// Runs all async client tests.
pub fn runAll(allocator: std.mem.Allocator, manager: *ServerManager) void {
    basic.runAll(allocator);
    publish.runAll(allocator);
    subscribe.runAll(allocator);
    multi_client.runAll(allocator);
    stats.runAll(allocator);
    stress.runAll(allocator);
    auth.runAll(allocator);
    connection.runAll(allocator, manager);
    request_reply.runAll(allocator);
    drain.runAll(allocator);
    edge_cases.runAll(allocator);
    wildcard.runAll(allocator);
    queue.runAll(allocator);
    server.runAll(allocator);
    protocol.runAll(allocator);
    concurrency.runAll(allocator);
    reconnect.runAll(allocator, manager);
}
