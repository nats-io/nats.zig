//! NATS Integration Tests
//!
//! Tests against a real nats-server instance.
//! Run with: zig build test-integration

const std = @import("std");
const nats = @import("nats");

// Import shared test utilities
const utils = @import("test_utils.zig");
const client_tests = @import("client_tests.zig");
const client_async_tests = @import("client_async_tests.zig");

const ServerManager = utils.ServerManager;

// Re-export from utils for use in this file
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create I/O system for server process management
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("\n=== NATS Integration Tests ===\n\n", .{});

    // Start server manager
    var manager = ServerManager.init(allocator);
    defer manager.deinit(allocator, io);

    // Start primary test server
    std.debug.print("Starting primary server on port {d}...\n", .{test_port});
    _ = manager.startServer(allocator, io, .{ .port = test_port }) catch |err| {
        std.debug.print("Failed to start primary server: {}\n", .{err});
        std.process.exit(1);
    };

    // Start auth test server
    std.debug.print("Starting auth server on port {d}...\n", .{auth_port});
    _ = manager.startServer(allocator, io, .{
        .port = auth_port,
        .auth_token = test_token,
    }) catch |err| {
        std.debug.print("Failed to start auth server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("\nRunning tests...\n\n", .{});

    client_tests.runAll(allocator, &manager);
    client_async_tests.runAll(allocator, &manager);

    // Print summary
    const summary = utils.getSummary();
    std.debug.print("\n=== Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{summary.passed});
    std.debug.print("Failed: {d}\n", .{summary.failed});
    std.debug.print("Total:  {d}\n\n", .{summary.total});

    if (summary.failed > 0) {
        std.process.exit(1);
    }
}
