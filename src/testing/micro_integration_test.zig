//! Focused microservices integration tests.
//!
//! Run with: zig build test-integration-micro

const std = @import("std");
const utils = @import("test_utils.zig");
const micro_tests = @import("client/micro.zig");

const ServerManager = utils.ServerManager;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    utils.setProcessEnviron(init.minimal.environ);

    const test_io = utils.newIo(allocator);
    defer test_io.deinit();
    const io = test_io.io();

    std.debug.print("\n=== NATS Micro Integration Tests ===\n\n", .{});

    var manager: ServerManager = .init(allocator);
    defer manager.deinit(allocator, io);

    std.debug.print("\nRunning micro tests...\n\n", .{});
    micro_tests.runAll(allocator, &manager);

    const summary = utils.getSummary();
    std.debug.print("\n=== Micro Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{summary.passed});
    std.debug.print("Failed: {d}\n", .{summary.failed});
    std.debug.print("Total:  {d}\n\n", .{summary.total});

    if (summary.failed > 0) std.process.exit(1);
}
