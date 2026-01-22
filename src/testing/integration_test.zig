//! NATS Integration Tests
//!
//! Tests against a real nats-server instance.
//! Run with: zig build test-integration

const std = @import("std");
const nats = @import("nats");

const utils = @import("test_utils.zig");
const client_tests = @import("client/tests.zig");

const ServerManager = utils.ServerManager;

const test_port = utils.test_port;
const auth_port = utils.auth_port;
const nkey_port = utils.nkey_port;
const test_token = utils.test_token;
const test_nkey_seed = utils.test_nkey_seed;
const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;

const nkey_config_path = "/tmp/nats-nkey-test.conf";

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("\n=== NATS Integration Tests ===\n\n", .{});

    var manager: ServerManager = .init(allocator);
    defer manager.deinit(allocator, io);

    std.debug.print("Starting primary server on port {d}...\n", .{test_port});
    _ = manager.startServer(allocator, io, .{ .port = test_port }) catch |err| {
        std.debug.print("Failed to start primary server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("Starting auth server on port {d}...\n", .{auth_port});
    _ = manager.startServer(allocator, io, .{
        .port = auth_port,
        .auth_token = test_token,
    }) catch |err| {
        std.debug.print("Failed to start auth server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("Starting NKey server on port {d}...\n", .{nkey_port});
    writeNKeyConfig(io) catch |err| {
        std.debug.print("Failed to write NKey config: {}\n", .{err});
        std.process.exit(1);
    };
    defer deleteNKeyConfig(io);

    _ = manager.startServer(allocator, io, .{
        .port = nkey_port,
        .config_file = nkey_config_path,
    }) catch |err| {
        std.debug.print("Failed to start NKey server: {}\n", .{err});
        std.process.exit(1);
    };

    io.sleep(.fromMilliseconds(200), .awake) catch {};

    std.debug.print("\nRunning tests...\n\n", .{});

    client_tests.runAll(allocator, &manager);

    const summary = utils.getSummary();
    std.debug.print("\n=== Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{summary.passed});
    std.debug.print("Failed: {d}\n", .{summary.failed});
    std.debug.print("Total:  {d}\n\n", .{summary.total});

    if (summary.failed > 0) {
        std.process.exit(1);
    }
}

/// Writes NKey server config file with derived public key.
fn writeNKeyConfig(io: std.Io) !void {
    const Dir = std.Io.Dir;

    var kp = nats.auth.KeyPair.fromSeed(test_nkey_seed) catch {
        return error.InvalidSeed;
    };
    defer kp.wipe();

    var pubkey_buf: [56]u8 = undefined;
    const pubkey = kp.publicKey(&pubkey_buf);

    const file = try Dir.createFile(Dir.cwd(), io, nkey_config_path, .{});
    defer file.close(io);

    var buf: [256]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.print(
        \\authorization {{
        \\  users = [{{ nkey: "{s}" }}]
        \\}}
        \\
    ,
        .{pubkey},
    );
    try writer.interface.flush();
}

/// Deletes NKey server config file.
fn deleteNKeyConfig(io: std.Io) void {
    const Dir = std.Io.Dir;
    Dir.deleteFile(Dir.cwd(), io, nkey_config_path) catch {};
}
