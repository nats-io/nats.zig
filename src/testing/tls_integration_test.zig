//! Focused JWT + TLS integration tests to debug hangs around the TLS block.
//!
//! Run with: zig build test-integration-tls

const std = @import("std");
const utils = @import("test_utils.zig");
const jwt_tests = @import("client/jwt.zig");
const tls_tests = @import("client/tls.zig");

const ServerManager = utils.ServerManager;
const Dir = std.Io.Dir;
const jwt_port = utils.jwt_port;
const tls_port = utils.tls_port;
const jwt_config_file = utils.jwt_config_file;
const tls_config_file = utils.tls_config_file;

fn probeTlsPort(io: std.Io) bool {
    const address = std.Io.net.IpAddress.parse("127.0.0.1", tls_port) catch return false;
    const stream = std.Io.net.IpAddress.connect(&address, io, .{
        .mode = .stream,
        .protocol = .tcp,
    }) catch return false;
    stream.close(io);
    return true;
}

fn probeCaLoad(allocator: std.mem.Allocator, io: std.Io) !void {
    const ca_path = try Dir.realPathFileAlloc(.cwd(), io, utils.tls_ca_file, allocator);
    defer allocator.free(ca_path);

    var bundle: std.crypto.Certificate.Bundle = .empty;
    defer bundle.deinit(allocator);

    const now = std.Io.Clock.real.now(io);
    try bundle.addCertsFromFilePathAbsolute(allocator, io, now, ca_path);
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    utils.setProcessEnviron(init.minimal.environ);

    const test_io = utils.newIo(allocator);
    defer test_io.deinit();
    const io = test_io.io();

    std.debug.print("\n=== NATS JWT/TLS Integration Tests ===\n\n", .{});

    var manager: ServerManager = .init(allocator);
    defer manager.deinit(allocator, io);

    std.debug.print("Starting JWT server on port {d}...\n", .{jwt_port});
    _ = manager.startServer(allocator, io, .{
        .port = jwt_port,
        .config_file = jwt_config_file,
    }) catch |err| {
        std.debug.print("Failed to start JWT server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("Starting TLS server on port {d}...\n", .{tls_port});
    _ = manager.startServer(allocator, io, .{
        .port = tls_port,
        .config_file = tls_config_file,
    }) catch |err| {
        std.debug.print("Failed to start TLS server: {}\n", .{err});
        std.process.exit(1);
    };

    io.sleep(.fromMilliseconds(200), .awake) catch {};

    std.debug.print("TLS port probe before tests: {s}\n", .{
        if (probeTlsPort(io)) "ok" else "failed",
    });

    std.debug.print("\nRunning JWT tests...\n\n", .{});
    jwt_tests.runAll(allocator);

    std.debug.print("\nRunning TLS tests...\n\n", .{});

    std.debug.print("[RUN ] tls_insecure_skip_verify\n", .{});
    tls_tests.testTlsInsecureSkipVerify(allocator);

    std.debug.print("[RUN ] ca_bundle_load\n", .{});
    probeCaLoad(allocator, io) catch |err| {
        std.debug.print("[FAIL] ca_bundle_load: {}\n", .{err});
        std.process.exit(1);
    };
    std.debug.print("[PASS] ca_bundle_load\n", .{});

    std.debug.print("[RUN ] tls_connection\n", .{});
    tls_tests.testTlsConnection(allocator);

    std.debug.print("[RUN ] tls_pubsub\n", .{});
    tls_tests.testTlsPubSub(allocator);

    std.debug.print("[RUN ] tls_server_info\n", .{});
    tls_tests.testTlsServerInfo(allocator);

    std.debug.print("[RUN ] tls_multiple_msgs\n", .{});
    tls_tests.testTlsMultipleMessages(allocator);

    std.debug.print("[RUN ] tls_scheme_rejects_plain_server\n", .{});
    tls_tests.testTlsSchemeRejectsPlainServer(allocator);

    std.debug.print("[RUN ] tls_reconnect\n", .{});
    tls_tests.testTlsReconnect(allocator, &manager);

    const summary = utils.getSummary();
    std.debug.print("\n=== JWT/TLS Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{summary.passed});
    std.debug.print("Failed: {d}\n", .{summary.failed});
    std.debug.print("Total:  {d}\n\n", .{summary.total});

    if (summary.failed > 0) std.process.exit(1);
}
