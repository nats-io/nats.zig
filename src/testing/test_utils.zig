//! Shared test utilities for NATS integration tests.

const std = @import("std");
pub const nats = @import("nats");
pub const server_manager = @import("server_manager.zig");

pub const ServerManager = server_manager.ServerManager;
pub const ServerConfig = server_manager.ServerConfig;

pub const test_port: u16 = 14222;
pub const auth_port: u16 = 14223;
pub const nkey_port: u16 = 14224;
pub const jwt_port: u16 = 14225;
pub const test_token = "test-secret-token";
pub const test_nkey_seed =
    "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
pub const test_nkey_seed_file = "/tmp/nats-test-nkey.seed";
pub const jwt_config_file = "src/testing/configs/jwt.conf";
pub const test_creds_file = "src/testing/configs/TestUser.creds";
pub const test_jwt_seed =
    "SUACH75SWCM5D2JMJM6EKLR2WDARVGZT4QC6LX3AGHSWOMVAKERABBBRWM";

// Dynamic JWT test constants
pub const dynamic_jwt_port: u16 = 14228;
pub const jetstream_port: u16 = 14229;
pub const micro_port: u16 = 14241;

// TLS test constants
pub const tls_port: u16 = 14226;
pub const tls_config_file = "src/testing/configs/tls.conf";
pub const tls_ca_file = "src/testing/certs/rootCA.pem";
pub const tls_server_cert = "src/testing/certs/server-cert.pem";
pub const tls_server_key = "src/testing/certs/server-key.pem";
pub const tls_client_cert = "src/testing/certs/client-cert.pem";
pub const tls_client_key = "src/testing/certs/client-key.pem";

pub var tests_passed: u32 = 0;
pub var tests_failed: u32 = 0;

/// Reports a test result and updates counters.
pub fn reportResult(name: []const u8, passed: bool, details: []const u8) void {
    if (passed) {
        tests_passed += 1;
        std.debug.print("[PASS] {s}\n", .{name});
    } else {
        tests_failed += 1;
        std.debug.print("[FAIL] {s}: {s}\n", .{ name, details });
    }
}

/// Reports a failed test step with the Zig error name included.
pub fn reportError(name: []const u8, step: []const u8, err: anyerror) void {
    var buf: [128]u8 = undefined;
    const details = std.fmt.bufPrint(
        &buf,
        "{s}: {s}",
        .{ step, @errorName(err) },
    ) catch step;
    reportResult(name, false, details);
}

/// Formats a NATS URL for the given port.
pub fn formatUrl(buf: []u8, port: u16) []const u8 {
    const fmt = "nats://127.0.0.1:{d}";
    return std.fmt.bufPrint(buf, fmt, .{port}) catch "invalid";
}

/// Formats a NATS URL with auth token.
pub fn formatAuthUrl(buf: []u8, port: u16, token: []const u8) []const u8 {
    return std.fmt.bufPrint(
        buf,
        "nats://{s}@127.0.0.1:{d}",
        .{ token, port },
    ) catch "invalid";
}

/// Formats a TLS NATS URL for the given port.
/// Uses localhost since test certificates are issued for localhost.
pub fn formatTlsUrl(buf: []u8, port: u16) []const u8 {
    const fmt = "tls://localhost:{d}";
    return std.fmt.bufPrint(buf, fmt, .{port}) catch "invalid";
}

pub fn resetCounters() void {
    tests_passed = 0;
    tests_failed = 0;
}

pub fn getSummary() struct { passed: u32, failed: u32, total: u32 } {
    return .{
        .passed = tests_passed,
        .failed = tests_failed,
        .total = tests_passed + tests_failed,
    };
}

const io_backend = @import("io_backend");

var process_environ: std.process.Environ = .empty;

/// Sets the environment used by test Io backends. Integration test entry
/// points call this once from their `std.process.Init` so child-process
/// lookups, such as `nats-server`, see the same PATH as the test runner.
pub fn setProcessEnviron(environ: std.process.Environ) void {
    process_environ = environ;
}

/// Heap-allocated wrapper around `io_backend.Backend` for use by
/// integration tests. Each `newIo()` call returns a fresh
/// `*TestIo` that owns its backend; calling `deinit()` releases
/// both the backend and the wrapper itself.
///
/// Existing test code that does:
///
///     var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
///     defer io.deinit();
///
/// becomes:
///
///     const io = utils.newIo(allocator);
///     defer io.deinit();
///
/// All `io.io()` and `io.deinit()` calls work unchanged through
/// the pointer because Zig auto-dereferences method calls when
/// the receiver matches.
pub const TestIo = struct {
    backend: io_backend.Backend,
    allocator: std.mem.Allocator,

    /// Tears down the backend and frees the wrapper. Must be
    /// called once per `newIo()` call (typically via `defer`).
    pub fn deinit(self: *TestIo) void {
        self.backend.deinit();
        self.allocator.destroy(self);
    }

    /// Returns the abstract `std.Io` for passing to client APIs.
    pub fn io(self: *TestIo) std.Io {
        return self.backend.io();
    }
};

/// Allocates and initializes a `TestIo` wrapper. Panics on
/// allocation or backend init failure — acceptable for tests
/// because every existing test function returns `void`, not
/// `!void`, and propagating an errorable here would force a
/// viral signature change across the entire suite.
pub fn newIo(allocator: std.mem.Allocator) *TestIo {
    const t = allocator.create(TestIo) catch
        @panic("OOM in test newIo");
    t.allocator = allocator;
    io_backend.initWithEnviron(&t.backend, allocator, process_environ) catch
        @panic("io_backend init failed in test newIo");
    return t;
}
