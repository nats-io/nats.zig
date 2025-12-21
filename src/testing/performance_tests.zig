//! NATS Performance Test Orchestrator
//!
//! Runs publisher/subscriber benchmark combinations to compare performance.
//! Parses actual benchmark output for accurate timing (not process timing).
//!
//! Usage: zig build run-perf-test
//!        zig build run-perf-test -- --msgs=100000 --size=128

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const server_manager = @import("server_manager.zig");
const ServerManager = server_manager.ServerManager;

const TestConfig = struct {
    msgs: u64 = 100_000,
    size: usize = 16,
    port: u16 = 14333,
    subject: []const u8 = "perf.test",
};

const TestResult = struct {
    name: []const u8,
    pub_msgs_per_sec: ?f64 = null,
    sub_msgs_per_sec: ?f64 = null,
    pub_kib_per_sec: ?f64 = null,
    sub_kib_per_sec: ?f64 = null,
    avg_latency_us: ?f64 = null,
};

/// Parsed stats from benchmark output.
const BenchStats = struct {
    msgs_per_sec: f64,
    kib_per_sec: f64,
    latency_us: f64,
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = try parseArgs(allocator);

    std.debug.print("\n=== NATS Performance Tests ===\n", .{});
    std.debug.print("Configuration: msgs={d}, size={d}B\n\n", .{
        config.msgs,
        config.size,
    });

    // Start server
    var manager = ServerManager.init(allocator);
    defer manager.deinit(allocator);

    std.debug.print("Starting NATS server on port {d}...\n", .{config.port});
    _ = manager.startServer(allocator, .{ .port = config.port }) catch |err| {
        std.debug.print("Failed to start server: {}\n", .{err});
        return err;
    };
    std.debug.print("Server started.\n\n", .{});

    var results: std.ArrayListUnmanaged(TestResult) = .{};
    defer results.deinit(allocator);

    // Test 1: z -> z (Zig publisher -> Zig subscriber)
    std.debug.print("--- Test 1: z -> z ---\n", .{});
    if (runZigToZig(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Test 2: z -> n (Zig publisher -> nats subscriber)
    std.debug.print("\n--- Test 2: z -> n ---\n", .{});
    if (runZigToNats(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Test 3: n -> z (nats publisher -> Zig subscriber)
    std.debug.print("\n--- Test 3: n -> z ---\n", .{});
    if (runNatsToZig(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Test 4: n -> n (nats bench baseline)
    std.debug.print("\n--- Test 4: n -> n ---\n", .{});
    if (runNatsBench(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Test 5: z -> (Zig publisher only)
    std.debug.print("\n--- Test 5: z -> ---\n", .{});
    if (runZigPub(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Test 6: n -> (nats publisher only)
    std.debug.print("\n--- Test 6: n -> ---\n", .{});
    if (runNatsPub(allocator, config)) |result| {
        try results.append(allocator, result);
        printResult(result);
    } else |err| {
        std.debug.print("Test failed: {}\n", .{err});
    }

    // Print summary
    std.debug.print("\n=== Summary ===\n", .{});
    std.debug.print(
        "{s:<20} {s:>10} {s:>10} {s:>10} {s:>10} {s:>10}\n",
        .{ "Test", "pub msg/s", "sub msg/s", "pub KiB/s", "sub KiB/s", "latency" },
    );
    std.debug.print(
        "{s:-<20} {s:->10} {s:->10} {s:->10} {s:->10} {s:->10}\n",
        .{ "", "", "", "", "", "" },
    );
    for (results.items) |r| {
        var pub_msg_buf: [12]u8 = undefined;
        var sub_msg_buf: [12]u8 = undefined;
        var pub_kib_buf: [12]u8 = undefined;
        var sub_kib_buf: [12]u8 = undefined;
        var lat_buf: [12]u8 = undefined;

        const pub_msg = fmtOptional(&pub_msg_buf, r.pub_msgs_per_sec);
        const sub_msg = fmtOptional(&sub_msg_buf, r.sub_msgs_per_sec);
        const pub_kib = fmtOptional(&pub_kib_buf, r.pub_kib_per_sec);
        const sub_kib = fmtOptional(&sub_kib_buf, r.sub_kib_per_sec);
        const lat = fmtLatency(&lat_buf, r.avg_latency_us);

        std.debug.print(
            "{s:<20} {s:>10} {s:>10} {s:>10} {s:>10} {s:>10}\n",
            .{ r.name, pub_msg, sub_msg, pub_kib, sub_kib, lat },
        );
    }
    std.debug.print("\n", .{});
}

/// Format optional f64 as string, "-" if null.
fn fmtOptional(buf: []u8, val: ?f64) []const u8 {
    if (val) |v| {
        return std.fmt.bufPrint(buf, "{d:.0}", .{v}) catch "-";
    }
    return "-";
}

/// Format latency with "us" suffix, "-" if null.
fn fmtLatency(buf: []u8, val: ?f64) []const u8 {
    if (val) |v| {
        return std.fmt.bufPrint(buf, "{d:.2}us", .{v}) catch "-";
    }
    return "-";
}

fn parseArgs(allocator: Allocator) !TestConfig {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // skip program name

    var config = TestConfig{};

    while (args.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--msgs=")) {
            const val = arg[7..];
            config.msgs = std.fmt.parseInt(u64, val, 10) catch {
                return error.InvalidArgument;
            };
        } else if (std.mem.startsWith(u8, arg, "--size=")) {
            const val = arg[7..];
            config.size = parseSizeArg(val) catch {
                return error.InvalidArgument;
            };
        }
    }

    return config;
}

fn parseSizeArg(val: []const u8) !usize {
    assert(val.len > 0);

    var num_end: usize = 0;
    for (val, 0..) |c, i| {
        if (c >= '0' and c <= '9') {
            num_end = i + 1;
        } else {
            break;
        }
    }

    if (num_end == 0) return error.InvalidSize;

    const num = std.fmt.parseInt(usize, val[0..num_end], 10) catch {
        return error.InvalidSize;
    };

    const suffix = val[num_end..];
    if (suffix.len == 0 or std.mem.eql(u8, suffix, "B")) {
        return num;
    } else if (std.mem.eql(u8, suffix, "K") or std.mem.eql(u8, suffix, "KB")) {
        return num * 1024;
    } else if (std.mem.eql(u8, suffix, "M") or std.mem.eql(u8, suffix, "MB")) {
        return num * 1024 * 1024;
    }

    return error.InvalidSize;
}

fn printResult(result: TestResult) void {
    var pub_msg_buf: [16]u8 = undefined;
    var sub_msg_buf: [16]u8 = undefined;
    var pub_kib_buf: [16]u8 = undefined;
    var sub_kib_buf: [16]u8 = undefined;
    var lat_buf: [16]u8 = undefined;

    const pub_msg = fmtOptional(&pub_msg_buf, result.pub_msgs_per_sec);
    const sub_msg = fmtOptional(&sub_msg_buf, result.sub_msgs_per_sec);
    const pub_kib = fmtOptional(&pub_kib_buf, result.pub_kib_per_sec);
    const sub_kib = fmtOptional(&sub_kib_buf, result.sub_kib_per_sec);
    const lat = fmtLatency(&lat_buf, result.avg_latency_us);

    std.debug.print(
        "  pub: {s} msg/s, {s} KiB/s | sub: {s} msg/s, {s} KiB/s | {s}\n",
        .{ pub_msg, pub_kib, sub_msg, sub_kib, lat },
    );
}

/// Parses benchmark output line:
/// "NATS publisher stats: 123456 msgs/sec ~ 1234 KiB/sec ~ 0.35us"
/// or
/// "NATS subscriber stats: 123456 msgs/sec ~ 1234 KiB/sec ~ 0.35us"
fn parseStatsLine(output: []const u8) ?BenchStats {
    // Find the stats line
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "stats:")) |_| {
            return parseStatsParts(line);
        }
    }
    return null;
}

fn parseStatsParts(line: []const u8) ?BenchStats {
    // Format: "NATS xxx stats: N msgs/sec ~ N KiB/sec ~ Nus"
    const stats_start = std.mem.indexOf(u8, line, "stats:") orelse return null;
    const data = line[stats_start + 6 ..];

    // Parse msgs/sec
    const msgs_end = std.mem.indexOf(u8, data, " msgs/sec") orelse return null;
    const msgs_str = std.mem.trim(u8, data[0..msgs_end], " ");
    const msgs_per_sec = std.fmt.parseFloat(f64, msgs_str) catch return null;

    // Find and parse KiB/sec
    const kib_marker = std.mem.indexOf(u8, data, "~ ") orelse return null;
    const after_first_tilde = data[kib_marker + 2 ..];
    const kib_end = std.mem.indexOf(u8, after_first_tilde, " KiB/sec") orelse
        return null;
    const kib_str = std.mem.trim(u8, after_first_tilde[0..kib_end], " ");
    const kib_per_sec = std.fmt.parseFloat(f64, kib_str) catch return null;

    // Find and parse latency
    const second_tilde = std.mem.indexOf(u8, after_first_tilde, "~ ") orelse
        return null;
    const after_second_tilde = after_first_tilde[second_tilde + 2 ..];
    const us_end = std.mem.indexOf(u8, after_second_tilde, "us") orelse
        return null;
    const latency_str = std.mem.trim(u8, after_second_tilde[0..us_end], " ");
    const latency_us = std.fmt.parseFloat(f64, latency_str) catch return null;

    return .{
        .msgs_per_sec = msgs_per_sec,
        .kib_per_sec = kib_per_sec,
        .latency_us = latency_us,
    };
}

/// Run Zig pub -> Zig sub, parse both outputs for stats.
fn runZigToZig(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "--msgs={d}", .{
        config.msgs,
    }) catch unreachable;

    var size_buf: [32]u8 = undefined;
    const size_arg = std.fmt.bufPrint(&size_buf, "--size={d}", .{
        config.size,
    }) catch unreachable;

    var url_arg_buf: [80]u8 = undefined;
    const url_arg = std.fmt.bufPrint(&url_arg_buf, "--url={s}", .{
        url,
    }) catch unreachable;

    // Start subscriber with stderr capture (--no-progress for clean output)
    var sub = std.process.Child.init(&.{
        "./zig-out/bin/bench-sub",
        config.subject,
        msgs_arg,
        url_arg,
        "--no-progress",
    }, allocator);
    sub.stderr_behavior = .Pipe;
    sub.stdout_behavior = .Inherit;
    try sub.spawn();

    // Give subscriber time to connect and subscribe
    std.posix.nanosleep(0, 500_000_000); // 500ms

    // Start publisher with stderr capture
    var pub_proc = std.process.Child.init(&.{
        "./zig-out/bin/bench-pub",
        config.subject,
        msgs_arg,
        size_arg,
        url_arg,
    }, allocator);
    pub_proc.stderr_behavior = .Pipe;
    pub_proc.stdout_behavior = .Inherit;
    try pub_proc.spawn();

    // Read publisher output and wait
    var pub_buf: [4096]u8 = undefined;
    const pub_output = readPipeWithTimeout(
        pub_proc.stderr,
        &pub_buf,
        10_000_000_000,
    );
    _ = pub_proc.wait() catch {};

    // Read subscriber output and wait
    var sub_buf: [4096]u8 = undefined;
    const sub_output = readPipeWithTimeout(sub.stderr, &sub_buf, 30_000_000_000);
    _ = sub.wait() catch {};

    // Parse both outputs
    const pub_stats = parseStatsLine(pub_output);
    const sub_stats = parseStatsLine(sub_output);

    return .{
        .name = "z -> z",
        .pub_msgs_per_sec = if (pub_stats) |s| s.msgs_per_sec else null,
        .sub_msgs_per_sec = if (sub_stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (pub_stats) |s| s.kib_per_sec else null,
        .sub_kib_per_sec = if (sub_stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (sub_stats) |s| s.latency_us else null,
    };
}

/// Run Zig pub -> nats sub, parse both outputs.
fn runZigToNats(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "--msgs={d}", .{
        config.msgs,
    }) catch unreachable;

    var size_buf: [32]u8 = undefined;
    const size_arg = std.fmt.bufPrint(&size_buf, "--size={d}", .{
        config.size,
    }) catch unreachable;

    var url_arg_buf: [80]u8 = undefined;
    const url_arg = std.fmt.bufPrint(&url_arg_buf, "--url={s}", .{
        url,
    }) catch unreachable;

    var nats_server_buf: [80]u8 = undefined;
    const nats_server_arg = std.fmt.bufPrint(
        &nats_server_buf,
        "--server={s}",
        .{url},
    ) catch unreachable;

    var nats_msgs_buf: [32]u8 = undefined;
    const nats_msgs_arg = std.fmt.bufPrint(&nats_msgs_buf, "{d}", .{
        config.msgs,
    }) catch unreachable;

    // Start nats bench sub first (capture stdout for stats)
    var sub = std.process.Child.init(&.{
        "nats",
        "bench",
        "sub",
        nats_server_arg,
        "--msgs",
        nats_msgs_arg,
        "--no-progress",
        config.subject,
    }, allocator);
    sub.stderr_behavior = .Inherit;
    sub.stdout_behavior = .Pipe;
    try sub.spawn();

    // Wait for subscriber to connect
    std.posix.nanosleep(0, 500_000_000); // 500ms

    // Start Zig publisher with stderr capture
    var pub_proc = std.process.Child.init(&.{
        "./zig-out/bin/bench-pub",
        config.subject,
        msgs_arg,
        size_arg,
        url_arg,
    }, allocator);
    pub_proc.stderr_behavior = .Pipe;
    pub_proc.stdout_behavior = .Inherit;
    try pub_proc.spawn();

    // Read publisher output and wait
    var pub_buf: [4096]u8 = undefined;
    const pub_output = readPipeWithTimeout(pub_proc.stderr, &pub_buf, 10_000_000_000);
    _ = pub_proc.wait() catch {};

    // Read subscriber output and wait
    var sub_buf: [4096]u8 = undefined;
    const sub_output = readPipeWithTimeout(sub.stdout, &sub_buf, 30_000_000_000);
    _ = sub.wait() catch {};

    // Parse both outputs
    const pub_stats = parseStatsLine(pub_output);
    const sub_stats = parseNatsBenchOutput(sub_output);

    return .{
        .name = "z -> n",
        .pub_msgs_per_sec = if (pub_stats) |s| s.msgs_per_sec else null,
        .sub_msgs_per_sec = if (sub_stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (pub_stats) |s| s.kib_per_sec else null,
        .sub_kib_per_sec = if (sub_stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (sub_stats) |s| s.latency_us else null,
    };
}

/// Read from pipe with timeout, return slice of data read.
fn readPipeWithTimeout(
    pipe: ?std.fs.File,
    buf: []u8,
    timeout_ns: u64,
) []const u8 {
    const file = pipe orelse return "";
    var total: usize = 0;
    const start = std.time.Instant.now() catch return "";

    while (total < buf.len) {
        const n = file.read(buf[total..]) catch break;
        if (n == 0) {
            const now = std.time.Instant.now() catch break;
            if (now.since(start) > timeout_ns) break;
            // Check if we have stats line
            if (std.mem.indexOf(u8, buf[0..total], "stats:") != null) break;
            std.posix.nanosleep(0, 10_000_000); // 10ms
            continue;
        }
        total += n;
        if (std.mem.indexOf(u8, buf[0..total], "stats:") != null) break;
    }
    return buf[0..total];
}

/// Run Zig publisher standalone, parse output for stats.
fn runZigPub(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "--msgs={d}", .{
        config.msgs,
    }) catch unreachable;

    var size_buf: [32]u8 = undefined;
    const size_arg = std.fmt.bufPrint(&size_buf, "--size={d}", .{
        config.size,
    }) catch unreachable;

    var url_arg_buf: [80]u8 = undefined;
    const url_arg = std.fmt.bufPrint(&url_arg_buf, "--url={s}", .{
        url,
    }) catch unreachable;

    // Run publisher with stderr capture
    var pub_proc = std.process.Child.init(&.{
        "./zig-out/bin/bench-pub",
        config.subject,
        msgs_arg,
        size_arg,
        url_arg,
    }, allocator);
    pub_proc.stderr_behavior = .Pipe;
    pub_proc.stdout_behavior = .Inherit;
    try pub_proc.spawn();

    // Read output and wait
    var buf: [4096]u8 = undefined;
    const output = readPipeWithTimeout(pub_proc.stderr, &buf, 10_000_000_000);
    _ = pub_proc.wait() catch {};

    // Parse output
    const stats = parseStatsLine(output);

    return .{
        .name = "z ->",
        .pub_msgs_per_sec = if (stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (stats) |s| s.latency_us else null,
    };
}

/// Run nats bench pub standalone, parse output for stats.
fn runNatsPub(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var nats_server_buf: [80]u8 = undefined;
    const nats_server_arg = std.fmt.bufPrint(
        &nats_server_buf,
        "--server={s}",
        .{url},
    ) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "{d}", .{
        config.msgs,
    }) catch unreachable;

    var size_buf: [32]u8 = undefined;
    const size_arg = std.fmt.bufPrint(&size_buf, "{d}B", .{
        config.size,
    }) catch unreachable;

    // Run nats bench pub with stdout capture
    var pub_proc = std.process.Child.init(&.{
        "nats",
        "bench",
        "pub",
        nats_server_arg,
        "--msgs",
        msgs_arg,
        "--size",
        size_arg,
        "--no-progress",
        config.subject,
    }, allocator);
    pub_proc.stderr_behavior = .Inherit;
    pub_proc.stdout_behavior = .Pipe;
    try pub_proc.spawn();

    // Read output and wait
    var buf: [4096]u8 = undefined;
    const output = readPipeWithTimeout(pub_proc.stdout, &buf, 10_000_000_000);
    _ = pub_proc.wait() catch {};

    // Parse output
    const stats = parseNatsBenchOutput(output);

    return .{
        .name = "n ->",
        .pub_msgs_per_sec = if (stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (stats) |s| s.latency_us else null,
    };
}

/// Run nats bench pub -> Zig sub, parse both outputs.
fn runNatsToZig(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "--msgs={d}", .{
        config.msgs,
    }) catch unreachable;

    var url_arg_buf: [80]u8 = undefined;
    const url_arg = std.fmt.bufPrint(&url_arg_buf, "--url={s}", .{
        url,
    }) catch unreachable;

    var nats_server_buf: [80]u8 = undefined;
    const nats_server_arg = std.fmt.bufPrint(
        &nats_server_buf,
        "--server={s}",
        .{url},
    ) catch unreachable;

    var nats_msgs_buf: [32]u8 = undefined;
    const nats_msgs_arg = std.fmt.bufPrint(&nats_msgs_buf, "{d}", .{
        config.msgs,
    }) catch unreachable;

    var nats_size_buf: [32]u8 = undefined;
    const nats_size_arg = std.fmt.bufPrint(&nats_size_buf, "{d}B", .{
        config.size,
    }) catch unreachable;

    // Start Zig subscriber with stderr capture
    var sub = std.process.Child.init(&.{
        "./zig-out/bin/bench-sub",
        config.subject,
        msgs_arg,
        url_arg,
        "--no-progress",
    }, allocator);
    sub.stderr_behavior = .Pipe;
    sub.stdout_behavior = .Inherit;
    try sub.spawn();

    std.posix.nanosleep(0, 500_000_000); // 500ms for subscribe

    // Start nats bench pub (outputs stats to stdout)
    var pub_proc = std.process.Child.init(&.{
        "nats",
        "bench",
        "pub",
        nats_server_arg,
        "--msgs",
        nats_msgs_arg,
        "--size",
        nats_size_arg,
        "--no-progress",
        config.subject,
    }, allocator);
    pub_proc.stderr_behavior = .Inherit;
    pub_proc.stdout_behavior = .Pipe;
    try pub_proc.spawn();

    // Read nats bench pub output and wait
    var pub_buf: [4096]u8 = undefined;
    const pub_output = readPipeWithTimeout(
        pub_proc.stdout,
        &pub_buf,
        10_000_000_000,
    );
    _ = pub_proc.wait() catch {};

    // Read Zig sub output and wait
    var sub_buf: [4096]u8 = undefined;
    const sub_output = readPipeWithTimeout(sub.stderr, &sub_buf, 30_000_000_000);
    _ = sub.wait() catch {};

    // Parse both outputs
    const pub_stats = parseNatsBenchOutput(pub_output);
    const sub_stats = parseStatsLine(sub_output);

    return .{
        .name = "n -> z",
        .pub_msgs_per_sec = if (pub_stats) |s| s.msgs_per_sec else null,
        .sub_msgs_per_sec = if (sub_stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (pub_stats) |s| s.kib_per_sec else null,
        .sub_kib_per_sec = if (sub_stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (sub_stats) |s| s.latency_us else null,
    };
}

/// Run nats bench pub + sub as baseline, parse both outputs.
fn runNatsBench(allocator: Allocator, config: TestConfig) !TestResult {
    var url_buf: [64]u8 = undefined;
    const url = std.fmt.bufPrint(&url_buf, "nats://127.0.0.1:{d}", .{
        config.port,
    }) catch unreachable;

    var nats_server_buf: [80]u8 = undefined;
    const nats_server_arg = std.fmt.bufPrint(
        &nats_server_buf,
        "--server={s}",
        .{url},
    ) catch unreachable;

    var msgs_buf: [32]u8 = undefined;
    const msgs_arg = std.fmt.bufPrint(&msgs_buf, "{d}", .{config.msgs}) catch
        unreachable;

    var size_buf: [32]u8 = undefined;
    const size_arg = std.fmt.bufPrint(&size_buf, "{d}B", .{config.size}) catch
        unreachable;

    // Start nats bench sub (capture stdout for stats)
    var sub = std.process.Child.init(&.{
        "nats",
        "bench",
        "sub",
        nats_server_arg,
        "--msgs",
        msgs_arg,
        "--no-progress",
        config.subject,
    }, allocator);
    sub.stderr_behavior = .Inherit;
    sub.stdout_behavior = .Pipe;
    try sub.spawn();

    // Wait for subscriber to connect
    std.posix.nanosleep(0, 500_000_000); // 500ms

    // Start nats bench pub (capture stdout for stats)
    var pub_proc = std.process.Child.init(&.{
        "nats",
        "bench",
        "pub",
        nats_server_arg,
        "--msgs",
        msgs_arg,
        "--size",
        size_arg,
        "--no-progress",
        config.subject,
    }, allocator);
    pub_proc.stderr_behavior = .Inherit;
    pub_proc.stdout_behavior = .Pipe;
    try pub_proc.spawn();

    // Read publisher output and wait
    var pub_buf: [4096]u8 = undefined;
    const pub_output = readPipeWithTimeout(pub_proc.stdout, &pub_buf, 10_000_000_000);
    _ = pub_proc.wait() catch {};

    // Read subscriber output and wait
    var sub_buf: [4096]u8 = undefined;
    const sub_output = readPipeWithTimeout(sub.stdout, &sub_buf, 30_000_000_000);
    _ = sub.wait() catch {};

    // Parse both outputs
    const pub_stats = parseNatsBenchOutput(pub_output);
    const sub_stats = parseNatsBenchOutput(sub_output);

    return .{
        .name = "n -> n",
        .pub_msgs_per_sec = if (pub_stats) |s| s.msgs_per_sec else null,
        .sub_msgs_per_sec = if (sub_stats) |s| s.msgs_per_sec else null,
        .pub_kib_per_sec = if (pub_stats) |s| s.kib_per_sec else null,
        .sub_kib_per_sec = if (sub_stats) |s| s.kib_per_sec else null,
        .avg_latency_us = if (sub_stats) |s| s.latency_us else null,
    };
}

/// Parse nats bench output format.
/// Example: "Pub/Sub stats: 1,234,567 msgs/sec ~ 123.45 MB/sec"
fn parseNatsBenchOutput(output: []const u8) ?BenchStats {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        // Look for stats line
        if (std.mem.indexOf(u8, line, "stats:")) |_| {
            return parseNatsBenchLine(line);
        }
    }
    return null;
}

/// Parse a nats bench stats line.
fn parseNatsBenchLine(line: []const u8) ?BenchStats {
    // Format: "NATS Core NATS publisher stats: N msgs/sec ~ N MiB/sec ~ Nus"
    const stats_start = std.mem.indexOf(u8, line, "stats:") orelse return null;
    var data = line[stats_start + 6 ..];

    // Remove commas from numbers (nats uses "1,234,567")
    var clean_buf: [256]u8 = undefined;
    var clean_len: usize = 0;
    for (data) |c| {
        if (c != ',') {
            clean_buf[clean_len] = c;
            clean_len += 1;
        }
    }
    data = clean_buf[0..clean_len];

    // Parse msgs/sec
    const msgs_end = std.mem.indexOf(u8, data, " msgs/sec") orelse return null;
    const msgs_str = std.mem.trim(u8, data[0..msgs_end], " ");
    const msgs_per_sec = std.fmt.parseFloat(f64, msgs_str) catch return null;

    // Parse throughput (MiB/sec -> KiB/sec)
    const tilde = std.mem.indexOf(u8, data, "~ ") orelse return null;
    const after_tilde = data[tilde + 2 ..];

    // Try GiB/sec, MiB/sec, then MB/sec
    var kib_per_sec: f64 = 0;
    if (std.mem.indexOf(u8, after_tilde, " GiB/sec")) |gib_end| {
        const gib_str = std.mem.trim(u8, after_tilde[0..gib_end], " ");
        const gib_per_sec = std.fmt.parseFloat(f64, gib_str) catch return null;
        kib_per_sec = gib_per_sec * 1024.0 * 1024.0; // GiB -> KiB
    } else if (std.mem.indexOf(u8, after_tilde, " MiB/sec")) |mib_end| {
        const mib_str = std.mem.trim(u8, after_tilde[0..mib_end], " ");
        const mib_per_sec = std.fmt.parseFloat(f64, mib_str) catch return null;
        kib_per_sec = mib_per_sec * 1024.0;
    } else if (std.mem.indexOf(u8, after_tilde, " MB/sec")) |mb_end| {
        const mb_str = std.mem.trim(u8, after_tilde[0..mb_end], " ");
        const mb_per_sec = std.fmt.parseFloat(f64, mb_str) catch return null;
        kib_per_sec = mb_per_sec * 1000.0; // MB is 1000 KB
    } else {
        return null;
    }

    // Parse latency if present
    var latency_us: f64 = 1_000_000.0 / msgs_per_sec; // Default: calc from rate
    if (std.mem.indexOf(u8, after_tilde, "~ ")) |second_tilde| {
        const after_second = after_tilde[second_tilde + 2 ..];
        if (std.mem.indexOf(u8, after_second, "us")) |us_end| {
            const us_str = std.mem.trim(u8, after_second[0..us_end], " ");
            latency_us = std.fmt.parseFloat(f64, us_str) catch latency_us;
        }
    }

    return .{
        .msgs_per_sec = msgs_per_sec,
        .kib_per_sec = kib_per_sec,
        .latency_us = latency_us,
    };
}

test "parseStatsLine" {
    const line = "NATS publisher stats: 2891871 msgs/sec ~ 361484 KiB/sec ~ 0.35us";
    const stats = parseStatsLine(line) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 2891871), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 361484), stats.kib_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 0.35), stats.latency_us, 0.01);
}

test "parseStatsParts" {
    const line = "stats: 100000 msgs/sec ~ 12500 KiB/sec ~ 10.00us";
    const stats = parseStatsParts(line) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 100000), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 12500), stats.kib_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 10.0), stats.latency_us, 0.01);
}
