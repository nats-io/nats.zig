//! NATS Performance Benchmark Orchestrator
//!
//! Runs publisher/subscriber benchmarks across multiple NATS clients:
//! - Zig std.Io (this library)
//! - Zig io_uring (nats-io_u)
//! - C (nats.c)
//! - Rust (nats.rs)
//! - Go (nats CLI)
//!
//! Usage: zig build perf-bench -- --msgs=100000 --size=16

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;

pub const TMOUT = 5_000_000_000;

/// Supported benchmark clients.
pub const Client = enum {
    zig,
    zig_iou,
    c,
    rust,
    go,

    /// Display name for table output.
    pub fn name(self: Client) []const u8 {
        return switch (self) {
            .zig => "Zig std",
            .zig_iou => "Zig io_u",
            .c => "C",
            .rust => "Rust",
            .go => "Go",
        };
    }

    /// Whether this client has a standalone subscriber.
    pub fn hasSubscriber(self: Client) bool {
        return self != .rust;
    }
};

/// Publisher or subscriber role.
pub const Role = enum {
    pub_,
    sub,
};

/// Benchmark configuration options.
pub const BenchOpts = struct {
    subject: []const u8 = "benchtest",
    num_msgs: u64 = 100_000,
    size: usize = 16,
    port: u16 = 4222,
};

/// Parsed benchmark statistics.
pub const BenchStats = struct {
    msgs_per_sec: f64,
    bandwidth_mb: f64,
    latency_us: ?f64 = null,
};

/// Combined pub/sub result.
pub const PubSubResult = struct {
    name: []const u8,
    pub_stats: ?BenchStats = null,
    sub_stats: ?BenchStats = null,
};

/// Argument buffer for building command lines.
const ArgBuffer = struct {
    args: [16][]const u8 = undefined,
    count: usize = 0,
    bufs: [8][64]u8 = undefined,
    buf_idx: usize = 0,

    fn add(self: *ArgBuffer, arg: []const u8) void {
        assert(self.count < self.args.len);
        self.args[self.count] = arg;
        self.count += 1;
    }

    fn addFmt(
        self: *ArgBuffer,
        comptime fmt: []const u8,
        args: anytype,
    ) void {
        assert(self.buf_idx < self.bufs.len);
        assert(self.count < self.args.len);
        const formatted = std.fmt.bufPrint(
            &self.bufs[self.buf_idx],
            fmt,
            args,
        ) catch unreachable;
        self.args[self.count] = formatted;
        self.count += 1;
        self.buf_idx += 1;
    }

    fn slice(self: *const ArgBuffer) []const []const u8 {
        return self.args[0..self.count];
    }
};

/// Build command arguments for a client/role combination.
/// Caller must provide ArgBuffer to avoid dangling pointers.
pub fn buildExeArgs(
    ab: *ArgBuffer,
    client: Client,
    role: Role,
    opts: BenchOpts,
    payload_buf: []u8,
) void {
    assert(opts.num_msgs > 0);
    assert(opts.size > 0);
    assert(opts.subject.len > 0);

    ab.count = 0;
    ab.buf_idx = 0;

    switch (client) {
        .zig => {
            const exe = if (role == .pub_)
                "./zig-out/bin/bench-pub"
            else
                "./zig-out/bin/bench-sub";
            ab.add(exe);
            ab.add(opts.subject);
            ab.addFmt("--msgs={d}", .{opts.num_msgs});
            if (role == .pub_) {
                ab.addFmt("--size={d}", .{opts.size});
            } else {
                ab.add("--no-progress");
            }
        },
        .zig_iou => {
            const exe = if (role == .pub_)
                "../../nats-io_u/zig-out/bin/bench_pub"
            else
                "../../nats-io_u/zig-out/bin/bench_sub";
            ab.add(exe);
            ab.add("--count");
            ab.addFmt("{d}", .{opts.num_msgs});
            ab.add("--size");
            ab.addFmt("{d}", .{opts.size});
            ab.add("--subject");
            ab.add(opts.subject);
        },
        .c => {
            if (role == .pub_) {
                ab.add("../nats.c/build/bin/nats-publisher");
                ab.add("-count");
                ab.addFmt("{d}", .{opts.num_msgs});
                ab.add("-txt");
                // Generate payload: repeat 'A' size times
                const payload_len = @min(opts.size, payload_buf.len);
                @memset(payload_buf[0..payload_len], 'A');
                ab.add(payload_buf[0..payload_len]);
                ab.add("-subj");
                ab.add(opts.subject);
            } else {
                ab.add("../nats.c/build/bin/nats-subscriber");
                ab.add("-sync");
                ab.add("-count");
                ab.addFmt("{d}", .{opts.num_msgs});
                ab.add("-subj");
                ab.add(opts.subject);
            }
        },
        .rust => {
            // Rust only has combined pub+sub benchmark, wrap with timeout
            ab.add("timeout");
            ab.add("5");
            ab.add("../nats.rs/target/release/examples/nats_bench");
            ab.add("-n");
            ab.addFmt("{d}", .{opts.num_msgs});
            ab.add("--message-size");
            ab.addFmt("{d}", .{opts.size});
            ab.add("-s");
            ab.add("1");
            ab.add("-p");
            ab.add("1");
            ab.add(opts.subject);
        },
        .go => {
            ab.add("nats");
            ab.add("bench");
            ab.add(if (role == .pub_) "pub" else "sub");
            ab.addFmt("--msgs={d}", .{opts.num_msgs});
            ab.addFmt("--size={d}B", .{opts.size});
            ab.add("--no-progress");
            ab.add(opts.subject);
        },
    }
}

/// Which pipe to capture for output (client-specific).
fn getOutputPipe(client: Client) enum { stdout, stderr } {
    return switch (client) {
        .zig, .zig_iou => .stderr,
        .c, .rust, .go => .stdout,
    };
}

/// Whether to suppress the non-captured pipe (avoid noisy output).
fn suppressOtherPipe(client: Client) bool {
    return client == .go;
}

/// Spawn a benchmark process.
pub fn runExe(
    allocator: Allocator,
    client: Client,
    role: Role,
    opts: BenchOpts,
    payload_buf: []u8,
) !std.process.Child {
    assert(client != .rust or role == .pub_);

    // Build args in local buffer to keep slices valid
    var ab = ArgBuffer{};
    buildExeArgs(&ab, client, role, opts, payload_buf);
    assert(ab.count > 0);

    var child = std.process.Child.init(ab.slice(), allocator);

    const pipe = getOutputPipe(client);
    const suppress = suppressOtherPipe(client);
    if (pipe == .stderr) {
        child.stderr_behavior = .Pipe;
        child.stdout_behavior = if (suppress) .Ignore else .Inherit;
    } else {
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = if (suppress) .Ignore else .Inherit;
    }

    try child.spawn();
    return child;
}

/// Parse benchmark output for a specific client.
pub fn parseOutput(client: Client, output: []const u8) ?BenchStats {
    assert(output.len > 0 or true);

    return switch (client) {
        .zig => parseZigOutput(output),
        .zig_iou => parseZigIouOutput(output),
        .c => parseCOutput(output),
        .rust => parseRustOutput(output),
        .go => parseGoOutput(output),
    };
}

/// Parse Zig std.Io output: "stats: N msgs/sec ~ N KiB/sec ~ Nus"
fn parseZigOutput(output: []const u8) ?BenchStats {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "stats:")) |idx| {
            const data = line[idx + 6 ..];
            return parseZigStatsLine(data);
        }
    }
    return null;
}

fn parseZigStatsLine(data: []const u8) ?BenchStats {
    // Format: " N msgs/sec ~ N KiB/sec ~ Nus"
    const msgs_end = std.mem.indexOf(u8, data, " msgs/sec") orelse return null;
    const msgs_str = std.mem.trim(u8, data[0..msgs_end], " ");
    const msgs = std.fmt.parseFloat(f64, msgs_str) catch return null;

    const tilde1 = std.mem.indexOf(u8, data, "~ ") orelse return null;
    const after1 = data[tilde1 + 2 ..];
    const kib_end = std.mem.indexOf(u8, after1, " KiB/sec") orelse return null;
    const kib_str = std.mem.trim(u8, after1[0..kib_end], " ");
    const kib = std.fmt.parseFloat(f64, kib_str) catch return null;

    var latency: ?f64 = null;
    if (std.mem.indexOf(u8, after1, "~ ")) |tilde2| {
        const after2 = after1[tilde2 + 2 ..];
        if (std.mem.indexOf(u8, after2, "us")) |us_end| {
            const lat_str = std.mem.trim(u8, after2[0..us_end], " ");
            latency = std.fmt.parseFloat(f64, lat_str) catch null;
        }
    }

    return .{
        .msgs_per_sec = msgs,
        .bandwidth_mb = kib / 1024.0,
        .latency_us = latency,
    };
}

/// Parse Zig io_uring output:
/// "Throughput:   44480028.47 msg/s"
/// "Bandwidth:    678.71 MB/s"
fn parseZigIouOutput(output: []const u8) ?BenchStats {
    var msgs: ?f64 = null;
    var bw: ?f64 = null;

    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "Throughput:")) |idx| {
            const rest = line[idx + 11 ..];
            var parts = std.mem.tokenizeAny(u8, rest, " \t");
            if (parts.next()) |val_str| {
                msgs = std.fmt.parseFloat(f64, val_str) catch null;
            }
        } else if (std.mem.indexOf(u8, line, "Bandwidth:")) |idx| {
            const rest = line[idx + 10 ..];
            var parts = std.mem.tokenizeAny(u8, rest, " \t");
            if (parts.next()) |val_str| {
                bw = std.fmt.parseFloat(f64, val_str) catch null;
            }
        }
    }

    if (msgs != null and bw != null) {
        return .{
            .msgs_per_sec = msgs.?,
            .bandwidth_mb = bw.?,
            .latency_us = null,
        };
    }
    return null;
}

/// Parse C output: "(N msgs/sec)"
fn parseCOutput(output: []const u8) ?BenchStats {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "msgs/sec")) |_| {
            // Format: "...(N msgs/sec)..."
            const start = std.mem.indexOf(u8, line, "(") orelse continue;
            const end = std.mem.indexOf(u8, line, " msgs/sec)") orelse continue;
            if (end <= start) continue;
            const rate_str = line[start + 1 .. end];
            const rate = std.fmt.parseFloat(f64, rate_str) catch continue;
            return .{
                .msgs_per_sec = rate,
                .bandwidth_mb = 0, // Calculated later from rate * size
                .latency_us = null,
            };
        }
    }
    return null;
}

/// Parse Rust output:
/// "duration: Xms frequency: N mbps: N"
/// "50th percentile: N ns"
fn parseRustOutput(output: []const u8) ?BenchStats {
    var freq: ?f64 = null;
    var mbps: ?f64 = null;
    var lat_ns: ?f64 = null;

    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        // Parse frequency and mbps from same line
        if (std.mem.indexOf(u8, line, "frequency:")) |freq_idx| {
            const freq_rest = line[freq_idx + 10 ..];
            var freq_parts = std.mem.tokenizeAny(u8, freq_rest, " \t");
            if (freq_parts.next()) |val_str| {
                freq = std.fmt.parseFloat(f64, val_str) catch null;
            }

            // mbps is on same line after frequency
            if (std.mem.indexOf(u8, line, "mbps:")) |mbps_idx| {
                const mbps_rest = line[mbps_idx + 5 ..];
                var mbps_parts = std.mem.tokenizeAny(u8, mbps_rest, " \t");
                if (mbps_parts.next()) |val_str| {
                    mbps = std.fmt.parseFloat(f64, val_str) catch null;
                }
            }
        } else if (std.mem.indexOf(u8, line, "50th percentile:")) |idx| {
            const rest = line[idx + 16 ..];
            var parts = std.mem.tokenizeAny(u8, rest, " \t");
            if (parts.next()) |val_str| {
                lat_ns = std.fmt.parseFloat(f64, val_str) catch null;
            }
        }
    }

    if (freq != null) {
        return .{
            .msgs_per_sec = freq.?,
            .bandwidth_mb = mbps orelse 0,
            .latency_us = if (lat_ns) |ns| ns / 1000.0 else null,
        };
    }
    return null;
}

/// Parse Go (nats bench) output: "stats: N msgs/sec ~ N MiB/sec ~ Nus"
fn parseGoOutput(output: []const u8) ?BenchStats {
    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "stats:")) |idx| {
            // Remove commas from numbers
            var clean: [256]u8 = undefined;
            var len: usize = 0;
            for (line[idx + 6 ..]) |ch| {
                if (ch != ',') {
                    clean[len] = ch;
                    len += 1;
                }
            }
            return parseGoStatsLine(clean[0..len]);
        }
    }
    return null;
}

fn parseGoStatsLine(data: []const u8) ?BenchStats {
    // Format: " N msgs/sec ~ N MiB/sec ~ Nus"
    const msgs_end = std.mem.indexOf(u8, data, " msgs/sec") orelse return null;
    const msgs_str = std.mem.trim(u8, data[0..msgs_end], " ");
    const msgs = std.fmt.parseFloat(f64, msgs_str) catch return null;

    const tilde1 = std.mem.indexOf(u8, data, "~ ") orelse return null;
    const after1 = data[tilde1 + 2 ..];

    // Try MiB/sec, then GiB/sec
    var bandwidth_mb: f64 = 0;
    if (std.mem.indexOf(u8, after1, " GiB/sec")) |gib_end| {
        const gib_str = std.mem.trim(u8, after1[0..gib_end], " ");
        const gib = std.fmt.parseFloat(f64, gib_str) catch return null;
        bandwidth_mb = gib * 1024.0;
    } else if (std.mem.indexOf(u8, after1, " MiB/sec")) |mib_end| {
        const mib_str = std.mem.trim(u8, after1[0..mib_end], " ");
        bandwidth_mb = std.fmt.parseFloat(f64, mib_str) catch return null;
    }

    var latency: ?f64 = null;
    if (std.mem.indexOf(u8, after1, "~ ")) |tilde2| {
        const after2 = after1[tilde2 + 2 ..];
        if (std.mem.indexOf(u8, after2, "us")) |us_end| {
            const lat_str = std.mem.trim(u8, after2[0..us_end], " ");
            latency = std.fmt.parseFloat(f64, lat_str) catch null;
        }
    }

    return .{
        .msgs_per_sec = msgs,
        .bandwidth_mb = bandwidth_mb,
        .latency_us = latency,
    };
}

/// Read from pipe with timeout.
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
            std.posix.nanosleep(0, 10_000_000);
            continue;
        }
        total += n;
        // Check for completion markers
        if (hasStatsMarker(buf[0..total])) break;
    }
    return buf[0..total];
}

fn hasStatsMarker(data: []const u8) bool {
    // Don't include "frequency:" - Rust output continues after it with latency
    return std.mem.indexOf(u8, data, "stats:") != null or
        std.mem.indexOf(u8, data, "Throughput:") != null or
        std.mem.indexOf(u8, data, "Bandwidth:") != null or
        std.mem.indexOf(u8, data, "msgs/sec)") != null or
        std.mem.indexOf(u8, data, "Done!") != null or
        std.mem.indexOf(u8, data, "max:") != null; // Rust ends with max latency
}

/// Read all available data from a pipe (non-blocking).
fn readAllFromPipe(pipe: ?std.fs.File, buf: []u8) []const u8 {
    const file = pipe orelse return "";
    var total: usize = 0;

    while (total < buf.len) {
        const n = file.read(buf[total..]) catch break;
        if (n == 0) break;
        total += n;
    }
    return buf[0..total];
}

/// Read until we see "Done!" or "stats:" (more reliable than partial markers).
fn readUntilDone(pipe: ?std.fs.File, buf: []u8, timeout_ns: u64) []const u8 {
    const file = pipe orelse return "";
    var total: usize = 0;
    const start = std.time.Instant.now() catch return "";

    while (total < buf.len) {
        const n = file.read(buf[total..]) catch break;
        if (n == 0) {
            const now = std.time.Instant.now() catch break;
            if (now.since(start) > timeout_ns) break;
            std.posix.nanosleep(0, 10_000_000);
            continue;
        }
        total += n;
        // Wait for reliable end markers
        if (std.mem.indexOf(u8, buf[0..total], "Done!") != null) break;
        if (std.mem.indexOf(u8, buf[0..total], "stats:") != null) break;
        if (std.mem.indexOf(u8, buf[0..total], "msgs/sec)") != null) break;
        if (std.mem.indexOf(u8, buf[0..total], "max:") != null) break;
    }
    return buf[0..total];
}

/// Run publisher and return stats.
pub fn runPublisher(
    allocator: Allocator,
    client: Client,
    opts: BenchOpts,
) !?BenchStats {
    var payload_buf: [4096]u8 = undefined;
    var child = try runExe(allocator, client, .pub_, opts, &payload_buf);

    var buf: [8192]u8 = undefined;
    const pipe = if (getOutputPipe(client) == .stderr)
        child.stderr
    else
        child.stdout;
    const output = readPipeWithTimeout(pipe, &buf, TMOUT);
    _ = child.wait() catch {};

    return parseOutput(client, output);
}

/// Run subscriber and return stats.
pub fn runSubscriber(
    allocator: Allocator,
    client: Client,
    opts: BenchOpts,
) !?BenchStats {
    assert(client.hasSubscriber());

    var payload_buf: [4096]u8 = undefined;
    var child = try runExe(allocator, client, .sub, opts, &payload_buf);

    var buf: [8192]u8 = undefined;
    const pipe = if (getOutputPipe(client) == .stderr)
        child.stderr
    else
        child.stdout;
    const output = readPipeWithTimeout(pipe, &buf, TMOUT);
    _ = child.wait() catch {};

    return parseOutput(client, output);
}

/// Run coordinated pub/sub test.
pub fn runPubSub(
    allocator: Allocator,
    pub_client: Client,
    sub_client: Client,
    opts: BenchOpts,
) !PubSubResult {
    assert(sub_client.hasSubscriber());

    var pub_payload: [4096]u8 = undefined;
    var sub_payload: [4096]u8 = undefined;

    // Start subscriber first
    var sub = try runExe(allocator, sub_client, .sub, opts, &sub_payload);

    // Wait for subscriber to connect
    std.posix.nanosleep(0, 750_000_000);

    // Start publisher
    var pub_ = try runExe(allocator, pub_client, .pub_, opts, &pub_payload);

    // Read publisher output (wait for "Done!" marker)
    var pub_buf: [16384]u8 = undefined;
    const pub_pipe = if (getOutputPipe(pub_client) == .stderr)
        pub_.stderr
    else
        pub_.stdout;
    const pub_output = readUntilDone(pub_pipe, &pub_buf, TMOUT);

    // Wait for publisher to complete
    _ = pub_.wait() catch {};

    // Give subscriber time to receive all messages
    std.posix.nanosleep(2, 0);

    // Read subscriber output
    var sub_buf: [16384]u8 = undefined;
    const sub_pipe = if (getOutputPipe(sub_client) == .stderr)
        sub.stderr
    else
        sub.stdout;
    const sub_output = readUntilDone(sub_pipe, &sub_buf, TMOUT);
    _ = sub.wait() catch {};

    return .{
        .name = pub_client.name(),
        .pub_stats = parseOutput(pub_client, pub_output),
        .sub_stats = parseOutput(sub_client, sub_output),
    };
}

/// Run Rust combined benchmark with 5 second timeout.
pub fn runRustBench(allocator: Allocator, opts: BenchOpts) !?BenchStats {
    var payload_buf: [4096]u8 = undefined;
    var child = try runExe(allocator, .rust, .pub_, opts, &payload_buf);

    // Read output with 5 second timeout (Rust bench can hang)
    var buf: [16384]u8 = undefined;
    const timeout_5s: u64 = 5_000_000_000;
    const output = readPipeWithTimeout(child.stdout, &buf, timeout_5s);

    // Kill if still running, then wait
    _ = child.kill() catch {};
    _ = child.wait() catch {};

    if (output.len == 0) return null;
    return parseOutput(.rust, output);
}

// ============================================================================
// Main and output formatting
// ============================================================================

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const opts = try parseArgs(allocator);

    // Ensure nats-server is running
    ensureNatsServer();

    printHeader(opts);

    // Table 1: Each client runs own pub+sub
    try runTable1(allocator, opts);

    // Table 2: Subscriber comparison with zig_iou publisher
    try runTable2(allocator, opts);
}

fn parseArgs(allocator: Allocator) !BenchOpts {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();
    _ = args.next();

    var opts = BenchOpts{};

    while (args.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--msgs=")) {
            opts.num_msgs = std.fmt.parseInt(u64, arg[7..], 10) catch
                return error.InvalidArgument;
        } else if (std.mem.startsWith(u8, arg, "--size=")) {
            opts.size = parseSizeArg(arg[7..]) catch
                return error.InvalidArgument;
        }
    }

    return opts;
}

fn parseSizeArg(val: []const u8) !usize {
    var num_end: usize = 0;
    for (val, 0..) |c, i| {
        if (c >= '0' and c <= '9') {
            num_end = i + 1;
        } else break;
    }
    if (num_end == 0) return error.InvalidSize;

    const num = std.fmt.parseInt(usize, val[0..num_end], 10) catch
        return error.InvalidSize;

    const suffix = val[num_end..];
    if (suffix.len == 0 or std.mem.eql(u8, suffix, "B")) return num;
    if (std.mem.eql(u8, suffix, "K") or std.mem.eql(u8, suffix, "KB"))
        return num * 1024;
    if (std.mem.eql(u8, suffix, "M") or std.mem.eql(u8, suffix, "MB"))
        return num * 1024 * 1024;
    return error.InvalidSize;
}

fn ensureNatsServer() void {
    // Check if nats-server is running
    var child = std.process.Child.init(
        &.{ "pgrep", "nats-server" },
        std.heap.page_allocator,
    );
    child.stdout_behavior = .Ignore;
    child.stderr_behavior = .Ignore;
    child.spawn() catch return;
    const term = child.wait() catch return;

    if (term.Exited != 0) {
        // Start nats-server
        var server = std.process.Child.init(
            &.{"nats-server"},
            std.heap.page_allocator,
        );
        server.stdout_behavior = .Ignore;
        server.stderr_behavior = .Ignore;
        server.spawn() catch {};
        std.posix.nanosleep(1, 0);
    }
}

fn printHeader(opts: BenchOpts) void {
    std.debug.print(
        \\
        \\======================================================
        \\  NATS CLIENT BENCHMARK
        \\  Zig io_uring vs Zig std.Io vs C vs Rust vs Go
        \\  Messages: {d}, Payload: {d} bytes
        \\======================================================
        \\
        \\
    , .{ opts.num_msgs, opts.size });
}

fn runTable1(allocator: Allocator, opts: BenchOpts) !void {
    // Collect all results first
    const clients = [_]Client{ .zig_iou, .zig, .c, .go };
    var results: [4]?PubSubResult = .{ null, null, null, null };
    var rust_stats: ?BenchStats = null;

    for (clients, 0..) |client, i| {
        std.debug.print("Running {s} benchmark...\n", .{client.name()});
        results[i] = runPubSub(allocator, client, client, opts) catch |err| {
            std.debug.print("  Error: {}\n", .{err});
            continue;
        };
    }

    std.debug.print("Running Rust benchmark...\n", .{});
    if (runRustBench(allocator, opts)) |stats| {
        rust_stats = stats;
    } else |err| {
        std.debug.print("  Rust error: {}\n", .{err});
    }

    // Print table
    std.debug.print("\n## Results: {d} messages, {d} bytes\n\n", .{
        opts.num_msgs,
        opts.size,
    });
    std.debug.print(
        "| {s:<10} | {s:>18} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ "Client", "Publisher", "Subscriber", "Bandwidth", "Latency" },
    );
    std.debug.print(
        "|{s:-<12}|{s:->20}|{s:->20}|{s:->12}|{s:->10}|\n",
        .{ "", "", "", "", "" },
    );

    for (clients, 0..) |client, i| {
        if (results[i]) |result| {
            printTable1Row(client.name(), result, opts.size);
        }
    }
    printTable1RowRust(rust_stats);

    std.debug.print("\n", .{});
}

fn printTable1Row(name: []const u8, result: PubSubResult, size: usize) void {
    var pub_buf: [24]u8 = undefined;
    var sub_buf: [24]u8 = undefined;
    var bw_buf: [16]u8 = undefined;
    var lat_buf: [12]u8 = undefined;

    const pub_str = if (result.pub_stats) |s|
        fmtRate(&pub_buf, s.msgs_per_sec)
    else
        "-";
    const sub_str = if (result.sub_stats) |s|
        fmtRate(&sub_buf, s.msgs_per_sec)
    else
        "-";

    // Use subscriber bandwidth (more meaningful)
    const bw_str = if (result.sub_stats) |s| blk: {
        const mb = if (s.bandwidth_mb > 0)
            s.bandwidth_mb
        else
            s.msgs_per_sec * @as(f64, @floatFromInt(size)) / 1_000_000.0;
        break :blk std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{mb}) catch "-";
    } else "-";

    const lat_str = if (result.sub_stats) |s|
        if (s.latency_us) |lat|
            std.fmt.bufPrint(&lat_buf, "{d:.2}us", .{lat}) catch "-"
        else
            "-"
    else
        "-";

    std.debug.print(
        "| {s:<10} | {s:>18} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, pub_str, sub_str, bw_str, lat_str },
    );
}

fn printTable1RowRust(stats: ?BenchStats) void {
    var sub_buf: [24]u8 = undefined;
    var bw_buf: [16]u8 = undefined;
    var lat_buf: [12]u8 = undefined;

    const sub_str = if (stats) |s| fmtRate(&sub_buf, s.msgs_per_sec) else "-";
    const bw_str = if (stats) |s|
        std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{s.bandwidth_mb}) catch "-"
    else
        "-";
    const lat_str = if (stats) |s|
        if (s.latency_us) |lat|
            std.fmt.bufPrint(&lat_buf, "{d:.2}us", .{lat}) catch "-"
        else
            "-"
    else
        "-";

    std.debug.print(
        "| {s:<10} | {s:>18} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ "Rust", "-", sub_str, bw_str, lat_str },
    );
}

fn runTable2(allocator: Allocator, opts: BenchOpts) !void {
    // Collect all results first
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .go };
    var results: [4]?BenchStats = .{ null, null, null, null };

    for (subscribers, 0..) |sub_client, i| {
        std.debug.print("  Testing {s} subscriber...\n", .{sub_client.name()});
        const result = runPubSub(
            allocator,
            .zig_iou,
            sub_client,
            opts,
        ) catch |err| {
            std.debug.print("    Error: {}\n", .{err});
            continue;
        };
        results[i] = result.sub_stats;
    }

    // Print table
    std.debug.print("\n## Subscriber Comparison (Zig io_uring publisher)\n\n", .{});
    std.debug.print(
        "| {s:<10} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ "Subscriber", "Rate", "Bandwidth", "Latency" },
    );
    std.debug.print(
        "|{s:-<12}|{s:->20}|{s:->12}|{s:->10}|\n",
        .{ "", "", "", "" },
    );

    for (subscribers, 0..) |sub_client, i| {
        printTable2Row(sub_client.name(), results[i], opts.size);
    }

    std.debug.print("\n", .{});
}

fn printTable2Row(name: []const u8, stats: ?BenchStats, size: usize) void {
    var rate_buf: [24]u8 = undefined;
    var bw_buf: [16]u8 = undefined;
    var lat_buf: [12]u8 = undefined;

    const rate_str = if (stats) |s| fmtRate(&rate_buf, s.msgs_per_sec) else "-";
    const bw_str = if (stats) |s| blk: {
        const mb = if (s.bandwidth_mb > 0)
            s.bandwidth_mb
        else
            s.msgs_per_sec * @as(f64, @floatFromInt(size)) / 1_000_000.0;
        break :blk std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{mb}) catch "-";
    } else "-";
    const lat_str = if (stats) |s|
        if (s.latency_us) |lat|
            std.fmt.bufPrint(&lat_buf, "{d:.2}us", .{lat}) catch "-"
        else
            "-"
    else
        "-";

    std.debug.print(
        "| {s:<10} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, rate_str, bw_str, lat_str },
    );
}

fn fmtRate(buf: []u8, rate: f64) []const u8 {
    // Format with thousands separator
    const r = @as(u64, @intFromFloat(rate));
    if (r >= 1_000_000) {
        return std.fmt.bufPrint(buf, "{d},{d:0>3},{d:0>3} msg/s", .{
            r / 1_000_000,
            (r / 1_000) % 1_000,
            r % 1_000,
        }) catch "-";
    } else if (r >= 1_000) {
        return std.fmt.bufPrint(buf, "{d},{d:0>3} msg/s", .{
            r / 1_000,
            r % 1_000,
        }) catch "-";
    } else {
        return std.fmt.bufPrint(buf, "{d} msg/s", .{r}) catch "-";
    }
}

// ============================================================================
// Unit tests
// ============================================================================

test "parseZigOutput" {
    const output = "NATS publisher stats: 2891871 msgs/sec ~ 361484 KiB/sec ~ 0.35us";
    const stats = parseZigOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 2891871), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 353.0), stats.bandwidth_mb, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 0.35), stats.latency_us.?, 0.01);
}

test "parseZigIouOutput" {
    const output =
        \\=== Publisher Statistics ===
        \\Messages:     1000
        \\Throughput:   3500000.50 msg/s
        \\Bandwidth:    56.25 MB/s
    ;
    const stats = parseZigIouOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 3500000.5), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 56.25), stats.bandwidth_mb, 0.01);
}

test "parseCOutput" {
    const output = "Received 100000 messages (3711494 msgs/sec)";
    const stats = parseCOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 3711494), stats.msgs_per_sec, 1);
}

test "parseRustOutput" {
    // Rust output format from nats_bench (frequency and mbps on same line)
    const output =
        \\duration: 5.354435ms frequency: 2500000 mbps: 40
        \\publish latency breakdown in nanoseconds:
        \\    50th percentile: 850
    ;
    const stats = parseRustOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 2500000), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 40), stats.bandwidth_mb, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 0.85), stats.latency_us.?, 0.01);
}

test "parseGoOutput" {
    const output =
        "NATS Core NATS subscriber stats: 1,234,567 msgs/sec ~ 123 MiB/sec ~ 1.25us";
    const stats = parseGoOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 1234567), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 123), stats.bandwidth_mb, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 1.25), stats.latency_us.?, 0.01);
}

test "buildExeArgs zig pub" {
    var payload: [64]u8 = undefined;
    var ab = ArgBuffer{};
    buildExeArgs(&ab, .zig, .pub_, .{}, &payload);
    try std.testing.expectEqual(@as(usize, 4), ab.count);
    try std.testing.expectEqualStrings("./zig-out/bin/bench-pub", ab.args[0]);
}

test "buildExeArgs go sub" {
    var payload: [64]u8 = undefined;
    var ab = ArgBuffer{};
    buildExeArgs(&ab, .go, .sub, .{}, &payload);
    try std.testing.expectEqual(@as(usize, 7), ab.count);
    try std.testing.expectEqualStrings("nats", ab.args[0]);
    try std.testing.expectEqualStrings("bench", ab.args[1]);
    try std.testing.expectEqualStrings("sub", ab.args[2]);
}

test "buildExeArgs c pub generates payload" {
    var payload: [64]u8 = undefined;
    var ab = ArgBuffer{};
    buildExeArgs(&ab, .c, .pub_, .{ .size = 16 }, &payload);
    // Should have payload argument
    var found_txt = false;
    for (ab.args[0..ab.count], 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "-txt")) {
            found_txt = true;
            // Next arg should be payload
            if (i + 1 < ab.count) {
                try std.testing.expectEqual(@as(usize, 16), ab.args[i + 1].len);
            }
        }
    }
    try std.testing.expect(found_txt);
}
