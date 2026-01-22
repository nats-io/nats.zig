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
const File = std.Io.File;
const Io = std.Io;
const TerminalUI = @import("terminal_ui.zig").TerminalUI;
const StdOut = @import("terminal_ui.zig").StdOut;
const TablePrinter = @import("terminal_ui.zig").TablePrinter;

pub const TMOUT = 5_000_000_000;
pub const MAX_RUNS = 32;

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
            .zig => "Zig",
            .zig_iou => "Zig io_u",
            .c => "C",
            .rust => "Rust",
            .go => "Go",
        };
    }
};

/// Publisher or subscriber role.
pub const Role = enum {
    publisher,
    subscriber,
};

/// Benchmark configuration options.
pub const BenchOpts = struct {
    subject: []const u8 = "benchtest",
    num_msgs: u64 = 100_000,
    size: usize = 16,
    port: u16 = 4222,
    num_runs: u32 = 1,
    output_file: ?[]const u8 = null,
    server_path: ?[]const u8 = null,
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

/// Individual benchmark run result with success/failure tracking.
pub const BenchRun = struct {
    success: bool = false,
    pub_stats: ?BenchStats = null,
    sub_stats: ?BenchStats = null,
    error_msg: []const u8 = "",
};

/// Collection of all benchmark results for markdown generation.
pub const AllResults = struct {
    // Table 1: Self pub/sub [client_idx][run_idx]
    table1: [5][MAX_RUNS]BenchRun = .{.{BenchRun{}} ** MAX_RUNS} ** 5,
    // Table 2.1: Zig publisher, various subscribers
    table2_1: [5][MAX_RUNS]BenchRun = .{.{BenchRun{}} ** MAX_RUNS} ** 5,
    // Table 2.2: Go publisher, various subscribers
    table2_2: [5][MAX_RUNS]BenchRun = .{.{BenchRun{}} ** MAX_RUNS} ** 5,
    // Table 3: Fire starter
    table3: [5][MAX_RUNS]BenchRun = .{.{BenchRun{}} ** MAX_RUNS} ** 5,

    // Track valid runs per client
    table1_counts: [5]usize = .{0} ** 5,
    table2_1_counts: [5]usize = .{0} ** 5,
    table2_2_counts: [5]usize = .{0} ** 5,
    table3_counts: [5]usize = .{0} ** 5,
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
            const exe = if (role == .publisher)
                "./zig-out/bin/bench-pub"
            else
                "./zig-out/bin/bench-sub";
            ab.add(exe);
            ab.add(opts.subject);
            ab.addFmt("--msgs={d}", .{opts.num_msgs});
            if (role == .publisher) {
                ab.addFmt("--size={d}", .{opts.size});
            } else {
                ab.add("--no-progress");
            }
        },
        .zig_iou => {
            const exe = if (role == .publisher)
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
            if (role == .publisher) {
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
            // Rust separate bench_pub/bench_sub utilities
            const exe = if (role == .publisher)
                "../nats.rs.bench/target/release/bench_pub"
            else
                "../nats.rs.bench/target/release/bench_sub";
            ab.add(exe);
            ab.add(opts.subject);
            ab.add("--msgs");
            ab.addFmt("{d}", .{opts.num_msgs});
            if (role == .publisher) {
                ab.add("--size");
                ab.addFmt("{d}", .{opts.size});
            }
        },
        .go => {
            ab.add("nats");
            ab.add("bench");
            ab.add(if (role == .publisher) "pub" else "sub");
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
    _: Allocator,
    io: Io,
    client: Client,
    role: Role,
    opts: BenchOpts,
    payload_buf: []u8,
) !std.process.Child {

    // Build args in local buffer to keep slices valid
    var ab = ArgBuffer{};
    buildExeArgs(&ab, client, role, opts, payload_buf);
    assert(ab.count > 0);

    const pipe = getOutputPipe(client);
    const suppress = suppressOtherPipe(client);

    const stdout_opt: std.process.SpawnOptions.StdIo = if (pipe == .stderr)
        (if (suppress) .ignore else .inherit)
    else
        .pipe;

    const stderr_opt: std.process.SpawnOptions.StdIo = if (pipe == .stderr)
        .pipe
    else
        (if (suppress) .ignore else .inherit);

    return try std.process.spawn(io, .{
        .argv = ab.slice(),
        .stdout = stdout_opt,
        .stderr = stderr_opt,
    });
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
    return parseThroughputBandwidth(output);
}

/// Parse fire_starter output (same format as zig_iou):
/// "Throughput:   4398969.54 msg/s"
/// "Bandwidth:    167.81 MB/s"
fn parseFireStarterOutput(output: []const u8) ?BenchStats {
    return parseThroughputBandwidth(output);
}

/// Common parser for "Throughput: N msg/s" and "Bandwidth: N MB/s" format.
fn parseThroughputBandwidth(output: []const u8) ?BenchStats {
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

/// Parse Rust bench_pub/bench_sub output:
/// "  Msg/sec:    2982417"
/// "  Throughput: 47.72 MB/s"
fn parseRustOutput(output: []const u8) ?BenchStats {
    var msgs: ?f64 = null;
    var throughput: ?f64 = null;

    var lines = std.mem.splitScalar(u8, output, '\n');
    while (lines.next()) |line| {
        if (std.mem.indexOf(u8, line, "Msg/sec:")) |idx| {
            const rest = line[idx + 8 ..];
            var parts = std.mem.tokenizeAny(u8, rest, " \t");
            if (parts.next()) |val_str| {
                msgs = std.fmt.parseFloat(f64, val_str) catch null;
            }
        } else if (std.mem.indexOf(u8, line, "Throughput:")) |idx| {
            const rest = line[idx + 11 ..];
            var parts = std.mem.tokenizeAny(u8, rest, " \t");
            if (parts.next()) |val_str| {
                throughput = std.fmt.parseFloat(f64, val_str) catch null;
            }
        }
    }

    if (msgs != null) {
        return .{
            .msgs_per_sec = msgs.?,
            .bandwidth_mb = throughput orelse 0,
            .latency_us = null,
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
    io: Io,
    pipe: ?File,
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
            io.sleep(.fromMilliseconds(10), .awake) catch {};
            continue;
        }
        total += n;
        // Check for completion markers
        if (hasStatsMarker(buf[0..total])) break;
    }
    return buf[0..total];
}

fn hasStatsMarker(data: []const u8) bool {
    return std.mem.indexOf(u8, data, "stats:") != null or
        std.mem.indexOf(u8, data, "Throughput:") != null or
        std.mem.indexOf(u8, data, "Bandwidth:") != null or
        std.mem.indexOf(u8, data, "msgs/sec)") != null or
        std.mem.indexOf(u8, data, "Done!") != null;
}

/// Read all available data from a pipe (non-blocking).
fn readAllFromPipe(io: Io, pipe: ?File, buf: []u8) []const u8 {
    const file = pipe orelse return "";
    var total: usize = 0;

    while (total < buf.len) {
        var slice = [_][]u8{buf[total..]};
        const n = file.readStreaming(io, &slice) catch break;
        if (n == 0) break;
        total += n;
    }
    return buf[0..total];
}

/// Read until end markers indicating output is complete.
fn readUntilDone(io: Io, pipe: ?File, buf: []u8, timeout_ns: u64) []const u8 {
    const file = pipe orelse return "";
    var total: usize = 0;
    const start = std.time.Instant.now() catch return "";

    while (total < buf.len) {
        var slice = [_][]u8{buf[total..]};
        const n = file.readStreaming(io, &slice) catch break;
        if (n == 0) {
            const now = std.time.Instant.now() catch break;
            if (now.since(start) > timeout_ns) break;
            io.sleep(.fromMilliseconds(10), .awake) catch {};
            continue;
        }
        total += n;

        // Check for end markers
        const has_marker =
            std.mem.indexOf(u8, buf[0..total], "Done!") != null or
            std.mem.indexOf(u8, buf[0..total], "stats:") != null or
            std.mem.indexOf(u8, buf[0..total], "msgs/sec)") != null or
            std.mem.indexOf(u8, buf[0..total], "Bandwidth:") != null or
            std.mem.indexOf(u8, buf[0..total], "Throughput:") != null;

        if (has_marker) {
            // Wait for trailing lines, then do one more read
            io.sleep(.fromMilliseconds(50), .awake) catch {};
            var extra_slice = [_][]u8{buf[total..]};
            const extra = file.readStreaming(io, &extra_slice) catch 0;
            total += extra;
            break;
        }
    }
    return buf[0..total];
}

/// Run publisher and return stats.
pub fn runPublisher(
    allocator: Allocator,
    io: Io,
    client: Client,
    opts: BenchOpts,
) !?BenchStats {
    var payload_buf: [4096]u8 = undefined;
    var child = try runExe(
        allocator,
        io,
        client,
        .publisher,
        opts,
        &payload_buf,
    );

    var buf: [8192]u8 = undefined;
    const pipe = if (getOutputPipe(client) == .stderr)
        child.stderr
    else
        child.stdout;
    const output = readPipeWithTimeout(io, pipe, &buf, TMOUT);
    _ = child.wait(io) catch {};

    return parseOutput(client, output);
}

/// Run subscriber and return stats.
pub fn runSubscriber(
    allocator: Allocator,
    io: Io,
    client: Client,
    opts: BenchOpts,
) !?BenchStats {
    var payload_buf: [4096]u8 = undefined;
    var child = try runExe(
        allocator,
        io,
        client,
        .subscriber,
        opts,
        &payload_buf,
    );

    var buf: [8192]u8 = undefined;
    const pipe = if (getOutputPipe(client) == .stderr)
        child.stderr
    else
        child.stdout;
    const output = readPipeWithTimeout(io, pipe, &buf, TMOUT);
    _ = child.wait(io) catch {};

    return parseOutput(client, output);
}

/// Run coordinated pub/sub test.
pub fn runPubSub(
    allocator: Allocator,
    io: Io,
    pub_client: Client,
    sub_client: Client,
    opts: BenchOpts,
) !PubSubResult {
    var pub_payload: [4096]u8 = undefined;
    var sub_payload: [4096]u8 = undefined;

    // Start subscriber first
    var sub = try runExe(
        allocator,
        io,
        sub_client,
        .subscriber,
        opts,
        &sub_payload,
    );

    // Wait for subscriber to connect
    io.sleep(.fromMilliseconds(750), .awake) catch {};

    // Start publisher
    var publ = try runExe(
        allocator,
        io,
        pub_client,
        .publisher,
        opts,
        &pub_payload,
    );

    // Read publisher output (wait for "Done!" marker)
    var pub_buf: [16384]u8 = undefined;
    const pub_pipe = if (getOutputPipe(pub_client) == .stderr)
        publ.stderr
    else
        publ.stdout;
    const pub_output = readUntilDone(io, pub_pipe, &pub_buf, TMOUT);

    // Wait for publisher to complete
    _ = publ.wait(io) catch {};

    // Give subscriber time to receive all messages
    io.sleep(.fromSeconds(2), .awake) catch {};

    // Read subscriber output
    var sub_buf: [16384]u8 = undefined;
    const sub_pipe = if (getOutputPipe(sub_client) == .stderr)
        sub.stderr
    else
        sub.stdout;
    const sub_output = readUntilDone(io, sub_pipe, &sub_buf, TMOUT);
    _ = sub.wait(io) catch {};

    return .{
        .name = pub_client.name(),
        .pub_stats = parseOutput(pub_client, pub_output),
        .sub_stats = parseOutput(sub_client, sub_output),
    };
}

/// Spawn fire_starter (io_uring server + publisher).
/// Usage: fire_starter <msg_count> <msg_size>
/// Fixed subject: stress.test
pub fn runFireStarter(io: Io, opts: BenchOpts) !std.process.Child {
    assert(opts.num_msgs > 0);
    assert(opts.size > 0);

    var ab = ArgBuffer{};
    ab.add("../../nats-io_u/zig-out/bin/fire_starter");
    ab.addFmt("{d}", .{opts.num_msgs});
    ab.addFmt("{d}", .{opts.size});

    // fire_starter outputs stats to stderr
    return try std.process.spawn(io, .{
        .argv = ab.slice(),
        .stdout = .ignore,
        .stderr = .pipe,
    });
}

/// Run fire_starter with a subscriber (max throughput test).
pub fn runFireStarterTest(
    allocator: Allocator,
    io: Io,
    sub_client: Client,
    opts: BenchOpts,
) !PubSubResult {
    assert(opts.num_msgs > 0);
    assert(opts.size > 0);

    // Start fire_starter
    var fire = try runFireStarter(io, opts);

    // Wait for fire_starter to be ready
    io.sleep(.fromMilliseconds(500), .awake) catch {};

    // Start subscriber with stress.test subject
    var sub_opts = opts;
    sub_opts.subject = "stress.test";

    var sub_payload: [4096]u8 = undefined;
    var sub = try runExe(
        allocator,
        io,
        sub_client,
        .subscriber,
        sub_opts,
        &sub_payload,
    );

    // Read subscriber output
    var sub_buf: [16384]u8 = undefined;
    const sub_pipe = if (getOutputPipe(sub_client) == .stderr)
        sub.stderr
    else
        sub.stdout;
    const sub_output = readUntilDone(io, sub_pipe, &sub_buf, TMOUT);
    _ = sub.wait(io) catch {};

    // Read fire_starter output from stderr (should have exited after sending)
    var fire_buf: [16384]u8 = undefined;
    const fire_output = readAllFromPipe(io, fire.stderr, &fire_buf);
    _ = fire.wait(io) catch {};

    return .{
        .name = sub_client.name(),
        .pub_stats = parseFireStarterOutput(fire_output),
        .sub_stats = parseOutput(sub_client, sub_output),
    };
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const opts = parseArgs(init) catch |err| {
        if (err == error.ShowHelp) {
            printHelp();
            return;
        }
        return err;
    };

    // Initialize terminal UI, stdout, and results storage
    var ui: TerminalUI = .init(io);
    var stdout: StdOut = .{};
    stdout.init(io);
    var all_results = AllResults{};

    // Ensure nats-server is running
    ensureNatsServer(io, opts.server_path);

    printHeader(&stdout, opts, &ui);
    stdout.flush();

    // Table 1: Each client runs own pub+sub
    try runTable1(allocator, io, opts, &ui, &all_results, &stdout);

    // Table 2.1: Subscriber comparison with Zig std publisher
    try runTable2(allocator, io, opts, &ui, &all_results, &stdout);

    // Table 2.2: Subscriber comparison with Go publisher
    try runTable2_2(allocator, io, opts, &ui, &all_results, &stdout);

    // Table 3: Fire starter test (needs nats-server stopped)
    stopNatsServer(io, opts.server_path);
    try runTable3(allocator, io, opts, &ui, &all_results, &stdout);

    stdout.flush();

    // Generate markdown report if requested
    if (opts.output_file) |filename| {
        try generateMarkdown(io, opts, &all_results, filename);
        ui.printHeader("Markdown report written to:");
        ui.print("  ");
        ui.writeGreen(filename);
        ui.print("\n");
    }
}

fn parseArgs(init: std.process.Init) !BenchOpts {
    var args_iter = std.process.Args.Iterator.initAllocator(
        init.minimal.args,
        init.gpa,
    ) catch |err| {
        std.process.fatal("failed to init args: {}", .{err});
    };
    defer args_iter.deinit();
    _ = args_iter.skip();

    var opts = BenchOpts{};
    var has_msgs = false;
    var has_size = false;
    var has_runs = false;

    while (args_iter.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--msgs=")) {
            opts.num_msgs = std.fmt.parseInt(u64, arg[7..], 10) catch
                return error.InvalidArgument;
            has_msgs = true;
        } else if (std.mem.startsWith(u8, arg, "--size=")) {
            opts.size = parseSizeArg(arg[7..]) catch
                return error.InvalidArgument;
            has_size = true;
        } else if (std.mem.startsWith(u8, arg, "--runs=")) {
            opts.num_runs = std.fmt.parseInt(u32, arg[7..], 10) catch
                return error.InvalidArgument;
            if (opts.num_runs == 0 or opts.num_runs > MAX_RUNS)
                return error.InvalidArgument;
            has_runs = true;
        } else if (std.mem.startsWith(u8, arg, "--output=")) {
            opts.output_file = arg[9..];
        } else if (std.mem.startsWith(u8, arg, "--server=")) {
            opts.server_path = arg[9..];
        } else if (std.mem.eql(u8, arg, "--help") or
            std.mem.eql(u8, arg, "-h"))
        {
            return error.ShowHelp;
        }
    }

    if (!has_msgs or !has_size or !has_runs) return error.ShowHelp;
    return opts;
}

fn printHelp() void {
    std.debug.print(
        \\
        \\Usage: zig build run-perf-bench -- --msgs=N --size=N --runs=N [OPTIONS]
        \\
        \\Options:
        \\  --msgs=N       Number of messages (required)
        \\  --size=N       Payload size in bytes (required)
        \\                 Supports suffixes: B, K/KB, M/MB
        \\  --runs=N       Number of runs per test (required, max 32)
        \\  --output=FILE  Write markdown report to FILE (optional)
        \\  --server=PATH  Use custom nats-server binary (optional)
        \\
        \\Examples:
        \\  zig build run-perf-bench -- --msgs=100000 --size=16 --runs=3
        \\  zig build run-perf-bench -- --msgs=1000000 --size=1K --runs=5 --output=results.md
        \\  zig build run-perf-bench -- --msgs=100000 --size=16 --runs=3 --server=/opt/nats/nats-server
        \\
    , .{});
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

fn ensureNatsServer(io: Io, server_path: ?[]const u8) void {
    const server_bin = server_path orelse "nats-server";
    const server_name = std.fs.path.basename(server_bin);

    // Check if nats-server is running
    var child = std.process.spawn(io, .{
        .argv = &.{ "pgrep", server_name },
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return;
    const term = child.wait(io) catch return;

    if (term.exited != 0) {
        // Start nats-server
        _ = std.process.spawn(io, .{
            .argv = &.{server_bin},
            .stdout = .ignore,
            .stderr = .ignore,
        }) catch {};
        io.sleep(.fromSeconds(1), .awake) catch {};
    }
}

fn stopNatsServer(io: Io, server_path: ?[]const u8) void {
    const server_bin = server_path orelse "nats-server";
    const server_name = std.fs.path.basename(server_bin);

    // Kill nats-server if running (needed before fire_starter test)
    var child = std.process.spawn(io, .{
        .argv = &.{ "pkill", server_name },
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch return;
    _ = child.wait(io) catch {};
    // Wait for port to be released
    io.sleep(.fromMilliseconds(500), .awake) catch {};
}

fn printHeader(out: *StdOut, opts: BenchOpts, ui: *TerminalUI) void {
    _ = out;
    const W: usize = 56;
    const Seg = TerminalUI.Segment;

    ui.stderr.writeStreamingAll(ui.io, "\n") catch {};
    ui.printBoxTop(W);

    // Line 1: Title in bold white
    ui.printBoxLineColored(
        W,
        TerminalUI.ESC_TITLE ++ TerminalUI.ESC_BOLD,
        "NATS CLIENT BENCHMARK",
    );

    // Line 2: Client names in slate blue, "vs" in gray
    ui.printBoxLineMulti(W, &[_]Seg{
        .{ .color = TerminalUI.ESC_HEADER, .text = "Zig io_u" },
        .{ .color = TerminalUI.ESC_PROGRESS, .text = " vs " },
        .{ .color = TerminalUI.ESC_HEADER, .text = "Zig" },
        .{ .color = TerminalUI.ESC_PROGRESS, .text = " vs " },
        .{ .color = TerminalUI.ESC_HEADER, .text = "C" },
        .{ .color = TerminalUI.ESC_PROGRESS, .text = " vs " },
        .{ .color = TerminalUI.ESC_HEADER, .text = "Rust" },
        .{ .color = TerminalUI.ESC_PROGRESS, .text = " vs " },
        .{ .color = TerminalUI.ESC_HEADER, .text = "Go" },
    });

    // Line 3: Parameters with colored numbers
    var num_buf: [16]u8 = undefined;
    var size_buf: [8]u8 = undefined;

    const num_str =
        std.fmt.bufPrint(&num_buf, "{d}", .{opts.num_msgs}) catch "?";

    const size_str =
        std.fmt.bufPrint(&size_buf, "{d}", .{opts.size}) catch "?";

    ui.printBoxLineMulti(W, &[_]Seg{
        .{ .color = TerminalUI.ESC_UNIT, .text = "Messages: " },
        .{ .color = TerminalUI.ESC_RATE, .text = num_str },
        .{ .color = TerminalUI.ESC_PROGRESS, .text = "  |  " },
        .{ .color = TerminalUI.ESC_UNIT, .text = "Payload: " },
        .{ .color = TerminalUI.ESC_RATE, .text = size_str },
        .{ .color = TerminalUI.ESC_UNIT, .text = " bytes" },
    });

    ui.printBoxBottom(W);
}

fn runTable1(
    allocator: Allocator,
    io: Io,
    opts: BenchOpts,
    ui: *TerminalUI,
    all_results: *AllResults,
    out: *StdOut,
) !void {
    const clients = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;
    const total_clients = clients.len;

    ui.printHeader("Table 1: Self Pub/Sub");

    // Run all benchmarks with interactive progress
    for (clients, 0..) |client, ci| {
        var success_count: usize = 0;
        var last_sub_rate: f64 = 0;

        for (0..num_runs) |run| {
            // Show running state with spinner
            ui.showRunning(
                ci + 1,
                total_clients,
                client.name(),
                run + 1,
                num_runs,
            );

            const result = runPubSub(
                allocator,
                io,
                client,
                client,
                opts,
            ) catch |err| {
                all_results.table1[ci][run] = .{
                    .success = false,
                    .error_msg = @errorName(err),
                };
                continue;
            };

            // Store result
            all_results.table1[ci][run] = .{
                .success = true,
                .pub_stats = result.pub_stats,
                .sub_stats = result.sub_stats,
            };
            all_results.table1_counts[ci] += 1;
            success_count += 1;

            if (result.sub_stats) |stats| {
                last_sub_rate = stats.msgs_per_sec;
            }
        }

        // Show final result for this client
        if (success_count > 0) {
            // Calculate median for display
            var rates: [MAX_RUNS]f64 = undefined;
            var rate_count: usize = 0;
            for (0..num_runs) |run| {
                if (all_results.table1[ci][run].sub_stats) |s| {
                    rates[rate_count] = s.msgs_per_sec;
                    rate_count += 1;
                }
            }
            const median_rate = if (rate_count > 0)
                calculateMedian(rates[0..rate_count])
            else
                last_sub_rate;

            var rate_buf: [32]u8 = undefined;
            const rate_str = fmtRate(&rate_buf, median_rate);
            ui.showSuccess(ci + 1, total_clients, client.name(), rate_str);
        } else {
            ui.showFailure(
                ci + 1,
                total_clients,
                client.name(),
                "all runs failed",
            );
        }
    }

    // Print detailed tables at the end
    printTable1Details(io, out, opts, all_results);
    out.flush();
}

/// Print the detailed Table 1 results (all runs + median).
fn printTable1Details(
    io: Io,
    out: *StdOut,
    opts: BenchOpts,
    all_results: *AllResults,
) void {
    _ = out; // Using ui for ANSI output

    const clients = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;

    // Convert AllResults to old format for existing print functions
    var results: [5][MAX_RUNS]?PubSubResult = .{.{null} ** MAX_RUNS} ** 5;
    for (clients, 0..) |client, ci| {
        for (0..num_runs) |run| {
            const r = all_results.table1[ci][run];
            if (r.success) {
                results[ci][run] = .{
                    .name = client.name(),
                    .pub_stats = r.pub_stats,
                    .sub_stats = r.sub_stats,
                };
            }
        }
    }

    // Use stderr for ANSI table output
    var ui: TerminalUI = .init(io);

    // Print section title
    var title_buf: [64]u8 = undefined;
    const title = std.fmt.bufPrint(
        &title_buf,
        "Table 1: All Runs ({d} runs)",
        .{num_runs},
    ) catch
        "Table 1: All Runs";
    ui.printHeader(title);

    const col_widths = [_]usize{ 12, 5, 20, 20, 12, 10 };
    var printer = TablePrinter{ .ui = &ui, .col_widths = &col_widths };

    printer.printTop();
    printer.printHeaderRow(&.{
        "Client",
        "Run",
        "Publisher",
        "Subscriber",
        "Bandwidth",
        "Latency",
    });
    printer.printSeparator();

    for (clients, 0..) |client, ci| {
        for (0..num_runs) |run| {
            if (results[ci][run]) |result| {
                var pub_buf: [24]u8 = undefined;
                var sub_buf: [24]u8 = undefined;
                var bw_buf: [16]u8 = undefined;
                var lat_buf: [12]u8 = undefined;
                var run_buf: [8]u8 = undefined;

                const pub_str = if (result.pub_stats) |s| fmtRate(
                    &pub_buf,
                    s.msgs_per_sec,
                ) else "-";
                const sub_str = if (result.sub_stats) |s| fmtRate(
                    &sub_buf,
                    s.msgs_per_sec,
                ) else "-";
                const bw_str = if (result.sub_stats) |s| blk: {
                    const mb = if (s.bandwidth_mb > 0)
                        s.bandwidth_mb
                    else
                        s.msgs_per_sec * @as(f64, @floatFromInt(opts.size)) /
                            1_000_000.0;
                    break :blk std.fmt.bufPrint(
                        &bw_buf,
                        "{d:.0} MB/s",
                        .{mb},
                    ) catch "-";
                } else "-";

                const lat_str = if (result.sub_stats) |s|
                    (if (s.latency_us) |lat| std.fmt.bufPrint(
                        &lat_buf,
                        "{d:.2}us",
                        .{lat},
                    ) catch "-" else "-")
                else
                    "-";
                const run_str = std.fmt.bufPrint(
                    &run_buf,
                    "{d}",
                    .{run + 1},
                ) catch "?";

                printer.printRowHighlight(
                    &.{
                        client.name(),
                        run_str,
                        pub_str,
                        sub_str,
                        bw_str,
                        lat_str,
                    },
                );
            }
        }
        // Separator between clients (not after last)
        if (ci < clients.len - 1) {
            printer.printSeparator();
        }
    }
    printer.printBottom();

    // Print median results table
    ui.printHeader("Table 1: Median Results");

    const med_widths = [_]usize{ 12, 20, 20, 12, 10 };
    var med_printer = TablePrinter{ .ui = &ui, .col_widths = &med_widths };

    med_printer.printTop();
    med_printer.printHeaderRow(
        &.{ "Client", "Publisher", "Subscriber", "Bandwidth", "Latency" },
    );
    med_printer.printSeparator();

    for (clients, 0..) |client, ci| {
        const median = calcMedianPubSub(results[ci][0..num_runs]);

        var pub_buf: [24]u8 = undefined;
        var sub_buf: [24]u8 = undefined;
        var bw_buf: [16]u8 = undefined;
        var lat_buf: [12]u8 = undefined;

        const pub_str = if (median.pub_stats) |s| fmtRate(
            &pub_buf,
            s.msgs_per_sec,
        ) else "-";
        const sub_str = if (median.sub_stats) |s| fmtRate(
            &sub_buf,
            s.msgs_per_sec,
        ) else "-";
        const bw_str = if (median.sub_stats) |s| blk: {
            const mb = if (s.bandwidth_mb > 0)
                s.bandwidth_mb
            else
                s.msgs_per_sec * @as(f64, @floatFromInt(opts.size)) /
                    1_000_000.0;

            break :blk std.fmt.bufPrint(
                &bw_buf,
                "{d:.0} MB/s",
                .{mb},
            ) catch "-";
        } else "-";
        const lat_str =
            if (median.sub_stats) |s|
                (if (s.latency_us) |lat| std.fmt.bufPrint(
                    &lat_buf,
                    "{d:.2}us",
                    .{lat},
                ) catch "-" else "-")
            else
                "-";

        med_printer.printRowHighlight(
            &.{ client.name(), pub_str, sub_str, bw_str, lat_str },
        );
    }
    med_printer.printBottom();
}

fn printTable1RowRun(
    out: *StdOut,
    name: []const u8,
    run: usize,
    result: PubSubResult,
    size: usize,
) void {
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

    out.print(
        "| {s:<10} | {d:>3} | {s:>18} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, run, pub_str, sub_str, bw_str, lat_str },
    );
}

/// Calculate median PubSubResult from multiple runs.
fn calcMedianPubSub(results: []?PubSubResult) PubSubResult {
    var pub_rates: [MAX_RUNS]f64 = undefined;
    var sub_rates: [MAX_RUNS]f64 = undefined;
    var sub_bw: [MAX_RUNS]f64 = undefined;
    var sub_lat: [MAX_RUNS]?f64 = .{null} ** MAX_RUNS;
    var pub_count: usize = 0;
    var sub_count: usize = 0;

    for (results) |r| {
        if (r) |result| {
            if (result.pub_stats) |ps| {
                pub_rates[pub_count] = ps.msgs_per_sec;
                pub_count += 1;
            }
            if (result.sub_stats) |ss| {
                sub_rates[sub_count] = ss.msgs_per_sec;
                sub_bw[sub_count] = ss.bandwidth_mb;
                sub_lat[sub_count] = ss.latency_us;
                sub_count += 1;
            }
        }
    }

    return .{
        .name = "",
        .pub_stats = if (pub_count > 0) .{
            .msgs_per_sec = calculateMedian(pub_rates[0..pub_count]),
            .bandwidth_mb = 0,
            .latency_us = null,
        } else null,
        .sub_stats = if (sub_count > 0) .{
            .msgs_per_sec = calculateMedian(sub_rates[0..sub_count]),
            .bandwidth_mb = calculateMedian(sub_bw[0..sub_count]),
            .latency_us = calculateMedianLatency(sub_lat[0..sub_count]),
        } else null,
    };
}

/// Calculate median BenchStats from multiple runs.
fn calcMedianStats(results: []?BenchStats) ?BenchStats {
    var rates: [MAX_RUNS]f64 = undefined;
    var bw: [MAX_RUNS]f64 = undefined;
    var lat: [MAX_RUNS]?f64 = .{null} ** MAX_RUNS;
    var count: usize = 0;

    for (results) |r| {
        if (r) |stats| {
            rates[count] = stats.msgs_per_sec;
            bw[count] = stats.bandwidth_mb;
            lat[count] = stats.latency_us;
            count += 1;
        }
    }

    if (count == 0) return null;
    return .{
        .msgs_per_sec = calculateMedian(rates[0..count]),
        .bandwidth_mb = calculateMedian(bw[0..count]),
        .latency_us = calculateMedianLatency(lat[0..count]),
    };
}

fn printTable1Row(
    out: *StdOut,
    name: []const u8,
    result: PubSubResult,
    size: usize,
) void {
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

    out.print(
        "| {s:<10} | {s:>18} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, pub_str, sub_str, bw_str, lat_str },
    );
}

fn runTable2(
    allocator: Allocator,
    io: Io,
    opts: BenchOpts,
    ui: *TerminalUI,
    all_results: *AllResults,
    out: *StdOut,
) !void {
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;
    const total = subscribers.len;

    ui.printHeader("Table 2.1: Subscriber Comparison (Zig std publisher)");

    for (subscribers, 0..) |sub_client, si| {
        var success_count: usize = 0;
        var last_rate: f64 = 0;

        for (0..num_runs) |run| {
            ui.showRunning(si + 1, total, sub_client.name(), run + 1, num_runs);

            const result = runPubSub(
                allocator,
                io,
                .zig,
                sub_client,
                opts,
            ) catch |err| {
                all_results.table2_1[si][run] = .{
                    .success = false,
                    .error_msg = @errorName(err),
                };
                continue;
            };

            all_results.table2_1[si][run] = .{
                .success = true,
                .sub_stats = result.sub_stats,
            };
            all_results.table2_1_counts[si] += 1;
            success_count += 1;

            if (result.sub_stats) |stats| {
                last_rate = stats.msgs_per_sec;
            }
        }

        if (success_count > 0) {
            var rates: [MAX_RUNS]f64 = undefined;
            var rate_count: usize = 0;
            for (0..num_runs) |run| {
                if (all_results.table2_1[si][run].sub_stats) |s| {
                    rates[rate_count] = s.msgs_per_sec;
                    rate_count += 1;
                }
            }
            const median_rate = if (rate_count > 0)
                calculateMedian(rates[0..rate_count])
            else
                last_rate;

            var rate_buf: [32]u8 = undefined;
            ui.showSuccess(
                si + 1,
                total,
                sub_client.name(),
                fmtRate(&rate_buf, median_rate),
            );
        } else {
            ui.showFailure(si + 1, total, sub_client.name(), "all runs failed");
        }
    }

    printTable2Details(
        io,
        out,
        "2.1",
        "Zig std",
        opts,
        all_results.table2_1[0..],
        num_runs,
    );
    out.flush();
}

/// Print detailed Table 2 results.
fn printTable2Details(
    io: Io,
    out: *StdOut,
    table_num: []const u8,
    pub_name: []const u8,
    opts: BenchOpts,
    results: []const [MAX_RUNS]BenchRun,
    num_runs: u32,
) void {
    _ = out; // Using ui for ANSI output

    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };

    // Convert to old format
    var old_results: [5][MAX_RUNS]?BenchStats = .{.{null} ** MAX_RUNS} ** 5;
    for (0..6) |si| {
        for (0..num_runs) |run| {
            if (results[si][run].success) {
                old_results[si][run] = results[si][run].sub_stats;
            }
        }
    }

    var ui: TerminalUI = .init(io);

    // Print all runs table
    var title_buf: [64]u8 = undefined;
    const title = std.fmt.bufPrint(
        &title_buf,
        "Table {s}: All Runs ({s} publisher)",
        .{
            table_num,
            pub_name,
        },
    ) catch "Table: All Runs";
    ui.printHeader(title);

    const col_widths = [_]usize{ 12, 5, 20, 12, 10 };
    var printer = TablePrinter{ .ui = &ui, .col_widths = &col_widths };

    printer.printTop();
    printer.printHeaderRow(
        &.{ "Subscriber", "Run", "Rate", "Bandwidth", "Latency" },
    );
    printer.printSeparator();

    for (subscribers, 0..) |sub_client, si| {
        for (0..num_runs) |run| {
            if (old_results[si][run]) |stats| {
                var rate_buf: [24]u8 = undefined;
                var bw_buf: [16]u8 = undefined;
                var lat_buf: [12]u8 = undefined;
                var run_buf: [8]u8 = undefined;

                const rate_str = fmtRate(&rate_buf, stats.msgs_per_sec);
                const bw_str = blk: {
                    const mb =
                        if (stats.bandwidth_mb > 0)
                            stats.bandwidth_mb
                        else
                            stats.msgs_per_sec *
                                @as(f64, @floatFromInt(opts.size)) /
                                1_000_000.0;
                    break :blk std.fmt.bufPrint(
                        &bw_buf,
                        "{d:.0} MB/s",
                        .{mb},
                    ) catch "-";
                };
                const lat_str = if (stats.latency_us) |lat| std.fmt.bufPrint(
                    &lat_buf,
                    "{d:.2}us",
                    .{lat},
                ) catch "-" else "-";
                const run_str = std.fmt.bufPrint(
                    &run_buf,
                    "{d}",
                    .{run + 1},
                ) catch "?";

                printer.printRowHighlight(
                    &.{ sub_client.name(), run_str, rate_str, bw_str, lat_str },
                );
            }
        }
        // Separator between clients (not after last)
        if (si < subscribers.len - 1) {
            printer.printSeparator();
        }
    }
    printer.printBottom();

    // Print median results table
    var med_title_buf: [64]u8 = undefined;
    const med_title = std.fmt.bufPrint(
        &med_title_buf,
        "Table {s}: Median Results",
        .{table_num},
    ) catch
        "Table: Median Results";
    ui.printHeader(med_title);

    const med_widths = [_]usize{ 12, 20, 12, 10 };
    var med_printer = TablePrinter{ .ui = &ui, .col_widths = &med_widths };

    med_printer.printTop();
    med_printer.printHeaderRow(
        &.{ "Subscriber", "Rate", "Bandwidth", "Latency" },
    );
    med_printer.printSeparator();

    for (subscribers, 0..) |sub_client, si| {
        const median = calcMedianStats(old_results[si][0..num_runs]);
        if (median) |stats| {
            var rate_buf: [24]u8 = undefined;
            var bw_buf: [16]u8 = undefined;
            var lat_buf: [12]u8 = undefined;

            const rate_str = fmtRate(&rate_buf, stats.msgs_per_sec);
            const bw_str = blk: {
                const mb =
                    if (stats.bandwidth_mb > 0)
                        stats.bandwidth_mb
                    else
                        stats.msgs_per_sec *
                            @as(f64, @floatFromInt(opts.size)) /
                            1_000_000.0;
                break :blk std.fmt.bufPrint(
                    &bw_buf,
                    "{d:.0} MB/s",
                    .{mb},
                ) catch "-";
            };
            const lat_str = if (stats.latency_us) |lat| std.fmt.bufPrint(
                &lat_buf,
                "{d:.2}us",
                .{lat},
            ) catch "-" else "-";

            med_printer.printRowHighlight(
                &.{ sub_client.name(), rate_str, bw_str, lat_str },
            );
        } else {
            med_printer.printRow(&.{ sub_client.name(), "-", "-", "-" });
        }
    }
    med_printer.printBottom();
}

fn runTable2_2(
    allocator: Allocator,
    io: Io,
    opts: BenchOpts,
    ui: *TerminalUI,
    all_results: *AllResults,
    out: *StdOut,
) !void {
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;
    const total = subscribers.len;

    ui.printHeader("Table 2.2: Subscriber Comparison (Go publisher)");

    for (subscribers, 0..) |sub_client, si| {
        var success_count: usize = 0;
        var last_rate: f64 = 0;

        for (0..num_runs) |run| {
            ui.showRunning(si + 1, total, sub_client.name(), run + 1, num_runs);

            const result = runPubSub(
                allocator,
                io,
                .go,
                sub_client,
                opts,
            ) catch |err| {
                all_results.table2_2[si][run] = .{
                    .success = false,
                    .error_msg = @errorName(err),
                };
                continue;
            };

            all_results.table2_2[si][run] = .{
                .success = true,
                .sub_stats = result.sub_stats,
            };
            all_results.table2_2_counts[si] += 1;
            success_count += 1;

            if (result.sub_stats) |stats| {
                last_rate = stats.msgs_per_sec;
            }
        }

        if (success_count > 0) {
            var rates: [MAX_RUNS]f64 = undefined;
            var rate_count: usize = 0;
            for (0..num_runs) |run| {
                if (all_results.table2_2[si][run].sub_stats) |s| {
                    rates[rate_count] = s.msgs_per_sec;
                    rate_count += 1;
                }
            }
            const median_rate = if (rate_count > 0)
                calculateMedian(rates[0..rate_count])
            else
                last_rate;

            var rate_buf: [32]u8 = undefined;
            ui.showSuccess(
                si + 1,
                total,
                sub_client.name(),
                fmtRate(&rate_buf, median_rate),
            );
        } else {
            ui.showFailure(si + 1, total, sub_client.name(), "all runs failed");
        }
    }

    printTable2Details(
        io,
        out,
        "2.2",
        "Go",
        opts,
        all_results.table2_2[0..],
        num_runs,
    );
    out.flush();
}

fn printTable2RowRun(
    out: *StdOut,
    name: []const u8,
    run: usize,
    stats: ?BenchStats,
    size: usize,
) void {
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

    out.print(
        "| {s:<10} | {d:>3} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, run, rate_str, bw_str, lat_str },
    );
}

fn printTable2Row(
    out: *StdOut,
    name: []const u8,
    stats: ?BenchStats,
    size: usize,
) void {
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

    out.print(
        "| {s:<10} | {s:>18} | {s:>10} | {s:>8} |\n",
        .{ name, rate_str, bw_str, lat_str },
    );
}

fn runTable3(
    allocator: Allocator,
    io: Io,
    opts: BenchOpts,
    ui: *TerminalUI,
    all_results: *AllResults,
    out: *StdOut,
) !void {
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;
    const total = subscribers.len;

    ui.printHeader("Table 3: Fire Starter (io_uring direct)");

    for (subscribers, 0..) |sub_client, si| {
        var success_count: usize = 0;
        var last_rate: f64 = 0;

        for (0..num_runs) |run| {
            ui.showRunning(si + 1, total, sub_client.name(), run + 1, num_runs);

            const result = runFireStarterTest(
                allocator,
                io,
                sub_client,
                opts,
            ) catch |err| {
                all_results.table3[si][run] = .{
                    .success = false,
                    .error_msg = @errorName(err),
                };
                continue;
            };

            all_results.table3[si][run] = .{
                .success = true,
                .pub_stats = result.pub_stats,
                .sub_stats = result.sub_stats,
            };
            all_results.table3_counts[si] += 1;
            success_count += 1;

            if (result.sub_stats) |stats| {
                last_rate = stats.msgs_per_sec;
            }
        }

        if (success_count > 0) {
            var rates: [MAX_RUNS]f64 = undefined;
            var rate_count: usize = 0;
            for (0..num_runs) |run| {
                if (all_results.table3[si][run].sub_stats) |s| {
                    rates[rate_count] = s.msgs_per_sec;
                    rate_count += 1;
                }
            }
            const median_rate = if (rate_count > 0)
                calculateMedian(rates[0..rate_count])
            else
                last_rate;

            var rate_buf: [32]u8 = undefined;
            ui.showSuccess(
                si + 1,
                total,
                sub_client.name(),
                fmtRate(&rate_buf, median_rate),
            );
        } else {
            ui.showFailure(si + 1, total, sub_client.name(), "all runs failed");
        }
    }

    printTable3Details(io, out, opts, all_results);
    out.flush();
}

/// Print detailed Table 3 results.
fn printTable3Details(
    io: Io,
    out: *StdOut,
    opts: BenchOpts,
    all_results: *AllResults,
) void {
    _ = out; // Using ui for ANSI output

    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;

    // Convert to old format
    var results: [5][MAX_RUNS]?PubSubResult = .{.{null} ** MAX_RUNS} ** 5;
    for (0..6) |si| {
        for (0..num_runs) |run| {
            const r = all_results.table3[si][run];
            if (r.success) {
                results[si][run] = .{
                    .name = subscribers[si].name(),
                    .pub_stats = r.pub_stats,
                    .sub_stats = r.sub_stats,
                };
            }
        }
    }

    var ui: TerminalUI = .init(io);

    // Print all runs table
    var title_buf: [64]u8 = undefined;
    const title = std.fmt.bufPrint(
        &title_buf,
        "Table 3: All Runs (Fire Starter, {d} runs)",
        .{num_runs},
    ) catch
        "Table 3: All Runs";
    ui.printHeader(title);

    const col_widths = [_]usize{ 12, 5, 20, 20, 12 };
    var printer = TablePrinter{ .ui = &ui, .col_widths = &col_widths };

    printer.printTop();
    printer.printHeaderRow(
        &.{ "Subscriber", "Run", "Fire Starter", "Subscriber", "Bandwidth" },
    );
    printer.printSeparator();

    for (subscribers, 0..) |sub_client, si| {
        for (0..num_runs) |run| {
            if (results[si][run]) |result| {
                var pub_buf: [24]u8 = undefined;
                var sub_buf: [24]u8 = undefined;
                var bw_buf: [16]u8 = undefined;
                var run_buf: [8]u8 = undefined;

                const pub_str =
                    if (result.pub_stats) |s| fmtRate(
                        &pub_buf,
                        s.msgs_per_sec,
                    ) else "-";
                const sub_str = if (result.sub_stats) |s| fmtRate(
                    &sub_buf,
                    s.msgs_per_sec,
                ) else "-";
                const bw_str = if (result.sub_stats) |s| blk: {
                    const mb =
                        if (s.bandwidth_mb > 0)
                            s.bandwidth_mb
                        else
                            s.msgs_per_sec *
                                @as(f64, @floatFromInt(opts.size)) /
                                1_000_000.0;
                    break :blk std.fmt.bufPrint(
                        &bw_buf,
                        "{d:.0} MB/s",
                        .{mb},
                    ) catch "-";
                } else "-";
                const run_str = std.fmt.bufPrint(
                    &run_buf,
                    "{d}",
                    .{run + 1},
                ) catch "?";

                printer.printRowHighlight(
                    &.{ sub_client.name(), run_str, pub_str, sub_str, bw_str },
                );
            }
        }
        // Separator between clients (not after last)
        if (si < subscribers.len - 1) {
            printer.printSeparator();
        }
    }
    printer.printBottom();

    // Print median results table
    ui.printHeader("Table 3: Median Results");

    const med_widths = [_]usize{ 12, 20, 20, 12 };
    var med_printer = TablePrinter{ .ui = &ui, .col_widths = &med_widths };

    med_printer.printTop();
    med_printer.printHeaderRow(
        &.{ "Subscriber", "Fire Starter", "Subscriber", "Bandwidth" },
    );
    med_printer.printSeparator();

    for (subscribers, 0..) |sub_client, si| {
        const median = calcMedianPubSub(results[si][0..num_runs]);

        var pub_buf: [24]u8 = undefined;
        var sub_buf: [24]u8 = undefined;
        var bw_buf: [16]u8 = undefined;

        const pub_str = if (median.pub_stats) |s| fmtRate(
            &pub_buf,
            s.msgs_per_sec,
        ) else "-";
        const sub_str = if (median.sub_stats) |s| fmtRate(
            &sub_buf,
            s.msgs_per_sec,
        ) else "-";
        const bw_str = if (median.sub_stats) |s| blk: {
            const mb =
                if (s.bandwidth_mb > 0)
                    s.bandwidth_mb
                else
                    s.msgs_per_sec * @as(f64, @floatFromInt(opts.size)) /
                        1_000_000.0;
            break :blk std.fmt.bufPrint(
                &bw_buf,
                "{d:.0} MB/s",
                .{mb},
            ) catch "-";
        } else "-";

        med_printer.printRowHighlight(
            &.{ sub_client.name(), pub_str, sub_str, bw_str },
        );
    }
    med_printer.printBottom();
}

fn printTable3RowRun(
    out: *StdOut,
    name: []const u8,
    run: usize,
    result: PubSubResult,
    size: usize,
) void {
    var pub_buf: [24]u8 = undefined;
    var sub_buf: [24]u8 = undefined;
    var bw_buf: [16]u8 = undefined;

    const pub_str = if (result.pub_stats) |s|
        fmtRate(&pub_buf, s.msgs_per_sec)
    else
        "-";
    const sub_str = if (result.sub_stats) |s|
        fmtRate(&sub_buf, s.msgs_per_sec)
    else
        "-";

    const bw_str = if (result.sub_stats) |s| blk: {
        const mb = if (s.bandwidth_mb > 0)
            s.bandwidth_mb
        else
            s.msgs_per_sec * @as(f64, @floatFromInt(size)) / 1_000_000.0;
        break :blk std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{mb}) catch "-";
    } else "-";

    out.print(
        "| {s:<10} | {d:>3} | {s:>18} | {s:>18} | {s:>10} |\n",
        .{ name, run, pub_str, sub_str, bw_str },
    );
}

fn printTable3Row(
    out: *StdOut,
    name: []const u8,
    result: PubSubResult,
    size: usize,
) void {
    var pub_buf: [24]u8 = undefined;
    var sub_buf: [24]u8 = undefined;
    var bw_buf: [16]u8 = undefined;

    const pub_str = if (result.pub_stats) |s|
        fmtRate(&pub_buf, s.msgs_per_sec)
    else
        "-";
    const sub_str = if (result.sub_stats) |s|
        fmtRate(&sub_buf, s.msgs_per_sec)
    else
        "-";

    const bw_str = if (result.sub_stats) |s| blk: {
        const mb = if (s.bandwidth_mb > 0)
            s.bandwidth_mb
        else
            s.msgs_per_sec * @as(f64, @floatFromInt(size)) / 1_000_000.0;
        break :blk std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{mb}) catch "-";
    } else "-";

    out.print(
        "| {s:<10} | {s:>18} | {s:>18} | {s:>10} |\n",
        .{ name, pub_str, sub_str, bw_str },
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

/// Calculate median of a slice of f64 values.
fn calculateMedian(values: []f64) f64 {
    assert(values.len > 0);
    if (values.len == 1) return values[0];

    // Sort the values
    std.mem.sort(f64, values, {}, std.sort.asc(f64));

    const mid = values.len / 2;
    if (values.len % 2 == 0) {
        return (values[mid - 1] + values[mid]) / 2.0;
    } else {
        return values[mid];
    }
}

/// Calculate median of optional latency values (ignores nulls).
fn calculateMedianLatency(values: []?f64) ?f64 {
    var valid: [MAX_RUNS]f64 = undefined;
    var count: usize = 0;
    for (values) |v| {
        if (v) |lat| {
            valid[count] = lat;
            count += 1;
        }
    }
    if (count == 0) return null;
    return calculateMedian(valid[0..count]);
}

/// Generate markdown report file with all benchmark results.
fn generateMarkdown(
    io: Io,
    opts: BenchOpts,
    all_results: *AllResults,
    filename: []const u8,
) !void {
    assert(filename.len > 0);

    const Dir = std.Io.Dir;
    const file = try Dir.createFile(Dir.cwd(), io, filename, .{});
    defer file.close(io);

    var write_buf: [8192]u8 = undefined;
    var file_writer = file.writer(io, &write_buf);
    const writer = &file_writer.interface;

    // Header with metadata
    try writer.print("# NATS Performance Benchmark Results\n\n", .{});

    // Date/time using Instant
    if (std.time.Instant.now()) |instant| {
        const epoch_secs: u64 = @intCast(instant.timestamp.sec);
        const epoch = std.time.epoch.EpochSeconds{ .secs = epoch_secs };
        const day_secs = epoch.getDaySeconds();
        const year_day = epoch.getEpochDay().calculateYearDay();
        const month_day = year_day.calculateMonthDay();

        try writer.print("**Date:** {d:0>4}-{d:0>2}-{d:0>2}" ++
            " {d:0>2}:{d:0>2}:{d:0>2}\n", .{
            year_day.year,
            @intFromEnum(month_day.month),
            month_day.day_index + 1,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
        });
    } else |_| {
        try writer.print("**Date:** (unavailable)\n", .{});
    }

    // System info
    const uname = std.posix.uname();
    try writer.print("**System:** {s} {s}\n", .{
        @as([*:0]const u8, &uname.sysname),
        @as([*:0]const u8, &uname.release),
    });

    try writer.print("**Messages:** {d}\n", .{opts.num_msgs});
    try writer.print("**Payload:** {d} bytes\n", .{opts.size});
    try writer.print("**Runs:** {d}\n\n", .{opts.num_runs});

    // Table 1: Self Pub/Sub
    try writeTable1Markdown(writer, opts, all_results);

    // Table 2.1: Zig std publisher
    try writeTable2Markdown(
        writer,
        "2.1",
        "Zig std",
        opts,
        all_results.table2_1[0..],
    );

    // Table 2.2: Go publisher
    try writeTable2Markdown(
        writer,
        "2.2",
        "Go",
        opts,
        all_results.table2_2[0..],
    );

    // Table 3: Fire starter
    try writeTable3Markdown(writer, opts, all_results);

    try writer.print("---\n\nGenerated by `zig build run-perf-bench`\n", .{});

    // Flush buffered data to file
    try writer.flush();
}

fn writeTable1Markdown(
    writer: anytype,
    opts: BenchOpts,
    all_results: *AllResults,
) !void {
    const clients = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;

    try writer.print("## Table 1: Self Pub/Sub (Median Results)\n\n", .{});
    try writer.print(
        "| Client     | Publisher       | Subscriber      | Bandwidth  | Latency  |\n",
        .{},
    );
    try writer.print(
        "|------------|-----------------|-----------------|------------|----------|\n",
        .{},
    );

    for (clients, 0..) |client, ci| {
        // Calculate median
        var pub_rates: [MAX_RUNS]f64 = undefined;
        var sub_rates: [MAX_RUNS]f64 = undefined;
        var sub_bw: [MAX_RUNS]f64 = undefined;
        var pub_count: usize = 0;
        var sub_count: usize = 0;

        for (0..num_runs) |run| {
            const r = all_results.table1[ci][run];
            if (r.success) {
                if (r.pub_stats) |ps| {
                    pub_rates[pub_count] = ps.msgs_per_sec;
                    pub_count += 1;
                }
                if (r.sub_stats) |ss| {
                    sub_rates[sub_count] = ss.msgs_per_sec;
                    sub_bw[sub_count] = ss.bandwidth_mb;
                    sub_count += 1;
                }
            }
        }

        var pub_buf: [24]u8 = undefined;
        var sub_buf: [24]u8 = undefined;
        var bw_buf: [16]u8 = undefined;

        const pub_str = if (pub_count > 0)
            fmtRate(&pub_buf, calculateMedian(pub_rates[0..pub_count]))
        else
            "-";
        const sub_str = if (sub_count > 0)
            fmtRate(&sub_buf, calculateMedian(sub_rates[0..sub_count]))
        else
            "-";
        const bw_str = if (sub_count > 0) blk: {
            const mb = calculateMedian(sub_bw[0..sub_count]);
            break :blk std.fmt.bufPrint(&bw_buf, "{d:.0} MB/s", .{mb}) catch "-";
        } else "-";

        try writer.print("| {s:<10} | {s:>15} | {s:>15} | {s:>10} | -        |\n", .{
            client.name(),
            pub_str,
            sub_str,
            bw_str,
        });
    }
    try writer.print("\n", .{});
}

fn writeTable2Markdown(
    writer: anytype,
    table_num: []const u8,
    pub_name: []const u8,
    opts: BenchOpts,
    results: []const [MAX_RUNS]BenchRun,
) !void {
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;

    try writer.print("## Table {s}: Subscriber Comparison ({s} publisher)\n\n", .{
        table_num,
        pub_name,
    });
    try writer.print(
        "| Subscriber | Rate            | Bandwidth  | Latency  |\n",
        .{},
    );
    try writer.print(
        "|------------|-----------------|------------|----------|\n",
        .{},
    );

    for (subscribers, 0..) |sub, si| {
        var rates: [MAX_RUNS]f64 = undefined;
        var bw: [MAX_RUNS]f64 = undefined;
        var count: usize = 0;

        for (0..num_runs) |run| {
            const r = results[si][run];
            if (r.success) {
                if (r.sub_stats) |ss| {
                    rates[count] = ss.msgs_per_sec;
                    bw[count] = ss.bandwidth_mb;
                    count += 1;
                }
            }
        }

        var rate_buf: [24]u8 = undefined;
        var bw_buf: [16]u8 = undefined;

        const rate_str = if (count > 0)
            fmtRate(&rate_buf, calculateMedian(rates[0..count]))
        else
            "-";
        const bw_str = if (count > 0) blk: {
            const mb = calculateMedian(bw[0..count]);
            break :blk std.fmt.bufPrint(
                &bw_buf,
                "{d:.0} MB/s",
                .{mb},
            ) catch "-";
        } else "-";

        try writer.print("| {s:<10} | {s:>15} | {s:>10} | -        |\n", .{
            sub.name(),
            rate_str,
            bw_str,
        });
    }
    try writer.print("\n", .{});
}

fn writeTable3Markdown(
    writer: anytype,
    opts: BenchOpts,
    all_results: *AllResults,
) !void {
    const subscribers = [_]Client{ .zig_iou, .zig, .c, .rust, .go };
    const num_runs = opts.num_runs;

    try writer.print("## Table 3: Fire Starter (io_uring direct)\n\n", .{});
    try writer.print(
        "| Subscriber | Fire Starter    | Subscriber      | Bandwidth  |\n",
        .{},
    );
    try writer.print(
        "|------------|-----------------|-----------------|------------|\n",
        .{},
    );

    for (subscribers, 0..) |sub, si| {
        var pub_rates: [MAX_RUNS]f64 = undefined;
        var sub_rates: [MAX_RUNS]f64 = undefined;
        var sub_bw: [MAX_RUNS]f64 = undefined;
        var pub_count: usize = 0;
        var sub_count: usize = 0;

        for (0..num_runs) |run| {
            const r = all_results.table3[si][run];
            if (r.success) {
                if (r.pub_stats) |ps| {
                    pub_rates[pub_count] = ps.msgs_per_sec;
                    pub_count += 1;
                }
                if (r.sub_stats) |ss| {
                    sub_rates[sub_count] = ss.msgs_per_sec;
                    sub_bw[sub_count] = ss.bandwidth_mb;
                    sub_count += 1;
                }
            }
        }

        var pub_buf: [24]u8 = undefined;
        var sub_buf: [24]u8 = undefined;
        var bw_buf: [16]u8 = undefined;

        const pub_str = if (pub_count > 0)
            fmtRate(&pub_buf, calculateMedian(pub_rates[0..pub_count]))
        else
            "-";
        const sub_str = if (sub_count > 0)
            fmtRate(&sub_buf, calculateMedian(sub_rates[0..sub_count]))
        else
            "-";
        const bw_str = if (sub_count > 0) blk: {
            const mb = calculateMedian(sub_bw[0..sub_count]);
            break :blk std.fmt.bufPrint(
                &bw_buf,
                "{d:.0} MB/s",
                .{mb},
            ) catch "-";
        } else "-";

        try writer.print("| {s:<10} | {s:>15} | {s:>15} | {s:>10} |\n", .{
            sub.name(),
            pub_str,
            sub_str,
            bw_str,
        });
    }
    try writer.print("\n", .{});
}

test "parseZigOutput" {
    const output =
        "NATS publisher stats: 2891871 msgs/sec ~ 361484 KiB/sec ~ 0.35us";
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

test "parseFireStarterOutput" {
    const output =
        \\=== Fire Starter Statistics ===
        \\Messages:     1000000
        \\Total Bytes:  40000000
        \\Duration:     227.326 ms (227325966 ns)
        \\Throughput:   4398969.54 msg/s
        \\Bandwidth:    167.81 MB/s
        \\============================
    ;
    const stats = parseFireStarterOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 4398969.54), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 167.81), stats.bandwidth_mb, 0.01);
}

test "parseCOutput" {
    const output = "Received 100000 messages (3711494 msgs/sec)";
    const stats = parseCOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 3711494), stats.msgs_per_sec, 1);
}

test "parseRustOutput" {
    // Rust bench_pub/bench_sub output format
    const output =
        \\Results:
        \\  Duration:   33.529854ms
        \\  Messages:   100000
        \\  Msg/sec:    2982417
        \\  Throughput: 47.72 MB/s
    ;
    const stats = parseRustOutput(output) orelse unreachable;
    try std.testing.expectApproxEqAbs(@as(f64, 2982417), stats.msgs_per_sec, 1);
    try std.testing.expectApproxEqAbs(@as(f64, 47.72), stats.bandwidth_mb, 0.01);
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
    buildExeArgs(&ab, .zig, .publisher, .{}, &payload);
    try std.testing.expectEqual(@as(usize, 4), ab.count);
    try std.testing.expectEqualStrings("./zig-out/bin/bench-pub", ab.args[0]);
}

test "buildExeArgs go sub" {
    var payload: [64]u8 = undefined;
    var ab = ArgBuffer{};
    buildExeArgs(&ab, .go, .subscriber, .{}, &payload);
    try std.testing.expectEqual(@as(usize, 7), ab.count);
    try std.testing.expectEqualStrings("nats", ab.args[0]);
    try std.testing.expectEqualStrings("bench", ab.args[1]);
    try std.testing.expectEqualStrings("sub", ab.args[2]);
}

test "buildExeArgs c pub generates payload" {
    var payload: [64]u8 = undefined;
    var ab = ArgBuffer{};
    buildExeArgs(&ab, .c, .publisher, .{ .size = 16 }, &payload);
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
