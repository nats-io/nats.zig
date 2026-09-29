//! Pre-port footprint baseline: one idle connected client.
//!
//! Connects, subscribes to a single subject with a manual (non-callback)
//! subscription, then idles. During the idle window it samples the process's
//! own `/proc/self` accounting so the numbers reflect exactly what a single
//! embedded client costs: resident/virtual memory, thread count, open fds, and
//! idle CPU. These figures are the baseline the evented port is compared against.
//!
//! URL comes from $NATS_URL (default TCP localhost) so the same binary measures
//! both transports. Run via `zig build bench-footprint`.

const std = @import("std");
const nats = @import("nats");
const io_backend = @import("io_backend");

const Io = std.Io;

const default_url = "nats://127.0.0.1:4222";
const idle_seconds = 10;
const ns_per_s = 1_000_000_000;

/// USER_HZ: /proc/*/stat reports CPU time in clock ticks. The kernel exposes
/// this as a fixed 100 on Linux regardless of CONFIG_HZ, so we avoid linking
/// libc just for sysconf(_SC_CLK_TCK).
const user_hz = 100;

const Footprint = struct {
    vm_rss_kb: u64,
    vm_size_kb: u64,
    threads: u64,
    open_fds: usize,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const url = init.environ_map.get("NATS_URL") orelse default_url;

    var backend: io_backend.Backend = undefined;
    try io_backend.init(&backend, allocator);
    defer backend.deinit();
    const io = backend.io();

    const client = try nats.Client.connect(allocator, io, url, .{
        .name = "bench-footprint",
    });
    defer client.deinit();

    const sub = try client.subscribeSync("bench.footprint");
    defer sub.deinit();

    // Confirm the subscription reached the server before measuring, so the
    // steady state is fully established.
    try client.flush(ns_per_s);

    const cpu_start = try readCpuTicks(io);
    const wall_start = Io.Timestamp.now(io, .awake).nanoseconds;

    try io.sleep(Io.Duration.fromSeconds(idle_seconds), .awake);

    const wall_end = Io.Timestamp.now(io, .awake).nanoseconds;
    const cpu_end = try readCpuTicks(io);
    const fp = try readFootprint(io);

    const wall_ns: u64 = @intCast(wall_end - wall_start);
    const cpu_ns: u64 = (cpu_end - cpu_start) * (ns_per_s / user_hz);
    const idle_cpu_pct: f64 =
        @as(f64, @floatFromInt(cpu_ns)) / @as(f64, @floatFromInt(wall_ns)) * 100.0;

    std.debug.print(
        \\footprint (idle, backend={s}, url={s})
        \\  VmRSS   : {d} kB
        \\  VmSize  : {d} kB
        \\  Threads : {d}
        \\  open fds: {d} (includes the enumerator's own dir handle)
        \\  idle CPU: {d:.3} % over {d}s
        \\
    , .{ io_backend.name, url, fp.vm_rss_kb, fp.vm_size_kb, fp.threads, fp.open_fds, idle_cpu_pct, idle_seconds });
}

/// Sum of utime+stime (self) from /proc/self/stat, in clock ticks. The `comm`
/// field can contain spaces and parentheses, so we scan past the final ')'
/// before splitting fields.
fn readCpuTicks(io: Io) !u64 {
    var buf: [4096]u8 = undefined;
    const stat = try Io.Dir.cwd().readFile(io, "/proc/self/stat", &buf);

    const rparen = std.mem.lastIndexOfScalar(u8, stat, ')') orelse
        return error.UnexpectedStatFormat;
    var it = std.mem.tokenizeScalar(u8, stat[rparen + 1 ..], ' ');

    // After ')': state, ppid, pgrp, session, tty_nr, tpgid, flags, minflt,
    // cminflt, majflt, cmajflt, utime, stime, ... — utime is the 12th token.
    var idx: usize = 0;
    var utime: ?u64 = null;
    var stime: ?u64 = null;
    while (it.next()) |tok| : (idx += 1) {
        if (idx == 11) utime = try std.fmt.parseInt(u64, tok, 10);
        if (idx == 12) {
            stime = try std.fmt.parseInt(u64, tok, 10);
            break;
        }
    }
    return (utime orelse return error.UnexpectedStatFormat) +
        (stime orelse return error.UnexpectedStatFormat);
}

fn readFootprint(io: Io) !Footprint {
    var buf: [8192]u8 = undefined;
    const status = try Io.Dir.cwd().readFile(io, "/proc/self/status", &buf);

    var fp: Footprint = .{ .vm_rss_kb = 0, .vm_size_kb = 0, .threads = 0, .open_fds = 0 };
    var lines = std.mem.tokenizeScalar(u8, status, '\n');
    while (lines.next()) |line| {
        if (parseKbField(line, "VmRSS:")) |v| fp.vm_rss_kb = v;
        if (parseKbField(line, "VmSize:")) |v| fp.vm_size_kb = v;
        if (parseKbField(line, "Threads:")) |v| fp.threads = v;
    }

    fp.open_fds = try countFds(io);
    return fp;
}

/// Parse `Name:\t   <number>[ kB]`. Returns null if `line` is not the field.
fn parseKbField(line: []const u8, name: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, line, name)) return null;
    var it = std.mem.tokenizeAny(u8, line[name.len..], " \t");
    const num = it.next() orelse return null;
    return std.fmt.parseInt(u64, num, 10) catch null;
}

fn countFds(io: Io) !usize {
    // Run on a fiber: the directory close is a grouped op that zio's blocking-
    // mode fallback (taken for io issued off the foreign main thread) cannot
    // service. On an executor it uses the normal evented path; Threaded runs it
    // on a pool thread. Mirrors server_manager.deleteTreeViaLoop.
    var future = try io.concurrent(countFdsInner, .{io});
    return future.await(io);
}

fn countFdsInner(io: Io) !usize {
    var dir = try Io.Dir.cwd().openDir(io, "/proc/self/fd", .{ .iterate = true });
    defer dir.close(io);
    var it = dir.iterate();
    var count: usize = 0;
    while (try it.next(io)) |_| count += 1;
    return count;
}
