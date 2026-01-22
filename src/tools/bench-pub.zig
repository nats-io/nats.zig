//! NATS Publisher Benchmark
//!
//! Measures publish throughput using Client.
//! Usage: bench-pub <subject> [--msgs=N] [--size=NB]

const std = @import("std");
const nats = @import("nats");
const assert = std.debug.assert;
const bench = @import("bench_common.zig");

const Allocator = std.mem.Allocator;

const BenchConfig = struct {
    subject: []const u8,
    msgs: u64 = 100_000,
    size: usize = 128,
    url: []const u8 = "nats://127.0.0.1:4222",
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    const config = parseArgs(init) catch |err| {
        printUsage();
        return err;
    };

    try runBenchmark(allocator, config);
}

fn parseArgs(init: std.process.Init) !BenchConfig {
    var args_iter = std.process.Args.Iterator.initAllocator(
        init.minimal.args,
        init.gpa,
    ) catch |err| {
        std.process.fatal("failed to init args: {}", .{err});
    };
    defer args_iter.deinit();

    _ = args_iter.skip(); // skip program name

    var config = BenchConfig{ .subject = "" };

    while (args_iter.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--msgs=")) {
            const val = arg[7..];
            config.msgs = std.fmt.parseInt(u64, val, 10) catch {
                return error.InvalidArgument;
            };
        } else if (std.mem.startsWith(u8, arg, "--size=")) {
            const val = arg[7..];
            config.size = bench.parseSizeArg(val) catch {
                return error.InvalidArgument;
            };
        } else if (std.mem.startsWith(u8, arg, "--url=")) {
            config.url = arg[6..];
        } else if (!std.mem.startsWith(u8, arg, "--")) {
            config.subject = arg;
        }
    }

    if (config.subject.len == 0) {
        return error.MissingSubject;
    }

    return config;
}

fn runBenchmark(allocator: Allocator, config: BenchConfig) !void {
    assert(config.subject.len > 0);
    assert(config.msgs > 0);

    if (bench.TimeOfDay.now()) |tod| {
        var buf: [8]u8 = undefined;
        std.debug.print(
            "{s} Starting publisher benchmark " ++
                "[msgs={d}, size={d}B, subject={s}]\n",
            .{ tod.format(&buf), config.msgs, config.size, config.subject },
        );
    } else {
        std.debug.print(
            "Starting publisher benchmark " ++
                "[msgs={d}, size={d}B, subject={s}]\n",
            .{ config.msgs, config.size, config.subject },
        );
    }

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, config.url, .{
        .name = "bench-pub",
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    const payload = try allocator.alloc(u8, config.size);
    defer allocator.free(payload);
    @memset(payload, 'A');

    var timer = std.time.Timer.start() catch {
        std.debug.print("Timer unavailable\n", .{});
        return error.TimerUnavailable;
    };

    var i: u64 = 0;
    while (i < config.msgs) : (i += 1) {
        client.publish(config.subject, payload) catch |err| {
            std.debug.print("Publish failed at msg {d}: {}\n", .{ i, err });
            return err;
        };
    }

    client.flush(allocator) catch |err| {
        std.debug.print("Flush failed: {}\n", .{err});
        return err;
    };

    const elapsed_ns = timer.read();
    const stats = bench.Stats{
        .elapsed_ns = elapsed_ns,
        .msg_count = config.msgs,
        .total_bytes = config.msgs * config.size,
    };
    stats.print("publisher");
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-pub <subject> [options]
        \\
        \\Publisher benchmark.
        \\
        \\Arguments:
        \\  <subject>       Subject to publish to (required)
        \\
        \\Options:
        \\  --msgs=N        Number of messages (default: 100000)
        \\  --size=NB       Message size in bytes (default: 128B)
        \\                  Supports suffixes: B, K, KB, M, MB
        \\  --url=URL       NATS server URL (default: nats://127.0.0.1:4222)
        \\
        \\Examples:
        \\  bench-pubtest.subject
        \\  bench-pubtest.subject --msgs=1000000 --size=256B
        \\  bench-pubtest.subject --size=1K
        \\
    , .{});
}
