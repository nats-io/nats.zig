//! NATS Async Publisher Benchmark
//!
//! Measures publish throughput using Client.
//! Usage: bench-pub-async <subject> [--msgs=N] [--size=NB]

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

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = parseArgs(allocator) catch |err| {
        printUsage();
        return err;
    };

    try runBenchmark(allocator, config);
}

fn parseArgs(allocator: Allocator) !BenchConfig {
    var args = try std.process.argsWithAllocator(allocator);
    defer args.deinit();

    _ = args.next(); // skip program name

    var config = BenchConfig{ .subject = "" };

    while (args.next()) |arg| {
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

    // Print start message with current time
    if (bench.TimeOfDay.now()) |tod| {
        var buf: [8]u8 = undefined;
        std.debug.print(
            "{s} Starting async publisher benchmark " ++
                "[msgs={d}, size={d}B, subject={s}]\n",
            .{ tod.format(&buf), config.msgs, config.size, config.subject },
        );
    } else {
        std.debug.print(
            "Starting async publisher benchmark " ++
                "[msgs={d}, size={d}B, subject={s}]\n",
            .{ config.msgs, config.size, config.subject },
        );
    }

    // Create I/O
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Connect using Client
    const client = nats.Client.connect(allocator, io, config.url, .{
        .name = "bench-pub-async",
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    // Create payload buffer
    const payload = try allocator.alloc(u8, config.size);
    defer allocator.free(payload);
    @memset(payload, 'A');

    // Start timing
    var timer = std.time.Timer.start() catch {
        std.debug.print("Timer unavailable\n", .{});
        return error.TimerUnavailable;
    };

    // Publish loop - tight loop, no syscalls per message
    var i: u64 = 0;
    while (i < config.msgs) : (i += 1) {
        client.publish(config.subject, payload) catch |err| {
            std.debug.print("Publish failed at msg {d}: {}\n", .{ i, err });
            return err;
        };
    }

    // Single flush at end
    client.flush() catch |err| {
        std.debug.print("Flush failed: {}\n", .{err});
        return err;
    };

    // Stop timing and print stats
    const elapsed_ns = timer.read();
    const stats = bench.Stats{
        .elapsed_ns = elapsed_ns,
        .msg_count = config.msgs,
        .total_bytes = config.msgs * config.size,
    };
    stats.print("async publisher");
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-pub-async <subject> [options]
        \\
        \\Async publisher benchmark using ClientAsync.
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
        \\  bench-pub-async test.subject
        \\  bench-pub-async test.subject --msgs=1000000 --size=256B
        \\  bench-pub-async test.subject --size=1K
        \\
    , .{});
}
