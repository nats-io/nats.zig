//! NATS Zero-Copy Subscriber Benchmark
//!
//! Measures subscribe throughput using Client with zero-copy nextRef().
//! No allocations in hot path - slices point directly to read buffer.
//! Usage: bench-sub <subject> [--msgs=N]

const std = @import("std");
const nats = @import("nats");
const assert = std.debug.assert;
const bench = @import("bench_common.zig");

const Allocator = std.mem.Allocator;

const BenchConfig = struct {
    subject: []const u8,
    msgs: u64 = 100_000,
    url: []const u8 = "nats://127.0.0.1:4222",
    progress: bool = true,
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
        } else if (std.mem.startsWith(u8, arg, "--url=")) {
            config.url = arg[6..];
        } else if (std.mem.eql(u8, arg, "--progress")) {
            config.progress = true;
        } else if (std.mem.eql(u8, arg, "--no-progress")) {
            config.progress = false;
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
            "{s} Starting zero-copy subscriber benchmark " ++
                "[msgs={d}, subject={s}]\n",
            .{ tod.format(&buf), config.msgs, config.subject },
        );
    } else {
        std.debug.print(
            "Starting zero-copy subscriber benchmark " ++
                "[msgs={d}, subject={s}]\n",
            .{ config.msgs, config.subject },
        );
    }

    // Create I/O
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Connect using Client in sync mode for zero-copy nextRefBlock()
    const client = nats.Client.connect(allocator, io, config.url, .{
        .name = "bench-sub",
        .sync_mode = true,
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    // Subscribe
    var sub = client.subscribe(allocator, config.subject) catch |err| {
        std.debug.print("Subscribe failed: {}\n", .{err});
        return err;
    };
    defer sub.deinit(allocator);

    // Flush subscription registration
    client.flush() catch |err| {
        std.debug.print("Flush failed: {}\n", .{err});
        return err;
    };

    std.debug.print("Subscribed to '{s}', waiting for messages...\n", .{
        config.subject,
    });

    // Timer starts on first message
    var timer: ?std.time.Timer = null;
    var msg_count: u64 = 0;
    var total_bytes: u64 = 0;

    // Progress interval
    const progress_interval = config.msgs / 10;
    var last_progress: u64 = 0;

    // Zero-copy receive loop using nextRefBlock()
    // No allocations - slices point directly to read buffer
    // 5 second timeout to wait for publisher to start
    while (msg_count < config.msgs) {
        const ref = sub.nextRefBlock(5000) catch |err| {
            std.debug.print("Receive error: {}\n", .{err});
            return err;
        } orelse {
            std.debug.print("Timeout or connection closed\n", .{});
            break;
        };

        // Start timer on first message
        if (timer == null) {
            timer = std.time.Timer.start() catch {
                std.debug.print("Timer unavailable\n", .{});
                return error.TimerUnavailable;
            };
            std.debug.print("First message received, timing...\n", .{});
        }

        // Count message - no deinit needed, buffer managed by client
        msg_count += 1;
        total_bytes += ref.data.len;

        // Progress every 10%
        if (config.progress and progress_interval > 0) {
            const current_progress = msg_count / progress_interval;
            if (current_progress > last_progress) {
                last_progress = current_progress;
                const pct = msg_count * 100 / config.msgs;
                std.debug.print("  {d}% ({d}/{d})\n", .{
                    pct,
                    msg_count,
                    config.msgs,
                });
            }
        }
    }

    // Stop timing and print stats
    if (timer) |*t| {
        const elapsed_ns = t.read();
        const stats = bench.Stats{
            .elapsed_ns = elapsed_ns,
            .msg_count = msg_count,
            .total_bytes = total_bytes,
        };
        stats.print("zero-copy subscriber");
    } else {
        std.debug.print("No messages received\n", .{});
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-sub <subject> [options]
        \\
        \\Zero-copy subscriber benchmark using nextRef().
        \\No allocations in hot path - maximum throughput.
        \\
        \\Arguments:
        \\  <subject>       Subject to subscribe to (required)
        \\
        \\Options:
        \\  --msgs=N        Number of messages to receive (default: 100000)
        \\  --url=URL       NATS server URL (default: nats://127.0.0.1:4222)
        \\  --[no-]progress Show/hide progress output (default: show)
        \\
        \\Examples:
        \\  bench-sub test.subject
        \\  bench-sub test.subject --msgs=1000000
        \\
    , .{});
}
