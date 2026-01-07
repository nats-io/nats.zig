//! NATS Async Subscriber Benchmark
//!
//! Measures subscribe throughput using ClientAsync with Io.Queue.
//! Background reader task pre-routes messages for maximum throughput.
//! Usage: bench-sub-async <subject> [--msgs=N]

const std = @import("std");
const nats = @import("nats");
const assert = std.debug.assert;
const bench = @import("bench_common.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

const BenchConfig = struct {
    subject: []const u8,
    msgs: u64 = 100_000,
    url: []const u8 = "nats://127.0.0.1:4222",
    progress: bool = true,
    queue_size: u16 = 4096, // Larger queue for burst handling
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
        } else if (std.mem.startsWith(u8, arg, "--queue-size=")) {
            const val = arg[13..];
            config.queue_size = std.fmt.parseInt(u16, val, 10) catch {
                return error.InvalidArgument;
            };
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
            "{s} Starting async subscriber benchmark " ++
                "[msgs={d}, subject={s}, queue={d}]\n",
            .{ tod.format(&buf), config.msgs, config.subject, config.queue_size },
        );
    } else {
        std.debug.print(
            "Starting async subscriber benchmark " ++
                "[msgs={d}, subject={s}, queue={d}]\n",
            .{ config.msgs, config.subject, config.queue_size },
        );
    }

    // Create I/O
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Connect using ClientAsync with larger queue for throughput
    const client = nats.ClientAsync.connect(allocator, io, config.url, .{
        .name = "bench-sub-async",
        .async_queue_size = config.queue_size,
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    // Subscribe - returns Sub with Io.Queue
    const sub = client.subscribe(allocator, config.subject) catch |err| {
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

    // Batch receive buffer - get multiple messages per queue operation
    const BATCH_SIZE = 64;
    var batch: [BATCH_SIZE]nats.client_async.Message = undefined;

    // Optimal receive loop using batch receive
    // Gets up to BATCH_SIZE messages per queue operation
    while (msg_count < config.msgs) {
        // Batch receive - waits for at least 1, returns up to BATCH_SIZE
        const n = sub.nextBatch(io, &batch) catch |err| {
            if (err == error.Closed) {
                std.debug.print("Connection closed\n", .{});
                break;
            }
            std.debug.print("Receive error: {}\n", .{err});
            return err;
        };

        // Start timer on first message
        if (timer == null) {
            timer = std.time.Timer.start() catch {
                std.debug.print("Timer unavailable\n", .{});
                return error.TimerUnavailable;
            };
            std.debug.print("First message received, timing...\n", .{});
        }

        // Process batch
        for (batch[0..n]) |*msg| {
            msg_count += 1;
            total_bytes += msg.data.len;
            msg.deinit(allocator);
        }

        // Progress every 10%
        if (config.progress and progress_interval > 0) {
            if (msg_count >= progress_interval and
                (msg_count - n) / progress_interval < msg_count / progress_interval)
            {
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
        stats.print("async subscriber");
    } else {
        std.debug.print("No messages received\n", .{});
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-sub-async <subject> [options]
        \\
        \\Async subscriber benchmark using ClientAsync with Io.Queue.
        \\Background reader task pre-routes messages for maximum throughput.
        \\
        \\Arguments:
        \\  <subject>       Subject to subscribe to (required)
        \\
        \\Options:
        \\  --msgs=N        Number of messages to receive (default: 100000)
        \\  --url=URL       NATS server URL (default: nats://127.0.0.1:4222)
        \\  --queue-size=N  Per-subscription queue size (default: 4096)
        \\  --[no-]progress Show/hide progress output (default: show)
        \\
        \\Examples:
        \\  bench-sub-async test.subject
        \\  bench-sub-async test.subject --msgs=1000000
        \\  bench-sub-async "test.>" --queue-size=8192
        \\
    , .{});
}
