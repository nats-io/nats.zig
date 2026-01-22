//! NATS Subscriber Benchmark
//!
//! Measures subscribe throughput using Client.
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
    queue_size: u32 = 8192,
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
        } else if (std.mem.startsWith(u8, arg, "--url=")) {
            config.url = arg[6..];
        } else if (std.mem.eql(u8, arg, "--progress")) {
            config.progress = true;
        } else if (std.mem.eql(u8, arg, "--no-progress")) {
            config.progress = false;
        } else if (std.mem.startsWith(u8, arg, "--queue-size=")) {
            const val = arg[13..];
            config.queue_size = std.fmt.parseInt(u32, val, 10) catch {
                return error.InvalidArgument;
            };
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

    const queue_size: u32 = if (config.queue_size > 0)
        config.queue_size
    else blk: {
        const auto_size = config.msgs;
        const clamped = @min(auto_size, std.math.maxInt(u32));
        break :blk clamped;
    };

    if (bench.TimeOfDay.now()) |tod| {
        var buf: [8]u8 = undefined;
        std.debug.print(
            "{s} Starting subscriber benchmark " ++
                "[msgs={d}, queue={d}, subject={s}]\n",
            .{ tod.format(&buf), config.msgs, queue_size, config.subject },
        );
    } else {
        std.debug.print(
            "Starting subscriber benchmark " ++
                "[msgs={d}, queue={d}, subject={s}]\n",
            .{ config.msgs, queue_size, config.subject },
        );
    }

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    const client = nats.Client.connect(allocator, io, config.url, .{
        .name = "bench-sub",
        .sub_queue_size = queue_size,
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    var sub = client.subscribe(allocator, config.subject) catch |err| {
        std.debug.print("Subscribe failed: {}\n", .{err});
        return err;
    };
    defer sub.deinit(allocator);

    client.flush(allocator) catch |err| {
        std.debug.print("Flush failed: {}\n", .{err});
        return err;
    };

    std.debug.print("Subscribed to '{s}', waiting for messages...\n", .{
        config.subject,
    });

    var timer: ?std.time.Timer = null;
    var msg_count: u64 = 0;
    var total_bytes: u64 = 0;

    const progress_interval = config.msgs / 10;
    var last_progress: u64 = 0;

    var batch_buf: [64]nats.Client.Message = undefined;
    while (msg_count < config.msgs) {
        const batch_count = sub.tryNextBatch(&batch_buf);
        if (batch_count > 0) {
            if (timer == null) {
                timer = std.time.Timer.start() catch {
                    std.debug.print("Timer unavailable\n", .{});
                    return error.TimerUnavailable;
                };
                std.debug.print("First message received, timing...\n", .{});
            }

            for (batch_buf[0..batch_count]) |*msg| {
                msg_count += 1;
                total_bytes += msg.data.len;
                msg.deinit(allocator);
            }

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
            continue;
        }

        const msg = sub.nextWithTimeout(allocator, 5000) catch |err| {
            std.debug.print("Receive error: {}\n", .{err});
            return err;
        } orelse {
            std.debug.print("Timeout or connection closed\n", .{});
            break;
        };
        defer msg.deinit(allocator);

        if (timer == null) {
            timer = std.time.Timer.start() catch {
                std.debug.print("Timer unavailable\n", .{});
                return error.TimerUnavailable;
            };
            std.debug.print("First message received, timing...\n", .{});
        }

        msg_count += 1;
        total_bytes += msg.data.len;

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

    if (timer) |*t| {
        const elapsed_ns = t.read();
        const stats = bench.Stats{
            .elapsed_ns = elapsed_ns,
            .msg_count = msg_count,
            .total_bytes = total_bytes,
        };
        stats.print("subscriber");

        const dropped = sub.getDroppedCount();
        const alloc_failed = sub.getAllocFailedCount();
        if (dropped > 0 or alloc_failed > 0) {
            std.debug.print(
                "  WARNING: dropped={d}, alloc_failed={d}\n",
                .{ dropped, alloc_failed },
            );
        }
    } else {
        std.debug.print("No messages received\n", .{});
    }
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-sub <subject> [options]
        \\
        \\Subscriber benchmark for measuring throughput.
        \\
        \\Arguments:
        \\  <subject>       Subject to subscribe to (required)
        \\
        \\Options:
        \\  --msgs=N        Number of messages to receive (default: 100000)
        \\  --queue-size=N  Subscription queue size (default: auto = 1.5x msgs)
        \\  --url=URL       NATS server URL (default: nats://127.0.0.1:4222)
        \\  --[no-]progress Show/hide progress output (default: show)
        \\
        \\Examples:
        \\  bench-sub test.subject
        \\  bench-sub test.subject --msgs=1000000
        \\  bench-sub test.subject --msgs=100000 --queue-size=200000
        \\
    , .{});
}
