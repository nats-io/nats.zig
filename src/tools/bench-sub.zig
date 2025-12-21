//! NATS Subscriber Benchmark
//!
//! Measures subscribe throughput from a NATS server.
//! Usage: bench-sub <subject> [--msgs=N]

const std = @import("std");
const nats = @import("nats");
const assert = std.debug.assert;

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

    // Print start message with current time
    const instant = std.time.Instant.now() catch {
        std.debug.print("Starting subscriber benchmark " ++
            "[msgs={d}, subject={s}]\n", .{ config.msgs, config.subject });
        return error.ClockUnavailable;
    };
    const secs: u64 = @intCast(instant.timestamp.sec);
    const hours: u64 = @mod(@divFloor(secs, 3600), 24);
    const minutes: u64 = @mod(@divFloor(secs, 60), 60);
    const seconds: u64 = @mod(secs, 60);

    std.debug.print(
        "{d:0>2}:{d:0>2}:{d:0>2} Starting subscriber benchmark " ++
            "[msgs={d}, subject={s}]\n",
        .{ hours, minutes, seconds, config.msgs, config.subject },
    );

    // Create I/O and connect
    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), config.url, .{
        .name = "bench-sub",
    }) catch |err| {
        std.debug.print("Failed to connect: {}\n", .{err});
        return err;
    };
    defer client.deinit(allocator);

    // Subscribe - returns *Subscription for Go-style polling
    const sub = client.subscribe(allocator, config.subject) catch |err| {
        std.debug.print("Subscribe failed: {}\n", .{err});
        return err;
    };
    defer sub.deinit(allocator);

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

    // Go-style receive loop with timeout
    while (msg_count < config.msgs) {
        // Block until message or timeout (15 seconds)
        const msg = sub.nextMessage(allocator, .{ .timeout_ms = 15000 }) catch |err| {
            std.debug.print("Receive error: {}\n", .{err});
            return err;
        };

        if (msg) |m| {
            // Start timer on first message
            if (timer == null) {
                timer = std.time.Timer.start() catch {
                    std.debug.print("Timer unavailable\n", .{});
                    return error.TimerUnavailable;
                };
                std.debug.print("First message received, timing...\n", .{});
            }

            msg_count += 1;
            total_bytes += m.data.len;

            // Progress every 10%
            if (config.progress and progress_interval > 0) {
                if (msg_count % progress_interval == 0) {
                    const pct = msg_count * 100 / config.msgs;
                    std.debug.print("  {d}% ({d}/{d})\n", .{
                        pct,
                        msg_count,
                        config.msgs,
                    });
                }
            }

            // Free message data
            m.deinit();
        } else {
            std.debug.print("Timeout waiting for messages\n", .{});
            break;
        }
    }

    // Stop timing
    if (timer) |*t| {
        const elapsed_ns = t.read();
        printStats(elapsed_ns, msg_count, total_bytes);
    } else {
        std.debug.print("No messages received\n", .{});
    }
}

fn printStats(elapsed_ns: u64, msg_count: u64, total_bytes: u64) void {
    assert(elapsed_ns > 0);
    assert(msg_count > 0);

    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const msgs_per_sec = @as(f64, @floatFromInt(msg_count)) / elapsed_s;
    const kib_per_sec = @as(f64, @floatFromInt(total_bytes)) / elapsed_s / 1024.0;
    const avg_latency_us = elapsed_s * 1e6 / @as(f64, @floatFromInt(msg_count));

    std.debug.print(
        "\nNATS subscriber stats: {d:.0} msgs/sec ~ {d:.0} KiB/sec ~ {d:.2}us\n",
        .{ msgs_per_sec, kib_per_sec, avg_latency_us },
    );
    std.debug.print(
        "  Total: {d} messages, {d} bytes in {d:.3}s\n",
        .{ msg_count, total_bytes, elapsed_s },
    );
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-sub <subject> [options]
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
        \\  bench-sub test.subject --msgs=1000000 --no-progress
        \\  bench-sub "test.>" --msgs=50000
        \\
    , .{});
}
