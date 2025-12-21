//! NATS Publisher Benchmark
//!
//! Measures publish throughput to a NATS server.
//! Usage: bench-pub <subject> [--msgs=N] [--size=NB]

const std = @import("std");
const nats = @import("nats");
const assert = std.debug.assert;

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
            config.size = parseSizeArg(val) catch {
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

/// Parses size argument like "128", "128B", "1K", "1KB", "1M", "1MB"
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

fn runBenchmark(allocator: Allocator, config: BenchConfig) !void {
    assert(config.subject.len > 0);
    assert(config.msgs > 0);

    // Print start message with current time
    const instant = std.time.Instant.now() catch {
        std.debug.print("Starting publisher benchmark " ++
            "[msgs={d}, size={d}B, subject={s}]\n", .{
            config.msgs,
            config.size,
            config.subject,
        });
        return error.ClockUnavailable;
    };
    const secs: u64 = @intCast(instant.timestamp.sec);
    const hours: u64 = @mod(@divFloor(secs, 3600), 24);
    const minutes: u64 = @mod(@divFloor(secs, 60), 60);
    const seconds: u64 = @mod(secs, 60);

    std.debug.print(
        "{d:0>2}:{d:0>2}:{d:0>2} Starting publisher benchmark " ++
            "[msgs={d}, size={d}B, subject={s}]\n",
        .{ hours, minutes, seconds, config.msgs, config.size, config.subject },
    );

    // Create I/O and connect
    var io = std.Io.Threaded.init(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), config.url, .{
        .name = "bench-pub",
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

    // Publish loop
    var i: u64 = 0;
    while (i < config.msgs) : (i += 1) {
        client.publish(config.subject, payload) catch |err| {
            std.debug.print("Publish failed at msg {d}: {}\n", .{ i, err });
            return err;
        };
    }

    // Flush
    client.flush() catch |err| {
        std.debug.print("Flush failed: {}\n", .{err});
        return err;
    };

    // Stop timing
    const elapsed_ns = timer.read();

    // Print stats
    printStats(elapsed_ns, config.msgs, config.size);
}

fn printStats(elapsed_ns: u64, msg_count: u64, msg_size: usize) void {
    assert(elapsed_ns > 0);
    assert(msg_count > 0);

    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1e9;
    const msgs_per_sec = @as(f64, @floatFromInt(msg_count)) / elapsed_s;
    const bytes_total = msg_count * msg_size;
    const kib_per_sec = @as(f64, @floatFromInt(bytes_total)) / elapsed_s / 1024.0;
    const avg_latency_us = elapsed_s * 1e6 / @as(f64, @floatFromInt(msg_count));

    std.debug.print(
        "\nNATS publisher stats: {d:.0} msgs/sec ~ {d:.0} KiB/sec ~ {d:.2}us\n",
        .{ msgs_per_sec, kib_per_sec, avg_latency_us },
    );
    std.debug.print(
        "  Total: {d} messages, {d} bytes in {d:.3}s\n",
        .{ msg_count, bytes_total, elapsed_s },
    );
}

fn printUsage() void {
    std.debug.print(
        \\Usage: bench-pub <subject> [options]
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
        \\  bench-pub test.subject
        \\  bench-pub test.subject --msgs=1000000 --size=256B
        \\  bench-pub test.subject --size=1K
        \\
    , .{});
}

test "parseSizeArg" {
    try std.testing.expectEqual(@as(usize, 128), try parseSizeArg("128"));
    try std.testing.expectEqual(@as(usize, 128), try parseSizeArg("128B"));
    try std.testing.expectEqual(@as(usize, 1024), try parseSizeArg("1K"));
    try std.testing.expectEqual(@as(usize, 1024), try parseSizeArg("1KB"));
    try std.testing.expectEqual(@as(usize, 1048576), try parseSizeArg("1M"));
    try std.testing.expectEqual(@as(usize, 1048576), try parseSizeArg("1MB"));
    try std.testing.expectEqual(@as(usize, 4096), try parseSizeArg("4K"));
}
