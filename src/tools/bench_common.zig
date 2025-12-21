//! Benchmark Common Utilities
//!
//! Shared utilities for bench-pub and bench-sub tools.

const std = @import("std");
const assert = std.debug.assert;

/// Parses size argument like "128", "128B", "1K", "1KB", "1M", "1MB"
pub fn parseSizeArg(val: []const u8) !usize {
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

/// Format current time as HH:MM:SS.
pub const TimeOfDay = struct {
    hours: u64,
    minutes: u64,
    seconds: u64,

    pub fn now() ?TimeOfDay {
        const instant = std.time.Instant.now() catch return null;
        const secs: u64 = @intCast(instant.timestamp.sec);
        return .{
            .hours = @mod(@divFloor(secs, 3600), 24),
            .minutes = @mod(@divFloor(secs, 60), 60),
            .seconds = @mod(secs, 60),
        };
    }

    pub fn format(self: TimeOfDay, buf: *[8]u8) []const u8 {
        return std.fmt.bufPrint(buf, "{d:0>2}:{d:0>2}:{d:0>2}", .{
            self.hours,
            self.minutes,
            self.seconds,
        }) catch "??:??:??";
    }
};

/// Benchmark statistics calculation.
pub const Stats = struct {
    elapsed_ns: u64,
    msg_count: u64,
    total_bytes: u64,

    pub fn elapsedSec(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1e9;
    }

    pub fn msgsPerSec(self: Stats) f64 {
        return @as(f64, @floatFromInt(self.msg_count)) / self.elapsedSec();
    }

    pub fn kibPerSec(self: Stats) f64 {
        const bytes: f64 = @floatFromInt(self.total_bytes);
        return bytes / self.elapsedSec() / 1024.0;
    }

    pub fn avgLatencyUs(self: Stats) f64 {
        const count: f64 = @floatFromInt(self.msg_count);
        return self.elapsedSec() * 1e6 / count;
    }

    /// Print stats in standard format.
    pub fn print(self: Stats, role: []const u8) void {
        assert(self.elapsed_ns > 0);
        assert(self.msg_count > 0);

        std.debug.print(
            "\nNATS {s} stats: {d:.0} msgs/sec ~ {d:.0} KiB/sec ~ {d:.2}us\n",
            .{ role, self.msgsPerSec(), self.kibPerSec(), self.avgLatencyUs() },
        );
        std.debug.print(
            "  Total: {d} messages, {d} bytes in {d:.3}s\n",
            .{ self.msg_count, self.total_bytes, self.elapsedSec() },
        );
    }
};

test "parseSizeArg" {
    try std.testing.expectEqual(@as(usize, 128), try parseSizeArg("128"));
    try std.testing.expectEqual(@as(usize, 128), try parseSizeArg("128B"));
    try std.testing.expectEqual(@as(usize, 1024), try parseSizeArg("1K"));
    try std.testing.expectEqual(@as(usize, 1024), try parseSizeArg("1KB"));
    try std.testing.expectEqual(@as(usize, 1048576), try parseSizeArg("1M"));
    try std.testing.expectEqual(@as(usize, 1048576), try parseSizeArg("1MB"));
    try std.testing.expectEqual(@as(usize, 4096), try parseSizeArg("4K"));
}

test "TimeOfDay format" {
    const tod = TimeOfDay{ .hours = 14, .minutes = 5, .seconds = 9 };
    var buf: [8]u8 = undefined;
    try std.testing.expectEqualStrings("14:05:09", tod.format(&buf));
}

test "Stats calculation" {
    const stats = Stats{
        .elapsed_ns = 1_000_000_000, // 1 second
        .msg_count = 1000,
        .total_bytes = 128_000,
    };
    const expect = std.testing.expectApproxEqAbs;
    try expect(@as(f64, 1.0), stats.elapsedSec(), 0.001);
    try expect(@as(f64, 1000.0), stats.msgsPerSec(), 0.1);
    try expect(@as(f64, 125.0), stats.kibPerSec(), 0.1);
    try expect(@as(f64, 1000.0), stats.avgLatencyUs(), 0.1);
}
