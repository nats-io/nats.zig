const std = @import("std");

pub fn nowRfc3339(io: std.Io, buf: []u8) ![]const u8 {
    const ts = std.Io.Timestamp.now(io, .real);
    const total_ns: u64 = @intCast(ts.nanoseconds);
    const epoch_secs = total_ns / std.time.ns_per_s;
    const ms = @as(u32, @intCast((total_ns % std.time.ns_per_s) / std.time.ns_per_ms));

    const epoch = std.time.epoch.EpochSeconds{
        .secs = epoch_secs,
    };
    const day_secs = epoch.getDaySeconds();
    const year_day = epoch.getEpochDay().calculateYearDay();
    const month_day = year_day.calculateMonthDay();

    return std.fmt.bufPrint(
        buf,
        "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>3}Z",
        .{
            year_day.year,
            @intFromEnum(month_day.month),
            month_day.day_index + 1,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
            ms,
        },
    );
}
