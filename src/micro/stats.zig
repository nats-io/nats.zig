const std = @import("std");
const protocol = @import("protocol.zig");
const SpinLock = @import("../sync/spin_lock.zig").SpinLock;

pub const EndpointStats = struct {
    num_requests: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    num_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    processing_time: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    last_error_code: std.atomic.Value(u16) = std.atomic.Value(u16).init(0),
    last_error_lock: SpinLock = .{},
    last_error_len: usize = 0,
    last_error_desc: [128]u8 = undefined,

    pub fn recordSuccess(self: *EndpointStats, elapsed_ns: u64) void {
        _ = self.num_requests.fetchAdd(1, .monotonic);
        _ = self.processing_time.fetchAdd(elapsed_ns, .monotonic);
    }

    pub fn recordError(
        self: *EndpointStats,
        elapsed_ns: u64,
        code: u16,
        description: []const u8,
    ) void {
        _ = self.num_requests.fetchAdd(1, .monotonic);
        _ = self.num_errors.fetchAdd(1, .monotonic);
        _ = self.processing_time.fetchAdd(elapsed_ns, .monotonic);
        self.last_error_lock.lock();
        defer self.last_error_lock.unlock();

        const copy_len = @min(description.len, self.last_error_desc.len);
        @memcpy(self.last_error_desc[0..copy_len], description[0..copy_len]);
        self.last_error_len = copy_len;
        self.last_error_code.store(code, .release);
    }

    pub fn reset(self: *EndpointStats) void {
        self.num_requests.store(0, .release);
        self.num_errors.store(0, .release);
        self.processing_time.store(0, .release);
        self.last_error_lock.lock();
        defer self.last_error_lock.unlock();
        self.last_error_len = 0;
        self.last_error_code.store(0, .release);
    }

    pub fn snapshot(
        self: *EndpointStats,
    ) struct {
        num_requests: u64,
        num_errors: u64,
        processing_time: u64,
        average_processing_time: u64,
        last_error: ?protocol.Error,
    } {
        const num_requests = self.num_requests.load(.acquire);
        const num_errors = self.num_errors.load(.acquire);
        const processing_time = self.processing_time.load(.acquire);
        const average_processing_time = if (num_requests == 0)
            0
        else
            @divTrunc(processing_time, num_requests);

        self.last_error_lock.lock();
        defer self.last_error_lock.unlock();

        const last_error = if (self.last_error_len == 0)
            null
        else
            protocol.Error{
                .code = self.last_error_code.load(.acquire),
                .description = self.last_error_desc[0..self.last_error_len],
            };

        return .{
            .num_requests = num_requests,
            .num_errors = num_errors,
            .processing_time = processing_time,
            .average_processing_time = average_processing_time,
            .last_error = last_error,
        };
    }
};

test "stats basic" {
    var stats: EndpointStats = .{};
    stats.recordSuccess(10);
    stats.recordError(20, 503, "down");
    const snap = stats.snapshot();
    try std.testing.expectEqual(@as(u64, 2), snap.num_requests);
    try std.testing.expectEqual(@as(u64, 1), snap.num_errors);
    try std.testing.expectEqual(@as(u64, 30), snap.processing_time);
    try std.testing.expect(snap.last_error != null);
}
