//! Reconnection Logic Unit Tests
//!
//! Tests for subscription backup/restore, backoff calculations,
//! and pending buffer operations.

const std = @import("std");
const Client = @import("../Client.zig");
const SubBackup = Client.SubBackup;

// SubBackup Structure Tests

test "SubBackup default initialization" {
    const backup: SubBackup = .{};
    try std.testing.expectEqual(@as(u64, 0), backup.sid);
    try std.testing.expectEqual(@as(u8, 0), backup.subject_len);
    try std.testing.expectEqual(@as(u8, 0), backup.queue_group_len);
    try std.testing.expect(backup.max_msgs == null);
    try std.testing.expectEqual(@as(u64, 0), backup.received_msgs);
}

test "SubBackup getSubject returns correct slice" {
    var backup: SubBackup = .{};
    const subject = "test.subject.name";
    @memcpy(backup.subject_buf[0..subject.len], subject);
    backup.subject_len = subject.len;

    try std.testing.expectEqualStrings(subject, backup.getSubject());
}

test "SubBackup getSubject empty" {
    const backup: SubBackup = .{};
    try std.testing.expectEqualStrings("", backup.getSubject());
}

test "SubBackup getQueueGroup returns correct slice" {
    var backup: SubBackup = .{};
    const qg = "my-queue-group";
    @memcpy(backup.queue_group_buf[0..qg.len], qg);
    backup.queue_group_len = qg.len;

    try std.testing.expectEqualStrings(qg, backup.getQueueGroup().?);
}

test "SubBackup getQueueGroup returns null when empty" {
    const backup: SubBackup = .{};
    try std.testing.expect(backup.getQueueGroup() == null);
}

test "SubBackup max subject length" {
    var backup: SubBackup = .{};
    // Fill entire buffer
    @memset(&backup.subject_buf, 'x');
    backup.subject_len = 255;

    try std.testing.expectEqual(@as(usize, 255), backup.getSubject().len);
}

test "SubBackup max queue group length" {
    var backup: SubBackup = .{};
    @memset(&backup.queue_group_buf, 'q');
    backup.queue_group_len = 64;

    try std.testing.expectEqual(@as(usize, 64), backup.getQueueGroup().?.len);
}

test "SubBackup preserves SID" {
    var backup: SubBackup = .{};
    backup.sid = 12345;
    try std.testing.expectEqual(@as(u64, 12345), backup.sid);
}

test "SubBackup preserves max_msgs" {
    var backup: SubBackup = .{};
    backup.max_msgs = 100;
    try std.testing.expectEqual(@as(u64, 100), backup.max_msgs.?);
}

test "SubBackup preserves received_msgs" {
    var backup: SubBackup = .{};
    backup.received_msgs = 42;
    try std.testing.expectEqual(@as(u64, 42), backup.received_msgs);
}

test "SubBackup max SID value" {
    var backup: SubBackup = .{};
    backup.sid = std.math.maxInt(u64);
    try std.testing.expectEqual(std.math.maxInt(u64), backup.sid);
}

// Backoff Calculation Tests

// These test the backoff calculation formula:
// exp_wait = base << attempt (capped at 10)
// capped = min(exp_wait, max_wait)
// jitter = ±(capped * jitter_percent / 100)

test "backoff base case" {
    const base_ms: u64 = 2000;
    const attempt: u4 = 0;
    const exp_wait = base_ms << attempt;
    try std.testing.expectEqual(@as(u64, 2000), exp_wait);
}

test "backoff exponential growth" {
    const base_ms: u64 = 2000;

    try std.testing.expectEqual(@as(u64, 2000), base_ms << @as(u4, 0));
    try std.testing.expectEqual(@as(u64, 4000), base_ms << @as(u4, 1));
    try std.testing.expectEqual(@as(u64, 8000), base_ms << @as(u4, 2));
    try std.testing.expectEqual(@as(u64, 16000), base_ms << @as(u4, 3));
    try std.testing.expectEqual(@as(u64, 32000), base_ms << @as(u4, 4));
}

test "backoff capped at max" {
    const base_ms: u64 = 2000;
    const max_ms: u64 = 30000;

    // Attempt 5 would be 64000, capped to 30000
    const exp_wait = base_ms << @as(u4, 5);
    const capped = @min(exp_wait, max_ms);
    try std.testing.expectEqual(@as(u64, 30000), capped);
}

test "backoff attempt capped at 10" {
    const base_ms: u64 = 2000;
    const max_attempt: u4 = 10;

    // Attempt 10 = 2000 << 10 = 2,048,000ms
    const exp_wait = base_ms << max_attempt;
    try std.testing.expectEqual(@as(u64, 2048000), exp_wait);
}

test "backoff jitter range calculation" {
    const capped: u64 = 30000;
    const jitter_percent: u8 = 10;

    const jitter_range = capped * jitter_percent / 100;
    try std.testing.expectEqual(@as(u64, 3000), jitter_range);
}

test "backoff jitter bounds" {
    const capped: u64 = 30000;
    const jitter_percent: u8 = 10;
    const jitter_range = capped * jitter_percent / 100;

    // Jitter should be in range [-3000, +3000]
    // Min wait = 30000 - 3000 = 27000
    // Max wait = 30000 + 3000 = 33000
    const min_wait = capped - jitter_range;
    const max_wait = capped + jitter_range;

    try std.testing.expectEqual(@as(u64, 27000), min_wait);
    try std.testing.expectEqual(@as(u64, 33000), max_wait);
}

test "backoff zero jitter" {
    const capped: u64 = 30000;
    const jitter_percent: u8 = 0;
    const jitter_range = capped * jitter_percent / 100;

    try std.testing.expectEqual(@as(u64, 0), jitter_range);
}

test "backoff max jitter 50 percent" {
    const capped: u64 = 30000;
    const jitter_percent: u8 = 50;
    const jitter_range = capped * jitter_percent / 100;

    try std.testing.expectEqual(@as(u64, 15000), jitter_range);
}

// Reconnection Options Tests

test "default reconnection options" {
    const opts: Client.Options = .{};

    try std.testing.expect(opts.reconnect);
    try std.testing.expectEqual(@as(u32, 60), opts.max_reconnect_attempts);
    try std.testing.expectEqual(@as(u32, 2000), opts.reconnect_wait_ms);
    try std.testing.expectEqual(@as(u32, 30000), opts.reconnect_wait_max_ms);
    try std.testing.expectEqual(@as(u8, 10), opts.reconnect_jitter_percent);
    try std.testing.expect(opts.discover_servers);
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), opts.pending_buffer_size);
}

test "disable reconnection" {
    const opts: Client.Options = .{ .reconnect = false };
    try std.testing.expect(!opts.reconnect);
}

test "infinite reconnect attempts" {
    const opts: Client.Options = .{ .max_reconnect_attempts = 0 };
    try std.testing.expectEqual(@as(u32, 0), opts.max_reconnect_attempts);
}

test "custom reconnect timing" {
    const opts: Client.Options = .{
        .reconnect_wait_ms = 500,
        .reconnect_wait_max_ms = 10000,
        .reconnect_jitter_percent = 25,
    };

    try std.testing.expectEqual(@as(u32, 500), opts.reconnect_wait_ms);
    try std.testing.expectEqual(@as(u32, 10000), opts.reconnect_wait_max_ms);
    try std.testing.expectEqual(@as(u8, 25), opts.reconnect_jitter_percent);
}

test "disable pending buffer" {
    const opts: Client.Options = .{ .pending_buffer_size = 0 };
    try std.testing.expectEqual(@as(usize, 0), opts.pending_buffer_size);
}

test "custom pending buffer size" {
    const opts: Client.Options = .{ .pending_buffer_size = 1024 * 1024 };
    try std.testing.expectEqual(@as(usize, 1024 * 1024), opts.pending_buffer_size);
}

// Health Check Options Tests

test "default health check options" {
    const opts: Client.Options = .{};

    try std.testing.expectEqual(@as(u32, 120000), opts.ping_interval_ms);
    try std.testing.expectEqual(@as(u8, 2), opts.max_pings_outstanding);
}

test "disable health check" {
    const opts: Client.Options = .{ .ping_interval_ms = 0 };
    try std.testing.expectEqual(@as(u32, 0), opts.ping_interval_ms);
}

test "aggressive health check" {
    const opts: Client.Options = .{
        .ping_interval_ms = 1000,
        .max_pings_outstanding = 1,
    };

    try std.testing.expectEqual(@as(u32, 1000), opts.ping_interval_ms);
    try std.testing.expectEqual(@as(u8, 1), opts.max_pings_outstanding);
}

// Stats Tests

test "stats default reconnects zero" {
    const stats: Client.Stats = .{};
    try std.testing.expectEqual(@as(u32, 0), stats.reconnects);
}

// Subscription Remaining Messages Calculation Tests

test "remaining messages calculation" {
    // Test the -| saturating subtraction pattern used in restoreSubscriptions
    const max_msgs: u64 = 100;
    const received: u64 = 30;
    const remaining = max_msgs -| received;
    try std.testing.expectEqual(@as(u64, 70), remaining);
}

test "remaining messages at zero" {
    const max_msgs: u64 = 100;
    const received: u64 = 100;
    const remaining = max_msgs -| received;
    try std.testing.expectEqual(@as(u64, 0), remaining);
}

test "remaining messages saturates" {
    const max_msgs: u64 = 100;
    const received: u64 = 150; // More than max (shouldn't happen but test anyway)
    const remaining = max_msgs -| received;
    try std.testing.expectEqual(@as(u64, 0), remaining);
}

test "remaining messages max values" {
    const max_msgs: u64 = std.math.maxInt(u64);
    const received: u64 = 1;
    const remaining = max_msgs -| received;
    try std.testing.expectEqual(std.math.maxInt(u64) - 1, remaining);
}

// Pending Buffer Size Estimation Tests

// Tests for the size estimation: "PUB subject len\r\npayload\r\n"
// encoded_size = 4 + subject.len + 1 + 10 + 2 + payload.len + 2

test "pending buffer size estimation minimal" {
    const subject = "x";
    const payload = "";
    // "PUB x 0\r\n\r\n" = 4 + 1 + 1 + 10 + 2 + 0 + 2 = 20 (estimate)
    const estimate = 4 + subject.len + 1 + 10 + 2 + payload.len + 2;
    try std.testing.expectEqual(@as(usize, 19), estimate);
}

test "pending buffer size estimation typical" {
    const subject = "my.test.subject";
    const payload = "hello world";
    const estimate = 4 + subject.len + 1 + 10 + 2 + payload.len + 2;
    try std.testing.expectEqual(@as(usize, 45), estimate);
}

test "pending buffer size estimation large payload" {
    const subject = "data.stream";
    const payload_size: usize = 1024 * 1024; // 1MB
    const estimate = 4 + subject.len + 1 + 10 + 2 + payload_size + 2;
    try std.testing.expectEqual(@as(usize, 1048606), estimate);
}

// Multiple Backup Array Tests

test "backup array initialization" {
    const backups = [_]SubBackup{.{}} ** Client.MAX_SUBSCRIPTIONS;
    try std.testing.expectEqual(Client.MAX_SUBSCRIPTIONS, backups.len);

    // All should be zeroed
    for (backups) |backup| {
        try std.testing.expectEqual(@as(u64, 0), backup.sid);
        try std.testing.expectEqual(@as(u8, 0), backup.subject_len);
    }
}

test "backup array modification" {
    var backups = [_]SubBackup{.{}} ** 4;

    backups[0].sid = 1;
    backups[1].sid = 2;
    backups[2].sid = 3;
    backups[3].sid = 4;

    try std.testing.expectEqual(@as(u64, 1), backups[0].sid);
    try std.testing.expectEqual(@as(u64, 2), backups[1].sid);
    try std.testing.expectEqual(@as(u64, 3), backups[2].sid);
    try std.testing.expectEqual(@as(u64, 4), backups[3].sid);
}

// Edge Case Tests

test "subject with special characters in backup" {
    var backup: SubBackup = .{};
    const subject = "test.*.>";
    @memcpy(backup.subject_buf[0..subject.len], subject);
    backup.subject_len = subject.len;

    try std.testing.expectEqualStrings("test.*.>", backup.getSubject());
}

test "queue group with hyphens and numbers" {
    var backup: SubBackup = .{};
    const qg = "worker-group-123";
    @memcpy(backup.queue_group_buf[0..qg.len], qg);
    backup.queue_group_len = qg.len;

    try std.testing.expectEqualStrings(qg, backup.getQueueGroup().?);
}

test "backup with all fields populated" {
    var backup: SubBackup = .{};

    backup.sid = 42;

    const subject = "orders.*.shipped";
    @memcpy(backup.subject_buf[0..subject.len], subject);
    backup.subject_len = subject.len;

    const qg = "processors";
    @memcpy(backup.queue_group_buf[0..qg.len], qg);
    backup.queue_group_len = qg.len;

    backup.max_msgs = 1000;
    backup.received_msgs = 500;

    try std.testing.expectEqual(@as(u64, 42), backup.sid);
    try std.testing.expectEqualStrings(subject, backup.getSubject());
    try std.testing.expectEqualStrings(qg, backup.getQueueGroup().?);
    try std.testing.expectEqual(@as(u64, 1000), backup.max_msgs.?);
    try std.testing.expectEqual(@as(u64, 500), backup.received_msgs);
}
