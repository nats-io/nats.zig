//! Error Handling Integration Tests

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;
const defaults = nats.defaults;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testSubjectTooLong(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("subject_too_long", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const max_len = defaults.Limits.max_subject_len;
    var long_subject: [max_len + 1]u8 = undefined;
    @memset(&long_subject, 'a');

    const result = client.subscribe(allocator, &long_subject);
    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("subject_too_long", false, "should have failed");
    } else |err| {
        if (err == error.SubjectTooLong) {
            reportResult("subject_too_long", true, "");
        } else {
            var buf: [64]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "wrong error: {s}",
                .{@errorName(err)},
            ) catch "fmt error";
            reportResult("subject_too_long", false, detail);
        }
    }
}

pub fn testQueueGroupTooLong(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("queue_group_too_long", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const max_len = defaults.Limits.max_queue_group_len;
    var long_qg: [max_len + 1]u8 = undefined;
    @memset(&long_qg, 'q');

    const result = client.subscribeQueue(allocator, "test.subject", &long_qg);
    if (result) |sub| {
        sub.deinit(allocator);
        reportResult("queue_group_too_long", false, "should have failed");
    } else |err| {
        if (err == error.QueueGroupTooLong) {
            reportResult("queue_group_too_long", true, "");
        } else {
            var buf: [64]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "wrong error: {s}",
                .{@errorName(err)},
            ) catch "fmt error";
            reportResult("queue_group_too_long", false, detail);
        }
    }
}

pub fn testUrlTooLong(allocator: std.mem.Allocator) void {
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const max_len = defaults.Server.max_url_len;
    var long_url: [max_len + 1]u8 = undefined;
    const prefix = "nats://localhost:";
    @memcpy(long_url[0..prefix.len], prefix);
    @memset(long_url[prefix.len..], '1');

    const result = nats.Client.connect(allocator, io.io(), &long_url, .{
        .reconnect = false,
    });
    if (result) |client| {
        client.deinit(allocator);
        reportResult("url_too_long", false, "should have failed");
    } else |err| {
        if (err == error.UrlTooLong) {
            reportResult("url_too_long", true, "");
        } else {
            var buf: [64]u8 = undefined;
            const detail = std.fmt.bufPrint(
                &buf,
                "wrong error: {s}",
                .{@errorName(err)},
            ) catch "fmt error";
            reportResult("url_too_long", false, detail);
        }
    }
}

pub fn testDrainResultIsClean(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("drain_result_clean", false, "connect failed");
        return;
    };

    const sub = client.subscribe(allocator, "drain.test") catch {
        client.deinit(allocator);
        reportResult("drain_result_clean", false, "subscribe failed");
        return;
    };

    const result = client.drain(allocator) catch {
        sub.deinit(allocator);
        client.deinit(allocator);
        reportResult("drain_result_clean", false, "drain failed");
        return;
    };
    sub.deinit(allocator);
    client.deinit(allocator);

    if (result.isClean()) {
        reportResult("drain_result_clean", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &buf,
            "unsub_fail={d} flush={any}",
            .{ result.unsub_failures, result.flush_failed },
        ) catch "fmt error";
        reportResult("drain_result_clean", false, detail);
    }
}

pub fn testSubjectExactLimit(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("subject_exact_limit", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const max_len = defaults.Limits.max_subject_len;
    var subject_max: [max_len]u8 = undefined;
    @memset(&subject_max, 'a');

    const sub = client.subscribe(allocator, &subject_max) catch {
        reportResult("subject_exact_limit", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    reportResult("subject_exact_limit", true, "");
}

pub fn testQueueGroupExactLimit(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("qg_exact_limit", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const max_len = defaults.Limits.max_queue_group_len;
    var qg_max: [max_len]u8 = undefined;
    @memset(&qg_max, 'q');

    const sub = client.subscribeQueue(allocator, "test.subject", &qg_max) catch {
        reportResult("qg_exact_limit", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    reportResult("qg_exact_limit", true, "");
}

pub fn testResetErrorNotifications(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    }) catch {
        reportResult("reset_error_notif", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    client.resetErrorNotifications();

    reportResult("reset_error_notif", true, "");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testSubjectTooLong(allocator);
    testQueueGroupTooLong(allocator);
    testUrlTooLong(allocator);
    testDrainResultIsClean(allocator);
    testSubjectExactLimit(allocator);
    testQueueGroupExactLimit(allocator);
    testResetErrorNotifications(allocator);
}
