//! Stress Tests for Subscriptions, Publishing, and Edge Cases
//!
//! Tests subscription counts, SidMap churn, payload sizes,
//! multi-client fan-out, and queue pressure scenarios.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

// --- A. Massive Subscription Tests ---

/// 5K subs on unique subjects, publish one msg to each, verify.
pub fn testFiveThousandSubs(
    allocator: std.mem.Allocator,
) void {
    const NUM_SUBS = 5_000;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 64, .reconnect = false },
    ) catch {
        reportResult("5k_subs", false, "sub connect");
        return;
    };
    defer sub_client.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("5k_subs", false, "pub connect");
        return;
    };
    defer pub_client.deinit();

    const subs = allocator.alloc(
        ?*nats.Subscription,
        NUM_SUBS,
    ) catch {
        reportResult("5k_subs", false, "alloc subs");
        return;
    };
    defer allocator.free(subs);
    @memset(subs, null);

    defer for (subs) |s| {
        if (s) |sub| sub.deinit();
    };

    // Create subs in batches of 500 with flush
    var created: usize = 0;
    var last_err: ?[]const u8 = null;
    for (0..NUM_SUBS) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "five.{d}",
            .{i},
        ) catch continue;
        subs[i] = sub_client.subscribeSync(
            subj,
        ) catch |e| {
            last_err = @errorName(e);
            break;
        };
        created += 1;
        // Flush every 500 subs to avoid write backlog

    }

    if (created != NUM_SUBS) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "{d}/{d} err={s}",
            .{
                created,
                NUM_SUBS,
                last_err orelse "none",
            },
        ) catch "count";
        reportResult("5k_subs", false, msg);
        return;
    }

    io.io().sleep(
        .fromMilliseconds(200),
        .awake,
    ) catch {};

    // Publish one msg to each subject
    for (0..NUM_SUBS) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "five.{d}",
            .{i},
        ) catch continue;
        pub_client.publish(subj, "x") catch {
            reportResult("5k_subs", false, "publish");
            return;
        };
    }


    // Wait for messages to arrive
    io.io().sleep(
        .fromMilliseconds(2000),
        .awake,
    ) catch {};

    // Drain (short timeout - msgs should be queued)
    var received: usize = 0;
    for (subs) |s| {
        if (s) |sub| {
            if (sub.nextMsgTimeout(50) catch null) |m| {
                m.deinit();
                received += 1;
            }
        }
    }

    const threshold = NUM_SUBS * 100 / 100;
    if (received >= threshold) {
        reportResult("5k_subs", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM_SUBS },
        ) catch "count";
        reportResult("5k_subs", false, msg);
    }
}

/// Sub/unsub churn stresses SidMap tombstones.
pub fn testSubUnsubChurn(
    allocator: std.mem.Allocator,
) void {
    const ITERATIONS = 5000;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 64, .reconnect = false },
    ) catch {
        reportResult("sub_unsub_churn", false, "connect");
        return;
    };
    defer client.deinit();

    for (0..ITERATIONS) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "churn.{d}",
            .{i},
        ) catch continue;

        const sub = client.subscribeSync(subj) catch |e| {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "fail at {d} {s}",
                .{ i, @errorName(e) },
            ) catch "sub";
            reportResult("sub_unsub_churn", false, msg);
            return;
        };
        sub.deinit();
        // Flush every 500 to keep write buffer clear

    }

    // Verify final sub works
    const final_sub = client.subscribeSync(
        "churn.final",
    ) catch {
        reportResult("sub_unsub_churn", false, "final");
        return;
    };
    defer final_sub.deinit();

    client.publish("churn.final", "ok") catch {
        reportResult("sub_unsub_churn", false, "pub");
        return;
    };

    if (final_sub.nextMsgTimeout(1000) catch null) |m| {
        m.deinit();
        reportResult("sub_unsub_churn", true, "");
    } else {
        reportResult("sub_unsub_churn", false, "no msg");
    }
}

/// Subscribe 2048, unsub all, resubscribe 2048 fresh.
pub fn testSubsThenResubscribe(
    allocator: std.mem.Allocator,
) void {
    const COUNT = 2048;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 64, .reconnect = false },
    ) catch {
        reportResult("resub", false, "connect");
        return;
    };
    defer client.deinit();

    const subs = allocator.alloc(
        ?*nats.Subscription,
        COUNT,
    ) catch {
        reportResult("resub", false, "alloc");
        return;
    };
    defer allocator.free(subs);
    @memset(subs, null);

    // Phase 1: subscribe COUNT
    var created: usize = 0;
    for (0..COUNT) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "rs.a.{d}",
            .{i},
        ) catch continue;
        subs[i] = client.subscribeSync(subj) catch break;
        created += 1;

    }

    if (created != COUNT) {
        for (subs) |s| if (s) |sub| sub.deinit();
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "phase1 {d}/{d}",
            .{ created, COUNT },
        ) catch "count";
        reportResult("resub", false, msg);
        return;
    }

    // Unsub all
    for (subs) |s| if (s) |sub| sub.deinit();
    @memset(subs, null);


    io.io().sleep(
        .fromMilliseconds(200),
        .awake,
    ) catch {};

    if (!client.isConnected()) {
        reportResult("resub", false, "disconnected");
        return;
    }

    // Phase 2: resubscribe fresh
    var created2: usize = 0;
    var last_err: ?[]const u8 = null;
    for (0..COUNT) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "rs.b.{d}",
            .{i},
        ) catch continue;
        subs[i] = client.subscribeSync(subj) catch |e| {
            last_err = @errorName(e);
            break;
        };
        created2 += 1;

    }

    defer for (subs) |s| if (s) |sub| sub.deinit();

    if (created2 != COUNT) {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "phase2 {d}/{d} {s}",
            .{
                created2,
                COUNT,
                last_err orelse "none",
            },
        ) catch "count";
        reportResult("resub", false, msg);
        return;
    }

    // Verify pub/sub on a resubscribed subject
    client.publish("rs.b.0", "resub-ok") catch {
        reportResult("resub", false, "publish");
        return;
    };

    if (subs[0]) |sub| {
        if (sub.nextMsgTimeout(1000) catch null) |m| {
            m.deinit();
            reportResult("resub", true, "");
        } else {
            reportResult("resub", false, "no msg");
        }
    } else {
        reportResult("resub", false, "null sub");
    }
}

/// 2K wildcard subs + wildcard catch-all, fan-out test.
pub fn testWildcardFanOut(
    allocator: std.mem.Allocator,
) void {
    const NUM_SUBS = 2_000;
    const NUM_MSGS = 100;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 128, .reconnect = false },
    ) catch {
        reportResult("wildcard_fanout", false, "connect");
        return;
    };
    defer client.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("wildcard_fanout", false, "pub con");
        return;
    };
    defer pub_client.deinit();

    const subs = allocator.alloc(
        ?*nats.Subscription,
        NUM_SUBS,
    ) catch {
        reportResult("wildcard_fanout", false, "alloc");
        return;
    };
    defer allocator.free(subs);
    @memset(subs, null);

    defer for (subs) |s| if (s) |sub| sub.deinit();

    var created: usize = 0;
    for (0..NUM_SUBS) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "fan.{d}",
            .{i},
        ) catch continue;
        subs[i] = client.subscribeSync(subj) catch break;
        created += 1;

    }

    if (created != NUM_SUBS) {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "created {d}/{d}",
            .{ created, NUM_SUBS },
        ) catch "count";
        reportResult("wildcard_fanout", false, msg);
        return;
    }

    // Wildcard subscriber
    const wc_sub = client.subscribeSync("fan.>") catch {
        reportResult("wildcard_fanout", false, "wc sub");
        return;
    };
    defer wc_sub.deinit();


    io.io().sleep(
        .fromMilliseconds(200),
        .awake,
    ) catch {};

    // Publish to fan.0 through fan.99
    for (0..NUM_MSGS) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "fan.{d}",
            .{i},
        ) catch continue;
        pub_client.publish(subj, "wc") catch {
            reportResult("wildcard_fanout", false, "pub");
            return;
        };
    }

    io.io().sleep(
        .fromMilliseconds(500),
        .awake,
    ) catch {};

    var wc_received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (wc_sub.nextMsgTimeout(100) catch null) |m| {
            m.deinit();
            wc_received += 1;
        } else break;
    }

    if (wc_received == NUM_MSGS) {
        reportResult("wildcard_fanout", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "wc got {d}/{d}",
            .{ wc_received, NUM_MSGS },
        ) catch "count";
        reportResult("wildcard_fanout", false, msg);
    }
}

// --- B. Multi-Client Tests ---

/// 10 clients, 200 subs each, cross-publish.
pub fn testTenClientsManySubs(
    allocator: std.mem.Allocator,
) void {
    const NUM_CLIENTS = 10;
    const SUBS_PER = 200;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var ios: [NUM_CLIENTS]std.Io.Threaded = undefined;
    var clients: [NUM_CLIENTS]?*nats.Client =
        [_]?*nats.Client{null} ** NUM_CLIENTS;
    var io_count: usize = 0;

    defer {
        for (0..io_count) |i| {
            if (clients[i]) |c| c.deinit();
            ios[i].deinit();
        }
    }

    for (0..NUM_CLIENTS) |i| {
        ios[i] = .init(allocator, .{ .environ = .empty });
        clients[i] = nats.Client.connect(
            allocator,
            ios[i].io(),
            url,
            .{
                .sub_queue_size = 64,
                .reconnect = false,
            },
        ) catch {
            reportResult(
                "10_clients_subs",
                false,
                "connect",
            );
            return;
        };
        io_count += 1;
    }

    const total_subs = NUM_CLIENTS * SUBS_PER;
    const all_subs = allocator.alloc(
        ?*nats.Subscription,
        total_subs,
    ) catch {
        reportResult("10_clients_subs", false, "alloc");
        return;
    };
    defer allocator.free(all_subs);
    @memset(all_subs, null);

    defer for (all_subs) |s| if (s) |sub| sub.deinit();

    var sub_count: usize = 0;
    for (0..NUM_CLIENTS) |ci| {
        const c = clients[ci] orelse continue;
        for (0..SUBS_PER) |si| {
            var sbuf: [32]u8 = undefined;
            const subj = std.fmt.bufPrint(
                &sbuf,
                "mc.{d}",
                .{si},
            ) catch continue;
            const idx = ci * SUBS_PER + si;
            all_subs[idx] = c.subscribeSync(subj) catch
                break;
            sub_count += 1;
        }

    }

    if (sub_count != total_subs) {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "subs {d}/{d}",
            .{ sub_count, total_subs },
        ) catch "count";
        reportResult("10_clients_subs", false, msg);
        return;
    }

    // Publisher
    var pub_io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer pub_io.deinit();

    const publisher = nats.Client.connect(
        allocator,
        pub_io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("10_clients_subs", false, "pub con");
        return;
    };
    defer publisher.deinit();

    pub_io.io().sleep(
        .fromMilliseconds(100),
        .awake,
    ) catch {};

    for (0..SUBS_PER) |si| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "mc.{d}",
            .{si},
        ) catch continue;
        publisher.publish(subj, "mc") catch {
            reportResult("10_clients_subs", false, "pub");
            return;
        };
    }


    pub_io.io().sleep(
        .fromMilliseconds(1000),
        .awake,
    ) catch {};

    var total_recv: usize = 0;
    for (all_subs) |s| {
        if (s) |sub| {
            if (sub.nextMsgTimeout(50) catch null) |m| {
                m.deinit();
                total_recv += 1;
            }
        }
    }

    const threshold = total_subs * 100 / 100;
    if (total_recv >= threshold) {
        reportResult("10_clients_subs", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ total_recv, total_subs },
        ) catch "count";
        reportResult("10_clients_subs", false, msg);
    }
}

/// 5 publishers, 5 subscribers, 100 subjects.
pub fn testMultiPubMultiSub(
    allocator: std.mem.Allocator,
) void {
    const NUM_PUB = 5;
    const NUM_SUB = 5;
    const NUM_SUBJECTS = 100;
    const MSGS_PER_SUBJECT = 100;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var sub_ios: [NUM_SUB]std.Io.Threaded = undefined;
    var sub_clients: [NUM_SUB]?*nats.Client =
        [_]?*nats.Client{null} ** NUM_SUB;
    var sub_io_count: usize = 0;

    defer {
        for (0..sub_io_count) |i| {
            if (sub_clients[i]) |c| c.deinit();
            sub_ios[i].deinit();
        }
    }

    for (0..NUM_SUB) |i| {
        sub_ios[i] = .init(
            allocator,
            .{ .environ = .empty },
        );
        sub_clients[i] = nats.Client.connect(
            allocator,
            sub_ios[i].io(),
            url,
            .{
                .sub_queue_size = 2048,
                .reconnect = false,
            },
        ) catch {
            reportResult("multi_pub_sub", false, "sub con");
            return;
        };
        sub_io_count += 1;
    }

    const total_subs = NUM_SUB * NUM_SUBJECTS;
    const all_subs = allocator.alloc(
        ?*nats.Subscription,
        total_subs,
    ) catch {
        reportResult("multi_pub_sub", false, "alloc");
        return;
    };
    defer allocator.free(all_subs);
    @memset(all_subs, null);
    defer for (all_subs) |s| if (s) |sub| sub.deinit();

    for (0..NUM_SUB) |si| {
        const c = sub_clients[si] orelse continue;
        for (0..NUM_SUBJECTS) |subj_i| {
            var sbuf: [32]u8 = undefined;
            const subj = std.fmt.bufPrint(
                &sbuf,
                "mp.{d}",
                .{subj_i},
            ) catch continue;
            const idx = si * NUM_SUBJECTS + subj_i;
            all_subs[idx] = c.subscribeSync(subj) catch
                break;
        }

    }

    var pub_ios: [NUM_PUB]std.Io.Threaded = undefined;
    var pub_clients: [NUM_PUB]?*nats.Client =
        [_]?*nats.Client{null} ** NUM_PUB;
    var pub_io_count: usize = 0;

    defer {
        for (0..pub_io_count) |i| {
            if (pub_clients[i]) |c| c.deinit();
            pub_ios[i].deinit();
        }
    }

    for (0..NUM_PUB) |i| {
        pub_ios[i] = .init(
            allocator,
            .{ .environ = .empty },
        );
        pub_clients[i] = nats.Client.connect(
            allocator,
            pub_ios[i].io(),
            url,
            .{ .reconnect = false },
        ) catch {
            reportResult("multi_pub_sub", false, "pub con");
            return;
        };
        pub_io_count += 1;
    }

    pub_ios[0].io().sleep(
        .fromMilliseconds(100),
        .awake,
    ) catch {};

    for (0..NUM_PUB) |pi| {
        const c = pub_clients[pi] orelse continue;
        for (0..NUM_SUBJECTS) |subj_i| {
            var sbuf: [32]u8 = undefined;
            const subj = std.fmt.bufPrint(
                &sbuf,
                "mp.{d}",
                .{subj_i},
            ) catch continue;
            for (0..MSGS_PER_SUBJECT) |_| {
                c.publish(subj, "mp") catch {};
            }
        }

    }

    pub_ios[0].io().sleep(
        .fromMilliseconds(2000),
        .awake,
    ) catch {};

    const expected_per_sub =
        NUM_PUB * MSGS_PER_SUBJECT;
    var total_recv: usize = 0;
    for (all_subs) |s| {
        if (s) |sub| {
            for (0..expected_per_sub) |_| {
                if (sub.nextMsgTimeout(20) catch null) |m| {
                    m.deinit();
                    total_recv += 1;
                } else break;
            }
        }
    }

    const total_expected =
        NUM_SUB * NUM_SUBJECTS * expected_per_sub;
    const threshold = total_expected * 100 / 100;
    if (total_recv >= threshold) {
        reportResult("multi_pub_sub", true, "");
    } else {
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d} (min {d})",
            .{ total_recv, total_expected, threshold },
        ) catch "count";
        reportResult("multi_pub_sub", false, msg);
    }
}

// --- C. Message Size Edge Cases ---

/// Tests payload sizes at slab tier boundaries.
pub fn testPayloadSizes(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("payload_sizes", false, "connect");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("sz.test") catch {
        reportResult("payload_sizes", false, "sub");
        return;
    };
    defer sub.deinit();

    const sizes = [_]usize{
        0,     1,     255,  256,  257,
        511,   512,   513,  1023, 1024,
        1025,  4095,  4096, 4097, 16383,
        16384, 16385,
    };

    for (sizes) |size| {
        const payload = if (size > 0)
            allocator.alloc(u8, size) catch {
                reportResult(
                    "payload_sizes",
                    false,
                    "alloc",
                );
                return;
            }
        else
            allocator.alloc(u8, 0) catch {
                reportResult(
                    "payload_sizes",
                    false,
                    "alloc0",
                );
                return;
            };

        defer allocator.free(payload);

        for (payload, 0..) |*b, i| {
            b.* = @truncate(i);
        }

        client.publish("sz.test", payload) catch {
            var buf: [48]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "pub failed sz={d}",
                .{size},
            ) catch "pub";
            reportResult("payload_sizes", false, msg);
            return;
        };

        const recv = sub.nextMsgTimeout(2000) catch {
            var buf: [48]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "timeout sz={d}",
                .{size},
            ) catch "timeout";
            reportResult("payload_sizes", false, msg);
            return;
        };

        if (recv) |m| {
            defer m.deinit();
            if (m.data.len != size) {
                var buf: [48]u8 = undefined;
                const msg = std.fmt.bufPrint(
                    &buf,
                    "sz {d}!={d}",
                    .{ m.data.len, size },
                ) catch "mismatch";
                reportResult(
                    "payload_sizes",
                    false,
                    msg,
                );
                return;
            }
            if (size > 0) {
                for (m.data, 0..) |b, i| {
                    const expected: u8 = @truncate(i);
                    if (b != expected) {
                        reportResult(
                            "payload_sizes",
                            false,
                            "corrupt",
                        );
                        return;
                    }
                }
            }
        } else {
            var buf: [48]u8 = undefined;
            const msg = std.fmt.bufPrint(
                &buf,
                "null sz={d}",
                .{size},
            ) catch "null";
            reportResult("payload_sizes", false, msg);
            return;
        }
    }

    reportResult("payload_sizes", true, "");
}

/// 1MB payload roundtrip with integrity check.
pub fn testMaxPayload1MB(
    allocator: std.mem.Allocator,
) void {
    const SIZE = 1_048_576;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("max_payload_1mb", false, "connect");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("big.1mb") catch {
        reportResult("max_payload_1mb", false, "sub");
        return;
    };
    defer sub.deinit();

    const payload = allocator.alloc(u8, SIZE) catch {
        reportResult("max_payload_1mb", false, "alloc");
        return;
    };
    defer allocator.free(payload);

    for (payload, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    client.publish("big.1mb", payload) catch {
        reportResult("max_payload_1mb", false, "publish");
        return;
    };

    if (sub.nextMsgTimeout(5000) catch null) |m| {
        defer m.deinit();
        if (m.data.len != SIZE) {
            reportResult(
                "max_payload_1mb",
                false,
                "wrong size",
            );
            return;
        }
        var ok = true;
        const checks = [_]usize{
            0, 1, 255, 256, 1023, 1024,
            SIZE / 2, SIZE - 1,
        };
        for (checks) |idx| {
            const expected: u8 = @truncate(idx);
            if (m.data[idx] != expected) {
                ok = false;
                break;
            }
        }
        if (ok) {
            reportResult("max_payload_1mb", true, "");
        } else {
            reportResult(
                "max_payload_1mb",
                false,
                "corrupt",
            );
        }
    } else {
        reportResult("max_payload_1mb", false, "no msg");
    }
}

/// Publish over max_payload, expect error.
pub fn testOverMaxPayload(
    allocator: std.mem.Allocator,
) void {
    const SIZE = 1_048_576 + 1;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("over_max_payload", false, "connect");
        return;
    };
    defer client.deinit();

    const payload = allocator.alloc(u8, SIZE) catch {
        reportResult("over_max_payload", false, "alloc");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'X');

    if (client.publish("over.big", payload)) |_| {
        reportResult(
            "over_max_payload",
            false,
            "no error",
        );
    } else |_| {
        if (client.isConnected()) {
            reportResult("over_max_payload", true, "");
        } else {
            reportResult(
                "over_max_payload",
                false,
                "disconnected",
            );
        }
    }
}

// --- D. Publishing Stress ---

/// 100K messages burst publish.
pub fn testBurstPublish100K(
    allocator: std.mem.Allocator,
) void {
    const NUM_MSGS = 100_000;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("burst_100k", false, "pub connect");
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .sub_queue_size = 131072,
            .reconnect = false,
        },
    ) catch {
        reportResult("burst_100k", false, "sub connect");
        return;
    };
    defer sub_client.deinit();

    const sub = sub_client.subscribeSync("burst") catch {
        reportResult("burst_100k", false, "sub");
        return;
    };
    defer sub.deinit();

    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    var payload: [128]u8 = undefined;
    @memset(&payload, 'B');

    for (0..NUM_MSGS) |_| {
        pub_client.publish("burst", &payload) catch {
            reportResult("burst_100k", false, "publish");
            return;
        };
    }


    io.io().sleep(
        .fromMilliseconds(3000),
        .awake,
    ) catch {};

    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (sub.nextMsgTimeout(10) catch null) |m| {
            m.deinit();
            received += 1;
        } else break;
    }

    const threshold = NUM_MSGS * 100 / 100;
    if (received >= threshold) {
        reportResult("burst_100k", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM_MSGS },
        ) catch "count";
        reportResult("burst_100k", false, msg);
    }
}

/// 1000 x 64KB messages (64 MB total).
pub fn testLargePayloadBurst(
    allocator: std.mem.Allocator,
) void {
    const NUM_MSGS = 1000;
    const SIZE = 64 * 1024;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("large_burst", false, "pub connect");
        return;
    };
    defer pub_client.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .sub_queue_size = 2048,
            .reconnect = false,
        },
    ) catch {
        reportResult("large_burst", false, "sub connect");
        return;
    };
    defer sub_client.deinit();

    const sub = sub_client.subscribeSync(
        "large.burst",
    ) catch {
        reportResult("large_burst", false, "sub");
        return;
    };
    defer sub.deinit();

    const payload = allocator.alloc(u8, SIZE) catch {
        reportResult("large_burst", false, "alloc");
        return;
    };
    defer allocator.free(payload);
    @memset(payload, 'L');

    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    for (0..NUM_MSGS) |_| {
        pub_client.publish(
            "large.burst",
            payload,
        ) catch {
            reportResult("large_burst", false, "pub");
            return;
        };
    }


    io.io().sleep(
        .fromMilliseconds(3000),
        .awake,
    ) catch {};

    var received: usize = 0;
    for (0..NUM_MSGS) |_| {
        if (sub.nextMsgTimeout(50) catch null) |m| {
            m.deinit();
            received += 1;
        } else break;
    }

    const threshold = NUM_MSGS * 100 / 100;
    if (received >= threshold) {
        reportResult("large_burst", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM_MSGS },
        ) catch "count";
        reportResult("large_burst", false, msg);
    }
}

/// 5K different subjects with wildcard subscriber.
pub fn testManySubjectsPublish(
    allocator: std.mem.Allocator,
) void {
    const NUM = 5_000;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .sub_queue_size = 8192,
            .reconnect = false,
        },
    ) catch {
        reportResult("many_subj_pub", false, "connect");
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("many.>") catch {
        reportResult("many_subj_pub", false, "sub");
        return;
    };
    defer sub.deinit();

    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("many_subj_pub", false, "pub con");
        return;
    };
    defer pub_client.deinit();

    for (0..NUM) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "many.{d}",
            .{i},
        ) catch continue;
        pub_client.publish(subj, "m") catch {
            reportResult("many_subj_pub", false, "pub");
            return;
        };
    }


    io.io().sleep(
        .fromMilliseconds(2000),
        .awake,
    ) catch {};

    var received: usize = 0;
    for (0..NUM) |_| {
        if (sub.nextMsgTimeout(10) catch null) |m| {
            m.deinit();
            received += 1;
        } else break;
    }

    const threshold = NUM * 100 / 100;
    if (received >= threshold) {
        reportResult("many_subj_pub", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "got {d}/{d}",
            .{ received, NUM },
        ) catch "count";
        reportResult("many_subj_pub", false, msg);
    }
}

// --- E. Subscription Queue Pressure ---

/// Slow consumer: queue overflows, verify drops.
pub fn testSlowConsumer(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 64, .reconnect = false },
    ) catch {
        reportResult("slow_consumer", false, "connect");
        return;
    };
    defer sub_client.deinit();

    const sub = sub_client.subscribeSync("slow") catch {
        reportResult("slow_consumer", false, "sub");
        return;
    };
    defer sub.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("slow_consumer", false, "pub con");
        return;
    };
    defer pub_client.deinit();

    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    for (0..200) |_| {
        pub_client.publish("slow", "flood") catch {};
    }

    io.io().sleep(
        .fromMilliseconds(500),
        .awake,
    ) catch {};

    const drops = sub.dropped();
    if (drops > 0) {
        reportResult("slow_consumer", true, "");
    } else {
        reportResult(
            "slow_consumer",
            false,
            "no drops",
        );
    }
}

/// Fill queue, drain, refill, verify recovery.
pub fn testQueueFillAndRecover(
    allocator: std.mem.Allocator,
) void {
    const Q_SIZE: usize = 128;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const sub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .sub_queue_size = @intCast(Q_SIZE),
            .reconnect = false,
        },
    ) catch {
        reportResult("queue_recover", false, "connect");
        return;
    };
    defer sub_client.deinit();

    const sub = sub_client.subscribeSync("qr") catch {
        reportResult("queue_recover", false, "sub");
        return;
    };
    defer sub.deinit();

    const pub_client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("queue_recover", false, "pub con");
        return;
    };
    defer pub_client.deinit();

    io.io().sleep(
        .fromMilliseconds(50),
        .awake,
    ) catch {};

    for (0..Q_SIZE) |_| {
        pub_client.publish("qr", "fill1") catch {};
    }

    io.io().sleep(
        .fromMilliseconds(200),
        .awake,
    ) catch {};

    var batch1: usize = 0;
    for (0..Q_SIZE) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            batch1 += 1;
        } else break;
    }

    for (0..Q_SIZE) |_| {
        pub_client.publish("qr", "fill2") catch {};
    }

    io.io().sleep(
        .fromMilliseconds(200),
        .awake,
    ) catch {};

    var batch2: usize = 0;
    for (0..Q_SIZE) |_| {
        if (sub.nextMsgTimeout(500) catch null) |m| {
            m.deinit();
            batch2 += 1;
        } else break;
    }

    if (batch1 > 0 and batch2 > 0) {
        reportResult("queue_recover", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(
            &buf,
            "b1={d} b2={d}",
            .{ batch1, batch2 },
        ) catch "count";
        reportResult("queue_recover", false, msg);
    }
}

// --- F. SidMap Stress ---

/// Tombstone accumulation stress test.
pub fn testSidMapTombstoneStress(
    allocator: std.mem.Allocator,
) void {
    const ROUNDS = 10;
    const PER_ROUND = 200;
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .sub_queue_size = 64, .reconnect = false },
    ) catch {
        reportResult("sidmap_tombstone", false, "connect");
        return;
    };
    defer client.deinit();

    for (0..ROUNDS) |round| {
        var round_subs: [PER_ROUND]?*nats.Subscription =
            [_]?*nats.Subscription{null} ** PER_ROUND;

        for (0..PER_ROUND) |i| {
            var sbuf: [48]u8 = undefined;
            const subj = std.fmt.bufPrint(
                &sbuf,
                "tomb.{d}.{d}",
                .{ round, i },
            ) catch continue;
            round_subs[i] =
                client.subscribeSync(subj) catch |e| {
                    for (&round_subs) |*s| {
                        if (s.*) |sub| sub.deinit();
                    }
                    var buf: [64]u8 = undefined;
                    const msg = std.fmt.bufPrint(
                        &buf,
                        "r={d} i={d} {s}",
                        .{ round, i, @errorName(e) },
                    ) catch "sub";
                    reportResult(
                        "sidmap_tombstone",
                        false,
                        msg,
                    );
                    return;
                };
        }

        for (&round_subs) |*s| {
            if (s.*) |sub| sub.deinit();
        }
        // Flush UNSUB commands between rounds
    
    }

    // Final: subscribe 100 fresh, verify pub/sub
    var final_subs: [100]?*nats.Subscription =
        [_]?*nats.Subscription{null} ** 100;

    defer for (&final_subs) |*s| {
        if (s.*) |sub| sub.deinit();
    };

    for (0..100) |i| {
        var sbuf: [32]u8 = undefined;
        const subj = std.fmt.bufPrint(
            &sbuf,
            "tomb.final.{d}",
            .{i},
        ) catch continue;
        final_subs[i] = client.subscribeSync(
            subj,
        ) catch {
            reportResult(
                "sidmap_tombstone",
                false,
                "final sub",
            );
            return;
        };
    }

    client.publish("tomb.final.0", "ok") catch {
        reportResult(
            "sidmap_tombstone",
            false,
            "publish",
        );
        return;
    };

    if (final_subs[0]) |sub| {
        if (sub.nextMsgTimeout(1000) catch null) |m| {
            m.deinit();
            reportResult("sidmap_tombstone", true, "");
        } else {
            reportResult(
                "sidmap_tombstone",
                false,
                "no msg",
            );
        }
    } else {
        reportResult(
            "sidmap_tombstone",
            false,
            "null sub",
        );
    }
}

/// Runs all stress subscription tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    // A. Massive Subscription Tests
    testFiveThousandSubs(allocator);
    testSubUnsubChurn(allocator);
    testSubsThenResubscribe(allocator);
    testWildcardFanOut(allocator);

    // B. Multi-Client Tests
    testTenClientsManySubs(allocator);
    testMultiPubMultiSub(allocator);

    // C. Message Size Edge Cases
    testPayloadSizes(allocator);
    testMaxPayload1MB(allocator);
    testOverMaxPayload(allocator);

    // D. Publishing Stress
    testBurstPublish100K(allocator);
    testLargePayloadBurst(allocator);
    testManySubjectsPublish(allocator);

    // E. Queue Pressure
    testSlowConsumer(allocator);
    testQueueFillAndRecover(allocator);

    // F. SidMap Stress
    testSidMapTombstoneStress(allocator);
}
