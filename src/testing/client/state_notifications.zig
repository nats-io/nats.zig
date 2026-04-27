//! State Notification Tests for NATS Client
//!
//! Tests for: LastError, discovered_servers event,
//! draining event, subscription_complete event.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

fn getNowNs(io: std.Io) i128 {
    return std.Io.Timestamp.now(io, .awake).nanoseconds;
}

fn threadSleepNs(ns: u64) void {
    var ts: std.posix.timespec = .{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = std.posix.system.nanosleep(&ts, &ts);
}

/// Test getLastError returns null initially and after clear.
pub fn testLastErrorInitialNull(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("last_error_initial", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Initially should be null
    const err = client.lastError();
    if (err != null) {
        reportResult("last_error_initial", false, "expected null");
        return;
    }

    reportResult("last_error_initial", true, "");
}

/// Test clearLastError works.
pub fn testClearLastError(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{ .reconnect = false },
    ) catch {
        reportResult("clear_last_error", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Clear and verify null
    client.clearLastError();
    const err = client.lastError();
    if (err != null) {
        reportResult("clear_last_error", false, "expected null after clear");
        return;
    }

    reportResult("clear_last_error", true, "");
}

/// Test draining event is fired during drain.
pub fn testDrainingEvent(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    // Track events with a handler
    const EventTracker = struct {
        draining_received: bool = false,

        pub fn onDraining(self: *@This()) void {
            self.draining_received = true;
        }
    };

    var tracker = EventTracker{};
    const handler = nats.EventHandler.init(EventTracker, &tracker);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = false,
            .event_handler = handler,
        },
    ) catch {
        reportResult("draining_event", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Subscribe to something
    const sub = client.subscribeSync("drain.test") catch {
        reportResult("draining_event", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    // Drain (this also cleans up subscriptions internally)
    _ = client.drain() catch {};

    // Give callback task time to process events
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    if (tracker.draining_received) {
        reportResult("draining_event", true, "");
    } else {
        reportResult("draining_event", false, "no draining event");
    }
}

/// Test subscription_complete event when auto-unsub limit is reached.
pub fn testSubscriptionCompleteEvent(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    // Track events with a handler
    const EventTracker = struct {
        complete_received: bool = false,
        complete_sid: u64 = 0,

        pub fn onSubscriptionComplete(self: *@This(), sid: u64) void {
            self.complete_received = true;
            self.complete_sid = sid;
        }
    };

    var tracker = EventTracker{};
    const handler = nats.EventHandler.init(EventTracker, &tracker);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = false,
            .event_handler = handler,
        },
    ) catch {
        reportResult("sub_complete_event", false, "connect failed");
        return;
    };
    defer client.deinit();

    // Subscribe with auto-unsub after 3 messages
    const sub = client.subscribeSync("complete.test") catch {
        reportResult("sub_complete_event", false, "subscribe failed");
        return;
    };
    defer sub.deinit();

    sub.autoUnsubscribe(3) catch {
        reportResult("sub_complete_event", false, "auto-unsub failed");
        return;
    };

    // Publish 3 messages
    for (0..3) |_| {
        client.publish("complete.test", "data") catch {};
    }

    // Receive messages to trigger the delivered count
    for (0..3) |_| {
        const msg = sub.nextMsgTimeout(500) catch break;
        if (msg) |m| {
            m.deinit();
        }
    }

    // Give callback task time to process events
    io.io().sleep(.fromMilliseconds(100), .awake) catch {};

    if (tracker.complete_received and tracker.complete_sid == sub.getSid()) {
        reportResult("sub_complete_event", true, "");
    } else {
        var buf: [48]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "recv={} sid={}", .{
            tracker.complete_received,
            tracker.complete_sid,
        }) catch "e";
        reportResult("sub_complete_event", false, detail);
    }
}

fn pushDrainingEventThread(
    client: *nats.Client,
    go: *std.atomic.Value(bool),
) void {
    while (!go.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
    client.pushEvent(.{ .draining = {} });
}

/// Stress the event queue with one producer from io_task and one
/// producer from user-thread code in the same window.
pub fn testEventQueueMultiProducerOverlap(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    const io = utils.newIo(allocator);
    defer io.deinit();

    const Tracker = struct {
        draining_count: std.atomic.Value(u32) =
            std.atomic.Value(u32).init(0),
        complete_count: std.atomic.Value(u32) =
            std.atomic.Value(u32).init(0),

        pub fn onDraining(self: *@This()) void {
            _ = self.draining_count.fetchAdd(1, .monotonic);
        }

        pub fn onSubscriptionComplete(self: *@This(), sid: u64) void {
            _ = sid;
            _ = self.complete_count.fetchAdd(1, .monotonic);
        }
    };

    var tracker = Tracker{};
    const handler = nats.EventHandler.init(Tracker, &tracker);

    const client = nats.Client.connect(
        allocator,
        io.io(),
        url,
        .{
            .reconnect = false,
            .event_handler = handler,
        },
    ) catch {
        reportResult("event_queue_multi_producer", false, "connect failed");
        return;
    };
    defer client.deinit();

    const iterations = 64;
    for (0..iterations) |i| {
        {
            var subject_buf: [48]u8 = undefined;
            const subject = std.fmt.bufPrint(
                &subject_buf,
                "event.mp.{d}",
                .{i},
            ) catch {
                reportResult("event_queue_multi_producer", false, "subject format failed");
                return;
            };

            const sub = client.subscribeSync(subject) catch {
                reportResult("event_queue_multi_producer", false, "subscribe failed");
                return;
            };
            defer sub.deinit();

            sub.autoUnsubscribe(1) catch {
                reportResult("event_queue_multi_producer", false, "auto-unsub failed");
                return;
            };

            var go = std.atomic.Value(bool).init(false);
            var t = std.Thread.spawn(
                .{},
                pushDrainingEventThread,
                .{ client, &go },
            ) catch {
                reportResult("event_queue_multi_producer", false, "thread spawn failed");
                return;
            };

            go.store(true, .release);
            client.publish(subject, "x") catch {
                t.join();
                reportResult("event_queue_multi_producer", false, "publish failed");
                return;
            };
            client.flush(1_000_000_000) catch {
                t.join();
                reportResult("event_queue_multi_producer", false, "flush failed");
                return;
            };
            t.join();

            if (sub.nextMsgTimeout(200) catch null) |msg| {
                msg.deinit();
            } else {
                reportResult("event_queue_multi_producer", false, "message not received");
                return;
            }

            const draining_target: u32 = @intCast(i + 1);
            const complete_target: u32 = @intCast(i + 1);
            const deadline_ns = getNowNs(io.io()) +
                200 * std.time.ns_per_ms;
            while (getNowNs(io.io()) < deadline_ns) {
                if (tracker.draining_count.load(.monotonic) >= draining_target and
                    tracker.complete_count.load(.monotonic) >= complete_target)
                {
                    break;
                }
                threadSleepNs(1 * std.time.ns_per_ms);
            }

            if (tracker.draining_count.load(.monotonic) < draining_target or
                tracker.complete_count.load(.monotonic) < complete_target)
            {
                var detail_buf: [96]u8 = undefined;
                const detail = std.fmt.bufPrint(
                    &detail_buf,
                    "drain={d} complete={d} at iter {d}",
                    .{
                        tracker.draining_count.load(.monotonic),
                        tracker.complete_count.load(.monotonic),
                        i,
                    },
                ) catch "event count mismatch";
                reportResult("event_queue_multi_producer", false, detail);
                return;
            }
        }
    }

    reportResult("event_queue_multi_producer", true, "");
}

pub fn runAll(allocator: std.mem.Allocator) void {
    testLastErrorInitialNull(allocator);
    testClearLastError(allocator);
    testDrainingEvent(allocator);
    testSubscriptionCompleteEvent(allocator);
    testEventQueueMultiProducerOverlap(allocator);
}
