//! State Notification Tests for NATS Client
//!
//! Tests for Phase 3 features: LastError, discovered_servers event,
//! draining event, subscription_complete event.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

/// Test getLastError returns null initially and after clear.
pub fn testLastErrorInitialNull(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    // Initially should be null
    const err = client.getLastError();
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    // Clear and verify null
    client.clearLastError();
    const err = client.getLastError();
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

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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

    // Subscribe to something
    const sub = client.subscribe(allocator, "drain.test") catch {
        client.deinit(allocator);
        reportResult("draining_event", false, "subscribe failed");
        return;
    };

    // Drain (this also cleans up subscriptions internally)
    _ = client.drain(allocator) catch {};

    // Subscription was cleaned up by drain(), but we still need to free memory
    sub.deinit(allocator);

    // Give callback task time to process events
    io.io().sleep(.fromMilliseconds(50), .awake) catch {};

    if (tracker.draining_received) {
        reportResult("draining_event", true, "");
    } else {
        reportResult("draining_event", false, "no draining event");
    }

    client.deinit(allocator);
}

/// Test subscription_complete event when auto-unsub limit is reached.
pub fn testSubscriptionCompleteEvent(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
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
    defer client.deinit(allocator);

    // Subscribe with auto-unsub after 3 messages
    const sub = client.subscribe(allocator, "complete.test") catch {
        reportResult("sub_complete_event", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    sub.autoUnsubscribe(3) catch {
        reportResult("sub_complete_event", false, "auto-unsub failed");
        return;
    };

    client.flush(allocator) catch {};

    // Publish 3 messages
    for (0..3) |_| {
        client.publish("complete.test", "data") catch {};
    }
    client.flush(allocator) catch {};

    // Receive messages to trigger the delivered count
    for (0..3) |_| {
        const msg = sub.nextWithTimeout(allocator, 500) catch break;
        if (msg) |m| {
            m.deinit(allocator);
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

pub fn runAll(allocator: std.mem.Allocator) void {
    testLastErrorInitialNull(allocator);
    testClearLastError(allocator);
    testDrainingEvent(allocator);
    testSubscriptionCompleteEvent(allocator);
}
