//! Multi-Client Tests for NATS Client
//!
//! Tests for cross-client messaging.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const test_port = utils.test_port;

pub fn testCrossClientRouting(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - the subscriber
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{
        .name = "client-A",
    }) catch {
        reportResult("cross_client_routing", false, "client A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - the publisher
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{
        .name = "client-B",
    }) catch {
        reportResult("cross_client_routing", false, "client B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client A subscribes
    const sub = client_a.subscribe(allocator, "cross.client.test") catch {
        reportResult("cross_client_routing", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client_a.flush() catch {};

    // Small delay for subscription to propagate
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // Client B publishes
    client_b.publish("cross.client.test", "hello from B") catch {
        reportResult("cross_client_routing", false, "publish failed");
        return;
    };
    client_b.flush() catch {};

    // Client A receives
    const msg = sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_routing", false, "receive failed");
        return;
    };

    if (msg) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "hello from B")) {
            reportResult("cross_client_routing", true, "");
        } else {
            reportResult("cross_client_routing", false, "wrong data");
        }
    } else {
        reportResult("cross_client_routing", false, "no message received");
    }
}

// Test 33: Cross-client request-reply (A requests, B responds)

pub fn testCrossClientRequestReply(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - the requester
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();

    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{
        .name = "requester",
    }) catch {
        reportResult("cross_client_request", false, "client A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - the responder (service)
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();

    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{
        .name = "responder",
    }) catch {
        reportResult("cross_client_request", false, "client B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client B subscribes to service subject
    const service_sub = client_b.subscribe(allocator, "math.add") catch {
        reportResult("cross_client_request", false, "service sub failed");
        return;
    };
    defer service_sub.deinit(allocator);
    client_b.flush() catch {};

    // Small delay for subscription to propagate
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // Client A creates inbox and subscribes
    const inbox = nats.newInbox(allocator) catch {
        reportResult("cross_client_request", false, "inbox failed");
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client_a.subscribe(allocator, inbox) catch {
        reportResult("cross_client_request", false, "reply sub failed");
        return;
    };
    defer reply_sub.deinit(allocator);
    client_a.flush() catch {};

    // Client A sends request
    client_a.publishRequest("math.add", inbox, "2+3") catch {
        reportResult("cross_client_request", false, "request failed");
        return;
    };
    client_a.flush() catch {};

    // Client B receives request
    const req = service_sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_request", false, "no request");
        return;
    };

    if (req) |r| {
        defer r.deinit(allocator);

        // Verify request has reply_to
        if (r.reply_to) |reply_to| {
            // Client B sends response
            client_b.publish(reply_to, "5") catch {
                reportResult("cross_client_request", false, "reply failed");
                return;
            };
            client_b.flush() catch {};
        } else {
            reportResult("cross_client_request", false, "no reply_to");
            return;
        }
    } else {
        reportResult("cross_client_request", false, "request timeout");
        return;
    }

    // Client A receives response
    const reply = reply_sub.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("cross_client_request", false, "reply receive failed");
        return;
    };

    if (reply) |rep| {
        defer rep.deinit(allocator);
        if (std.mem.eql(u8, rep.data, "5")) {
            reportResult("cross_client_request", true, "");
        } else {
            reportResult("cross_client_request", false, "wrong response");
        }
    } else {
        reportResult("cross_client_request", false, "no response");
    }
}

// Test 34: Three clients in a chain (A -> B -> C)

pub fn testThreeClientChain(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    // Client A - initial publisher
    var io_a: std.Io.Threaded = .init(allocator, .{});
    defer io_a.deinit();
    const client_a = nats.Client.connect(allocator, io_a.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "A connect failed");
        return;
    };
    defer client_a.deinit(allocator);

    // Client B - middleware (receives from A, forwards to C)
    var io_b: std.Io.Threaded = .init(allocator, .{});
    defer io_b.deinit();
    const client_b = nats.Client.connect(allocator, io_b.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "B connect failed");
        return;
    };
    defer client_b.deinit(allocator);

    // Client C - final receiver
    var io_c: std.Io.Threaded = .init(allocator, .{});
    defer io_c.deinit();
    const client_c = nats.Client.connect(allocator, io_c.io(), url, .{}) catch {
        reportResult("three_client_chain", false, "C connect failed");
        return;
    };
    defer client_c.deinit(allocator);

    // B subscribes to "step1"
    const sub_b = client_b.subscribe(allocator, "chain.step1") catch {
        reportResult("three_client_chain", false, "B sub failed");
        return;
    };
    defer sub_b.deinit(allocator);

    // C subscribes to "step2"
    const sub_c = client_c.subscribe(allocator, "chain.step2") catch {
        reportResult("three_client_chain", false, "C sub failed");
        return;
    };
    defer sub_c.deinit(allocator);

    client_b.flush() catch {};
    client_c.flush() catch {};
    std.posix.nanosleep(0, 50_000_000); // 50ms

    // A publishes to step1
    client_a.publish("chain.step1", "start") catch {
        reportResult("three_client_chain", false, "A publish failed");
        return;
    };
    client_a.flush() catch {};

    // B receives and forwards to step2
    const msg_b = sub_b.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("three_client_chain", false, "B receive failed");
        return;
    };
    if (msg_b) |m| {
        defer m.deinit(allocator);
        client_b.publish("chain.step2", "forwarded") catch {
            reportResult("three_client_chain", false, "B forward failed");
            return;
        };
        client_b.flush() catch {};
    } else {
        reportResult("three_client_chain", false, "B no message");
        return;
    }

    // C receives final message
    const msg_c = sub_c.nextMessage(allocator, .{ .timeout_ms = 2000 }) catch {
        reportResult("three_client_chain", false, "C receive failed");
        return;
    };
    if (msg_c) |m| {
        defer m.deinit(allocator);
        if (std.mem.eql(u8, m.data, "forwarded")) {
            reportResult("three_client_chain", true, "");
        } else {
            reportResult("three_client_chain", false, "wrong data");
        }
    } else {
        reportResult("three_client_chain", false, "C no message");
    }
}

// Test 35: Publish after disconnect should fail

pub fn testMultipleSubscribersSameSubject(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{});
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("multi_sub_same_subject", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create 3 subscribers to same subject
    const sub1 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub1 failed");
        return;
    };
    defer sub1.deinit(allocator);

    const sub2 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    const sub3 = client.subscribe(allocator, "broadcast.test") catch {
        reportResult("multi_sub_same_subject", false, "sub3 failed");
        return;
    };
    defer sub3.deinit(allocator);

    client.flush() catch {};

    // Publish one message
    client.publish("broadcast.test", "hello all") catch {
        reportResult("multi_sub_same_subject", false, "publish failed");
        return;
    };
    client.flush() catch {};

    // All 3 should receive the message
    var count: u32 = 0;

    if (sub1.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub2.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }
    if (sub3.nextMessage(allocator, .{ .timeout_ms = 500 }) catch null) |m| {
        m.deinit(allocator);
        count += 1;
    }

    if (count == 3) {
        reportResult("multi_sub_same_subject", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/3", .{count}) catch "err";
        reportResult("multi_sub_same_subject", false, detail);
    }
}

// Test 29: Message ordering preserved

/// Runs all multi-client tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testCrossClientRouting(allocator);
    testCrossClientRequestReply(allocator);
    testThreeClientChain(allocator);
    testMultipleSubscribersSameSubject(allocator);
}
