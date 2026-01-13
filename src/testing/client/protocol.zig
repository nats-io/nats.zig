//! Protocol Tests for NATS Async Client
//!
//! Tests for NATS protocol handling including -ERR responses,
//! PING/PONG keep-alive, INFO parsing, and edge cases.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const formatAuthUrl = utils.formatAuthUrl;
const test_port = utils.test_port;
const auth_port = utils.auth_port;
const test_token = utils.test_token;
const ServerManager = utils.ServerManager;

// Test: Server INFO parsing verification
// Verifies that client correctly parses and stores server INFO fields.
pub fn testServerInfoParsing(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("server_info_parsing", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("server_info_parsing", false, "no server info");
        return;
    }

    const server_info = info.?;

    // Verify required fields are present and valid
    if (server_info.server_id.len == 0) {
        reportResult("server_info_parsing", false, "empty server_id");
        return;
    }

    if (server_info.version.len == 0) {
        reportResult("server_info_parsing", false, "empty version");
        return;
    }

    // max_payload should be > 0 (typically 1MB)
    if (server_info.max_payload == 0) {
        reportResult("server_info_parsing", false, "max_payload is 0");
        return;
    }

    // proto should be >= 1
    if (server_info.proto < 1) {
        reportResult("server_info_parsing", false, "proto < 1");
        return;
    }

    reportResult("server_info_parsing", true, "");
}

// Test: PING/PONG keep-alive exchange
// Verifies client responds to server PING with PONG.
// Connection should stay alive after PING/PONG exchange.
pub fn testPingPongKeepAlive(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("ping_pong_keep_alive", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe and do pub/sub to trigger background reader activity
    const sub = client.subscribe(allocator, "ping.test") catch {
        reportResult("ping_pong_keep_alive", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);

    // Multiple publish/flush cycles with delays
    // Server should send PINGs during idle periods, client should respond
    for (0..5) |_| {
        client.publish("ping.test", "keep-alive") catch {
            reportResult("ping_pong_keep_alive", false, "publish failed");
            return;
        };
        client.flush() catch {
            reportResult("ping_pong_keep_alive", false, "flush failed");
            return;
        };
        // Small delay between operations
        io.io().sleep(.fromMilliseconds(100), .awake) catch {};
    }

    // Connection should still be alive after all operations
    if (!client.isConnected()) {
        reportResult("ping_pong_keep_alive", false, "disconnected");
        return;
    }

    // Drain messages to ensure reader task is healthy
    var received: u32 = 0;
    for (0..5) |_| {
        if (sub.nextWithTimeout(allocator, 200) catch null) |m| {
            m.deinit(allocator);
            received += 1;
        }
    }

    if (received == 5) {
        reportResult("ping_pong_keep_alive", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/5", .{received}) catch "e";
        reportResult("ping_pong_keep_alive", false, detail);
    }
}

// Test: Authorization error on wrong token
// Verifies that -ERR Authorization Violation is properly detected.
pub fn testProtocolAuthError(allocator: std.mem.Allocator) void {
    var url_buf: [128]u8 = undefined;
    // Connect to auth server with WRONG token
    const url = formatAuthUrl(&url_buf, auth_port, "wrong-token");

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{});

    if (result) |client| {
        // Should have failed with auth error
        client.deinit(allocator);
        reportResult("protocol_auth_error", false, "should have failed");
    } else |err| {
        // Expect AuthorizationViolation
        if (err == error.AuthorizationViolation) {
            reportResult("protocol_auth_error", true, "");
        } else {
            reportResult("protocol_auth_error", false, "wrong error type");
        }
    }
}

// Test: Message for unknown SID is silently dropped
// Verifies that messages arriving for unsubscribed SIDs don't crash.
pub fn testUnknownSidHandling(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("unknown_sid_handling", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Subscribe, then unsubscribe
    const sub1 = client.subscribe(allocator, "unknown.sid.test") catch {
        reportResult("unknown_sid_handling", false, "subscribe failed");
        return;
    };

    client.flush() catch {
        sub1.deinit(allocator);
        reportResult("unknown_sid_handling", false, "flush1 failed");
        return;
    };

    // Unsubscribe
    sub1.unsubscribe() catch {
        sub1.deinit(allocator);
        reportResult("unknown_sid_handling", false, "unsubscribe failed");
        return;
    };
    sub1.deinit(allocator);

    client.flush() catch {
        reportResult("unknown_sid_handling", false, "flush2 failed");
        return;
    };

    // Create a new subscription to same subject (different SID)
    const sub2 = client.subscribe(allocator, "unknown.sid.test") catch {
        reportResult("unknown_sid_handling", false, "subscribe2 failed");
        return;
    };
    defer sub2.deinit(allocator);

    client.flush() catch {
        reportResult("unknown_sid_handling", false, "flush3 failed");
        return;
    };

    // Publish - only sub2 should receive
    client.publish("unknown.sid.test", "test") catch {
        reportResult("unknown_sid_handling", false, "publish failed");
        return;
    };
    client.flush() catch {
        reportResult("unknown_sid_handling", false, "flush4 failed");
        return;
    };

    // sub2 should receive the message
    if (sub2.nextWithTimeout(allocator, 500) catch null) |m| {
        m.deinit(allocator);
        if (client.isConnected()) {
            reportResult("unknown_sid_handling", true, "");
        } else {
            reportResult("unknown_sid_handling", false, "disconnected");
        }
    } else {
        reportResult("unknown_sid_handling", false, "no message");
    }
}

// Test: Protocol parser rejects invalid command
// Tests that malformed protocol commands are handled gracefully.
pub fn testInvalidProtocolCommand(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    // Try to parse invalid command
    const result = parser.parse(allocator, "INVALID_CMD\r\n", &consumed);

    if (result) |_| {
        reportResult("invalid_protocol_cmd", false, "should have failed");
    } else |err| {
        // Expect InvalidCommand error
        if (err == error.InvalidCommand) {
            reportResult("invalid_protocol_cmd", true, "");
        } else {
            reportResult("invalid_protocol_cmd", false, "wrong error type");
        }
    }
}

// Test: Protocol parser handles partial data correctly
// Verifies parser returns null for incomplete commands.
pub fn testProtocolPartialData(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    // Partial PING (no \r\n)
    const partial_result = parser.parse(allocator, "PIN", &consumed) catch {
        reportResult("protocol_partial_data", false, "unexpected error");
        return;
    };

    if (partial_result != null) {
        reportResult("protocol_partial_data", false, "should return null");
        return;
    }

    if (consumed != 0) {
        reportResult("protocol_partial_data", false, "consumed != 0");
        return;
    }

    // Complete PING
    const full_result = parser.parse(allocator, "PING\r\n", &consumed) catch {
        reportResult("protocol_partial_data", false, "parse error");
        return;
    };

    if (full_result == null) {
        reportResult("protocol_partial_data", false, "should return command");
        return;
    }

    if (consumed != 6) {
        reportResult("protocol_partial_data", false, "wrong consumed");
        return;
    }

    reportResult("protocol_partial_data", true, "");
}

// Test: Protocol parser handles MSG with partial payload
// Verifies parser waits for complete payload before returning.
pub fn testProtocolPartialMsgPayload(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    // MSG header says 10 bytes, but we only provide 5
    const partial_msg = "MSG test.subject 1 10\r\nhello";

    const partial_result = parser.parse(allocator, partial_msg, &consumed) catch {
        reportResult("protocol_partial_msg", false, "unexpected error");
        return;
    };

    if (partial_result != null) {
        reportResult("protocol_partial_msg", false, "should return null");
        return;
    }

    if (consumed != 0) {
        reportResult("protocol_partial_msg", false, "consumed should be 0");
        return;
    }

    // Complete message
    const full_msg = "MSG test.subject 1 10\r\nhelloworld\r\n";
    const full_result = parser.parse(allocator, full_msg, &consumed) catch {
        reportResult("protocol_partial_msg", false, "parse error");
        return;
    };

    if (full_result == null) {
        reportResult("protocol_partial_msg", false, "should return command");
        return;
    }

    const msg = full_result.?.msg;
    if (!std.mem.eql(u8, msg.payload, "helloworld")) {
        reportResult("protocol_partial_msg", false, "wrong payload");
        return;
    }

    reportResult("protocol_partial_msg", true, "");
}

// Test: Protocol -ERR parsing
// Verifies different -ERR messages are categorized correctly.
pub fn testProtocolErrorParsing(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    // Test Authorization Violation
    const auth_err = "-ERR 'Authorization Violation'\r\n";
    const auth_result = parser.parse(allocator, auth_err, &consumed) catch {
        reportResult("protocol_err_parsing", false, "parse error");
        return;
    };

    if (auth_result == null) {
        reportResult("protocol_err_parsing", false, "auth null");
        return;
    }

    const err_msg = auth_result.?.err;
    if (!std.mem.eql(u8, err_msg, "'Authorization Violation'")) {
        reportResult("protocol_err_parsing", false, "wrong auth msg");
        return;
    }

    // Verify parseServerError categorizes correctly
    const err = protocol.parseServerError(err_msg);
    if (err != error.AuthorizationViolation) {
        reportResult("protocol_err_parsing", false, "wrong error type");
        return;
    }

    // Test Maximum Payload error
    consumed = 0;
    const payload_err = "-ERR 'Maximum Payload Exceeded'\r\n";
    const payload_result = parser.parse(allocator, payload_err, &consumed) catch {
        reportResult("protocol_err_parsing", false, "payload parse error");
        return;
    };

    if (payload_result == null) {
        reportResult("protocol_err_parsing", false, "payload null");
        return;
    }

    const payload_err_msg = payload_result.?.err;
    const payload_err_type = protocol.parseServerError(payload_err_msg);
    if (payload_err_type != error.MaxPayloadExceeded) {
        reportResult("protocol_err_parsing", false, "wrong payload error");
        return;
    }

    reportResult("protocol_err_parsing", true, "");
}

// Test: Server max_payload is enforced
// Verifies that server's max_payload limit is available to client.
pub fn testMaxPayloadLimit(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("max_payload_limit", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    const info = client.getServerInfo();
    if (info == null) {
        reportResult("max_payload_limit", false, "no server info");
        return;
    }

    const max_payload = info.?.max_payload;

    // Server typically has 1MB max payload
    if (max_payload < 1024) {
        reportResult("max_payload_limit", false, "max_payload too small");
        return;
    }

    // Verify it's a reasonable value (not corrupted)
    if (max_payload > 64 * 1024 * 1024) {
        reportResult("max_payload_limit", false, "max_payload too large");
        return;
    }

    reportResult("max_payload_limit", true, "");
}

// Test: Protocol +OK handling
// Verifies +OK responses are parsed correctly.
pub fn testProtocolOkResponse(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    const ok_result = parser.parse(allocator, "+OK\r\n", &consumed) catch {
        reportResult("protocol_ok_response", false, "parse error");
        return;
    };

    if (ok_result == null) {
        reportResult("protocol_ok_response", false, "null result");
        return;
    }

    if (ok_result.? != .ok) {
        reportResult("protocol_ok_response", false, "wrong command type");
        return;
    }

    if (consumed != 5) {
        reportResult("protocol_ok_response", false, "wrong consumed");
        return;
    }

    reportResult("protocol_ok_response", true, "");
}

// Test: Connection survives protocol activity
// Verifies connection remains healthy after various protocol exchanges.
pub fn testProtocolStability(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, test_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{}) catch {
        reportResult("protocol_stability", false, "connect failed");
        return;
    };
    defer client.deinit(allocator);

    // Create multiple subscriptions
    var subs: [5]?*nats.Subscription = [_]?*nats.Subscription{null} ** 5;
    defer for (&subs) |*s| {
        if (s.*) |sub| sub.deinit(allocator);
    };

    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(&buf, "stability.{d}", .{i}) catch continue;
        subs[i] = client.subscribe(allocator, subject) catch {
            reportResult("protocol_stability", false, "subscribe failed");
            return;
        };
    }

    client.flush() catch {
        reportResult("protocol_stability", false, "flush1 failed");
        return;
    };

    // Publish to all subjects
    for (0..5) |i| {
        var buf: [32]u8 = undefined;
        const subject = std.fmt.bufPrint(&buf, "stability.{d}", .{i}) catch continue;
        client.publish(subject, "test") catch {
            reportResult("protocol_stability", false, "publish failed");
            return;
        };
    }

    client.flush() catch {
        reportResult("protocol_stability", false, "flush2 failed");
        return;
    };

    // Receive all messages
    var received: u32 = 0;
    for (0..5) |i| {
        if (subs[i]) |sub| {
            if (sub.nextWithTimeout(allocator, 500) catch null) |m| {
                m.deinit(allocator);
                received += 1;
            }
        }
    }

    // Unsubscribe some
    for (0..3) |i| {
        if (subs[i]) |sub| {
            sub.unsubscribe() catch {};
        }
    }

    // Connection should still be healthy
    if (!client.isConnected()) {
        reportResult("protocol_stability", false, "disconnected");
        return;
    }

    if (received == 5) {
        reportResult("protocol_stability", true, "");
    } else {
        var buf: [32]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "got {d}/5", .{received}) catch "e";
        reportResult("protocol_stability", false, detail);
    }
}

// Test: MSG with reply-to parsing
// Verifies MSG with reply-to field is parsed correctly.
pub fn testProtocolMsgWithReplyTo(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    const msg_data = "MSG test.subject 42 _INBOX.reply123 5\r\nhello\r\n";
    const result = parser.parse(allocator, msg_data, &consumed) catch {
        reportResult("protocol_msg_reply", false, "parse error");
        return;
    };

    if (result == null) {
        reportResult("protocol_msg_reply", false, "null result");
        return;
    }

    const msg = result.?.msg;

    if (!std.mem.eql(u8, msg.subject, "test.subject")) {
        reportResult("protocol_msg_reply", false, "wrong subject");
        return;
    }

    if (msg.sid != 42) {
        reportResult("protocol_msg_reply", false, "wrong sid");
        return;
    }

    if (msg.reply_to == null) {
        reportResult("protocol_msg_reply", false, "no reply_to");
        return;
    }

    if (!std.mem.eql(u8, msg.reply_to.?, "_INBOX.reply123")) {
        reportResult("protocol_msg_reply", false, "wrong reply_to");
        return;
    }

    if (!std.mem.eql(u8, msg.payload, "hello")) {
        reportResult("protocol_msg_reply", false, "wrong payload");
        return;
    }

    reportResult("protocol_msg_reply", true, "");
}

// Test: HMSG (headers message) parsing
// Verifies HMSG with headers is parsed correctly.
pub fn testProtocolHmsgParsing(allocator: std.mem.Allocator) void {
    const protocol = @import("nats").protocol;
    var parser: protocol.Parser = .{};
    var consumed: usize = 0;

    // HMSG format: HMSG <subject> <sid> [reply-to] <header-len> <total-len>
    const hmsg_data = "HMSG test.subject 1 12 17\r\nNATS/1.0\r\n\r\nhello\r\n";
    const result = parser.parse(allocator, hmsg_data, &consumed) catch {
        reportResult("protocol_hmsg_parsing", false, "parse error");
        return;
    };

    if (result == null) {
        reportResult("protocol_hmsg_parsing", false, "null result");
        return;
    }

    const hmsg = result.?.hmsg;

    if (!std.mem.eql(u8, hmsg.subject, "test.subject")) {
        reportResult("protocol_hmsg_parsing", false, "wrong subject");
        return;
    }

    if (hmsg.sid != 1) {
        reportResult("protocol_hmsg_parsing", false, "wrong sid");
        return;
    }

    if (hmsg.header_len != 12) {
        reportResult("protocol_hmsg_parsing", false, "wrong header_len");
        return;
    }

    if (!std.mem.eql(u8, hmsg.payload, "hello")) {
        reportResult("protocol_hmsg_parsing", false, "wrong payload");
        return;
    }

    reportResult("protocol_hmsg_parsing", true, "");
}

/// Runs all protocol tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testServerInfoParsing(allocator);
    testPingPongKeepAlive(allocator);
    testProtocolAuthError(allocator);
    testUnknownSidHandling(allocator);
    testInvalidProtocolCommand(allocator);
    testProtocolPartialData(allocator);
    testProtocolPartialMsgPayload(allocator);
    testProtocolErrorParsing(allocator);
    testMaxPayloadLimit(allocator);
    testProtocolOkResponse(allocator);
    testProtocolStability(allocator);
    testProtocolMsgWithReplyTo(allocator);
    testProtocolHmsgParsing(allocator);
}
