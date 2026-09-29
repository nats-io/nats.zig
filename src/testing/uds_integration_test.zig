//! UNIX domain socket integration tests.
//!
//! Exercises the nats+uds:// transport end-to-end against a real
//! nats-server that has UDS support (the snats nats-server fork).
//!
//! Run with: zig build test-integration-uds
//!
//! UDS is enabled purely through a generated config file (the `uds { }`
//! block plus a peer-credential authorization rule), so this suite reuses
//! the shared TestServer harness unchanged via its `-c` config support.
//! Keeping everything in this one file minimises the merge surface with
//! upstream nats.zig.

const std = @import("std");
const utils = @import("test_utils.zig");
const nats = utils.nats;

const ServerManager = utils.ServerManager;
const reportResult = utils.reportResult;

/// TCP port kept open alongside the socket; only used by the harness's
/// readiness probe. The tests themselves connect over the socket.
const uds_tcp_port: u16 = 14243;
const socket_path = "/tmp/nats-zig-uds-it.sock";
const config_path = "/tmp/nats-zig-uds-it.conf";
const uds_url = "nats+uds://" ++ socket_path;

// TLS-over-UDS: the server's UDS accept loop runs the same createClient
// as TCP, so a server-global tls{} block applies to UDS connections too. Second
// server on its own socket/port with tls enabled.
const tls_uds_tcp_port: u16 = 14244;
const tls_socket_path = "/tmp/nats-zig-uds-tls-it.sock";
const tls_config_path = "/tmp/nats-zig-uds-tls-it.conf";
const tls_uds_url = "nats+uds://" ++ tls_socket_path;
// Token auth for the TLS+UDS server: snats's UDS peer-credential (uid) auth
// reads SO_PEERCRED off a *net.UnixConn and cannot see through the TLS wrapper
// (getUDSPeerCreds type-asserts *net.UnixConn, which a *tls.Conn is not), so
// uid-match auth does not compose with TLS. Token auth rides the CONNECT line
// over the encrypted stream and is transport-agnostic.
const uds_tls_token = "uds-tls-token";

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    utils.setProcessEnviron(init.minimal.environ);

    const test_io = utils.newIo(allocator);
    defer test_io.deinit();
    const io = test_io.io();

    std.debug.print("\n=== NATS UDS Integration Tests ===\n\n", .{});

    writeUdsConfig(io) catch |err| {
        std.debug.print("Failed to write UDS config: {}\n", .{err});
        std.process.exit(1);
    };
    defer deleteUdsConfig(io);
    writeUdsTlsConfig(io) catch |err| {
        std.debug.print("Failed to write UDS+TLS config: {}\n", .{err});
        std.process.exit(1);
    };
    defer std.Io.Dir.deleteFile(std.Io.Dir.cwd(), io, tls_config_path) catch {};

    var manager: ServerManager = .init(allocator);
    defer manager.deinit(allocator, io);

    std.debug.print("Starting UDS server (socket {s})...\n", .{socket_path});
    _ = manager.startServer(allocator, io, .{
        .port = uds_tcp_port,
        .config_file = config_path,
    }) catch |err| {
        std.debug.print("Failed to start UDS server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("Starting UDS+TLS server (socket {s})...\n", .{tls_socket_path});
    _ = manager.startServer(allocator, io, .{
        .port = tls_uds_tcp_port,
        .config_file = tls_config_path,
    }) catch |err| {
        std.debug.print("Failed to start UDS+TLS server: {}\n", .{err});
        std.process.exit(1);
    };

    std.debug.print("\nRunning UDS tests...\n\n", .{});

    testConnect(allocator);
    testPubSub(allocator);
    testRequestReply(allocator);
    testTlsPubSub(allocator);

    const summary = utils.getSummary();
    std.debug.print("\n=== UDS Test Summary ===\n", .{});
    std.debug.print("Passed: {d}\n", .{summary.passed});
    std.debug.print("Failed: {d}\n", .{summary.failed});
    std.debug.print("Total:  {d}\n\n", .{summary.total});

    if (summary.failed > 0) std.process.exit(1);
}

/// Connect over the socket and confirm the peer-credential handshake
/// authenticated us.
fn testConnect(allocator: std.mem.Allocator) void {
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), uds_url, .{
        .name = "uds-connect",
        .reconnect = false,
    }) catch |err| {
        reportError("uds_connect", "connect", err);
        return;
    };
    defer client.deinit();

    reportResult("uds_connect", client.isConnected(), "not connected");
}

/// Publish and receive a message over the socket.
fn testPubSub(allocator: std.mem.Allocator) void {
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), uds_url, .{
        .name = "uds-pubsub",
        .reconnect = false,
    }) catch |err| {
        reportError("uds_pubsub", "connect", err);
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("uds.echo") catch |err| {
        reportError("uds_pubsub", "subscribe", err);
        return;
    };
    defer sub.deinit();

    client.flush(5_000_000_000) catch |err| {
        reportError("uds_pubsub", "flush", err);
        return;
    };

    const payload = "hello over uds";
    client.publish("uds.echo", payload) catch |err| {
        reportError("uds_pubsub", "publish", err);
        return;
    };

    const msg = (sub.nextMsgTimeout(2000) catch |err| {
        reportError("uds_pubsub", "receive", err);
        return;
    }) orelse {
        reportResult("uds_pubsub", false, "no message received");
        return;
    };
    defer msg.deinit();

    reportResult(
        "uds_pubsub",
        std.mem.eql(u8, msg.data, payload),
        "payload mismatch",
    );
}

/// Request/reply round-trip over the socket. A second sync subscription on
/// the same client acts as the responder.
fn testRequestReply(allocator: std.mem.Allocator) void {
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), uds_url, .{
        .name = "uds-reqrep",
        .reconnect = false,
    }) catch |err| {
        reportError("uds_request_reply", "connect", err);
        return;
    };
    defer client.deinit();

    const service = client.subscribeSync("uds.service") catch |err| {
        reportError("uds_request_reply", "subscribe", err);
        return;
    };
    defer service.deinit();

    client.flush(5_000_000_000) catch |err| {
        reportError("uds_request_reply", "flush", err);
        return;
    };

    // Send the request, then service it from the same thread before reading
    // the reply: publishRequest only buffers, so the request subject is
    // delivered to our service subscription once we pump it.
    const inbox = client.newInbox() catch |err| {
        reportError("uds_request_reply", "inbox", err);
        return;
    };
    defer allocator.free(inbox);

    const reply_sub = client.subscribeSync(inbox) catch |err| {
        reportError("uds_request_reply", "reply_sub", err);
        return;
    };
    defer reply_sub.deinit();

    client.publishRequest("uds.service", inbox, "ping") catch |err| {
        reportError("uds_request_reply", "publish_request", err);
        return;
    };

    const req = (service.nextMsgTimeout(2000) catch |err| {
        reportError("uds_request_reply", "receive_request", err);
        return;
    }) orelse {
        reportResult("uds_request_reply", false, "no request received");
        return;
    };
    defer req.deinit();

    if (req.reply_to) |rt| {
        client.publish(rt, "pong") catch |err| {
            reportError("uds_request_reply", "respond", err);
            return;
        };
    } else {
        reportResult("uds_request_reply", false, "request had no reply_to");
        return;
    }

    const reply = (reply_sub.nextMsgTimeout(2000) catch |err| {
        reportError("uds_request_reply", "receive_reply", err);
        return;
    }) orelse {
        reportResult("uds_request_reply", false, "no reply received");
        return;
    };
    defer reply.deinit();

    reportResult(
        "uds_request_reply",
        std.mem.eql(u8, reply.data, "pong"),
        "reply mismatch",
    );
}

fn reportError(name: []const u8, step: []const u8, err: anyerror) void {
    var buf: [128]u8 = undefined;
    const details = std.fmt.bufPrint(
        &buf,
        "{s}: {s}",
        .{ step, @errorName(err) },
    ) catch step;
    reportResult(name, false, details);
}

/// Writes a minimal UDS server config: a socket listener plus a peer-cred
/// rule authorising the current uid. UDS connections have no default-allow
/// policy, so an explicit (allow-all) permissions block is required.
fn writeUdsConfig(io: std.Io) !void {
    const Dir = std.Io.Dir;

    const uid = std.os.linux.getuid();

    const file = try Dir.createFile(Dir.cwd(), io, config_path, .{});
    defer file.close(io);

    var buf: [512]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.print(
        \\uds {{
        \\  path: "{s}"
        \\}}
        \\authorization {{
        \\  users = [
        \\    {{
        \\      user: "uds-test"
        \\      uds {{ match {{ uid: {d} }} }}
        \\      permissions {{
        \\        publish {{ allow: [ ">" ] }}
        \\        subscribe {{ allow: [ ">" ] }}
        \\      }}
        \\    }}
        \\  ]
        \\}}
        \\
    ,
        .{ socket_path, uid },
    );
    try writer.interface.flush();
}

fn deleteUdsConfig(io: std.Io) void {
    const Dir = std.Io.Dir;
    Dir.deleteFile(Dir.cwd(), io, config_path) catch {};
}

/// Like writeUdsConfig, plus a server-global tls{} block. snats applies global
/// TLS to the UDS listener (same createClient path as TCP), so this exercises
/// the TLS x UDS matrix cell. Cert paths mirror configs/tls.conf (relative to
/// the server's CWD = repo root).
fn writeUdsTlsConfig(io: std.Io) !void {
    const Dir = std.Io.Dir;

    const file = try Dir.createFile(Dir.cwd(), io, tls_config_path, .{});
    defer file.close(io);

    var buf: [1024]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.print(
        \\uds {{
        \\  path: "{s}"
        \\}}
        \\tls {{
        \\  cert_file: "src/testing/certs/server-cert.pem"
        \\  key_file: "src/testing/certs/server-key.pem"
        \\  ca_file: "src/testing/certs/rootCA.pem"
        \\  timeout: 60
        \\}}
        \\authorization {{
        \\  token: "{s}"
        \\}}
        \\
    ,
        .{ tls_socket_path, uds_tls_token },
    );
    try writer.interface.flush();
}

/// TLS over UDS: connect over the socket with TLS forced, and round-trip a
/// message. `tls_insecure_skip_verify` because a UDS peer has no hostname to
/// match the server cert against.
fn testTlsPubSub(allocator: std.mem.Allocator) void {
    const io = utils.newIo(allocator);
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), tls_uds_url, .{
        .name = "uds-tls-pubsub",
        .reconnect = false,
        .tls_required = true,
        .tls_insecure_skip_verify = true,
        .auth_token = uds_tls_token,
    }) catch |err| {
        reportError("uds_tls_pubsub", "connect", err);
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync("uds.tls.echo") catch |err| {
        reportError("uds_tls_pubsub", "subscribe", err);
        return;
    };
    defer sub.deinit();

    client.flush(5_000_000_000) catch |err| {
        reportError("uds_tls_pubsub", "flush", err);
        return;
    };

    const payload = "hello over tls+uds";
    client.publish("uds.tls.echo", payload) catch |err| {
        reportError("uds_tls_pubsub", "publish", err);
        return;
    };

    const msg = (sub.nextMsgTimeout(2000) catch |err| {
        reportError("uds_tls_pubsub", "receive", err);
        return;
    }) orelse {
        reportResult("uds_tls_pubsub", false, "no message received");
        return;
    };
    defer msg.deinit();

    reportResult(
        "uds_tls_pubsub",
        std.mem.eql(u8, msg.data, payload),
        "payload mismatch",
    );
}
