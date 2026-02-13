//! Dynamic JWT Integration Tests
//!
//! Generates operator/account/user JWTs at test time using
//! the auth API, writes a temporary server config, and
//! verifies pub/sub with dynamically generated credentials.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const ServerManager = utils.ServerManager;

const Ed25519 = std.crypto.sign.Ed25519;
const nkey_mod = nats.auth.nkey;
const jwt_mod = nats.auth.jwt;
const creds_mod = nats.auth.creds;

const dynamic_jwt_port = utils.dynamic_jwt_port;
const config_path = "/tmp/nats-dynamic-jwt-test.conf";

const Dir = std.Io.Dir;

/// Deterministic keypair seeds for reproducibility.
const op_raw_seed = [_]u8{11} ** 32;
const acct_raw_seed = [_]u8{22} ** 32;
const user_raw_seed = [_]u8{33} ** 32;

/// Generates all keypairs and JWTs, writes config.
/// Returns formatted credentials string in out_creds.
fn setupDynamicAuth(
    io: std.Io,
    out_creds: *[8192]u8,
) ?[]const u8 {
    // Operator keypair (self-signed)
    const op_ed = Ed25519.KeyPair.generateDeterministic(
        op_raw_seed,
    ) catch return null;
    const op_kp = nkey_mod.KeyPair{
        .kp = op_ed,
        .key_type = .operator,
    };

    // Account keypair
    const acct_ed = Ed25519.KeyPair.generateDeterministic(
        acct_raw_seed,
    ) catch return null;
    const acct_kp = nkey_mod.KeyPair{
        .kp = acct_ed,
        .key_type = .account,
    };

    // User keypair
    const user_ed = Ed25519.KeyPair.generateDeterministic(
        user_raw_seed,
    ) catch return null;
    const user_kp = nkey_mod.KeyPair{
        .kp = user_ed,
        .key_type = .user,
    };

    // Public keys
    var op_pk_buf: [56]u8 = undefined;
    const op_pub = op_kp.publicKey(&op_pk_buf);

    var acct_pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&acct_pk_buf);

    var user_pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&user_pk_buf);

    // Encode operator JWT (self-signed)
    var op_jwt_buf: [2048]u8 = undefined;
    const op_jwt = jwt_mod.encodeOperatorClaims(
        &op_jwt_buf,
        op_pub,
        "dyn-operator",
        op_kp,
        1700000000,
        .{},
    ) catch return null;

    // Encode account JWT (signed by operator)
    var acct_jwt_buf: [2048]u8 = undefined;
    const acct_jwt = jwt_mod.encodeAccountClaims(
        &acct_jwt_buf,
        acct_pub,
        "dyn-account",
        op_kp,
        1700000000,
        .{},
    ) catch return null;

    // Encode user JWT (signed by account)
    var user_jwt_buf: [2048]u8 = undefined;
    const user_jwt = jwt_mod.encodeUserClaims(
        &user_jwt_buf,
        user_pub,
        "dyn-user",
        acct_kp,
        1700000000,
        .{
            .pub_allow = &.{">"},
            .sub_allow = &.{">"},
        },
    ) catch return null;

    // Encode user seed for creds file
    var seed_buf: [58]u8 = undefined;
    const user_seed = user_kp.encodeSeed(&seed_buf);

    // Format credentials
    const creds_str = creds_mod.format(
        out_creds,
        user_jwt,
        user_seed,
    );

    // Write server config file
    writeConfig(io, op_jwt, acct_pub, acct_jwt) catch
        return null;

    return creds_str;
}

/// Writes nats-server config to temp file.
fn writeConfig(
    io: std.Io,
    op_jwt: []const u8,
    acct_pub: []const u8,
    acct_jwt: []const u8,
) !void {
    const file = try Dir.createFile(
        Dir.cwd(),
        io,
        config_path,
        .{},
    );
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.print(
        "operator: {s}\n" ++
            "resolver: MEMORY\n" ++
            "resolver_preload: {{\n" ++
            "  {s}: {s}\n" ++
            "}}\n",
        .{ op_jwt, acct_pub, acct_jwt },
    );
    try writer.interface.flush();
}

/// Cleans up temp config file.
fn cleanupConfig(io: std.Io) void {
    Dir.deleteFile(Dir.cwd(), io, config_path) catch {};
}

/// Tests connecting with dynamically generated JWT creds.
pub fn testDynamicJwtConnect(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    var threaded: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    var creds_buf: [8192]u8 = undefined;
    const creds_str = setupDynamicAuth(
        io,
        &creds_buf,
    ) orelse {
        reportResult(
            "dynamic_jwt_connect",
            false,
            "setup failed",
        );
        return;
    };

    // Start server with dynamic config
    _ = manager.startServer(allocator, io, .{
        .port = dynamic_jwt_port,
        .config_file = config_path,
    }) catch {
        reportResult(
            "dynamic_jwt_connect",
            false,
            "server start failed",
        );
        cleanupConfig(io);
        return;
    };

    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, dynamic_jwt_port);

    const client = nats.Client.connect(
        allocator,
        io,
        url,
        .{
            .reconnect = false,
            .creds = creds_str,
        },
    ) catch |err| {
        var ebuf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &ebuf,
            "connect: {}",
            .{err},
        ) catch "fmt";
        reportResult(
            "dynamic_jwt_connect",
            false,
            detail,
        );
        return;
    };
    defer client.deinit();

    if (client.isConnected()) {
        reportResult(
            "dynamic_jwt_connect",
            true,
            "",
        );
    } else {
        reportResult(
            "dynamic_jwt_connect",
            false,
            "not connected",
        );
    }
}

/// Tests pub/sub with dynamically generated JWT creds.
pub fn testDynamicJwtPubSub(
    allocator: std.mem.Allocator,
) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, dynamic_jwt_port);

    var threaded: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    // Regenerate creds (deterministic, same output)
    var creds_buf: [8192]u8 = undefined;
    const creds_str = setupDynamicAuth(
        io,
        &creds_buf,
    ) orelse {
        reportResult(
            "dynamic_jwt_pubsub",
            false,
            "setup failed",
        );
        return;
    };

    const client = nats.Client.connect(
        allocator,
        io,
        url,
        .{
            .reconnect = false,
            .creds = creds_str,
        },
    ) catch |err| {
        var ebuf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(
            &ebuf,
            "connect: {}",
            .{err},
        ) catch "fmt";
        reportResult(
            "dynamic_jwt_pubsub",
            false,
            detail,
        );
        return;
    };
    defer client.deinit();

    const sub = client.subscribeSync(
        "dynamic.jwt.test",
    ) catch {
        reportResult(
            "dynamic_jwt_pubsub",
            false,
            "subscribe failed",
        );
        return;
    };
    defer sub.deinit();

    const test_msg = "dynamic jwt message";
    client.publish(
        "dynamic.jwt.test",
        test_msg,
    ) catch {
        reportResult(
            "dynamic_jwt_pubsub",
            false,
            "publish failed",
        );
        return;
    };

    client.flush(500_000_000) catch {};

    if (sub.nextMsgTimeout(
        1000,
    ) catch null) |m| {
        defer m.deinit();
        if (std.mem.eql(u8, m.data, test_msg)) {
            reportResult(
                "dynamic_jwt_pubsub",
                true,
                "",
            );
        } else {
            reportResult(
                "dynamic_jwt_pubsub",
                false,
                "message mismatch",
            );
        }
    } else {
        reportResult(
            "dynamic_jwt_pubsub",
            false,
            "no message received",
        );
    }
}

/// Runs all dynamic JWT tests.
pub fn runAll(
    allocator: std.mem.Allocator,
    manager: *ServerManager,
) void {
    testDynamicJwtConnect(allocator, manager);
    testDynamicJwtPubSub(allocator);

    // Stop dynamic JWT server (last started)
    const idx = manager.count() - 1;
    var threaded: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    manager.stopServer(idx, io);
    cleanupConfig(io);
}
