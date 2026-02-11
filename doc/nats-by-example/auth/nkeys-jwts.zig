//! NKeys and JWTs
//!
//! This example demonstrates NATS decentralized authentication using
//! the operator/account/user keypair hierarchy. It generates NKey
//! keypairs, encodes JWTs, and formats a credentials file - all
//! using pure Zig cryptography with zero external dependencies.
//!
//! No NATS server is needed - this is a pure cryptography example.
//!
//! Key concepts shown:
//! - NKey generation for operator, account, and user entities
//! - JWT encoding with Ed25519 signatures
//! - Credentials file formatting for client authentication
//! - The three-level trust hierarchy: operator > account > user
//!
//! Based on:
//!   https://natsbyexample.com/examples/auth/nkeys-jwts/go
//!
//! Run with: zig build run-nbe-auth-nkeys-jwts

const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    // Initialize async I/O runtime (needed for key generation)
    var threaded: std.Io.Threaded = .init(
        init.gpa,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    // Set up buffered stdout writer
    var stdout_buf: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(
        io,
        &stdout_buf,
    );
    const stdout = &stdout_writer.interface;

    // --- Operator ---
    // The operator is the top-level entity that manages
    // accounts. It signs account JWTs.
    try stdout.print(
        "== Operator ==\n",
        .{},
    );

    var op_kp = nats.auth.KeyPair.generate(io, .operator);
    defer op_kp.wipe();

    var op_pk_buf: [56]u8 = undefined;
    const op_pub = op_kp.publicKey(&op_pk_buf);
    try stdout.print(
        "operator public key: {s}\n",
        .{op_pub},
    );

    var op_seed_buf: [58]u8 = undefined;
    const op_seed = op_kp.encodeSeed(&op_seed_buf);
    try stdout.print(
        "operator seed:       {s}\n\n",
        .{op_seed},
    );

    // --- Account ---
    // An account groups users and defines resource limits.
    // The operator signs account JWTs.
    try stdout.print(
        "== Account ==\n",
        .{},
    );

    var acct_kp = nats.auth.KeyPair.generate(io, .account);
    defer acct_kp.wipe();

    var acct_pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&acct_pk_buf);
    try stdout.print(
        "account public key: {s}\n",
        .{acct_pub},
    );

    var acct_seed_buf: [58]u8 = undefined;
    const acct_seed = acct_kp.encodeSeed(&acct_seed_buf);
    try stdout.print(
        "account seed:       {s}\n\n",
        .{acct_seed},
    );

    // Encode account JWT (signed by operator)
    const ts = std.Io.Timestamp.now(io, .real);
    const iat: i64 = @intCast(
        @as(u64, @intCast(ts.nanoseconds)) /
            std.time.ns_per_s,
    );

    var acct_jwt_buf: [2048]u8 = undefined;
    const acct_jwt = try nats.auth.jwt.encodeAccountClaims(
        &acct_jwt_buf,
        acct_pub,
        "my-account",
        op_kp,
        iat,
        .{},
    );
    try stdout.print(
        "account JWT:\n{s}\n\n",
        .{acct_jwt},
    );

    // --- User ---
    // A user belongs to an account. The account signs
    // user JWTs with publish/subscribe permissions.
    try stdout.print(
        "== User ==\n",
        .{},
    );

    var user_kp = nats.auth.KeyPair.generate(io, .user);
    defer user_kp.wipe();

    var user_pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&user_pk_buf);
    try stdout.print(
        "user public key: {s}\n",
        .{user_pub},
    );

    var user_seed_buf: [58]u8 = undefined;
    const user_seed = user_kp.encodeSeed(&user_seed_buf);
    try stdout.print(
        "user seed:       {s}\n\n",
        .{user_seed},
    );

    // Encode user JWT with permissions (signed by account)
    var user_jwt_buf: [2048]u8 = undefined;
    const user_jwt = try nats.auth.jwt.encodeUserClaims(
        &user_jwt_buf,
        user_pub,
        "my-user",
        acct_kp,
        iat,
        .{
            .pub_allow = &.{"app.>"},
            .sub_allow = &.{ "app.>", "_INBOX.>" },
        },
    );
    try stdout.print(
        "user JWT:\n{s}\n\n",
        .{user_jwt},
    );

    // --- Credentials File ---
    // Format a .creds file containing the user JWT and seed.
    // This file is what a NATS client uses to authenticate.
    try stdout.print(
        "== Credentials File ==\n",
        .{},
    );

    var creds_buf: [4096]u8 = undefined;
    const creds = nats.auth.creds.format(
        &creds_buf,
        user_jwt,
        user_seed,
    );
    try stdout.print("{s}\n", .{creds});

    try stdout.flush();
}
