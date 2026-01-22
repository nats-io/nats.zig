//! NKey Authentication Tests for NATS Client
//!
//! Tests NKey (Ed25519) authentication against nats-server.

const std = @import("std");
const utils = @import("../test_utils.zig");
const nats = utils.nats;

const reportResult = utils.reportResult;
const formatUrl = utils.formatUrl;
const nkey_port = utils.nkey_port;
const test_nkey_seed = utils.test_nkey_seed;

/// Tests successful NKey authentication with valid seed.
pub fn testNKeyAuthentication(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed = test_nkey_seed,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("nkey_authentication", false, detail);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("nkey_authentication", true, "");
    } else {
        reportResult("nkey_authentication", false, "not connected");
    }
}

/// Tests that authentication fails with wrong NKey seed.
pub fn testNKeyAuthFailure(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const wrong_seed =
        "SUAIBDPBAUTWCWBKIO6XHQNINK5FWJW4OHLXC3HQ2KFE4PEJUA44CNHTC4";

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed = wrong_seed,
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("nkey_auth_failure", false, "should have failed");
    } else |_| {
        reportResult("nkey_auth_failure", true, "");
    }
}

/// Tests pub/sub works after NKey authentication.
pub fn testNKeyPubSub(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed = test_nkey_seed,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("nkey_pubsub", false, detail);
        return;
    };
    defer client.deinit(allocator);

    const sub = client.subscribe(allocator, "nkey.test.subject") catch {
        reportResult("nkey_pubsub", false, "subscribe failed");
        return;
    };
    defer sub.deinit(allocator);
    client.flush(allocator) catch {};

    client.publish("nkey.test.subject", "nkey message") catch {
        reportResult("nkey_pubsub", false, "publish failed");
        return;
    };
    client.flush(allocator) catch {};

    if (sub.nextWithTimeout(allocator, 1000) catch null) |m| {
        m.deinit(allocator);
        reportResult("nkey_pubsub", true, "");
    } else {
        reportResult("nkey_pubsub", false, "no message");
    }
}

/// Tests that connecting without seed when NKey is required fails.
pub fn testNKeyNoSeedFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("nkey_no_seed_fails", false, "should have failed");
    } else |_| {
        reportResult("nkey_no_seed_fails", true, "");
    }
}

/// Tests that invalid seed format returns error.
pub fn testNKeyInvalidSeedFormat(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed = "SUAMK2FG",
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("nkey_invalid_seed", false, "should have failed");
    } else |_| {
        reportResult("nkey_invalid_seed", true, "");
    }
}

/// Tests authentication with NKey seed loaded from file.
pub fn testNKeySeedFile(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    // Create temp seed file using std.Io
    var io_setup: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io_setup.deinit();
    const setup_io = io_setup.io();

    const seed_file_path = utils.test_nkey_seed_file;

    // Write seed to file
    const file = std.Io.Dir.createFile(.cwd(), setup_io, seed_file_path, .{
        .truncate = true,
    }) catch {
        reportResult("nkey_seed_file", false, "failed to create seed file");
        return;
    };
    file.writeStreamingAll(setup_io, test_nkey_seed) catch {
        file.close(setup_io);
        reportResult("nkey_seed_file", false, "failed to write seed file");
        return;
    };
    file.close(setup_io);

    // Connect using seed file
    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed_file = seed_file_path,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("nkey_seed_file", false, detail);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("nkey_seed_file", true, "");
    } else {
        reportResult("nkey_seed_file", false, "not connected");
    }
}

/// Tests error when seed file does not exist.
pub fn testNKeySeedFileMissing(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_seed_file = "/nonexistent/path/to/seed.txt",
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("nkey_seed_file_missing", false, "should have failed");
    } else |_| {
        reportResult("nkey_seed_file_missing", true, "");
    }
}

/// Tests authentication with custom signing callback.
pub fn testNKeySigningCallback(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    // Derive public key from seed (must match server config)
    var kp = nats.auth.KeyPair.fromSeed(test_nkey_seed) catch {
        reportResult("nkey_signing_callback", false, "failed to parse seed");
        return;
    };
    defer kp.wipe();

    var pubkey_buf: [56]u8 = undefined;
    const pubkey = kp.publicKey(&pubkey_buf);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const client = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_pubkey = pubkey,
        .nkey_sign_fn = &testSignCallback,
    }) catch |err| {
        var buf: [64]u8 = undefined;
        const detail = std.fmt.bufPrint(&buf, "connect: {}", .{err}) catch "fmt";
        reportResult("nkey_signing_callback", false, detail);
        return;
    };
    defer client.deinit(allocator);

    if (client.isConnected()) {
        reportResult("nkey_signing_callback", true, "");
    } else {
        reportResult("nkey_signing_callback", false, "not connected");
    }
}

/// Test signing callback using the test seed.
fn testSignCallback(nonce: []const u8, sig: *[64]u8) bool {
    var kp = nats.auth.KeyPair.fromSeed(test_nkey_seed) catch return false;
    defer kp.wipe();
    sig.* = kp.sign(nonce);
    return true;
}

/// Tests error when signing callback returns false.
pub fn testNKeyCallbackFails(allocator: std.mem.Allocator) void {
    var url_buf: [64]u8 = undefined;
    const url = formatUrl(&url_buf, nkey_port);

    // Derive public key from seed
    var kp = nats.auth.KeyPair.fromSeed(test_nkey_seed) catch {
        reportResult("nkey_callback_fails", false, "failed to parse seed");
        return;
    };
    defer kp.wipe();

    var pubkey_buf: [56]u8 = undefined;
    const pubkey = kp.publicKey(&pubkey_buf);

    var io: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const result = nats.Client.connect(allocator, io.io(), url, .{
        .reconnect = false,
        .nkey_pubkey = pubkey,
        .nkey_sign_fn = &failingSignCallback,
    });

    if (result) |client| {
        client.deinit(allocator);
        reportResult("nkey_callback_fails", false, "should have failed");
    } else |_| {
        reportResult("nkey_callback_fails", true, "");
    }
}

/// Signing callback that always fails.
fn failingSignCallback(nonce: []const u8, sig: *[64]u8) bool {
    _ = nonce;
    _ = sig;
    return false;
}

/// Runs all NKey authentication tests.
pub fn runAll(allocator: std.mem.Allocator) void {
    testNKeyAuthentication(allocator);
    testNKeyAuthFailure(allocator);
    testNKeyPubSub(allocator);
    testNKeyNoSeedFails(allocator);
    testNKeyInvalidSeedFormat(allocator);
    testNKeySeedFile(allocator);
    testNKeySeedFileMissing(allocator);
    testNKeySigningCallback(allocator);
    testNKeyCallbackFails(allocator);
}
