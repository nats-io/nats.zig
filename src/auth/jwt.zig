//! JWT encoding for NATS decentralized authentication.
//!
//! Encodes account and user JWTs signed with NKey Ed25519 keypairs.
//! No allocator needed - all encoding uses caller-provided buffers.

const std = @import("std");
const assert = std.debug.assert;

const Io = std.Io;
const Ed25519 = std.crypto.sign.Ed25519;
const Sha512_256 = std.crypto.hash.sha2.Sha512_256;
const base64 = std.base64.url_safe_no_pad;

const nkey = @import("nkey.zig");
const base32 = @import("base32.zig");

pub const Error = error{
    BufferTooSmall,
    WriteFailed,
};

/// Pre-encoded JWT header: {"typ":"JWT","alg":"ed25519-nkey"}
const HEADER_B64 =
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ";

/// Account JWT options (NATS account limits).
pub const AccountOptions = struct {
    subs: i64 = -1,
    data: i64 = -1,
    payload: i64 = -1,
    imports: i64 = -1,
    exports: i64 = -1,
    conn: i64 = -1,
    leaf: i64 = -1,
    mem_storage: i64 = -1,
    disk_storage: i64 = -1,
    wildcards: bool = true,
};

/// Operator JWT options.
pub const OperatorOptions = struct {
    system_account: []const u8 = "",
};

/// User JWT options (permissions and limits).
pub const UserOptions = struct {
    pub_allow: []const []const u8 = &.{},
    sub_allow: []const []const u8 = &.{},
    subs: i64 = -1,
    data: i64 = -1,
    payload: i64 = -1,
};

/// Encodes a NATS account JWT signed by an operator keypair.
pub fn encodeAccountClaims(
    buf: []u8,
    subject: []const u8,
    name: []const u8,
    signer: nkey.KeyPair,
    iat: i64,
    opts: AccountOptions,
) Error![]const u8 {
    assert(subject.len > 0);
    assert(name.len > 0);
    assert(buf.len >= 512);

    var pk_buf: [56]u8 = undefined;
    const iss = signer.publicKey(&pk_buf);

    // Pass 1: build payload without JTI to compute hash
    var tmp: [1024]u8 = undefined;
    const pre_jti = writeAccountJson(
        &tmp,
        "",
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    const jti = computeJti(pre_jti);

    // Pass 2: build payload with JTI
    var payload_buf: [1024]u8 = undefined;
    const payload = writeAccountJson(
        &payload_buf,
        &jti.str,
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    return assembleJwt(buf, payload, signer);
}

/// Encodes a NATS user JWT signed by an account keypair.
pub fn encodeUserClaims(
    buf: []u8,
    subject: []const u8,
    name: []const u8,
    signer: nkey.KeyPair,
    iat: i64,
    opts: UserOptions,
) Error![]const u8 {
    assert(subject.len > 0);
    assert(name.len > 0);
    assert(buf.len >= 512);

    var pk_buf: [56]u8 = undefined;
    const iss = signer.publicKey(&pk_buf);

    // Pass 1: build payload without JTI
    var tmp: [1024]u8 = undefined;
    const pre_jti = writeUserJson(
        &tmp,
        "",
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    const jti = computeJti(pre_jti);

    // Pass 2: build payload with JTI
    var payload_buf: [1024]u8 = undefined;
    const payload = writeUserJson(
        &payload_buf,
        &jti.str,
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    return assembleJwt(buf, payload, signer);
}

/// Encodes a NATS operator JWT (self-signed by operator).
pub fn encodeOperatorClaims(
    buf: []u8,
    subject: []const u8,
    name: []const u8,
    signer: nkey.KeyPair,
    iat: i64,
    opts: OperatorOptions,
) Error![]const u8 {
    assert(subject.len > 0);
    assert(name.len > 0);
    assert(buf.len >= 512);

    var pk_buf: [56]u8 = undefined;
    const iss = signer.publicKey(&pk_buf);

    // Pass 1: build payload without JTI
    var tmp: [1024]u8 = undefined;
    const pre_jti = writeOperatorJson(
        &tmp,
        "",
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    const jti = computeJti(pre_jti);

    // Pass 2: build payload with JTI
    var payload_buf: [1024]u8 = undefined;
    const payload = writeOperatorJson(
        &payload_buf,
        &jti.str,
        iat,
        iss,
        name,
        subject,
        opts,
    ) orelse return error.WriteFailed;

    return assembleJwt(buf, payload, signer);
}

const Jti = struct { str: [52]u8 };

/// SHA-512/256 hash of payload, base32-encoded.
fn computeJti(payload: []const u8) Jti {
    assert(payload.len > 0);

    var hash: [32]u8 = undefined;
    Sha512_256.hash(payload, &hash, .{});

    var result: Jti = undefined;
    _ = base32.encode(&result.str, &hash) catch unreachable;
    return result;
}

/// Assembles header.payload.signature into buf.
fn assembleJwt(
    buf: []u8,
    payload: []const u8,
    signer: nkey.KeyPair,
) Error![]const u8 {
    assert(payload.len > 0);
    assert(buf.len >= 512);

    const payload_b64_len = base64.Encoder.calcSize(
        payload.len,
    );
    // header + "." + payload_b64 + "." + sig_b64(86)
    const sig_b64_len = 86;
    const total = HEADER_B64.len + 1 + payload_b64_len +
        1 + sig_b64_len;
    if (buf.len < total) return error.BufferTooSmall;

    var pos: usize = 0;

    // Header
    @memcpy(buf[pos..][0..HEADER_B64.len], HEADER_B64);
    pos += HEADER_B64.len;

    // Dot
    buf[pos] = '.';
    pos += 1;

    // Base64url-encoded payload
    _ = base64.Encoder.encode(
        buf[pos..][0..payload_b64_len],
        payload,
    );
    pos += payload_b64_len;

    // Sign everything before the second dot
    const sign_data = buf[0..pos];
    const sig = signer.kp.sign(
        sign_data,
        null,
    ) catch unreachable;
    const sig_bytes = sig.toBytes();

    // Second dot
    buf[pos] = '.';
    pos += 1;

    // Base64url-encoded signature
    _ = base64.Encoder.encode(
        buf[pos..][0..sig_b64_len],
        &sig_bytes,
    );
    pos += sig_b64_len;

    assert(pos == total);
    return buf[0..pos];
}

/// Writes account claims JSON into buf. Returns slice or null.
fn writeAccountJson(
    buf: []u8,
    jti: []const u8,
    iat: i64,
    iss: []const u8,
    name: []const u8,
    sub: []const u8,
    opts: AccountOptions,
) ?[]const u8 {
    assert(iss.len > 0);
    assert(sub.len > 0);
    assert(buf.len >= 256);

    var w = Io.Writer.fixed(buf);
    w.writeAll("{\"jti\":\"") catch return null;
    w.writeAll(jti) catch return null;
    w.writeAll("\",\"iat\":") catch return null;
    w.print("{d}", .{iat}) catch return null;
    w.writeAll(",\"iss\":\"") catch return null;
    w.writeAll(iss) catch return null;
    w.writeAll("\",\"name\":\"") catch return null;
    w.writeAll(name) catch return null;
    w.writeAll("\",\"sub\":\"") catch return null;
    w.writeAll(sub) catch return null;
    w.writeAll("\",\"nats\":{\"limits\":{") catch return null;
    w.print("\"subs\":{d}", .{opts.subs}) catch return null;
    w.print(",\"data\":{d}", .{opts.data}) catch return null;
    w.print(
        ",\"payload\":{d}",
        .{opts.payload},
    ) catch return null;
    w.print(
        ",\"imports\":{d}",
        .{opts.imports},
    ) catch return null;
    w.print(
        ",\"exports\":{d}",
        .{opts.exports},
    ) catch return null;
    w.print(",\"conn\":{d}", .{opts.conn}) catch return null;
    w.print(",\"leaf\":{d}", .{opts.leaf}) catch return null;
    w.print(
        ",\"mem_storage\":{d}",
        .{opts.mem_storage},
    ) catch return null;
    w.print(
        ",\"disk_storage\":{d}",
        .{opts.disk_storage},
    ) catch return null;
    if (opts.wildcards) {
        w.writeAll(",\"wildcards\":true") catch return null;
    } else {
        w.writeAll(",\"wildcards\":false") catch return null;
    }
    w.writeAll(
        "},\"type\":\"account\",\"version\":2}}",
    ) catch return null;

    const result = w.buffered();
    assert(result.len > 0);
    return result;
}

/// Writes user claims JSON into buf. Returns slice or null.
fn writeUserJson(
    buf: []u8,
    jti: []const u8,
    iat: i64,
    iss: []const u8,
    name: []const u8,
    sub: []const u8,
    opts: UserOptions,
) ?[]const u8 {
    assert(iss.len > 0);
    assert(sub.len > 0);
    assert(buf.len >= 256);

    var w = Io.Writer.fixed(buf);
    w.writeAll("{\"jti\":\"") catch return null;
    w.writeAll(jti) catch return null;
    w.writeAll("\",\"iat\":") catch return null;
    w.print("{d}", .{iat}) catch return null;
    w.writeAll(",\"iss\":\"") catch return null;
    w.writeAll(iss) catch return null;
    w.writeAll("\",\"name\":\"") catch return null;
    w.writeAll(name) catch return null;
    w.writeAll("\",\"sub\":\"") catch return null;
    w.writeAll(sub) catch return null;
    w.writeAll("\",\"nats\":{") catch return null;

    // Publish permissions
    if (opts.pub_allow.len > 0) {
        w.writeAll("\"pub\":{\"allow\":[") catch return null;
        writeStringArray(&w, opts.pub_allow) orelse
            return null;
        w.writeAll("]},") catch return null;
    }

    // Subscribe permissions
    if (opts.sub_allow.len > 0) {
        w.writeAll("\"sub\":{\"allow\":[") catch return null;
        writeStringArray(&w, opts.sub_allow) orelse
            return null;
        w.writeAll("]},") catch return null;
    }

    w.print("\"subs\":{d}", .{opts.subs}) catch return null;
    w.print(",\"data\":{d}", .{opts.data}) catch return null;
    w.print(
        ",\"payload\":{d}",
        .{opts.payload},
    ) catch return null;
    w.writeAll(
        ",\"type\":\"user\",\"version\":2}}",
    ) catch return null;

    const result = w.buffered();
    assert(result.len > 0);
    return result;
}

/// Writes a JSON string array (without brackets).
fn writeStringArray(
    w: *Io.Writer,
    items: []const []const u8,
) ?void {
    assert(items.len > 0);

    for (items, 0..) |item, i| {
        if (i > 0) w.writeAll(",") catch return null;
        w.writeAll("\"") catch return null;
        w.writeAll(item) catch return null;
        w.writeAll("\"") catch return null;
    }
}

/// Writes operator claims JSON into buf.
fn writeOperatorJson(
    buf: []u8,
    jti: []const u8,
    iat: i64,
    iss: []const u8,
    name: []const u8,
    sub: []const u8,
    opts: OperatorOptions,
) ?[]const u8 {
    assert(iss.len > 0);
    assert(sub.len > 0);
    assert(buf.len >= 256);

    var w = Io.Writer.fixed(buf);
    w.writeAll("{\"jti\":\"") catch return null;
    w.writeAll(jti) catch return null;
    w.writeAll("\",\"iat\":") catch return null;
    w.print("{d}", .{iat}) catch return null;
    w.writeAll(",\"iss\":\"") catch return null;
    w.writeAll(iss) catch return null;
    w.writeAll("\",\"name\":\"") catch return null;
    w.writeAll(name) catch return null;
    w.writeAll("\",\"sub\":\"") catch return null;
    w.writeAll(sub) catch return null;
    w.writeAll("\",\"nats\":{") catch return null;
    if (opts.system_account.len > 0) {
        w.writeAll(
            "\"system_account\":\"",
        ) catch return null;
        w.writeAll(
            opts.system_account,
        ) catch return null;
        w.writeAll("\",") catch return null;
    }
    w.writeAll(
        "\"type\":\"operator\",\"version\":2}}",
    ) catch return null;

    const result = w.buffered();
    assert(result.len > 0);
    return result;
}

test "encode account JWT structure" {
    const test_seed = [_]u8{10} ** 32;
    const op_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            test_seed,
        ) catch unreachable;
    const op_kp = nkey.KeyPair{
        .kp = op_kp_inner,
        .key_type = .operator,
    };

    const acct_seed = [_]u8{20} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    var pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeAccountClaims(
        &jwt_buf,
        acct_pub,
        "test-account",
        op_kp,
        1700000000,
        .{},
    );

    // Split on dots
    assert(jwt.len > 0);
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;

    // Verify header decodes to expected JSON
    const hdr_exp =
        "{\"typ\":\"JWT\",\"alg\":\"ed25519-nkey\"}";
    var hdr_buf: [64]u8 = undefined;
    const hdr_len = base64.Decoder.calcSizeForSlice(
        jwt[0..dot1],
    ) catch unreachable;
    base64.Decoder.decode(
        hdr_buf[0..hdr_len],
        jwt[0..dot1],
    ) catch unreachable;
    try std.testing.expectEqualStrings(
        hdr_exp,
        hdr_buf[0..hdr_len],
    );

    // Verify payload contains expected fields
    var pay_buf: [1024]u8 = undefined;
    const payload_b64 = jwt[dot1 + 1 .. dot2];
    const pay_len = base64.Decoder.calcSizeForSlice(
        payload_b64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..pay_len],
        payload_b64,
    ) catch unreachable;
    const pay = pay_buf[0..pay_len];

    // Check key fields exist
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"jti\":\"") != null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"iat\":1700000000") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"name\":\"test-account\"") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            pay,
            "\"type\":\"account\"",
        ) != null,
    );

    // Verify Ed25519 signature
    const sig_b64 = jwt[dot2 + 1 ..];
    var sig_raw: [64]u8 = undefined;
    base64.Decoder.decode(
        &sig_raw,
        sig_b64,
    ) catch unreachable;
    const sig = Ed25519.Signature.fromBytes(sig_raw);
    sig.verify(jwt[0..dot2], op_kp.kp.public_key) catch {
        return error.WriteFailed;
    };
}

test "encode user JWT with permissions" {
    const acct_seed = [_]u8{30} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    const user_seed = [_]u8{40} ** 32;
    const user_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            user_seed,
        ) catch unreachable;
    const user_kp = nkey.KeyPair{
        .kp = user_kp_inner,
        .key_type = .user,
    };

    var pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeUserClaims(
        &jwt_buf,
        user_pub,
        "test-user",
        acct_kp,
        1700000000,
        .{
            .pub_allow = &.{ "foo.>", "bar.>" },
            .sub_allow = &.{"_INBOX.>"},
        },
    );

    assert(jwt.len > 0);
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;

    var pay_buf: [1024]u8 = undefined;
    const payload_b64 = jwt[dot1 + 1 .. dot2];
    const pay_len = base64.Decoder.calcSizeForSlice(
        payload_b64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..pay_len],
        payload_b64,
    ) catch unreachable;
    const pay = pay_buf[0..pay_len];

    // Verify permissions in payload
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"pub\":{\"allow\":[") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"foo.>\"") != null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"_INBOX.>\"") != null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"type\":\"user\"") !=
            null,
    );

    // Verify signature
    const sig_b64 = jwt[dot2 + 1 ..];
    var sig_raw: [64]u8 = undefined;
    base64.Decoder.decode(
        &sig_raw,
        sig_b64,
    ) catch unreachable;
    const sig = Ed25519.Signature.fromBytes(sig_raw);
    sig.verify(
        jwt[0..dot2],
        acct_kp.kp.public_key,
    ) catch {
        return error.WriteFailed;
    };
}

test "encode operator JWT self-signed" {
    const op_seed = [_]u8{10} ** 32;
    const op_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            op_seed,
        ) catch unreachable;
    const op_kp = nkey.KeyPair{
        .kp = op_kp_inner,
        .key_type = .operator,
    };

    var pk_buf: [56]u8 = undefined;
    const op_pub = op_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeOperatorClaims(
        &jwt_buf,
        op_pub,
        "test-operator",
        op_kp,
        1700000000,
        .{},
    );

    assert(jwt.len > 0);
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;

    // Decode and verify payload
    var pay_buf: [1024]u8 = undefined;
    const payload_b64 = jwt[dot1 + 1 .. dot2];
    const pay_len = base64.Decoder.calcSizeForSlice(
        payload_b64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..pay_len],
        payload_b64,
    ) catch unreachable;
    const pay = pay_buf[0..pay_len];

    try std.testing.expect(
        std.mem.indexOf(
            u8,
            pay,
            "\"type\":\"operator\"",
        ) != null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            pay,
            "\"name\":\"test-operator\"",
        ) != null,
    );

    // Self-signed: verify with operator's own key
    const sig_b64 = jwt[dot2 + 1 ..];
    var sig_raw: [64]u8 = undefined;
    base64.Decoder.decode(
        &sig_raw,
        sig_b64,
    ) catch unreachable;
    const sig = Ed25519.Signature.fromBytes(sig_raw);
    sig.verify(
        jwt[0..dot2],
        op_kp.kp.public_key,
    ) catch {
        return error.WriteFailed;
    };
}

test "JTI determinism - same input same output" {
    const op_seed = [_]u8{10} ** 32;
    const op_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            op_seed,
        ) catch unreachable;
    const op_kp = nkey.KeyPair{
        .kp = op_kp_inner,
        .key_type = .operator,
    };

    const acct_seed = [_]u8{20} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    var pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&pk_buf);

    var buf1: [2048]u8 = undefined;
    const jwt1 = try encodeAccountClaims(
        &buf1,
        acct_pub,
        "det-test",
        op_kp,
        1700000000,
        .{},
    );

    var buf2: [2048]u8 = undefined;
    const jwt2 = try encodeAccountClaims(
        &buf2,
        acct_pub,
        "det-test",
        op_kp,
        1700000000,
        .{},
    );

    try std.testing.expectEqualStrings(jwt1, jwt2);
}

test "JTI uniqueness - different names different JTI" {
    const op_seed = [_]u8{10} ** 32;
    const op_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            op_seed,
        ) catch unreachable;
    const op_kp = nkey.KeyPair{
        .kp = op_kp_inner,
        .key_type = .operator,
    };

    const acct_seed = [_]u8{20} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    var pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&pk_buf);

    var buf1: [2048]u8 = undefined;
    const jwt1 = try encodeAccountClaims(
        &buf1,
        acct_pub,
        "account-alpha",
        op_kp,
        1700000000,
        .{},
    );

    var buf2: [2048]u8 = undefined;
    const jwt2 = try encodeAccountClaims(
        &buf2,
        acct_pub,
        "account-beta",
        op_kp,
        1700000000,
        .{},
    );

    // JWTs must differ (different name -> different JTI)
    try std.testing.expect(
        !std.mem.eql(u8, jwt1, jwt2),
    );
}

test "custom account limits in payload" {
    const op_seed = [_]u8{10} ** 32;
    const op_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            op_seed,
        ) catch unreachable;
    const op_kp = nkey.KeyPair{
        .kp = op_kp_inner,
        .key_type = .operator,
    };

    const acct_seed = [_]u8{20} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    var pk_buf: [56]u8 = undefined;
    const acct_pub = acct_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeAccountClaims(
        &jwt_buf,
        acct_pub,
        "limited-acct",
        op_kp,
        1700000000,
        .{
            .subs = 100,
            .conn = 50,
            .wildcards = false,
        },
    );

    // Decode payload
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;
    var pay_buf: [1024]u8 = undefined;
    const pb64 = jwt[dot1 + 1 .. dot2];
    const plen = base64.Decoder.calcSizeForSlice(
        pb64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..plen],
        pb64,
    ) catch unreachable;
    const pay = pay_buf[0..plen];

    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"subs\":100") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"conn\":50") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(
            u8,
            pay,
            "\"wildcards\":false",
        ) != null,
    );
}

test "user JWT with empty permissions" {
    const acct_seed = [_]u8{30} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    const user_seed = [_]u8{40} ** 32;
    const user_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            user_seed,
        ) catch unreachable;
    const user_kp = nkey.KeyPair{
        .kp = user_kp_inner,
        .key_type = .user,
    };

    var pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeUserClaims(
        &jwt_buf,
        user_pub,
        "no-perms-user",
        acct_kp,
        1700000000,
        .{},
    );

    // Decode payload
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;
    var pay_buf: [1024]u8 = undefined;
    const pb64 = jwt[dot1 + 1 .. dot2];
    const plen = base64.Decoder.calcSizeForSlice(
        pb64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..plen],
        pb64,
    ) catch unreachable;
    const pay = pay_buf[0..plen];

    // No "pub": or "sub":{ permission blocks
    // (only "subs":, "sub":" for subject)
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"pub\":{") == null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"sub\":{") == null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"type\":\"user\"") !=
            null,
    );
}

test "user JWT with single permission" {
    const acct_seed = [_]u8{30} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    const user_seed = [_]u8{40} ** 32;
    const user_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            user_seed,
        ) catch unreachable;
    const user_kp = nkey.KeyPair{
        .kp = user_kp_inner,
        .key_type = .user,
    };

    var pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeUserClaims(
        &jwt_buf,
        user_pub,
        "single-perm-user",
        acct_kp,
        1700000000,
        .{
            .pub_allow = &.{"single.subject"},
        },
    );

    // Decode payload
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;
    var pay_buf: [1024]u8 = undefined;
    const pb64 = jwt[dot1 + 1 .. dot2];
    const plen = base64.Decoder.calcSizeForSlice(
        pb64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..plen],
        pb64,
    ) catch unreachable;
    const pay = pay_buf[0..plen];

    try std.testing.expect(
        std.mem.indexOf(
            u8,
            pay,
            "\"pub\":{\"allow\":[\"single.subject\"]}",
        ) != null,
    );
}

test "custom user limits in payload" {
    const acct_seed = [_]u8{30} ** 32;
    const acct_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            acct_seed,
        ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_kp_inner,
        .key_type = .account,
    };

    const user_seed = [_]u8{40} ** 32;
    const user_kp_inner =
        Ed25519.KeyPair.generateDeterministic(
            user_seed,
        ) catch unreachable;
    const user_kp = nkey.KeyPair{
        .kp = user_kp_inner,
        .key_type = .user,
    };

    var pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt = try encodeUserClaims(
        &jwt_buf,
        user_pub,
        "limited-user",
        acct_kp,
        1700000000,
        .{
            .subs = 10,
            .data = 1024,
            .payload = 512,
        },
    );

    // Decode payload
    const dot1 = std.mem.indexOf(u8, jwt, ".") orelse
        unreachable;
    const dot2 = std.mem.indexOfPos(
        u8,
        jwt,
        dot1 + 1,
        ".",
    ) orelse unreachable;
    var pay_buf: [1024]u8 = undefined;
    const pb64 = jwt[dot1 + 1 .. dot2];
    const plen = base64.Decoder.calcSizeForSlice(
        pb64,
    ) catch unreachable;
    base64.Decoder.decode(
        pay_buf[0..plen],
        pb64,
    ) catch unreachable;
    const pay = pay_buf[0..plen];

    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"subs\":10") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"data\":1024") !=
            null,
    );
    try std.testing.expect(
        std.mem.indexOf(u8, pay, "\"payload\":512") !=
            null,
    );
}
