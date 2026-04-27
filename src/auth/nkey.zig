//! NKey authentication for NATS.
//!
//! Generates, parses, and encodes NKey seeds. Signs server nonces
//! for authentication. NKeys use Ed25519 with base32-encoded keys.

const std = @import("std");
const assert = std.debug.assert;

const Ed25519 = std.crypto.sign.Ed25519;
const base32 = @import("base32.zig");
const crc16 = @import("crc16.zig");

pub const Error = error{
    InvalidSeed,
    InvalidPrefix,
    InvalidChecksum,
    InvalidKeyType,
    InvalidLength,
    IdentityElement,
};

/// NKey entity types (encoded in seed prefix).
pub const KeyType = enum(u8) {
    user = 160, // 20 << 3
    account = 0, // 0 << 3
    server = 104, // 13 << 3
    cluster = 16, // 2 << 3
    operator = 112, // 14 << 3

    /// Converts byte value to KeyType enum.
    pub fn fromByte(b: u8) ?KeyType {
        return switch (b) {
            160 => .user,
            0 => .account,
            104 => .server,
            16 => .cluster,
            112 => .operator,
            else => null,
        };
    }
};

/// Seed prefix byte value (S = 18 << 3 = 144).
const SEED_PREFIX: u8 = 144;

/// NKey keypair for signing and verification.
pub const KeyPair = struct {
    kp: Ed25519.KeyPair,
    key_type: KeyType,

    /// Generates a random Ed25519 keypair for the given type.
    pub fn generate(io: std.Io, key_type: KeyType) KeyPair {
        assert(KeyType.fromByte(@intFromEnum(key_type)) != null);
        const kp = Ed25519.KeyPair.generate(io);
        return .{ .kp = kp, .key_type = key_type };
    }

    /// Encodes the keypair's seed in NKey format (base32).
    ///
    /// Reverse of `fromSeed`. Packs prefix + seed + CRC16,
    /// then base32-encodes to 58-character string.
    pub fn encodeSeed(
        self: KeyPair,
        out: *[58]u8,
    ) []const u8 {
        assert(KeyType.fromByte(
            @intFromEnum(self.key_type),
        ) != null);

        var raw: [36]u8 = undefined;
        const kt = @intFromEnum(self.key_type);
        raw[0] = (SEED_PREFIX & 0xF8) | ((kt >> 5) & 0x07);
        raw[1] = (kt & 0x1F) << 3;
        raw[2..34].* = self.kp.secret_key.seed();

        const crc = crc16.compute(raw[0..34]);
        std.mem.writeInt(u16, raw[34..36], crc, .little);

        defer std.crypto.secureZero(
            u8,
            @volatileCast(&raw),
        );

        return base32.encode(out, &raw) catch unreachable;
    }

    /// Parses an NKey seed and derives the Ed25519 keypair.
    ///
    /// Seed format (57 chars base32):
    /// - Bytes 0-1: Packed prefix (seed prefix + key type)
    /// - Bytes 2-33: 32-byte Ed25519 seed
    /// - Bytes 34-35: CRC16 checksum (little-endian)
    pub fn fromSeed(encoded_seed: []const u8) Error!KeyPair {
        assert(encoded_seed.len > 0);

        // Base32 decode the seed
        var raw: [64]u8 = undefined;
        defer std.crypto.secureZero(
            u8,
            @volatileCast(&raw),
        );
        const decoded = base32.decode(&raw, encoded_seed) catch {
            return error.InvalidSeed;
        };

        // Expect 35 bytes: 2 prefix + 32 seed + 2 CRC (57 chars / 8 * 5 = 35)
        if (decoded.len < 35) return error.InvalidLength;
        assert(decoded.len >= 35);

        // Extract prefix bytes
        const b1 = decoded[0] & 0xF8;
        const b2 = ((decoded[0] & 0x07) << 5) | ((decoded[1] & 0xF8) >> 3);

        // Validate seed prefix
        if (b1 != SEED_PREFIX) return error.InvalidPrefix;

        // Validate key type
        const key_type = KeyType.fromByte(b2) orelse {
            return error.InvalidKeyType;
        };

        // Validate CRC16 checksum (last 2 bytes, little-endian)
        const data_len = decoded.len - 2;
        const stored_crc = std.mem.readInt(
            u16,
            decoded[data_len..][0..2],
            .little,
        );
        if (!crc16.validate(decoded[0..data_len], stored_crc)) {
            return error.InvalidChecksum;
        }

        // Extract 32-byte seed
        const seed: [32]u8 = decoded[2..34].*;

        // Derive Ed25519 keypair
        const kp = Ed25519.KeyPair.generateDeterministic(seed) catch {
            return error.IdentityElement;
        };

        return .{ .kp = kp, .key_type = key_type };
    }

    /// Signs data and returns raw 64-byte signature.
    pub fn sign(self: KeyPair, data: []const u8) [64]u8 {
        assert(data.len > 0);

        // Deterministic signature (null noise)
        const sig = self.kp.sign(data, null) catch unreachable;
        return sig.toBytes();
    }

    /// Signs data and returns base64url-encoded signature (no padding).
    /// Writes to provided buffer and returns slice.
    pub fn signEncoded(
        self: KeyPair,
        data: []const u8,
        out: *[86]u8,
    ) []const u8 {
        assert(data.len > 0);
        assert(out.len >= 86);

        const sig = self.sign(data);
        return std.base64.url_safe_no_pad.Encoder.encode(out, &sig);
    }

    /// Returns base32-encoded public key.
    /// Format: [key_type_prefix][32-byte-pubkey][crc16]
    pub fn publicKey(self: KeyPair, out: *[56]u8) []const u8 {
        assert(out.len >= 56);

        // Build raw: 1 byte type + 32 bytes pubkey + 2 bytes CRC = 35 bytes
        var raw: [35]u8 = undefined;
        raw[0] = @intFromEnum(self.key_type);
        raw[1..33].* = self.kp.public_key.toBytes();

        const crc = crc16.compute(raw[0..33]);
        std.mem.writeInt(u16, raw[33..35], crc, .little);

        // Base32 encode: 35 bytes -> 56 chars
        return base32.encode(out, &raw) catch unreachable;
    }

    /// Securely wipes keypair from memory.
    pub fn wipe(self: *KeyPair) void {
        std.crypto.secureZero(
            u8,
            @volatileCast(&self.kp.secret_key.bytes),
        );
    }
};

// Test vectors from NATS C client
test "parse valid user seed" {
    const seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
    var kp = try KeyPair.fromSeed(seed);
    defer kp.wipe();

    try std.testing.expectEqual(KeyType.user, kp.key_type);
}

test "sign nonce matches test vector" {
    const seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
    var kp = try KeyPair.fromSeed(seed);
    defer kp.wipe();

    const sig = kp.sign("nonce");
    // First bytes from C client test
    try std.testing.expectEqual(@as(u8, 155), sig[0]);
    try std.testing.expectEqual(@as(u8, 157), sig[1]);
}

test "sign encoded" {
    const seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
    var kp = try KeyPair.fromSeed(seed);
    defer kp.wipe();

    var buf: [86]u8 = undefined;
    const encoded = kp.signEncoded("nonce", &buf);

    // Base64url encoding of 64 bytes = 86 chars (no padding)
    try std.testing.expectEqual(@as(usize, 86), encoded.len);

    // First char should correspond to sig[0]=155
    try std.testing.expect(encoded[0] == 'm');
}

test "public key format" {
    const seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
    var kp = try KeyPair.fromSeed(seed);
    defer kp.wipe();

    var buf: [56]u8 = undefined;
    const pubkey = kp.publicKey(&buf);

    // User public keys start with 'U'
    try std.testing.expectEqual(@as(u8, 'U'), pubkey[0]);
    try std.testing.expectEqual(@as(usize, 56), pubkey.len);
}

test "invalid seed - bad prefix" {
    // Valid 57-char base32 but wrong prefix (starts with 'N' instead of 'S')
    // NAAA... decodes to prefix byte that is not SEED_PREFIX (144)
    const bad = "NAAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY";
    try std.testing.expectError(error.InvalidPrefix, KeyPair.fromSeed(bad));
}

test "invalid seed - too short" {
    const bad = "SUAMK2FG";
    try std.testing.expectError(error.InvalidLength, KeyPair.fromSeed(bad));
}

test "invalid seed - bad characters" {
    // Contains invalid base32 char '!'
    const bad = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4!Y";
    try std.testing.expectError(error.InvalidSeed, KeyPair.fromSeed(bad));
}

test "encodeSeed roundtrip with known seed" {
    const seed =
        "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV" ++
        "7NSWFFEW63UXMRLFM2XLAXK4GY";
    var kp = try KeyPair.fromSeed(seed);
    defer kp.wipe();

    var buf: [58]u8 = undefined;
    const encoded = kp.encodeSeed(&buf);
    try std.testing.expectEqualStrings(seed, encoded);
}

test "encodeSeed roundtrip with deterministic key" {
    const test_seed = [_]u8{1} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    const kp = KeyPair{ .kp = ed_kp, .key_type = .user };

    var seed_buf: [58]u8 = undefined;
    const encoded_seed = kp.encodeSeed(&seed_buf);

    var kp2 = try KeyPair.fromSeed(encoded_seed);
    defer kp2.wipe();

    var pk1: [56]u8 = undefined;
    var pk2: [56]u8 = undefined;
    try std.testing.expectEqualStrings(
        kp.publicKey(&pk1),
        kp2.publicKey(&pk2),
    );
}

test "seed prefix chars per key type" {
    const test_seed = [_]u8{42} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    var buf: [58]u8 = undefined;

    // User seed starts with SU
    const user_kp = KeyPair{
        .kp = ed_kp,
        .key_type = .user,
    };
    const user_enc = user_kp.encodeSeed(&buf);
    try std.testing.expectEqual(@as(u8, 'S'), user_enc[0]);
    try std.testing.expectEqual(@as(u8, 'U'), user_enc[1]);

    // Account seed starts with SA
    const acct_kp = KeyPair{
        .kp = ed_kp,
        .key_type = .account,
    };
    const acct_enc = acct_kp.encodeSeed(&buf);
    try std.testing.expectEqual(@as(u8, 'S'), acct_enc[0]);
    try std.testing.expectEqual(@as(u8, 'A'), acct_enc[1]);

    // Operator seed starts with SO
    const op_kp = KeyPair{
        .kp = ed_kp,
        .key_type = .operator,
    };
    const op_enc = op_kp.encodeSeed(&buf);
    try std.testing.expectEqual(@as(u8, 'S'), op_enc[0]);
    try std.testing.expectEqual(@as(u8, 'O'), op_enc[1]);
}

test "server and cluster seed prefix roundtrip" {
    const test_seed = [_]u8{99} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    var buf: [58]u8 = undefined;

    // Server seed starts with SN
    const srv_kp = KeyPair{
        .kp = ed_kp,
        .key_type = .server,
    };
    const srv_enc = srv_kp.encodeSeed(&buf);
    try std.testing.expectEqual(@as(u8, 'S'), srv_enc[0]);
    try std.testing.expectEqual(@as(u8, 'N'), srv_enc[1]);
    var srv_kp2 = try KeyPair.fromSeed(srv_enc);
    defer srv_kp2.wipe();
    try std.testing.expectEqual(
        KeyType.server,
        srv_kp2.key_type,
    );

    // Cluster seed starts with SC
    const cls_kp = KeyPair{
        .kp = ed_kp,
        .key_type = .cluster,
    };
    const cls_enc = cls_kp.encodeSeed(&buf);
    try std.testing.expectEqual(@as(u8, 'S'), cls_enc[0]);
    try std.testing.expectEqual(@as(u8, 'C'), cls_enc[1]);
    var cls_kp2 = try KeyPair.fromSeed(cls_enc);
    defer cls_kp2.wipe();
    try std.testing.expectEqual(
        KeyType.cluster,
        cls_kp2.key_type,
    );
}

test "public key prefix for all key types" {
    const test_seed = [_]u8{77} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    var pk_buf: [56]u8 = undefined;

    const Expected = struct { kt: KeyType, ch: u8 };
    const cases = [_]Expected{
        .{ .kt = .user, .ch = 'U' },
        .{ .kt = .account, .ch = 'A' },
        .{ .kt = .operator, .ch = 'O' },
        .{ .kt = .server, .ch = 'N' },
        .{ .kt = .cluster, .ch = 'C' },
    };

    for (cases) |c| {
        const kp = KeyPair{
            .kp = ed_kp,
            .key_type = c.kt,
        };
        const pk = kp.publicKey(&pk_buf);
        try std.testing.expectEqual(c.ch, pk[0]);
    }
}

test "full crypto chain: encode parse sign verify" {
    const test_seed = [_]u8{55} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    const kp = KeyPair{
        .kp = ed_kp,
        .key_type = .user,
    };

    // Encode seed
    var seed_buf: [58]u8 = undefined;
    const encoded_seed = kp.encodeSeed(&seed_buf);

    // Parse back
    var kp2 = try KeyPair.fromSeed(encoded_seed);
    defer kp2.wipe();

    // Public keys must match
    var pk1: [56]u8 = undefined;
    var pk2: [56]u8 = undefined;
    try std.testing.expectEqualStrings(
        kp.publicKey(&pk1),
        kp2.publicKey(&pk2),
    );

    // Sign with parsed keypair, verify with original
    const data = "test payload for signing";
    const sig_bytes = kp2.sign(data);
    const sig = Ed25519.Signature.fromBytes(sig_bytes);
    try sig.verify(data, kp.kp.public_key);
}

test "CRC16 corruption detection" {
    const test_seed = [_]u8{88} ** 32;
    const ed_kp = Ed25519.KeyPair.generateDeterministic(
        test_seed,
    ) catch unreachable;
    const kp = KeyPair{
        .kp = ed_kp,
        .key_type = .user,
    };

    var seed_buf: [58]u8 = undefined;
    const encoded = kp.encodeSeed(&seed_buf);

    // Copy and corrupt one byte in the middle
    var corrupt: [58]u8 = undefined;
    @memcpy(corrupt[0..encoded.len], encoded);
    corrupt[20] = if (corrupt[20] == 'A') 'B' else 'A';

    try std.testing.expectError(
        error.InvalidChecksum,
        KeyPair.fromSeed(corrupt[0..encoded.len]),
    );
}
