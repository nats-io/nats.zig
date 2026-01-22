//! NKey authentication for NATS.
//!
//! Parses NKey seeds and signs server nonces for authentication.
//! NKeys use Ed25519 signatures with base32-encoded public keys.

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

/// NKey keypair for signing nonces.
pub const KeyPair = struct {
    kp: Ed25519.KeyPair,
    key_type: KeyType,

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

        // Securely wipe decoded buffer
        std.crypto.secureZero(u8, @volatileCast(&raw));

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
