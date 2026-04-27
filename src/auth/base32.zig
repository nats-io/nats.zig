//! Base32 encoder/decoder (RFC 4648).
//!
//! Uses standard alphabet: ABCDEFGHIJKLMNOPQRSTUVWXYZ234567

const std = @import("std");
const assert = std.debug.assert;

pub const Error = error{
    InvalidCharacter,
    InvalidPadding,
    OutputTooSmall,
};

/// Standard RFC 4648 base32 alphabet.
const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Decoding lookup table (256 entries, 0xFF = invalid).
const decode_table: [256]u8 = blk: {
    var t: [256]u8 = .{0xFF} ** 256;
    for (alphabet, 0..) |c, i| {
        t[c] = @intCast(i);
        // Also accept lowercase
        if (c >= 'A' and c <= 'Z') {
            t[c + 32] = @intCast(i);
        }
    }
    break :blk t;
};

/// Calculates decoded byte length from encoded character length.
/// Does not account for padding characters.
pub fn decodedLen(encoded_len: usize) usize {
    return (encoded_len * 5) / 8;
}

/// Calculates encoded character length from decoded byte length.
pub fn encodedLen(decoded_len: usize) usize {
    return (decoded_len * 8 + 4) / 5;
}

/// Decodes base32 string into dest buffer.
/// Returns slice of decoded bytes.
pub fn decode(dest: []u8, source: []const u8) Error![]u8 {
    if (source.len == 0) return dest[0..0];

    const needed = decodedLen(source.len);
    if (dest.len < needed) return error.OutputTooSmall;
    assert(dest.len >= needed);

    var bits: u32 = 0;
    var bit_count: u5 = 0;
    var out_idx: usize = 0;

    for (source) |c| {
        if (c == '=') break;

        const val = decode_table[c];
        if (val == 0xFF) return error.InvalidCharacter;

        bits = (bits << 5) | val;
        bit_count += 5;

        if (bit_count >= 8) {
            bit_count -= 8;
            dest[out_idx] = @intCast((bits >> bit_count) & 0xFF);
            out_idx += 1;
        }
    }

    return dest[0..out_idx];
}

/// Encodes bytes into base32 string in dest buffer.
/// Returns slice of encoded characters.
pub fn encode(dest: []u8, source: []const u8) Error![]u8 {
    if (source.len == 0) return dest[0..0];

    const needed = encodedLen(source.len);
    if (dest.len < needed) return error.OutputTooSmall;
    assert(dest.len >= needed);

    var bits: u32 = 0;
    var bit_count: u5 = 0;
    var out_idx: usize = 0;

    for (source) |byte| {
        bits = (bits << 8) | byte;
        bit_count += 8;

        while (bit_count >= 5) {
            bit_count -= 5;
            dest[out_idx] = alphabet[(bits >> bit_count) & 0x1F];
            out_idx += 1;
        }
    }

    // Handle remaining bits (if any)
    if (bit_count > 0) {
        dest[out_idx] = alphabet[(bits << (5 - bit_count)) & 0x1F];
        out_idx += 1;
    }

    return dest[0..out_idx];
}

test "decode empty" {
    var buf: [1]u8 = undefined;
    const result = try decode(&buf, "");
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "decode single char" {
    var buf: [1]u8 = undefined;
    const result = try decode(&buf, "ME");
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(u8, 'a'), result[0]);
}

test "decode lowercase accepted" {
    var buf: [1]u8 = undefined;
    const result = try decode(&buf, "me");
    try std.testing.expectEqual(@as(u8, 'a'), result[0]);
}

test "decode test string" {
    var buf: [16]u8 = undefined;
    const result = try decode(&buf, "ORSXG5A");
    try std.testing.expectEqualSlices(u8, "test", result);
}

test "decode invalid character" {
    var buf: [16]u8 = undefined;
    try std.testing.expectError(error.InvalidCharacter, decode(&buf, "ME!!"));
}

test "decode with padding" {
    var buf: [16]u8 = undefined;
    const result = try decode(&buf, "ORSXG5A=");
    try std.testing.expectEqualSlices(u8, "test", result);
}

test "encode empty" {
    var buf: [1]u8 = undefined;
    const result = try encode(&buf, "");
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "encode single byte" {
    var buf: [8]u8 = undefined;
    const result = try encode(&buf, "a");
    try std.testing.expectEqualSlices(u8, "ME", result);
}

test "encode test string" {
    var buf: [16]u8 = undefined;
    const result = try encode(&buf, "test");
    try std.testing.expectEqualSlices(u8, "ORSXG5A", result);
}

test "encode decode roundtrip" {
    const original = "Hello, World!";
    var enc_buf: [64]u8 = undefined;
    var dec_buf: [64]u8 = undefined;

    const encoded = try encode(&enc_buf, original);
    const decoded = try decode(&dec_buf, encoded);

    try std.testing.expectEqualSlices(u8, original, decoded);
}

test "decodedLen" {
    try std.testing.expectEqual(@as(usize, 0), decodedLen(0));
    try std.testing.expectEqual(@as(usize, 0), decodedLen(1));
    try std.testing.expectEqual(@as(usize, 1), decodedLen(2));
    try std.testing.expectEqual(@as(usize, 2), decodedLen(4));
    try std.testing.expectEqual(@as(usize, 5), decodedLen(8));
    // NKey seed: 57 chars -> 35 bytes
    try std.testing.expectEqual(@as(usize, 35), decodedLen(57));
}

test "encodedLen" {
    try std.testing.expectEqual(@as(usize, 0), encodedLen(0));
    try std.testing.expectEqual(@as(usize, 2), encodedLen(1));
    try std.testing.expectEqual(@as(usize, 8), encodedLen(5));
    // Public key: 35 bytes -> 56 chars
    try std.testing.expectEqual(@as(usize, 56), encodedLen(35));
}
