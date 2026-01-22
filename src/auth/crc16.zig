//! CRC16-CCITT checksum for NKey validation.
//!
//! Thin wrapper around std.hash.crc.Crc16Xmodem.

const std = @import("std");
const assert = std.debug.assert;

const Crc16Xmodem = std.hash.crc.Crc16Xmodem;

/// Computes CRC16-CCITT (XMODEM) checksum over data.
pub fn compute(data: []const u8) u16 {
    assert(data.len > 0);
    return Crc16Xmodem.hash(data);
}

/// Validates CRC16 checksum against expected value.
pub fn validate(data: []const u8, expected: u16) bool {
    assert(data.len > 0);
    return compute(data) == expected;
}

test "compute single byte" {
    const result = compute(&.{0x31});
    try std.testing.expectEqual(@as(u16, 0x2672), result);
}

test "compute ascii string" {
    const result = compute("123456789");
    // CRC16-CCITT (XMODEM) of "123456789" = 0x31C3
    try std.testing.expectEqual(@as(u16, 0x31C3), result);
}

test "validate correct checksum" {
    try std.testing.expect(validate("123456789", 0x31C3));
}

test "validate incorrect checksum" {
    try std.testing.expect(!validate("123456789", 0x0000));
}
