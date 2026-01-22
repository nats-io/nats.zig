//! Authentication modules for NATS.
//!
//! Provides NKey authentication (Ed25519 signatures) for secure
//! connection to NATS servers.

pub const nkey = @import("auth/nkey.zig");
pub const base32 = @import("auth/base32.zig");
pub const crc16 = @import("auth/crc16.zig");

pub const KeyPair = nkey.KeyPair;
pub const KeyType = nkey.KeyType;

test {
    _ = nkey;
    _ = base32;
    _ = crc16;
}
