//! Authentication modules for NATS.
//!
//! Provides NKey authentication (Ed25519 signatures) and credentials
//! file parsing for JWT authentication.

pub const nkey = @import("auth/nkey.zig");
pub const creds = @import("auth/creds.zig");
pub const base32 = @import("auth/base32.zig");
pub const crc16 = @import("auth/crc16.zig");

pub const KeyPair = nkey.KeyPair;
pub const KeyType = nkey.KeyType;
pub const Credentials = creds.Credentials;
pub const parseCredentials = creds.parse;
pub const loadCredentialsFile = creds.loadFile;

test {
    _ = nkey;
    _ = creds;
    _ = base32;
    _ = crc16;
}
