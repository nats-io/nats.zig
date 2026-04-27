//! Credentials file parser and formatter for NATS JWT authentication.
//!
//! Parses and formats .creds files containing JWT and NKey seed.

const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

pub const Error = error{
    InvalidCredentials,
    MissingJwt,
    MissingSeed,
};

/// Parsed credentials containing JWT and NKey seed.
/// Slices point into the original content buffer.
pub const Credentials = struct {
    jwt: []const u8,
    seed: []const u8,
};

const JWT_BEGIN = "-----BEGIN NATS USER JWT-----";
const JWT_END = "------END NATS USER JWT------";
const SEED_BEGIN = "-----BEGIN USER NKEY SEED-----";
const SEED_END = "------END USER NKEY SEED------";

/// Parses credentials from content buffer.
/// Returns slices into the input buffer.
pub fn parse(content: []const u8) Error!Credentials {
    assert(content.len > 0);

    // Find JWT section
    const jwt_start_idx = std.mem.indexOf(u8, content, JWT_BEGIN) orelse {
        return error.MissingJwt;
    };
    const jwt_content_start = jwt_start_idx + JWT_BEGIN.len;

    const jwt_end_idx = std.mem.indexOfPos(
        u8,
        content,
        jwt_content_start,
        JWT_END,
    ) orelse {
        return error.MissingJwt;
    };

    // Find seed section
    const seed_start_idx = std.mem.indexOf(u8, content, SEED_BEGIN) orelse {
        return error.MissingSeed;
    };
    const seed_content_start = seed_start_idx + SEED_BEGIN.len;

    const seed_end_idx = std.mem.indexOfPos(
        u8,
        content,
        seed_content_start,
        SEED_END,
    ) orelse {
        return error.MissingSeed;
    };

    // Extract and trim JWT
    const jwt_raw = content[jwt_content_start..jwt_end_idx];
    const jwt = trimWhitespace(jwt_raw);
    if (jwt.len == 0) return error.MissingJwt;

    // Extract and trim seed
    const seed_raw = content[seed_content_start..seed_end_idx];
    const seed = trimWhitespace(seed_raw);
    if (seed.len == 0) return error.MissingSeed;

    assert(jwt.len > 0);
    assert(seed.len > 0);

    return .{
        .jwt = jwt,
        .seed = seed,
    };
}

/// Trims leading and trailing ASCII whitespace.
/// Returns slice into original buffer.
fn trimWhitespace(s: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = s.len;

    while (start < end and std.ascii.isWhitespace(s[start])) {
        start += 1;
    }
    while (end > start and std.ascii.isWhitespace(s[end - 1])) {
        end -= 1;
    }

    return s[start..end];
}

const WARN_TEXT =
    "\n\n" ++
    "************************* IMPORTANT ****" ++
    "*********************\n" ++
    "  NKEY Seed printed below can be used to" ++
    " sign and prove identity.\n" ++
    "  NKEYs are sensitive and should be treat" ++
    "ed as secrets.\n\n" ++
    "  *************************************" ++
    "************************\n\n";

/// Formats a credentials file from JWT and seed strings.
/// Writes into caller-provided buffer, returns slice.
pub fn format(
    buf: []u8,
    jwt_str: []const u8,
    seed_str: []const u8,
) error{BufferTooSmall}![]const u8 {
    assert(jwt_str.len > 0);
    assert(seed_str.len > 0);
    if (buf.len < jwt_str.len + seed_str.len + 256)
        return error.BufferTooSmall;

    var w = Io.Writer.fixed(buf);
    w.writeAll(JWT_BEGIN) catch unreachable;
    w.writeAll("\n") catch unreachable;
    w.writeAll(jwt_str) catch unreachable;
    w.writeAll("\n") catch unreachable;
    w.writeAll(JWT_END) catch unreachable;
    w.writeAll(WARN_TEXT) catch unreachable;
    w.writeAll(SEED_BEGIN) catch unreachable;
    w.writeAll("\n") catch unreachable;
    w.writeAll(seed_str) catch unreachable;
    w.writeAll("\n") catch unreachable;
    w.writeAll(SEED_END) catch unreachable;
    w.writeAll("\n") catch unreachable;

    const result = w.buffered();
    assert(result.len > 0);
    return result;
}

/// Loads and parses credentials from file path.
/// Caller provides buffer for file content.
/// Returns slices pointing into buf.
pub fn loadFile(
    io: Io,
    path: []const u8,
    buf: *[8192]u8,
) !Credentials {
    assert(path.len > 0);

    const data = try Io.Dir.readFile(.cwd(), io, path, buf);
    if (data.len == 0) return error.InvalidCredentials;

    return parse(data);
}

test "parse valid credentials" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJBQkNERUZHIiw
        \\------END NATS USER JWT------
        \\
        \\************************* IMPORTANT *************************
        \\NKEY Seed printed below can be used to sign and prove identity.
        \\
        \\-----BEGIN USER NKEY SEED-----
        \\SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY
        \\------END USER NKEY SEED------
    ;

    const creds = try parse(content);

    try std.testing.expectEqualStrings(
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ.eyJqdGkiOiJBQkNERUZHIiw",
        creds.jwt,
    );
    try std.testing.expectEqualStrings(
        "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY",
        creds.seed,
    );
}

test "parse credentials with extra whitespace" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\
        \\  eyJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJVQSJ9
        \\
        \\------END NATS USER JWT------
        \\
        \\-----BEGIN USER NKEY SEED-----
        \\   SUATEST1234567890ABCDEF
        \\------END USER NKEY SEED------
    ;

    const creds = try parse(content);

    try std.testing.expectEqualStrings(
        "eyJhbGciOiJlZDI1NTE5In0.eyJzdWIiOiJVQSJ9",
        creds.jwt,
    );
    try std.testing.expectEqualStrings("SUATEST1234567890ABCDEF", creds.seed);
}

test "parse credentials missing JWT" {
    const content =
        \\-----BEGIN USER NKEY SEED-----
        \\SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY
        \\------END USER NKEY SEED------
    ;

    try std.testing.expectError(error.MissingJwt, parse(content));
}

test "parse credentials missing seed" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\eyJhbGciOiJlZDI1NTE5In0
        \\------END NATS USER JWT------
    ;

    try std.testing.expectError(error.MissingSeed, parse(content));
}

test "parse credentials empty JWT" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\
        \\------END NATS USER JWT------
        \\-----BEGIN USER NKEY SEED-----
        \\SUATEST
        \\------END USER NKEY SEED------
    ;

    try std.testing.expectError(error.MissingJwt, parse(content));
}

test "parse credentials empty seed" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\eyJhbGciOiJlZDI1NTE5In0
        \\------END NATS USER JWT------
        \\-----BEGIN USER NKEY SEED-----
        \\
        \\------END USER NKEY SEED------
    ;

    try std.testing.expectError(error.MissingSeed, parse(content));
}

test "parse credentials malformed - no end marker for JWT" {
    const content =
        \\-----BEGIN NATS USER JWT-----
        \\eyJhbGciOiJlZDI1NTE5In0
        \\-----BEGIN USER NKEY SEED-----
        \\SUATEST
        \\------END USER NKEY SEED------
    ;

    try std.testing.expectError(error.MissingJwt, parse(content));
}

test "trimWhitespace" {
    try std.testing.expectEqualStrings("hello", trimWhitespace("  hello  "));
    try std.testing.expectEqualStrings("hello", trimWhitespace("hello"));
    try std.testing.expectEqualStrings("hello", trimWhitespace("\n\thello\r\n"));
    try std.testing.expectEqualStrings("", trimWhitespace("   "));
    try std.testing.expectEqualStrings("", trimWhitespace(""));
}

test "format and parse roundtrip" {
    const jwt_str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5In0.test";
    const seed_str =
        "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV" ++
        "7NSWFFEW63UXMRLFM2XLAXK4GY";

    var buf: [2048]u8 = undefined;
    const formatted = try format(&buf, jwt_str, seed_str);

    const creds = try parse(formatted);
    try std.testing.expectEqualStrings(jwt_str, creds.jwt);
    try std.testing.expectEqualStrings(
        seed_str,
        creds.seed,
    );
}

test "realistic generated content roundtrip" {
    const nkey = @import("nkey.zig");
    const jwt = @import("jwt.zig");
    const Ed25519 = std.crypto.sign.Ed25519;

    // Deterministic account keypair
    const acct_seed = [_]u8{30} ** 32;
    const acct_ed = Ed25519.KeyPair.generateDeterministic(
        acct_seed,
    ) catch unreachable;
    const acct_kp = nkey.KeyPair{
        .kp = acct_ed,
        .key_type = .account,
    };

    // Deterministic user keypair
    const user_seed = [_]u8{40} ** 32;
    const user_ed = Ed25519.KeyPair.generateDeterministic(
        user_seed,
    ) catch unreachable;
    const user_kp = nkey.KeyPair{
        .kp = user_ed,
        .key_type = .user,
    };

    // Encode user JWT
    var pk_buf: [56]u8 = undefined;
    const user_pub = user_kp.publicKey(&pk_buf);

    var jwt_buf: [2048]u8 = undefined;
    const jwt_str = try jwt.encodeUserClaims(
        &jwt_buf,
        user_pub,
        "roundtrip-user",
        acct_kp,
        1700000000,
        .{ .pub_allow = &.{">"} },
    );

    // Encode user seed
    var seed_buf: [58]u8 = undefined;
    const seed_str = user_kp.encodeSeed(&seed_buf);

    // Format credentials
    var creds_buf: [4096]u8 = undefined;
    const formatted = try format(
        &creds_buf,
        jwt_str,
        seed_str,
    );

    // Parse back
    const parsed = try parse(formatted);
    try std.testing.expectEqualStrings(
        jwt_str,
        parsed.jwt,
    );
    try std.testing.expectEqualStrings(
        seed_str,
        parsed.seed,
    );

    // Verify parsed seed creates valid keypair
    var kp = try nkey.KeyPair.fromSeed(parsed.seed);
    defer kp.wipe();
    var pk2: [56]u8 = undefined;
    try std.testing.expectEqualStrings(
        user_pub,
        kp.publicKey(&pk2),
    );
}
