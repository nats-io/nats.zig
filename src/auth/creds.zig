//! Credentials file parser for NATS JWT authentication.
//!
//! Parses .creds files containing JWT and NKey seed.

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
