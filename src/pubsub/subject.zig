//! Subject Validation and Matching
//!
//! NATS subjects are dot-separated tokens. Wildcards:
//! - `*` matches exactly one token
//! - `>` matches one or more tokens (must be last)

const std = @import("std");
const assert = std.debug.assert;

/// Errors during subject validation.
pub const ValidationError = error{
    EmptySubject,
    EmptyToken,
    InvalidCharacter,
    WildcardNotLast,
    SpaceInSubject,
};

/// Validates a subject for publishing (no wildcards allowed).
pub fn validatePublish(subject: []const u8) ValidationError!void {
    if (subject.len == 0) return error.EmptySubject;

    var token_start: usize = 0;
    for (subject, 0..) |c, i| {
        if (c == '.') {
            if (i == token_start) return error.EmptyToken;
            token_start = i + 1;
        } else if (c == ' ' or c == '\t') {
            return error.SpaceInSubject;
        } else if (c == '*' or c == '>' or c < 0x20 or c == 0x7f) {
            // Wildcards and control chars (includes CR/LF/null)
            return error.InvalidCharacter;
        }
    }

    // Check last token isn't empty
    if (token_start >= subject.len) return error.EmptyToken;
}

/// Validates a subject for subscribing (wildcards allowed).
pub fn validateSubscribe(subject: []const u8) ValidationError!void {
    if (subject.len == 0) return error.EmptySubject;

    var token_start: usize = 0;
    var has_full_wildcard = false;

    for (subject, 0..) |c, i| {
        if (c == '.') {
            if (i == token_start) return error.EmptyToken;
            if (has_full_wildcard) return error.WildcardNotLast;
            token_start = i + 1;
        } else if (c == ' ' or c == '\t') {
            return error.SpaceInSubject;
        } else if (c == '>') {
            // > must be alone in its token (at start AND next is . or end)
            if (i != token_start) return error.InvalidCharacter;
            if (i + 1 < subject.len and subject[i + 1] != '.') {
                return error.InvalidCharacter;
            }
            has_full_wildcard = true;
        } else if (c == '*') {
            // * must be alone in its token (at start AND next is . or end)
            if (i != token_start) return error.InvalidCharacter;
            if (i + 1 < subject.len and subject[i + 1] != '.') {
                return error.InvalidCharacter;
            }
        } else if (c < 0x20 or c == 0x7f) {
            // Control chars (includes CR/LF/null)
            return error.InvalidCharacter;
        }
    }

    // Check last token isn't empty
    if (token_start >= subject.len) return error.EmptyToken;
}

/// Validates a reply-to address for protocol safety.
pub fn validateReplyTo(reply_to: []const u8) ValidationError!void {
    if (reply_to.len == 0) return error.EmptySubject;
    for (reply_to) |c| {
        if (c <= 0x20 or c == 0x7f) return error.InvalidCharacter;
    }
}

/// Validates a queue group name for protocol safety.
pub fn validateQueueGroup(queue: []const u8) ValidationError!void {
    if (queue.len == 0) return error.EmptySubject;
    for (queue) |c| {
        if (c <= 0x20 or c == 0x7f) return error.InvalidCharacter;
    }
}

/// Checks if a subject matches a pattern (with wildcards).
pub fn matches(pattern: []const u8, subject: []const u8) bool {
    // Empty inputs are invalid subjects - return false
    if (pattern.len == 0 or subject.len == 0) return false;
    var pat_iter = std.mem.tokenizeScalar(u8, pattern, '.');
    var subj_iter = std.mem.tokenizeScalar(u8, subject, '.');

    while (pat_iter.next()) |pat_token| {
        if (std.mem.eql(u8, pat_token, ">")) {
            // > matches rest of subject (one or more tokens)
            return subj_iter.next() != null;
        }

        const subj_token = subj_iter.next() orelse return false;

        if (std.mem.eql(u8, pat_token, "*")) {
            // * matches any single token
            continue;
        }

        if (!std.mem.eql(u8, pat_token, subj_token)) {
            return false;
        }
    }

    // Both must be exhausted for exact match
    return subj_iter.next() == null;
}

/// Counts the number of tokens in a subject.
pub fn tokenCount(subject: []const u8) usize {
    if (subject.len == 0) return 0;

    var count: usize = 1;
    for (subject) |c| {
        if (c == '.') count += 1;
    }
    return count;
}

/// Extracts a specific token from a subject (0-indexed).
pub fn getToken(subject: []const u8, index: usize) ?[]const u8 {
    var iter = std.mem.tokenizeScalar(u8, subject, '.');
    var i: usize = 0;
    while (iter.next()) |token| {
        if (i == index) return token;
        i += 1;
    }
    return null;
}

test {
    _ = @import("subject_test.zig");
}
