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

    // Reject CR/LF to prevent protocol injection
    if (std.mem.indexOfAny(u8, subject, "\r\n") != null) {
        return error.InvalidCharacter;
    }

    var token_start: usize = 0;
    for (subject, 0..) |c, i| {
        if (c == '.') {
            if (i == token_start) return error.EmptyToken;
            token_start = i + 1;
        } else if (c == ' ' or c == '\t') {
            return error.SpaceInSubject;
        } else if (c == '*' or c == '>') {
            return error.InvalidCharacter;
        }
    }

    // Check last token isn't empty
    if (token_start >= subject.len) return error.EmptyToken;
}

/// Validates a subject for subscribing (wildcards allowed).
pub fn validateSubscribe(subject: []const u8) ValidationError!void {
    if (subject.len == 0) return error.EmptySubject;

    // Reject CR/LF to prevent protocol injection
    if (std.mem.indexOfAny(u8, subject, "\r\n") != null) {
        return error.InvalidCharacter;
    }

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
            // > must be alone in its token
            if (i != token_start) return error.InvalidCharacter;
            has_full_wildcard = true;
        } else if (c == '*') {
            // * must be alone in its token
            if (i != token_start) return error.InvalidCharacter;
        }
    }

    // Check last token isn't empty
    if (token_start >= subject.len) return error.EmptyToken;
}

/// Validates a reply-to address for protocol safety.
pub fn validateReplyTo(reply_to: []const u8) ValidationError!void {
    assert(reply_to.len > 0);
    if (std.mem.indexOfAny(u8, reply_to, "\r\n \t") != null) {
        return error.InvalidCharacter;
    }
}

/// Validates a queue group name for protocol safety.
pub fn validateQueueGroup(queue: []const u8) ValidationError!void {
    assert(queue.len > 0);
    if (std.mem.indexOfAny(u8, queue, "\r\n \t") != null) {
        return error.InvalidCharacter;
    }
}

/// Checks if a subject matches a pattern (with wildcards).
pub fn matches(pattern: []const u8, subject: []const u8) bool {
    assert(pattern.len > 0);
    assert(subject.len > 0);
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

test "validate publish subject" {
    try validatePublish("foo");
    try validatePublish("foo.bar");
    try validatePublish("foo.bar.baz");
    try validatePublish("_INBOX.abc123");

    try std.testing.expectError(error.EmptySubject, validatePublish(""));
    try std.testing.expectError(error.EmptyToken, validatePublish("foo."));
    try std.testing.expectError(error.EmptyToken, validatePublish(".foo"));
    try std.testing.expectError(error.EmptyToken, validatePublish("foo..bar"));
    const inv_char = error.InvalidCharacter;
    try std.testing.expectError(inv_char, validatePublish("foo.*"));
    try std.testing.expectError(inv_char, validatePublish("foo.>"));
    const space_err = error.SpaceInSubject;
    try std.testing.expectError(space_err, validatePublish("foo bar"));

    // CR/LF injection protection
    try std.testing.expectError(inv_char, validatePublish("test\r\nINFO"));
    try std.testing.expectError(inv_char, validatePublish("test\nfoo"));
    try std.testing.expectError(inv_char, validatePublish("test\rfoo"));
}

test "validate subscribe subject" {
    try validateSubscribe("foo");
    try validateSubscribe("foo.bar");
    try validateSubscribe("foo.*");
    try validateSubscribe("*.bar");
    try validateSubscribe("foo.>");
    try validateSubscribe(">");

    try std.testing.expectError(error.EmptySubject, validateSubscribe(""));
    try std.testing.expectError(error.EmptyToken, validateSubscribe("foo."));
    const wc_err = error.WildcardNotLast;
    try std.testing.expectError(wc_err, validateSubscribe("foo.>.bar"));
    const inv_char = error.InvalidCharacter;
    try std.testing.expectError(inv_char, validateSubscribe("foo.bar>"));
    try std.testing.expectError(inv_char, validateSubscribe("foo.bar*"));

    // CR/LF injection protection
    try std.testing.expectError(inv_char, validateSubscribe("test\r\nUNSUB"));
    try std.testing.expectError(inv_char, validateSubscribe("test\nfoo"));
}

test "subject matching" {
    // Exact match
    try std.testing.expect(matches("foo.bar", "foo.bar"));
    try std.testing.expect(!matches("foo.bar", "foo.baz"));

    // Single token wildcard
    try std.testing.expect(matches("foo.*", "foo.bar"));
    try std.testing.expect(matches("foo.*", "foo.baz"));
    try std.testing.expect(!matches("foo.*", "foo.bar.baz"));
    try std.testing.expect(matches("*.bar", "foo.bar"));

    // Full wildcard
    try std.testing.expect(matches("foo.>", "foo.bar"));
    try std.testing.expect(matches("foo.>", "foo.bar.baz"));
    try std.testing.expect(!matches("foo.>", "foo"));
    try std.testing.expect(matches(">", "foo"));
    try std.testing.expect(matches(">", "foo.bar.baz"));
}

test "token count" {
    try std.testing.expectEqual(@as(usize, 0), tokenCount(""));
    try std.testing.expectEqual(@as(usize, 1), tokenCount("foo"));
    try std.testing.expectEqual(@as(usize, 2), tokenCount("foo.bar"));
    try std.testing.expectEqual(@as(usize, 3), tokenCount("foo.bar.baz"));
}

test "get token" {
    try std.testing.expectEqualSlices(u8, "foo", getToken("foo.bar.baz", 0).?);
    try std.testing.expectEqualSlices(u8, "bar", getToken("foo.bar.baz", 1).?);
    try std.testing.expectEqualSlices(u8, "baz", getToken("foo.bar.baz", 2).?);
    try std.testing.expect(getToken("foo.bar.baz", 3) == null);
}

test "validateReplyTo rejects injection" {
    // Valid reply-to addresses
    try validateReplyTo("_INBOX.abc123");
    try validateReplyTo("reply.to.subject");

    // CR/LF injection
    const inv_char = error.InvalidCharacter;
    try std.testing.expectError(inv_char, validateReplyTo("inbox\r\nUNSUB"));
    try std.testing.expectError(inv_char, validateReplyTo("inbox\nfoo"));
    try std.testing.expectError(inv_char, validateReplyTo("inbox\rfoo"));

    // Spaces and tabs
    try std.testing.expectError(inv_char, validateReplyTo("inbox foo"));
    try std.testing.expectError(inv_char, validateReplyTo("inbox\tfoo"));
}

test "validateQueueGroup rejects injection" {
    // Valid queue groups
    try validateQueueGroup("workers");
    try validateQueueGroup("queue-1");

    // CR/LF injection
    const inv_char = error.InvalidCharacter;
    try std.testing.expectError(inv_char, validateQueueGroup("workers\r\n"));
    try std.testing.expectError(inv_char, validateQueueGroup("workers\nfoo"));
    try std.testing.expectError(inv_char, validateQueueGroup("workers\rfoo"));

    // Spaces and tabs
    try std.testing.expectError(inv_char, validateQueueGroup("workers foo"));
    try std.testing.expectError(inv_char, validateQueueGroup("workers\tfoo"));
}
