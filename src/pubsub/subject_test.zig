//! Subject Validation Edge Case Tests
//!
//! Comprehensive test coverage for subject validation including:
//! - Empty/boundary inputs
//! - Null bytes and control characters
//! - Wildcard position validation
//! - Pattern matching edge cases
//! - Token counting edge cases

const std = @import("std");
const subject = @import("subject.zig");
const validatePublish = subject.validatePublish;
const validateSubscribe = subject.validateSubscribe;
const validateReplyTo = subject.validateReplyTo;
const validateQueueGroup = subject.validateQueueGroup;
const matches = subject.matches;
const tokenCount = subject.tokenCount;
const getToken = subject.getToken;
const ValidationError = subject.ValidationError;

// Section 1: Existing Tests (moved from subject.zig)

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

// Section 2: validatePublish Edge Cases

test "validatePublish single character subject" {
    try validatePublish("a");
    try validatePublish("x");
    try validatePublish("1");
    try validatePublish("_");
}

test "validatePublish single dot rejected" {
    // Single dot = two empty tokens
    try std.testing.expectError(error.EmptyToken, validatePublish("."));
}

test "validatePublish multiple consecutive dots rejected" {
    try std.testing.expectError(error.EmptyToken, validatePublish(".."));
    try std.testing.expectError(error.EmptyToken, validatePublish("..."));
    try std.testing.expectError(error.EmptyToken, validatePublish("foo...bar"));
}

test "validatePublish null byte rejected" {
    // Null bytes could be used for injection - should be rejected
    const result = validatePublish("foo\x00bar");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validatePublish control characters rejected" {
    // Various control characters that should be rejected
    try std.testing.expectError(error.InvalidCharacter, validatePublish("foo\x01bar"));
    try std.testing.expectError(error.InvalidCharacter, validatePublish("foo\x7fbar"));
}

test "validatePublish unicode characters" {
    // Unicode should probably be allowed (common in international use)
    try validatePublish("日本語");
    try validatePublish("foo.émoji.bar");
}

test "validatePublish very long subject" {
    // Very long subject - should there be a limit?
    const long_subject = "a" ** 10000;
    try validatePublish(long_subject);
}

test "validatePublish subject with numbers and special chars" {
    try validatePublish("foo-bar");
    try validatePublish("foo_bar");
    try validatePublish("foo123");
    try validatePublish("123");
    try validatePublish("foo-bar_baz.123");
}

test "validatePublish leading/trailing dots" {
    try std.testing.expectError(error.EmptyToken, validatePublish(".foo.bar"));
    try std.testing.expectError(error.EmptyToken, validatePublish("foo.bar."));
    try std.testing.expectError(error.EmptyToken, validatePublish("."));
}

// Section 3: validateSubscribe Edge Cases

test "validateSubscribe single wildcard tokens" {
    try validateSubscribe("*");
    try validateSubscribe(">");
}

test "validateSubscribe multiple single wildcards" {
    try validateSubscribe("*.*");
    try validateSubscribe("*.*.*");
    try validateSubscribe("foo.*.*");
    try validateSubscribe("*.*.bar");
}

test "validateSubscribe wildcard in middle of token rejected" {
    // "*abc" or "abc*" should be rejected
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe("*abc"));
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe("abc*"));
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe("a*c"));
}

test "validateSubscribe > in middle of token rejected" {
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe(">abc"));
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe("abc>"));
    try std.testing.expectError(error.InvalidCharacter, validateSubscribe("a>c"));
}

test "validateSubscribe > not at end rejected" {
    try std.testing.expectError(error.WildcardNotLast, validateSubscribe(">.bar"));
    try std.testing.expectError(error.WildcardNotLast, validateSubscribe("foo.>.bar"));
    try std.testing.expectError(error.WildcardNotLast, validateSubscribe(">.*"));
}

test "validateSubscribe null byte rejected" {
    const result = validateSubscribe("foo\x00bar");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateSubscribe single dot rejected" {
    try std.testing.expectError(error.EmptyToken, validateSubscribe("."));
}

test "validateSubscribe empty token before wildcard" {
    try std.testing.expectError(error.EmptyToken, validateSubscribe(".*"));
    try std.testing.expectError(error.EmptyToken, validateSubscribe(".>"));
}

// Section 4: validateReplyTo Edge Cases

test "validateReplyTo empty string" {
    const result = validateReplyTo("");
    try std.testing.expectError(error.EmptySubject, result);
}

test "validateReplyTo null byte rejected" {
    const result = validateReplyTo("inbox\x00inject");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo allows dots and special chars" {
    // Reply-to can contain dots (for inbox subjects)
    try validateReplyTo("_INBOX.abc.123.def");
    try validateReplyTo("reply-to");
    try validateReplyTo("reply_to");
}

test "validateReplyTo tab rejected" {
    const result = validateReplyTo("inbox\treply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo CR only rejected" {
    const result = validateReplyTo("inbox\rreply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo LF only rejected" {
    const result = validateReplyTo("inbox\nreply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo space rejected" {
    const result = validateReplyTo("inbox reply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo DEL char rejected" {
    const result = validateReplyTo("inbox\x7freply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo control char 0x01 rejected" {
    const result = validateReplyTo("inbox\x01reply");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateReplyTo very long string" {
    // 10000 character reply-to should be valid (no length limit)
    const long_reply = "a" ** 10000;
    try validateReplyTo(long_reply);
}

// Section 5: validateQueueGroup Edge Cases

test "validateQueueGroup empty string" {
    const result = validateQueueGroup("");
    try std.testing.expectError(error.EmptySubject, result);
}

test "validateQueueGroup null byte rejected" {
    const result = validateQueueGroup("workers\x00inject");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup allows dots" {
    // Queue groups can contain dots
    try validateQueueGroup("worker.group.1");
}

test "validateQueueGroup tab rejected" {
    const result = validateQueueGroup("workers\tgroup");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup CR only rejected" {
    const result = validateQueueGroup("workers\rgroup");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup LF only rejected" {
    const result = validateQueueGroup("workers\ngroup");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup space rejected" {
    const result = validateQueueGroup("workers group");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup DEL char rejected" {
    const result = validateQueueGroup("workers\x7fgroup");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup control char 0x01 rejected" {
    const result = validateQueueGroup("workers\x01group");
    try std.testing.expectError(error.InvalidCharacter, result);
}

test "validateQueueGroup very long string" {
    // 10000 character queue group should be valid (no length limit)
    const long_qg = "w" ** 10000;
    try validateQueueGroup(long_qg);
}

test "validateQueueGroup allows special chars" {
    try validateQueueGroup("worker-pool_1");
    try validateQueueGroup("worker.pool.1");
    try validateQueueGroup("WORKERS");
}

// Section 6: matches() Edge Cases

test "matches empty pattern" {
    const result = matches("", "foo");
    try std.testing.expect(!result);
}

test "matches empty subject" {
    const result = matches("foo", "");
    try std.testing.expect(!result);
}

test "matches both empty" {
    // Both empty = both invalid, return false
    const result = matches("", "");
    try std.testing.expect(!result);
}

test "matches single token exact" {
    try std.testing.expect(matches("foo", "foo"));
    try std.testing.expect(!matches("foo", "bar"));
}

test "matches pattern longer than subject" {
    try std.testing.expect(!matches("foo.bar.baz", "foo.bar"));
    try std.testing.expect(!matches("foo.bar", "foo"));
}

test "matches subject longer than pattern" {
    try std.testing.expect(!matches("foo", "foo.bar"));
    try std.testing.expect(!matches("foo.bar", "foo.bar.baz"));
}

test "matches * matches empty token behavior" {
    // What happens with "foo.*" matching "foo." (empty last token)?
    // tokenizeScalar skips empty tokens, so this might have unexpected behavior
    try std.testing.expect(!matches("foo.*", "foo."));
}

test "matches > requires at least one token" {
    // ">" should require at least one token to match
    try std.testing.expect(matches(">", "a"));
    try std.testing.expect(matches(">", "a.b.c"));
    // "foo.>" requires at least one token after foo
    try std.testing.expect(!matches("foo.>", "foo"));
    try std.testing.expect(matches("foo.>", "foo.x"));
}

test "matches multiple * wildcards" {
    try std.testing.expect(matches("*.*", "a.b"));
    try std.testing.expect(!matches("*.*", "a"));
    try std.testing.expect(!matches("*.*", "a.b.c"));
    try std.testing.expect(matches("*.*.*", "a.b.c"));
}

test "matches * and > combination" {
    try std.testing.expect(matches("*.>", "a.b"));
    try std.testing.expect(matches("*.>", "a.b.c"));
    try std.testing.expect(!matches("*.>", "a"));
}

test "matches with dots in pattern edge cases" {
    // Pattern and subject with trailing/leading dots
    // tokenizeScalar skips empty tokens, so "foo." -> ["foo"] and ".foo" -> ["foo"]
    // Both become equivalent, so they match (garbage in, garbage out for invalid subjects)
    try std.testing.expect(matches("foo.", "foo."));
    try std.testing.expect(matches(".foo", ".foo"));
}

// Section 7: tokenCount Edge Cases

test "tokenCount single dot" {
    // "." has two empty tokens
    const count = tokenCount(".");
    try std.testing.expectEqual(@as(usize, 2), count);
}

test "tokenCount multiple dots" {
    // ".." has three empty tokens
    try std.testing.expectEqual(@as(usize, 3), tokenCount(".."));
    try std.testing.expectEqual(@as(usize, 4), tokenCount("..."));
}

test "tokenCount trailing dot" {
    // "foo." counts as 2 tokens even though second is empty
    try std.testing.expectEqual(@as(usize, 2), tokenCount("foo."));
}

test "tokenCount leading dot" {
    // ".foo" counts as 2 tokens even though first is empty
    try std.testing.expectEqual(@as(usize, 2), tokenCount(".foo"));
}

test "tokenCount very long subject" {
    const long_subject = "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x.y.z";
    try std.testing.expectEqual(@as(usize, 26), tokenCount(long_subject));
}

// Section 8: getToken Edge Cases

test "getToken empty subject" {
    // Empty subject should return null for any index
    try std.testing.expect(getToken("", 0) == null);
    try std.testing.expect(getToken("", 1) == null);
}

test "getToken single dot" {
    // "." splits into empty tokens - tokenizeScalar skips them
    try std.testing.expect(getToken(".", 0) == null);
}

test "getToken trailing dot" {
    // "foo." - what does getToken return for index 1?
    const token0 = getToken("foo.", 0);
    try std.testing.expectEqualSlices(u8, "foo", token0.?);
    // Index 1 should be null (empty token skipped by tokenizeScalar)
    try std.testing.expect(getToken("foo.", 1) == null);
}

test "getToken leading dot" {
    // ".foo" - what does getToken return for index 0?
    const token0 = getToken(".foo", 0);
    try std.testing.expectEqualSlices(u8, "foo", token0.?);
    try std.testing.expect(getToken(".foo", 1) == null);
}

test "getToken very large index" {
    try std.testing.expect(getToken("foo.bar", 1000) == null);
}
