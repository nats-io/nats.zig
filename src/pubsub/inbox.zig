//! Inbox Generation
//!
//! Generates unique inbox subjects for request/reply patterns.
//! Inbox format: _INBOX.<random-22-chars>

const std = @import("std");
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Inbox prefix used by NATS.
pub const prefix = "_INBOX.";

/// Length of the random portion of inbox.
pub const random_len = 22;

/// Total length of a generated inbox.
pub const total_len = prefix.len + random_len;

/// Characters used in inbox generation (base62).
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ" ++
    "abcdefghijklmnopqrstuvwxyz";

/// Generates a new unique inbox subject.
/// Caller owns returned memory.
pub fn newInbox(allocator: Allocator, io: Io) Allocator.Error![]u8 {
    const result = try allocator.alloc(u8, total_len);
    @memcpy(result[0..prefix.len], prefix);
    fillRandom(io, result[prefix.len..]);
    assert(result.len == total_len);
    return result;
}

/// Generates inbox into provided buffer.
/// Buffer must be at least total_len bytes.
pub fn newInboxBuf(io: Io, buf: []u8) error{BufferTooSmall}![]u8 {
    if (buf.len < total_len) return error.BufferTooSmall;
    assert(buf.len >= total_len);
    @memcpy(buf[0..prefix.len], prefix);
    fillRandom(io, buf[prefix.len..][0..random_len]);
    return buf[0..total_len];
}

/// Fills buffer with random base62 characters.
fn fillRandom(io: Io, buf: []u8) void {
    assert(buf.len > 0);
    io.random(buf);
    for (buf) |*b| {
        b.* = alphabet[@mod(b.*, alphabet.len)];
    }
}

/// Checks if a subject is an inbox.
pub fn isInbox(subject: []const u8) bool {
    return std.mem.startsWith(u8, subject, prefix);
}

/// Generates inbox with custom prefix for wildcards.
/// Format: _INBOX.<prefix>.<random>
/// Caller owns returned memory.
pub fn newInboxWithPrefix(
    allocator: Allocator,
    io: Io,
    custom_prefix: []const u8,
) Allocator.Error![]u8 {
    assert(custom_prefix.len > 0);
    const len = prefix.len + custom_prefix.len + 1 + random_len;
    const result = try allocator.alloc(u8, len);

    var pos: usize = 0;
    @memcpy(result[pos..][0..prefix.len], prefix);
    pos += prefix.len;

    @memcpy(result[pos..][0..custom_prefix.len], custom_prefix);
    pos += custom_prefix.len;

    result[pos] = '.';
    pos += 1;

    fillRandom(io, result[pos..][0..random_len]);

    return result;
}

test "new inbox" {
    const allocator = std.testing.allocator;
    var io: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const inbox = try newInbox(allocator, io.io());
    defer allocator.free(inbox);

    try std.testing.expectEqual(total_len, inbox.len);
    try std.testing.expect(std.mem.startsWith(u8, inbox, prefix));
    try std.testing.expect(isInbox(inbox));
}

test "new inbox buf" {
    const allocator = std.testing.allocator;
    var io: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    var buf: [64]u8 = undefined;
    const inbox = try newInboxBuf(io.io(), &buf);

    try std.testing.expectEqual(total_len, inbox.len);
    try std.testing.expect(std.mem.startsWith(u8, inbox, prefix));
}

test "new inbox buf too small" {
    const allocator = std.testing.allocator;
    var io: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    var buf: [10]u8 = undefined;
    try std.testing.expectError(
        error.BufferTooSmall,
        newInboxBuf(io.io(), &buf),
    );
}

test "inbox uniqueness" {
    const allocator = std.testing.allocator;
    var io: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const inbox1 = try newInbox(allocator, io.io());
    defer allocator.free(inbox1);

    const inbox2 = try newInbox(allocator, io.io());
    defer allocator.free(inbox2);

    try std.testing.expect(!std.mem.eql(u8, inbox1, inbox2));
}

test "is inbox" {
    try std.testing.expect(isInbox("_INBOX.abc123"));
    try std.testing.expect(isInbox("_INBOX."));
    try std.testing.expect(!isInbox("foo.bar"));
    try std.testing.expect(!isInbox("_INBOX"));
}

test "inbox with prefix" {
    const allocator = std.testing.allocator;
    var io: Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer io.deinit();

    const inbox = try newInboxWithPrefix(allocator, io.io(), "myprefix");
    defer allocator.free(inbox);

    try std.testing.expect(std.mem.startsWith(u8, inbox, "_INBOX.myprefix."));
    try std.testing.expect(isInbox(inbox));
}
