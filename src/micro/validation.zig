const std = @import("std");
const pubsub = @import("../pubsub.zig");

pub const Error = error{
    InvalidName,
    InvalidVersion,
    InvalidPrefix,
    InvalidGroup,
} || pubsub.subject.ValidationError;

pub fn validateName(name: []const u8) Error!void {
    if (name.len == 0) return error.InvalidName;
    for (name) |c| {
        switch (c) {
            'A'...'Z', 'a'...'z', '0'...'9', '-', '_' => {},
            else => return error.InvalidName,
        }
    }
}

pub fn validateVersion(version: []const u8) Error!void {
    _ = std.SemanticVersion.parse(version) catch {
        return error.InvalidVersion;
    };
}

pub fn validatePrefix(prefix: []const u8) Error!void {
    pubsub.validatePublish(prefix) catch {
        return error.InvalidPrefix;
    };
}

pub fn validateGroup(prefix: []const u8) Error!void {
    pubsub.validatePublish(prefix) catch {
        return error.InvalidGroup;
    };
}

test "validate name" {
    try validateName("svc_1");
    try std.testing.expectError(error.InvalidName, validateName(""));
    try std.testing.expectError(error.InvalidName, validateName("bad name"));
    try std.testing.expectError(error.InvalidName, validateName("bad/name"));
}

test "validate version" {
    try validateVersion("1.0.0");
    try validateVersion("1.0.0-rc1+build.5");
    try std.testing.expectError(error.InvalidVersion, validateVersion("1.0"));
}
