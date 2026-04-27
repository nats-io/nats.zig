const std = @import("std");

const json_stringify_opts: std.json.Stringify.Options = .{
    .emit_null_optional_fields = false,
};

const json_parse_opts: std.json.ParseOptions = .{
    .ignore_unknown_fields = true,
};

pub fn jsonStringify(
    allocator: std.mem.Allocator,
    value: anytype,
) error{OutOfMemory}![]u8 {
    return std.json.Stringify.valueAlloc(
        allocator,
        value,
        json_stringify_opts,
    );
}

pub fn jsonParse(
    comptime T: type,
    allocator: std.mem.Allocator,
    data: []const u8,
) std.json.ParseError(std.json.Scanner)!std.json.Parsed(T) {
    return std.json.parseFromSlice(
        T,
        allocator,
        data,
        json_parse_opts,
    );
}
