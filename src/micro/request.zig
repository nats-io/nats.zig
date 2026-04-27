const std = @import("std");
const Client = @import("../Client.zig");
const headers_mod = @import("../protocol/headers.zig");
const json_util = @import("json_util.zig");

pub const HandlerFn = *const fn (*Request) void;

pub const Request = struct {
    client: *Client,
    msg: *const Client.Message,
    errored: bool = false,
    error_code: u16 = 0,
    error_desc_len: usize = 0,
    error_desc_buf: [128]u8 = undefined,

    pub fn data(self: *const Request) []const u8 {
        return self.msg.data;
    }

    pub fn subject(self: *const Request) []const u8 {
        return self.msg.subject;
    }

    pub fn reply(self: *const Request) ?[]const u8 {
        return self.msg.reply_to;
    }

    pub fn headers(self: *const Request) ?[]const u8 {
        return self.msg.headers;
    }

    pub fn respond(self: *Request, payload: []const u8) !void {
        const reply_to = self.msg.reply_to orelse return error.NoReplyTo;
        try self.client.publish(reply_to, payload);
    }

    pub fn respondJson(self: *Request, value: anytype) !void {
        const payload = try json_util.jsonStringify(self.client.allocator, value);
        defer self.client.allocator.free(payload);
        try self.respond(payload);
    }

    pub fn respondError(
        self: *Request,
        code: u16,
        description: []const u8,
        payload: []const u8,
    ) !void {
        const reply_to = self.msg.reply_to orelse return error.NoReplyTo;
        var code_buf: [16]u8 = undefined;
        const code_str = try std.fmt.bufPrint(&code_buf, "{d}", .{code});
        const hdrs = [_]headers_mod.Entry{
            .{ .key = "Nats-Service-Error", .value = description },
            .{ .key = "Nats-Service-Error-Code", .value = code_str },
        };
        try self.client.publishWithHeaders(reply_to, &hdrs, payload);
        self.errored = true;
        self.error_code = code;
        self.error_desc_len = @min(description.len, self.error_desc_buf.len);
        @memcpy(
            self.error_desc_buf[0..self.error_desc_len],
            description[0..self.error_desc_len],
        );
    }

    pub fn errorDescription(self: *const Request) []const u8 {
        return self.error_desc_buf[0..self.error_desc_len];
    }
};

pub const Handler = struct {
    impl: Impl,

    pub const Impl = union(enum) {
        vtable: VTableImpl,
        bare_fn: HandlerFn,
    };

    pub const VTableImpl = struct {
        ptr: *anyopaque,
        call: *const fn (*anyopaque, *Request) void,
    };

    pub fn init(comptime T: type, ptr: *T) Handler {
        const gen = struct {
            fn call(p: *anyopaque, req: *Request) void {
                const self: *T = @ptrCast(@alignCast(p));
                self.onRequest(req);
            }
        };
        return .{ .impl = .{ .vtable = .{
            .ptr = ptr,
            .call = gen.call,
        } } };
    }

    pub fn fromFn(f: HandlerFn) Handler {
        return .{ .impl = .{ .bare_fn = f } };
    }

    pub fn dispatch(self: Handler, req: *Request) void {
        switch (self.impl) {
            .vtable => |v| v.call(v.ptr, req),
            .bare_fn => |f| f(req),
        }
    }
};
