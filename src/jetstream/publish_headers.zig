const std = @import("std");

const nats = @import("../nats.zig");
const headers = nats.protocol.headers;
const types = @import("types.zig");

pub const PublishHeaderSet = struct {
    entries: [6]headers.Entry = undefined,
    count: usize = 0,
    expected_last_seq_buf: [20]u8 = undefined,
    expected_last_subj_seq_buf: [20]u8 = undefined,

    pub fn slice(
        self: *const PublishHeaderSet,
    ) []const headers.Entry {
        return self.entries[0..self.count];
    }

    pub fn populate(
        self: *PublishHeaderSet,
        opts: types.PublishOpts,
    ) void {
        self.* = .{};

        if (opts.msg_id) |v| {
            self.entries[self.count] = .{
                .key = headers.HeaderName.msg_id,
                .value = v,
            };
            self.count += 1;
        }
        if (opts.expected_stream) |v| {
            self.entries[self.count] = .{
                .key = headers.HeaderName.expected_stream,
                .value = v,
            };
            self.count += 1;
        }
        if (opts.expected_last_msg_id) |v| {
            self.entries[self.count] = .{
                .key = headers.HeaderName.expected_last_msg_id,
                .value = v,
            };
            self.count += 1;
        }
        if (opts.expected_last_seq) |v| {
            const s = std.fmt.bufPrint(
                &self.expected_last_seq_buf,
                "{d}",
                .{v},
            ) catch unreachable;
            self.entries[self.count] = .{
                .key = headers.HeaderName.expected_last_seq,
                .value = s,
            };
            self.count += 1;
        }
        if (opts.expected_last_subj_seq) |v| {
            const s = std.fmt.bufPrint(
                &self.expected_last_subj_seq_buf,
                "{d}",
                .{v},
            ) catch unreachable;
            self.entries[self.count] = .{
                .key = headers.HeaderName.expected_last_subj_seq,
                .value = s,
            };
            self.count += 1;
        }
        if (opts.ttl) |v| {
            self.entries[self.count] = .{
                .key = headers.HeaderName.msg_ttl,
                .value = v,
            };
            self.count += 1;
        }
    }
};
