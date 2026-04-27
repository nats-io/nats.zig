const std = @import("std");
const nats = @import("nats");

const Echo = struct {
    pub fn onRequest(_: *@This(), req: *nats.micro.Request) void {
        req.respond(req.data()) catch {};
    }
};

pub fn main(init: std.process.Init) !void {
    const client = try nats.Client.connect(
        init.gpa,
        init.io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit();

    var echo = Echo{};
    const service = try nats.micro.addService(client, .{
        .name = "echo",
        .version = "1.0.0",
        .endpoint = .{
            .subject = "echo",
            .handler = nats.micro.Handler.init(Echo, &echo),
        },
    });
    defer service.deinit();

    std.debug.print("micro echo service running on 'echo'\n", .{});
    while (true) {
        init.io.sleep(.fromSeconds(1), .awake) catch {};
    }
}
