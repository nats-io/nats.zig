//! Request/Reply with Callback Subscription
//!
//! Demonstrates building a service responder using callback-style
//! subscriptions. The service handler receives requests via
//! onMessage and sends replies using msg.respond().
//!
//! Run with: zig build run-request-reply-callback
//!
//! Prerequisites: nats-server running on localhost:4222
//!   nats-server -DV

const std = @import("std");
const nats = @import("nats");

/// Doubler service -- doubles any number sent to it.
const DoublerService = struct {
    client: *nats.Client,
    handled: u32 = 0,

    pub fn onMessage(
        self: *@This(),
        msg: *const nats.Message,
    ) void {
        self.handled += 1;

        const num = std.fmt.parseInt(
            i32,
            msg.data,
            10,
        ) catch 0;
        var buf: [32]u8 = undefined;
        const result = std.fmt.bufPrint(
            &buf,
            "{d}",
            .{num * 2},
        ) catch "error";

        std.debug.print(
            "  [service] {d} * 2 = {s}\n",
            .{ num, result },
        );

        msg.respond(self.client, result) catch |err| {
            std.debug.print(
                "  [service] respond failed: {s}\n",
                .{@errorName(err)},
            );
        };
    }
};

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(
        allocator,
        .{ .environ = .empty },
    );
    defer threaded.deinit();
    const io = threaded.io();

    // Service client
    const service_client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "doubler-service" },
    );
    defer service_client.deinit(allocator);

    // Requester client
    const requester = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{ .name = "requester" },
    );
    defer requester.deinit(allocator);

    std.debug.print("Connected to NATS!\n\n", .{});

    // Start service with callback subscription
    var svc = DoublerService{ .client = service_client };

    const sub = try service_client.subscribeWithCallback(
        allocator,
        "math.double",
        nats.MsgHandler.init(DoublerService, &svc),
    );
    defer sub.deinit(allocator);

    std.debug.print("Service listening on 'math.double'\n\n", .{});

    // Wait for subscription to propagate
    io.sleep(.fromMilliseconds(50), .awake) catch {};

    // Send requests
    const numbers = [_][]const u8{ "21", "50", "100" };
    for (numbers) |n| {
        std.debug.print("Requesting: {s} * 2\n", .{n});
        if (try requester.request(allocator, "math.double", n, 1000)) |reply| {
            defer reply.deinit(allocator);
            std.debug.print("  Reply: {s}\n\n", .{reply.data});
        } else {
            std.debug.print("  Timed out\n\n", .{});
        }
    }

    std.debug.print(
        "Service handled {d} requests.\n",
        .{svc.handled},
    );

    // Verify all requests were handled
    std.debug.assert(svc.handled == 3);

    std.debug.print("Done!\n", .{});
}
