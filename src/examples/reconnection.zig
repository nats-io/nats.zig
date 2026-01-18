//! Reconnection and Resilience
//!
//! Demonstrates NATS client resilience features:
//! - Reconnection configuration options
//! - Connection state monitoring
//! - Handling publish during disconnect
//!
//! Run with: zig build run-reconnection
//!
//! To test reconnection:
//! 1. Start nats-server
//! 2. Run this example
//! 3. Restart nats-server while example is running
//! 4. Watch the client reconnect automatically
//!
//! Prerequisites: nats-server running on localhost:4222

const std = @import("std");
const nats = @import("nats");

const Io = std.Io;

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    std.debug.print("Connecting with reconnection enabled...\n", .{});

    // Connect with explicit reconnection settings
    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://localhost:4222",
        .{
            .name = "reconnection-example",

            // Reconnection settings
            .reconnect = true, // Enable auto-reconnect (default)
            .max_reconnect_attempts = 10, // Max attempts (0 = infinite)
            .reconnect_wait_ms = 1000, // Initial backoff: 1 second
            .reconnect_wait_max_ms = 10_000, // Max backoff: 10 seconds
            .reconnect_jitter_percent = 10, // Add 10% jitter to backoff

            // Keepalive settings (detect stale connections)
            .ping_interval_ms = 30_000, // PING every 30 seconds
            .max_pings_outstanding = 2, // Disconnect after 2 missed PONGs

            // Buffer publishes during reconnect (8MB default)
            .pending_buffer_size = 8 * 1024 * 1024,
        },
    );
    defer client.deinit(allocator);

    std.debug.print("Connected!\n", .{});
    printConnectionInfo(client);

    // Subscribe
    const sub = try client.subscribe(allocator, "demo.reconnect");
    defer sub.deinit(allocator);
    try client.flush(allocator);

    std.debug.print("\nSubscribed to 'demo.reconnect'\n", .{});
    std.debug.print("Monitoring connection for 10 seconds...\n", .{});
    std.debug.print("(Try restarting nats-server to see reconnection)\n\n", .{});

    // Monitor connection and publish periodically
    var iteration: u32 = 0;
    const max_iterations: u32 = 20;

    while (iteration < max_iterations) : (iteration += 1) {
        io.sleep(.fromMilliseconds(500), .awake) catch {};

        // Check connection state
        const connected = client.isConnected();
        const state_str = if (connected) "CONNECTED" else "DISCONNECTED";

        // Try to publish
        var buf: [64]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, "Ping {d}", .{iteration + 1}) catch "Ping";

        const publish_result = client.publish("demo.reconnect", msg);

        if (publish_result) |_| {
            // Publish succeeded - try to flush
            if (client.flush(allocator)) |_| {
                std.debug.print(
                    "[{d:2}] {s} - Published and flushed: {s}\n",
                    .{ iteration + 1, state_str, msg },
                );
            } else |flush_err| {
                std.debug.print(
                    "[{d:2}] {s} - Published, flush failed: {}\n",
                    .{ iteration + 1, state_str, flush_err },
                );
            }
        } else |pub_err| {
            std.debug.print(
                "[{d:2}] {s} - Publish failed: {}\n",
                .{ iteration + 1, state_str, pub_err },
            );
        }

        // Try to receive any messages
        while (sub.tryNext()) |recv_msg| {
            defer recv_msg.deinit(allocator);
            std.debug.print("      Received: {s}\n", .{recv_msg.data});
        }

        // Print reconnection stats periodically
        if (iteration > 0 and (iteration + 1) % 5 == 0) {
            printStats(client);
        }
    }

    std.debug.print("\nFinal connection state:\n", .{});
    printConnectionInfo(client);
    printStats(client);

    std.debug.print("\nDone!\n", .{});
}

fn printConnectionInfo(client: *nats.Client) void {
    std.debug.print("Connection info:\n", .{});
    std.debug.print("  Connected: {}\n", .{client.isConnected()});

    if (client.getServerInfo()) |info| {
        if (info.server_name.len > 0) {
            std.debug.print("  Server: {s}\n", .{info.server_name});
        }
        if (info.version.len > 0) {
            std.debug.print("  Version: {s}\n", .{info.version});
        }
        std.debug.print("  Max payload: {d} bytes\n", .{info.max_payload});
    }
}

fn printStats(client: *nats.Client) void {
    const stats = client.getStats();
    std.debug.print("  Stats: {d} msgs out, {d} msgs in, {d} reconnects\n", .{
        stats.msgs_out,
        stats.msgs_in,
        stats.reconnects,
    });
}
