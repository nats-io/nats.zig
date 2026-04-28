//! Key-Value Watch -- real-time change notifications.
//!
//! KV watch creates an ephemeral consumer that delivers
//! change events as they happen. The watcher first
//! delivers all existing matching keys (the "initial
//! values"), then switches to live updates. A null from
//! next() after the initial batch signals the transition
//! to live mode.
//!
//! Run with: zig build run-kv-watch
//!
//! Prerequisites: nats-server -js

const std = @import("std");
const nats = @import("nats");
const js_mod = nats.jetstream;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    const client = try nats.Client.connect(
        allocator,
        io,
        "nats://127.0.0.1:4222",
        .{ .name = "kv-watch-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    var js = try js_mod.JetStream.init(client, .{});

    var kv = try js.createKeyValue(.{
        .bucket = "demo-watch",
        .storage = .memory,
    });

    // Seed an initial value before creating the
    // watcher. This will appear as the first "initial
    // value" delivered to the watcher.
    _ = try kv.put("config.version", "1.0");

    std.debug.print(
        "Seeded 'config.version'='1.0'\n\n",
        .{},
    );

    // Watch all keys matching "config.>". The ">"
    // wildcard matches one or more tokens, so this
    // catches config.version, config.debug, etc.
    var watcher = try kv.watch("config.>");
    defer watcher.deinit();

    // Read initial values. The watcher delivers all
    // existing matching keys first. A null return
    // signals that all existing keys have been
    // delivered and we're now in live mode.
    std.debug.print(
        "-- Initial values --\n",
        .{},
    );

    while (try watcher.next(3000)) |entry| {
        var e = entry;
        defer e.deinit();
        printEntry(&e);
    }

    std.debug.print(
        "  (initial sync complete)\n\n",
        .{},
    );

    // Now make some live changes. These will be
    // delivered to the watcher in real time.
    _ = try kv.put("config.version", "2.0");
    _ = try kv.put("config.debug", "true");
    _ = try kv.put("config.log_level", "info");

    // Flush to ensure all puts reach the server
    try client.flush(2_000_000_000);

    // Read live updates. Each put above generates
    // one watcher event.
    std.debug.print("-- Live updates --\n", .{});

    var live_count: u32 = 0;
    while (live_count < 3) {
        if (try watcher.next(3000)) |entry| {
            var e = entry;
            defer e.deinit();
            printEntry(&e);
            live_count += 1;
        } else break;
    }

    // Delete a key and watch the delete marker
    _ = try kv.delete("config.debug");
    try client.flush(2_000_000_000);

    std.debug.print(
        "\n-- Delete event --\n",
        .{},
    );

    if (try watcher.next(3000)) |entry| {
        var e = entry;
        defer e.deinit();
        printEntry(&e);
    }

    // Cleanup
    var del = try js.deleteKeyValue("demo-watch");
    del.deinit();

    std.debug.print("\nBucket deleted. Done!\n", .{});
}

/// Prints a KV entry showing key, value, operation,
/// and revision. Handles all three operations: put,
/// delete, and purge.
fn printEntry(
    entry: *const js_mod.KeyValueEntry,
) void {
    const op_str: []const u8 = switch (entry.operation) {
        .put => "PUT",
        .delete => "DEL",
        .purge => "PURGE",
    };
    if (entry.operation == .put) {
        std.debug.print(
            "  {s} '{s}'='{s}' rev={d}\n",
            .{
                op_str,
                entry.key,
                entry.value,
                entry.revision,
            },
        );
    } else {
        std.debug.print(
            "  {s} '{s}' rev={d}\n",
            .{
                op_str,
                entry.key,
                entry.revision,
            },
        );
    }
}
