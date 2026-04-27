//! Key-Value Store -- CRUD, concurrency, listing.
//!
//! NATS KV is a distributed key-value store backed by
//! JetStream. Keys are NATS subjects, values are message
//! payloads. Supports history, optimistic concurrency
//! (compare-and-swap via revision numbers), and TTL.
//!
//! Run with: zig-out/bin/example-kv
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
        .{ .name = "kv-example" },
    );
    defer client.deinit();

    std.debug.print("Connected to NATS!\n\n", .{});

    var js = js_mod.JetStream.init(client, .{});

    // Create a KV bucket with history=5. This means
    // up to 5 revisions per key are kept. Backed by
    // a JetStream stream named "KV_demo-kv".
    var kv = try js.createKeyValue(.{
        .bucket = "demo-kv",
        .history = 5,
        .storage = .memory,
    });

    std.debug.print(
        "Bucket 'demo-kv' created.\n\n",
        .{},
    );

    // -- Basic CRUD --

    // Put stores a value and returns the revision
    // (stream sequence number). Each put creates a
    // new revision.
    const rev1 = try kv.put("user.name", "Alice");
    std.debug.print(
        "Put 'user.name'='Alice' rev={d}\n",
        .{rev1},
    );

    // Get returns the latest value for a key.
    // Returns null if the key was never written.
    if (try kv.get("user.name")) |entry| {
        var e = entry;
        defer e.deinit();
        std.debug.print(
            "Get 'user.name'='{s}' rev={d}\n",
            .{ e.value, e.revision },
        );
    }

    // Update overwrites the value. New revision
    // returned.
    const rev2 = try kv.put("user.name", "Bob");
    std.debug.print(
        "Put 'user.name'='Bob' rev={d}\n\n",
        .{rev2},
    );

    // -- Optimistic concurrency --
    // update() takes a revision parameter: the write
    // only succeeds if the key's current revision
    // matches. This prevents lost updates when
    // multiple clients write concurrently.

    std.debug.print(
        "-- Optimistic concurrency --\n",
        .{},
    );

    const rev3 = try kv.update(
        "user.name",
        "Charlie",
        rev2,
    );
    std.debug.print(
        "Update with rev={d}: ok, new rev={d}\n",
        .{ rev2, rev3 },
    );

    // Attempt to update with a stale revision.
    // This simulates a concurrent writer that read
    // an older value.
    if (kv.update("user.name", "Dave", rev1)) |_| {
        std.debug.print("Unexpected success!\n", .{});
    } else |_| {
        std.debug.print(
            "Update with stale rev={d}: " ++
                "rejected (expected)\n\n",
            .{rev1},
        );
    }

    // -- Create (if not exists) --
    // create() only succeeds if the key does not yet
    // exist. Useful for distributed locks or
    // one-time initialization.

    std.debug.print(
        "-- Create if not exists --\n",
        .{},
    );

    const email_rev = try kv.create(
        "user.email",
        "alice@example.com",
    );
    std.debug.print(
        "Created 'user.email' rev={d}\n",
        .{email_rev},
    );

    // Second create fails because key already exists
    if (kv.create("user.email", "bob@example.com")) |_| {
        std.debug.print("Unexpected success!\n", .{});
    } else |_| {
        std.debug.print(
            "Create duplicate: rejected\n\n",
            .{},
        );
    }

    // -- Delete --
    // Soft-delete publishes a delete marker. The key
    // still appears in history but get() returns the
    // delete marker with operation=.delete.

    const del_rev = try kv.delete("user.email");
    std.debug.print(
        "Deleted 'user.email' rev={d}\n\n",
        .{del_rev},
    );

    // -- List keys --
    // keys() returns all non-deleted keys in the
    // bucket. Uses an ephemeral consumer under the
    // hood.
    std.debug.print("-- All keys --\n", .{});

    // Add a few more keys for listing
    _ = try kv.put("user.age", "30");
    _ = try kv.put("user.city", "Portland");

    const key_list = try kv.keys(allocator);
    defer {
        for (key_list) |k| allocator.free(k);
        allocator.free(key_list);
    }

    for (key_list) |key| {
        std.debug.print("  {s}\n", .{key});
    }
    std.debug.print(
        "  ({d} keys total)\n\n",
        .{key_list.len},
    );

    // -- History --
    // history() returns all revisions for a key,
    // including puts, updates, and deletes.
    std.debug.print(
        "-- History for 'user.name' --\n",
        .{},
    );

    const hist = try kv.history(
        allocator,
        "user.name",
    );
    defer {
        for (hist) |*e| {
            var entry = e.*;
            entry.deinit();
        }
        allocator.free(hist);
    }

    for (hist) |entry| {
        const op_str: []const u8 = switch (entry.operation) {
            .put => "PUT",
            .delete => "DEL",
            .purge => "PURGE",
        };
        std.debug.print(
            "  rev={d} op={s} val='{s}'\n",
            .{ entry.revision, op_str, entry.value },
        );
    }

    // -- Bucket status --
    // status() returns the underlying stream info
    // for the KV bucket.
    std.debug.print("\n-- Bucket status --\n", .{});

    var st = try kv.status();
    defer st.deinit();

    if (st.value.state) |state| {
        std.debug.print(
            "  messages={d} bytes={d}\n",
            .{ state.messages, state.bytes },
        );
    }

    // -- Cleanup --
    var del_resp = try js.deleteKeyValue("demo-kv");
    del_resp.deinit();

    std.debug.print(
        "\nBucket deleted. Done!\n",
        .{},
    );
}
