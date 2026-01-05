# nats.zig

Production-grade NATS client library for Zig 0.16+.

Built on `std.Io` for native async support. The client stores Io internally
for connection-scoped state, providing a clean API without io parameters.

## Features

- Native Zig implementation (zero C dependencies)
- Built on `std.Io` for async-aware I/O
- Zero-copy message handling
- Go-inspired API design
- Tiger-style pre-allocation for predictable performance
- Event queues (not callbacks)
- Full cancellation support

## Requirements

- Zig 0.16.0 or later
- NATS server (for runtime)

## Installation

Add to your `build.zig.zon`:

```zig
.dependencies = .{
    .nats = .{
        .url = "https://github.com/nats-io/nats.zig/archive/refs/heads/master.tar.gz",
        // Add hash after first build attempt
    },
},
```

Then in `build.zig`:

```zig
const nats = b.dependency("nats", .{
    .target = target,
    .optimize = optimize,
});
exe.root_module.addImport("nats", nats.module("nats"));
```

## Quick Start

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    // Set up allocator
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Set up Io implementation (Andrew Kelley pattern)
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    // Connect to NATS (io passed only here)
    const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
        .name = "my-app",
    });
    defer client.deinit(allocator);

    // Publish a message
    try client.publish("greet.hello", "Hello, NATS!");
    try client.flush();

    // Subscribe and receive
    const sub = try client.subscribe(allocator, "greet.*");
    defer sub.cancel() catch {};

    if (try sub.nextMessage(allocator, .{ .timeout_ms = 1000 })) |msg| {
        defer msg.deinit(allocator);
        std.debug.print("Received: {s}\n", .{msg.data});
    }
}
```

## API Reference

### Client

#### Connection

```zig
// Connect to NATS server (io passed only at connection time)
const client = try nats.Client.connect(allocator, io, url, .{
    .name = "app-name",           // Optional client name
    .verbose = false,             // Protocol verbosity
    .pedantic = false,            // Strict protocol checking
    .connect_timeout_ms = 5000,   // Connection timeout
});
defer client.deinit(allocator);

// Check connection status
const connected = client.isConnected();
const info = client.getServerInfo();
```

#### Publishing

```zig
// Simple publish
try client.publish("subject", "payload");

// Publish with reply-to
try client.publishRequest("subject", "reply.inbox", "payload");

// Flush pending writes
try client.flush();
try client.flushWithTimeout(5000);  // With timeout

// Ping server
try client.ping();
```

#### Request/Reply

```zig
// Send request and wait for reply
if (try client.request(allocator, "service.add", "1+2", 5000)) |reply| {
    defer reply.deinit(allocator);
    std.debug.print("Result: {s}\n", .{reply.data});
}
```

### Subscriptions

#### Creating Subscriptions

```zig
// Simple subscription
const sub = try client.subscribe(allocator, "events.>");
defer sub.cancel() catch {};

// Queue subscription (load balancing)
const qsub = try client.subscribeQueue(allocator, "tasks.*", "workers");
defer qsub.cancel() catch {};
```

#### Receiving Messages

**Blocking with timeout (Go-style):**

```zig
while (try sub.nextMessage(allocator, .{ .timeout_ms = 1000 })) |msg| {
    defer msg.deinit(allocator);

    std.debug.print("Subject: {s}\n", .{msg.subject});
    std.debug.print("Data: {s}\n", .{msg.data});

    // Reply if requested
    if (msg.reply_to) |reply| {
        try client.publish(reply, "response");
    }
}
```

**Async with Future:**

```zig
// Launch async receive - runs in background
var future = sub.nextMessageAsync(allocator);
defer if (future.cancel(io)) |m| {
    if (m) |msg| msg.deinit(allocator);
} else |_| {};

// Do other work while waiting...

// Await result - DON'T call deinit here, defer handles cleanup!
// cancel() and await() are idempotent - they return the same result
if (try future.await(io)) |msg| {
    processMessage(msg);
}
// When scope exits, defer runs cancel() which frees the message
```

**Parallel operations:**

For true parallel async receives, use separate clients (each client has one
connection, so concurrent async receives on the same client would contend
for the same stream):

```zig
// Client A with its own connection
var io_a: std.Io.Threaded = .init(allocator, .{});
const client_a = try nats.Client.connect(allocator, io_a.io(), url, .{});
const sub_a = try client_a.subscribe(allocator, "events.a");

// Client B with its own connection
var io_b: std.Io.Threaded = .init(allocator, .{});
const client_b = try nats.Client.connect(allocator, io_b.io(), url, .{});
const sub_b = try client_b.subscribe(allocator, "events.b");

// Launch parallel receives (each on its own connection)
var future_a = sub_a.nextMessageAsync(allocator);
defer if (future_a.cancel(io_a.io())) |m| {
    if (m) |msg| msg.deinit(allocator);
} else |_| {};

var future_b = sub_b.nextMessageAsync(allocator);
defer if (future_b.cancel(io_b.io())) |m| {
    if (m) |msg| msg.deinit(allocator);
} else |_| {};

// Await both - defers handle cleanup
const msg_a = try future_a.await(io_a.io());
const msg_b = try future_b.await(io_b.io());
```

#### Unsubscribing

```zig
// Immediate unsubscribe
try sub.unsubscribe();

// Graceful drain (finish pending messages)
try sub.drain(allocator, .{ .timeout_ms = 5000 });

// Cancel (recommended - use with defer)
sub.cancel() catch {};
```

### Messages

```zig
pub const Message = struct {
    subject: []const u8,      // Message subject
    sid: u64,                 // Subscription ID
    reply_to: ?[]const u8,    // Reply inbox (for requests)
    data: []const u8,         // Message payload
    headers: ?[]const u8,     // NATS headers (if any)

    pub fn deinit(self: *const Message, allocator: Allocator) void;
};
```

**Zero-copy vs Owned:**

- Messages from `nextMessage()` are zero-copy slices into the read buffer
- Valid only until the next `poll()` or `nextMessage()` call
- Use `nextMessageOwned()` for messages that outlive the poll cycle

### Events

```zig
// Process client events
while (client.nextEvent()) |event| {
    switch (event) {
        .connected => |info| {
            std.debug.print("Connected to {s}\n", .{info.server_name});
        },
        .disconnected => |info| {
            std.debug.print("Disconnected: {s}\n", .{info.reason});
        },
        .message => |msg| {
            // Handle message
        },
        .server_error => |err| {
            std.debug.print("Server error: {s}\n", .{err});
        },
    }
}
```

## Design Philosophy

### Dual API: Sync and Async

This library provides both synchronous and asynchronous APIs:

**Synchronous (Go-style):**
- `nextMessage()` - blocks with optional timeout
- Simple, predictable control flow
- Good for single-threaded consumers

**Asynchronous (Future-based):**
- `nextMessageAsync()` - returns `std.Io.Future`
- `flushAsync()`, `requestAsync()` - async client operations
- Enables parallel operations
- Cancellation via `future.cancel(io)`
- Follows Zig 0.16 `io.async()` patterns

```zig
// Async flush
var flush_future = client.flushAsync();
defer flush_future.cancel(io) catch {};
try flush_future.await(io);

// Async request/reply
var req_future = client.requestAsync(allocator, "service", "data", 5000);
defer _ = req_future.cancel(io) catch {};
if (try req_future.await(io)) |reply| {
    std.debug.print("Reply: {s}\n", .{reply.data});
}
```

### Connection-Scoped Io

This client follows a connection-scoped pattern for `std.Io`:

1. **Io stored in Client** - Passed once at `connect()`, stored for lifetime
2. **Reader/Writer stored** - Preserves buffer state across method calls
3. **Never stores Allocator** - Allocator passed to methods that allocate

This design enables:
- Clean API without io parameters on every method
- Correct buffer state preservation (Reader/Writer store seek positions)
- Same code works with blocking, threaded, or evented I/O

### Memory Management

- **Slab allocator** for fast message allocation
- **Pre-allocated slots** for subscriptions (Tiger-style)
- **Zero-copy** when possible, owned copies when needed
- **No allocations** in hot paths (publish, poll)

### The Golden Patterns

**Sync - always defer cancel:**

```zig
const sub = try client.subscribe(allocator, subject);
defer sub.cancel() catch {};  // ALWAYS defer cancel
// ... use subscription ...
```

**Async - always defer cancel on futures:**

```zig
var future = sub.nextMessageAsync(allocator);
defer if (future.cancel(io)) |m| {
    if (m) |msg| msg.deinit(allocator);
} else |_| {};

// Use the result - DON'T call deinit() here!
// cancel() and await() are idempotent - return same result
if (try future.await(io)) |msg| {
    processMessage(msg);
}
// Defer handles cleanup when scope exits
```

This pattern works because:
- `cancel()` and `await()` are **idempotent** - both return the same result
- If the operation completed, `cancel()` returns the result (not an error)
- The defer frees the message regardless of how the scope exits
- Never call `msg.deinit()` after `await()` when using this pattern!

## Examples

See `src/examples/` for complete examples:

- `pubsub.zig` - Basic publish/subscribe
- `connect.zig` - Connection handling

Run examples:

```bash
# Start NATS server
nats-server

# Run pub/sub example
zig build run-pubsub

# Run connection example
zig build run-connect
```

## Building

```bash
# Build library
zig build

# Run unit tests
zig build test

# Run integration tests (requires nats-server)
zig build test-integration

# Run performance benchmarks
zig build perf-test --msgs=100000 --size=16B

# Format code
zig build fmt
```

## Benchmarks

Built-in benchmark tools:

```bash
# Publisher benchmark
zig-out/bin/bench-pub test.subject --msgs=100000 --size=128B

# Subscriber benchmark
zig-out/bin/bench-sub test.subject --msgs=100000
```

Compare with official NATS CLI:

```bash
nats bench pub test.subject --msgs=100000 --size=128
nats bench sub test.subject --msgs=100000
```

## Error Handling

All I/O operations return errors that should be handled:

```zig
client.publish(subject, data) catch |err| switch (err) {
    error.ConnectionClosed => {
        // Reconnect logic
    },
    error.InvalidSubject => {
        // Invalid subject format
    },
    else => return err,
};
```

Common errors:
- `error.ConnectionClosed` - Server disconnected
- `error.Timeout` - Operation timed out
- `error.InvalidSubject` - Malformed subject
- `error.MaxPayloadExceeded` - Message too large
- `error.Canceled` - Operation was cancelled

## Thread Safety

- Single `Client` instance is NOT thread-safe
- Use separate clients per thread, or synchronize access
- `Subscription` message queues are thread-safe for multi-consumer patterns

## Async Considerations

- Each client has **one connection** to the server
- Multiple async receives on same client poll the same connection stream
- For true parallel async receives, use **separate clients**
- `flushAsync()` and `requestAsync()` work on the same client (they don't
  compete for the receive buffer)

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Complete |
| Pub/Sub | Complete |
| Request/Reply | Complete |
| JetStream | Planned |
| Key-Value | Planned |
| Object Store | Planned |
| TLS | Planned |

## License

Apache 2.0

## Contributing

Contributions welcome! Please read CONTRIBUTING.md first.
