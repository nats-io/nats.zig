# nats.zig

Production-grade NATS client library for Zig 0.16+.

Built on `std.Io` for native async support with two client types:
- **Client** - Sync, poll-based for simple use cases
- **ClientAsync** - Background reader with `Io.Queue` per subscription

## Features

- Native Zig implementation (zero C dependencies)
- Built on `std.Io` for async-aware I/O
- Two client types: sync polling or async queues
- Zero-copy message handling
- Go-inspired API design
- Tiger-style pre-allocation for predictable performance
- Full cancellation support via `std.Io.Future`

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

### Sync Client (Simple Polling)

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Setup Io (Andrew Kelley pattern)
    var threaded: std.Io.Threaded = .init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    // Connect
    const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{});
    defer client.deinit(allocator);

    // Subscribe
    const sub = try client.subscribe(allocator, "greet.*");
    defer sub.deinit(allocator);

    // Publish
    try client.publish("greet.hello", "Hello, NATS!");
    try client.flush();

    // Poll for message (zero-copy)
    if (try client.pollDirect(allocator, 1000)) |msg| {
        std.debug.print("Received: {s}\n", .{msg.data});
        client.tossPending();  // Release buffer
    }
}
```

### Async Client (Multiple Subscriptions)

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main() !void {
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var threaded: std.Io.Threaded = .init(allocator);
    defer threaded.deinit();
    const io = threaded.io();

    // Connect (starts background reader automatically)
    const client = try nats.ClientAsync.connect(allocator, io, "nats://localhost:4222", .{});
    defer client.deinit(allocator);

    // Create multiple subscriptions
    const sub1 = try client.subscribe(allocator, "events.a");
    defer sub1.deinit(allocator);

    const sub2 = try client.subscribe(allocator, "events.b");
    defer sub2.deinit(allocator);

    // Publish test messages
    try client.publish("events.a", "Event A");
    try client.publish("events.b", "Event B");
    try client.flush();

    // Receive from each subscription (blocking)
    const msg1 = try sub1.next(io);
    defer msg1.deinit(allocator);
    std.debug.print("Sub1: {s}\n", .{msg1.data});

    const msg2 = try sub2.next(io);
    defer msg2.deinit(allocator);
    std.debug.print("Sub2: {s}\n", .{msg2.data});
}
```

## When to Use Which

| Scenario | Recommendation |
|----------|----------------|
| Single subscription, simple polling | **Client** |
| Multiple subscriptions, concurrent handling | **ClientAsync** |
| Request/reply with timeout | Both work, **ClientAsync** preferred |
| High-throughput pub/sub | **ClientAsync** |
| Low-latency, zero-copy receives | **Client** with `pollDirect()` |
| Background message processing | **ClientAsync** |

## Client (Sync) API

The sync client uses poll-based message retrieval. You explicitly call `poll()`
or `pollDirect()` to check for incoming messages.

### Connection

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .name = "my-app",           // Optional client name
    .verbose = false,           // Protocol verbosity
    .pedantic = false,          // Strict protocol checking
    .user = "user",             // Optional username
    .pass = "pass",             // Optional password
    .auth_token = "token",      // Optional auth token
    .connect_timeout_ns = 5_000_000_000,  // 5 second timeout
});
defer client.deinit(allocator);

// Check connection
if (client.isConnected()) { ... }
const info = client.getServerInfo();
```

### Publishing

```zig
// Simple publish
try client.publish("subject", "payload");

// Publish with reply-to
try client.publishRequest("subject", "reply.inbox", "payload");

// Flush pending writes
try client.flush();
try client.flushWithTimeout(5000);  // With timeout (ms)

// Ping server
try client.ping();
```

### Subscribing

```zig
// Simple subscription
const sub = try client.subscribe(allocator, "events.>");
defer sub.deinit(allocator);

// Queue subscription (load balancing)
const qsub = try client.subscribeQueue(allocator, "tasks.*", "workers");
defer qsub.deinit(allocator);

// Unsubscribe
try client.unsubscribe(allocator, sub);
```

### Receiving Messages

**Poll-based (routes to subscription queues):**

```zig
// Poll for messages (routes MSG to subscription queues)
while (try client.poll(allocator, 1000)) {
    // Messages routed to subscription.messages queue
}

// Get message from subscription queue
while (sub.messages.pop()) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Subject: {s}, Data: {s}\n", .{ msg.subject, msg.data });
}
```

**Zero-copy direct polling:**

```zig
// Poll directly - returns zero-copy slice into read buffer
if (try client.pollDirect(allocator, 1000)) |msg| {
    std.debug.print("Subject: {s}\n", .{msg.subject});
    std.debug.print("Data: {s}\n", .{msg.data});

    // IMPORTANT: Release buffer before next pollDirect
    client.tossPending();
}
```

### Request/Reply

```zig
// Send request and wait for reply (timeout in ms)
if (try client.request(allocator, "service.add", "1+2", 5000)) |reply| {
    std.debug.print("Result: {s}\n", .{reply.data});
    client.tossPending();  // Release buffer
}
```

### Async Variants

The sync Client provides async variants that return `std.Io.Future`:

```zig
// Async flush
var flush_future = client.flushAsync();
defer flush_future.cancel(io) catch {};
try flush_future.await(io);

// Async request/reply
var req_future = client.requestAsync(allocator, "service", "data", 5000);
defer if (req_future.cancel(io)) |m| {
    if (m) |msg| client.tossPending();
} else |_| {};

if (try req_future.await(io)) |reply| {
    std.debug.print("Reply: {s}\n", .{reply.data});
}
```

### Drain and Cleanup

```zig
// Graceful drain (unsubscribes all, flushes, closes)
try client.drain(allocator);

// Or just close
client.deinit(allocator);
```

## ClientAsync API

The async client runs a background reader task via `io.concurrent()` that
automatically routes incoming messages to per-subscription `Io.Queue` instances.

### Connection

```zig
const client = try nats.ClientAsync.connect(allocator, io, "nats://localhost:4222", .{
    .name = "my-app",
    .async_queue_size = 256,  // Per-subscription queue size
});
defer client.deinit(allocator);
// Background reader starts automatically
```

### Publishing

Same as sync Client:

```zig
try client.publish("subject", "payload");
try client.publishRequest("subject", "reply.inbox", "payload");
try client.flush();
```

### Subscribing

```zig
// Simple subscription (returns Sub with Io.Queue)
const sub = try client.subscribe(allocator, "events.>");
defer sub.deinit(allocator);

// Queue subscription
const qsub = try client.subscribeQueue(allocator, "tasks.*", "workers");
defer qsub.deinit(allocator);

// Unsubscribe
try sub.unsubscribe();
```

### Receiving Messages

**Blocking receive:**

```zig
// Blocks until message available (uses Io.Queue.getOne)
const msg = try sub.next(io);
defer msg.deinit(allocator);
std.debug.print("Received: {s}\n", .{msg.data});
```

**Non-blocking check:**

```zig
// Returns immediately, null if no message
if (sub.tryNext()) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Got: {s}\n", .{msg.data});
}
```

**With timeout:**

```zig
// Blocks up to timeout_ms, returns null on timeout
if (try sub.nextWithTimeout(allocator, 5000)) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Got: {s}\n", .{msg.data});
} else {
    std.debug.print("Timeout\n", .{});
}
```

### Request/Reply

```zig
// Send request and wait for reply
if (try client.request(allocator, "service.add", "1+2", 5000)) |reply| {
    defer reply.deinit(allocator);
    std.debug.print("Result: {s}\n", .{reply.data});
}
```

### Drain and Cleanup

```zig
// Graceful drain
try client.drain(allocator);

// Or just close
client.deinit(allocator);
```

## Async Patterns with std.Io

### The Golden Pattern

Always defer cancel when using `io.async()`:

```zig
var future = io.async(someFn, .{args});
defer future.cancel(io) catch {};  // ALWAYS defer cancel
const result = try future.await(io);
```

### Wrapping sub.next() in a Future

```zig
// Launch async receive
var future = io.async(nats.ClientAsync.Sub.next, .{ sub, io });

// Defer handles cleanup (cancel and await are idempotent)
defer if (future.cancel(io)) |msg| msg.deinit(allocator) else |_| {};

// Do other work while waiting...

// Await result
const msg = try future.await(io);
std.debug.print("Received: {s}\n", .{msg.data});
```

### Wait for First of Multiple Subscriptions

Use `io.select()` to wait for the first message from any subscription:

```zig
// Create futures for each subscription
var f1 = io.async(nats.ClientAsync.Sub.next, .{ sub1, io });
var f2 = io.async(nats.ClientAsync.Sub.next, .{ sub2, io });

// Wait for first to complete
const result = io.select(.{
    .sub1 = &f1,
    .sub2 = &f2,
}) catch |err| {
    // On error, cancel both
    if (f1.cancel(io)) |m| m.deinit(allocator) else |_| {}
    if (f2.cancel(io)) |m| m.deinit(allocator) else |_| {}
    return err;
};

switch (result) {
    .sub1 => |msg_result| {
        // sub1 won - cancel sub2
        if (f2.cancel(io)) |m| m.deinit(allocator) else |_| {}
        const msg = try msg_result;
        defer msg.deinit(allocator);
        std.debug.print("Sub1 received: {s}\n", .{msg.data});
    },
    .sub2 => |msg_result| {
        // sub2 won - cancel sub1
        if (f1.cancel(io)) |m| m.deinit(allocator) else |_| {}
        const msg = try msg_result;
        defer msg.deinit(allocator);
        std.debug.print("Sub2 received: {s}\n", .{msg.data});
    },
}
```

### Timeout with io.select()

```zig
fn sleepMs(io: std.Io, ms: u32) void {
    io.sleep(.fromMilliseconds(ms), .awake) catch {};
}

// Receive with custom timeout using select
var recv_future = io.async(nats.ClientAsync.Sub.next, .{ sub, io });
var timeout_future = io.async(sleepMs, .{ io, 5000 });

const result = io.select(.{
    .message = &recv_future,
    .timeout = &timeout_future,
}) catch |err| {
    _ = timeout_future.cancel(io);
    if (recv_future.cancel(io)) |m| m.deinit(allocator) else |_| {}
    return err;
};

switch (result) {
    .message => |msg_result| {
        _ = timeout_future.cancel(io);  // Cancel unused timeout
        const msg = try msg_result;
        defer msg.deinit(allocator);
        std.debug.print("Received: {s}\n", .{msg.data});
    },
    .timeout => |_| {
        // Cancel recv and clean up
        if (recv_future.cancel(io)) |m| m.deinit(allocator) else |_| {}
        std.debug.print("Timeout!\n", .{});
    },
}
```

## Architecture

### Client (Sync) - Poll-Based

```
User Code                          Client
    |                                |
    +-- client.poll() ------------->+
    |                               | reads socket
    |                               | parses MSG/HMSG
    |                               | routes to sub.messages queue
    |                               |
    +-- sub.messages.pop() -------->+ returns message
    |                                |
```

### ClientAsync - Background Reader

```
ClientAsync.connect()
    |
    +-- io.concurrent(readerTaskFn)
           |
           +-- [Background Task] ----+
                    |                 |
                    | loop:           |
                    |   read socket   |
                    |   parse command |
                    |   route MSG --> Io.Queue(sub1)
                    |   route MSG --> Io.Queue(sub2)
                    |   ...           |
                    +-----------------|

User Code                          Subscription
    |                                |
    +-- sub.next(io) -------------->+
    |                               | queue.getOne(io)  [blocks]
    |                               | <-- message from background reader
    +<-- msg -----------------------+
```

## Message Types

### Client.DirectMsg (zero-copy)

```zig
pub const DirectMsg = struct {
    subject: []const u8,      // Slice into read buffer
    sid: u64,                 // Subscription ID
    reply_to: ?[]const u8,    // Reply inbox (slice)
    data: []const u8,         // Payload (slice)
    headers: ?[]const u8,     // NATS headers (slice)
    consumed: usize,          // Bytes consumed
};
// Valid only until client.tossPending() or next pollDirect()
```

### ClientAsync.Message (owned)

```zig
pub const Message = struct {
    subject: []const u8,      // Owned copy
    sid: u64,
    reply_to: ?[]const u8,    // Owned copy
    data: []const u8,         // Owned copy
    headers: ?[]const u8,     // Owned copy
    owned: bool,

    pub fn deinit(self: *const Message, allocator: Allocator) void;
};
// Must call deinit() when done
```

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

```zig
client.publish(subject, data) catch |err| switch (err) {
    error.NotConnected => {
        // Connection lost
    },
    error.EncodingFailed => {
        // Protocol encoding error
    },
    else => return err,
};
```

Common errors:
- `error.NotConnected` - Not connected to server
- `error.ConnectionFailed` - Failed to connect
- `error.InvalidAddress` - Invalid server address
- `error.AuthorizationViolation` - Auth failed
- `error.ProtocolError` - Protocol parse error
- `error.TooManySubscriptions` - Subscription limit reached
- `error.Canceled` - Operation was cancelled

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Complete |
| Pub/Sub | Complete |
| Request/Reply | Complete |
| Client (Sync) | Complete |
| ClientAsync | Complete |
| JetStream | Planned |
| Key-Value | Planned |
| Object Store | Planned |
| TLS | Planned |

## License

Apache 2.0

## Contributing

Contributions welcome! Please read CONTRIBUTING.md first.
