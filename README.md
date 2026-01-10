# nats.zig

Production-grade NATS client for Zig 0.16+.

## Features

- Native Zig implementation (zero C dependencies)
- Built on `std.Io` for async-aware I/O
- Zero-copy message handling with `MessageRef`
- Inline routing architecture for optimal performance
- Full cancellation support via `std.Io.Future`
- Go-inspired API design

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
    var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Setup Io
    var threaded: std.Io.Threaded = .init(allocator, .{});
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

    // Receive message
    const msg = try sub.next(allocator, io);
    defer msg.deinit(allocator);
    std.debug.print("Received: {s}\n", .{msg.data});
}
```

## Connection

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .name = "my-app",              // Client name for identification
    .user = "user",                // Username for auth
    .pass = "pass",                // Password for auth
    .auth_token = "token",         // Auth token
    .connect_timeout_ns = 5_000_000_000,  // 5 second timeout
    .async_queue_size = 256,       // Per-subscription queue size
    .buffer_size = 2 * 1024 * 1024,  // Read/write buffer (2MB)
    .tcp_rcvbuf = 256 * 1024,      // TCP receive buffer hint
});
defer client.deinit(allocator);

// Check connection
if (client.isConnected()) { ... }
const info = client.getServerInfo();
```

## Publishing

```zig
// Simple publish
try client.publish("subject", "payload");

// Publish with reply-to
try client.publishRequest("subject", "reply.inbox", "payload");

// Flush pending writes
try client.flush();

// Ping server
try client.ping();
```

## Subscribing

```zig
// Simple subscription
const sub = try client.subscribe(allocator, "events.>");
defer sub.deinit(allocator);

// Queue subscription (load balancing)
const qsub = try client.subscribeQueue(allocator, "tasks.*", "workers");
defer qsub.deinit(allocator);

// Unsubscribe
try sub.unsubscribe();
```

## Receiving Messages

### Blocking Receive

```zig
// Blocks until message available
const msg = try sub.next(allocator, io);
defer msg.deinit(allocator);
std.debug.print("Subject: {s}, Data: {s}\n", .{ msg.subject, msg.data });
```

### With Timeout

```zig
// Returns null on timeout
if (try sub.nextWithTimeout(allocator, 5000)) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Received: {s}\n", .{msg.data});
} else {
    std.debug.print("Timeout\n", .{});
}
```

### Zero-Copy (Advanced)

```zig
// Returns MessageRef - slices borrow from read buffer
// Valid only until next read operation
if (try sub.nextRef(allocator, io)) |ref| {
    std.debug.print("Data: {s}\n", .{ref.data});
    // Convert to owned if needed
    const owned = try ref.toOwned(allocator);
    defer owned.deinit(allocator);
}
```

### Non-Blocking Check

```zig
// Returns immediately, null if no message
if (sub.tryNext()) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Got: {s}\n", .{msg.data});
}
```

## Message Delivery Semantics

### Inline Routing Architecture

This client uses **inline routing** for optimal performance. When a subscription
calls `next()`, it becomes the reader and routes messages to other subscriptions:

```
sub1.next(io)
      |
      +---> acquire read_mutex
      |
      +---> read from socket
      |
      +---> if msg.sid == sub1.sid:
      |         return message (zero-copy!)
      |
      +---> else:
                route to other_sub.queue
                continue reading
```

### Queue Behavior

| Scenario | Behavior |
|----------|----------|
| Reading subscription | Direct from socket (zero-copy, queue bypassed) |
| Other subscriptions | Messages routed to their `Io.Queue` |
| Queue full | Message dropped, `getDroppedCount()` incremented |

### Monitoring Dropped Messages

```zig
// Check if messages were dropped due to queue overflow
const dropped = sub.getDroppedCount();
if (dropped > 0) {
    std.debug.print("Warning: {d} messages dropped\n", .{dropped});
}
```

### Slow Consumer Handling

1. **TCP receive buffer** - Kernel buffers incoming data (configurable via `tcp_rcvbuf`)
2. **Subscription queues** - Messages for inactive subscriptions queue up to `async_queue_size`
3. **Server detection** - NATS server has slow-consumer detection and will disconnect

For high-throughput scenarios, increase `tcp_rcvbuf` and `async_queue_size`:

```zig
const client = try nats.Client.connect(allocator, io, url, .{
    .async_queue_size = 1024,      // Larger subscription queues
    .tcp_rcvbuf = 512 * 1024,      // 512KB TCP buffer
});
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
var future = io.async(nats.Subscription.next, .{ sub, allocator, io });
defer if (future.cancel(io)) |msg| msg.deinit(allocator) else |_| {};

const msg = try future.await(io);
std.debug.print("Received: {s}\n", .{msg.data});
```

### Timeout with io.select()

```zig
fn sleepMs(io_ctx: std.Io, ms: u32) void {
    io_ctx.sleep(.fromMilliseconds(ms), .awake) catch {};
}

var recv_future = io.async(nats.Subscription.next, .{ sub, allocator, io });
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
        _ = timeout_future.cancel(io);
        const msg = try msg_result;
        defer msg.deinit(allocator);
        std.debug.print("Received: {s}\n", .{msg.data});
    },
    .timeout => |_| {
        if (recv_future.cancel(io)) |m| m.deinit(allocator) else |_| {}
        std.debug.print("Timeout!\n", .{});
    },
}
```

### Concurrent Subscriptions

```zig
fn processEvents(io_ctx: std.Io, sub: *nats.Subscription, alloc: Allocator) void {
    while (true) {
        const msg = sub.next(alloc, io_ctx) catch break;
        defer msg.deinit(alloc);
        // Process message...
    }
}

// Spawn concurrent handlers
var task1 = try io.concurrent(processEvents, .{ io, sub1, allocator });
defer task1.cancel(io) catch {};

var task2 = try io.concurrent(processEvents, .{ io, sub2, allocator });
defer task2.cancel(io) catch {};

// Wait for both
_ = task1.await(io);
_ = task2.await(io);
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
- `error.AuthorizationViolation` - Auth failed
- `error.ProtocolError` - Protocol parse error
- `error.TooManySubscriptions` - Subscription limit reached (256 max)
- `error.Closed` - Connection closed
- `error.Canceled` - Operation was cancelled

## Building

```bash
# Build library
zig build

# Run unit tests
zig build test

# Run integration tests (requires nats-server)
zig build test-integration

# Run performance benchmarks
zig build perf-test -- --msgs=100000 --size=16B

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

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Complete |
| Pub/Sub | Complete |
| Request/Reply | Complete |
| Client | Complete |
| JetStream | Planned |
| Key-Value | Planned |
| Object Store | Planned |
| TLS | Planned |

## License

Apache 2.0

## Contributing

Contributions welcome! Please read CONTRIBUTING.md first.
