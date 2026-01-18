# nats.zig

Production-grade NATS client for Zig 0.16+.

## Features

- Native Zig implementation (zero C dependencies)
- Built on `std.Io` for async-aware I/O
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
    var threaded: std.Io.Threaded = .init(allocator, .{ .environ = .empty });
    defer threaded.deinit();
    const io = threaded.io();

    // Connect
    const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{});
    defer client.deinit(allocator);

    // Subscribe
    const sub = try client.subscribe(allocator, "greet.*");
    defer sub.deinit(allocator);

    // Publish (buffered)
    try client.publish("greet.hello", "Hello, NATS!");
    try client.flush(allocator);  // Send to network

    // Receive message
    const msg = try sub.next(allocator, io);
    defer msg.deinit(allocator);
    std.debug.print("Received: {s}\n", .{msg.data});
}
```

## API Quick Reference

| Want to... | Method | Returns |
|------------|--------|---------|
| Publish message | `client.publish(subject, data)` | `!void` |
| Publish with reply-to | `client.publishRequest(subject, reply_to, data)` | `!void` |
| Flush to network | `client.flush(allocator)` | `!void` |
| Subscribe | `client.subscribe(allocator, subject)` | `!*Sub` |
| Queue subscribe | `client.subscribeQueue(allocator, subject, group)` | `!*Sub` |
| Request/reply | `client.request(allocator, subject, data, timeout_ms)` | `!?Message` |
| Receive (blocking) | `sub.next(allocator, io)` | `!Message` |
| Receive (non-blocking) | `sub.tryNext()` | `?Message` |
| Receive (with timeout) | `sub.nextWithTimeout(allocator, timeout_ms)` | `!?Message` |
| Batch receive | `sub.nextBatch(io, buf)` | `!usize` |
| Check drops | `sub.getDroppedCount()` | `u64` |

---

## Publishing

### Buffered Writes

Messages are buffered in memory. They do NOT hit the network until you flush:

```zig
// Writes to buffer (fast, does not block on network)
try client.publish("events.click", "button1");
try client.publish("events.click", "button2");
try client.publish("events.click", "button3");

// Now send all buffered messages to server
try client.flush(allocator);  // Blocks until server confirms receipt
```

### Fire-and-Forget Pattern

For high-throughput scenarios, batch multiple publishes before flushing:

```zig
for (events) |event| {
    try client.publish("events", event);
}
try client.flush(allocator);  // Single network round-trip
```

### Publish with Reply-To

For request/reply patterns where you manage the inbox:

```zig
try client.publishRequest("service.echo", "my.inbox", "ping");
try client.flush(allocator);
```

### When Does Data Hit the Network?

| Method | Network I/O |
|--------|-------------|
| `publish()` | No - writes to buffer |
| `publishRequest()` | No - writes to buffer |
| `flush()` | Yes - sends buffer + PING, waits for PONG |
| `request()` | Yes - flushes, waits for response |

---

## Subscribing

### Simple Subscription

```zig
const sub = try client.subscribe(allocator, "events.>");
defer sub.deinit(allocator);

// Wildcards:
// * matches single token: "events.*" matches "events.click" but not "events.user.login"
// > matches remainder: "events.>" matches "events.click" and "events.user.login"
```

### Queue Groups (Load Balancing)

Distribute messages across workers - only one subscriber in the group receives each message:

```zig
// Worker 1
const sub1 = try client.subscribeQueue(allocator, "tasks.*", "workers");

// Worker 2 (different process)
const sub2 = try client.subscribeQueue(allocator, "tasks.*", "workers");

// Message goes to either sub1 OR sub2, not both
```

---

## Receiving Messages

### Blocking Receive: `next()`

Blocks until a message arrives. Use in dedicated receiver loops:

```zig
while (true) {
    const msg = try sub.next(allocator, io);
    defer msg.deinit(allocator);  // ALWAYS defer deinit

    std.debug.print("Subject: {s}\n", .{msg.subject});
    std.debug.print("Data: {s}\n", .{msg.data});
    if (msg.reply_to) |rt| {
        std.debug.print("Reply-to: {s}\n", .{rt});
    }
}
```

### Non-Blocking Poll: `tryNext()`

Returns immediately. Use for event loops or polling:

```zig
// Process all available messages without waiting
while (sub.tryNext()) |msg| {
    defer msg.deinit(allocator);
    processMessage(msg);
}
// No more messages - continue with other work
```

### Receive with Timeout: `nextWithTimeout()`

Returns `null` on timeout. Uses `io.select()` internally:

```zig
if (try sub.nextWithTimeout(allocator, 5000)) |msg| {
    defer msg.deinit(allocator);
    std.debug.print("Got: {s}\n", .{msg.data});
} else {
    std.debug.print("No message within 5 seconds\n", .{});
}
```

### Batch Receive: `nextBatch()` / `tryNextBatch()`

Receive multiple messages at once for efficiency:

```zig
var buf: [64]Message = undefined;

// Blocking - waits for at least 1 message, returns up to 64
const count = try sub.nextBatch(io, &buf);
for (buf[0..count]) |*msg| {
    defer msg.deinit(allocator);
    processMessage(msg.*);
}

// Non-blocking - returns immediately with available messages
const available = sub.tryNextBatch(&buf);
for (buf[0..available]) |*msg| {
    defer msg.deinit(allocator);
    processMessage(msg.*);
}
```

### Receive Method Comparison

| Method | Blocks | Returns | Use Case |
|--------|--------|---------|----------|
| `next()` | Yes | `!Message` | Dedicated receiver loop |
| `tryNext()` | No | `?Message` | Polling, event loops |
| `nextWithTimeout()` | Yes (bounded) | `!?Message` | Request/reply, timed waits |
| `nextBatch()` | Yes | `!usize` | High-throughput batching |
| `tryNextBatch()` | No | `usize` | Drain queue without blocking |

---

## Request/Reply

### Using `request()` (Recommended)

The simplest way - handles inbox creation, subscription, and timeout:

```zig
// Returns null on timeout
if (try client.request(allocator, "math.double", "21", 5000)) |reply| {
    defer reply.deinit(allocator);
    std.debug.print("Result: {s}\n", .{reply.data});  // "42"
} else {
    std.debug.print("Service did not respond\n", .{});
}
```

### Building a Service

Respond to requests by publishing to the `reply_to` subject:

```zig
const service = try client.subscribe(allocator, "math.double");
defer service.deinit(allocator);

while (true) {
    const req = try service.next(allocator, io);
    defer req.deinit(allocator);

    // Parse request
    const num = std.fmt.parseInt(i32, req.data, 10) catch 0;

    // Build response
    var buf: [32]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "{d}", .{num * 2}) catch "error";

    // Send reply
    if (req.reply_to) |reply_to| {
        try client.publish(reply_to, result);
        try client.flush(allocator);
    }
}
```

### Manual Request/Reply Pattern

For more control, manage the inbox yourself:

```zig
// Create inbox subscription
const inbox = try nats.newInbox(allocator, io);
defer allocator.free(inbox);

const reply_sub = try client.subscribe(allocator, inbox);
defer reply_sub.deinit(allocator);
try client.flush(allocator);  // Ensure subscription is active

// Send request with reply-to
try client.publishRequest("service", inbox, "request data");
try client.flush(allocator);

// Wait for response with timeout
if (try reply_sub.nextWithTimeout(allocator, 5000)) |reply| {
    defer reply.deinit(allocator);
    // Process reply
} else {
    // Timeout
}
```

---

## Async Patterns with std.Io

### The Golden Rule

Always defer cancel when using `io.async()`:

```zig
var future = io.async(someFn, .{args});
defer future.cancel(io) catch {};  // ALWAYS defer cancel
const result = try future.await(io);
```

### Concurrent Subscription Handlers

Run multiple subscriptions in parallel:

```zig
fn handleEvents(io_ctx: std.Io, sub: *nats.Client.Sub, alloc: Allocator) void {
    while (true) {
        const msg = sub.next(alloc, io_ctx) catch break;
        defer msg.deinit(alloc);
        // Process message...
    }
}

// Spawn concurrent handlers
var task1 = try io.concurrent(handleEvents, .{ io, events_sub, allocator });
defer task1.cancel(io) catch {};

var task2 = try io.concurrent(handleEvents, .{ io, orders_sub, allocator });
defer task2.cancel(io) catch {};

// Wait for both to complete
_ = task1.await(io);
_ = task2.await(io);
```

### Racing Operations with `io.select()`

Wait for the first of multiple operations to complete:

```zig
fn sleepMs(io_ctx: std.Io, ms: u32) void {
    io_ctx.sleep(.fromMilliseconds(ms), .awake) catch {};
}

var recv_future = io.async(nats.Client.Subscription.next, .{ sub, allocator, io });
var timeout_future = io.async(sleepMs, .{ io, 5000 });

const result = io.select(.{
    .message = &recv_future,
    .timeout = &timeout_future,
}) catch |err| {
    timeout_future.cancel(io);
    if (recv_future.cancel(io)) |m| m.deinit(allocator) else |_| {}
    return err;
};

switch (result) {
    .message => |msg_result| {
        timeout_future.cancel(io);
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

---

## Connection Options

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    // Identity
    .name = "my-app",              // Client name (visible in server logs)

    // Authentication
    .user = "user",                // Username
    .pass = "pass",                // Password
    .auth_token = "token",         // Token auth (alternative to user/pass)

    // Buffers
    .buffer_size = 256 * 1024,     // Read/write buffer (default 256KB)
    .async_queue_size = 256,       // Per-subscription queue size
    .tcp_rcvbuf = 256 * 1024,      // TCP receive buffer hint

    // Timeouts
    .connect_timeout_ns = 5_000_000_000,  // 5 second connect timeout

    // Reconnection
    .reconnect = true,             // Enable auto-reconnect
    .max_reconnect_attempts = 60,  // Max attempts (0 = infinite)
    .reconnect_wait_ms = 2000,     // Initial backoff

    // Keepalive
    .ping_interval_ms = 120_000,   // PING every 2 minutes
    .max_pings_outstanding = 2,    // Disconnect after 2 missed PONGs
});
```

---

## Message Lifecycle

### Memory Ownership

Messages returned by `next()`, `tryNext()`, and `nextWithTimeout()` are **owned**.
You **must** call `deinit()` to free memory:

```zig
const msg = try sub.next(allocator, io);
defer msg.deinit(allocator);  // ALWAYS do this

// Access message fields (valid until deinit)
std.debug.print("Subject: {s}\n", .{msg.subject});
std.debug.print("Data: {s}\n", .{msg.data});
```

### Message Structure

```zig
pub const Message = struct {
    subject: []const u8,       // Message subject
    sid: u64,                  // Subscription ID
    reply_to: ?[]const u8,     // Reply-to address (for request/reply)
    data: []const u8,          // Message payload
    headers: ?[]const u8,      // NATS headers (if enabled)
};
```

---

## Handling Slow Consumers

### Queue Overflow Detection

When messages arrive faster than you process them, the queue fills up and messages are dropped:

```zig
while (true) {
    const msg = try sub.next(allocator, io);
    defer msg.deinit(allocator);

    // Check for dropped messages periodically
    const dropped = sub.getDroppedCount();
    if (dropped > 0) {
        std.log.warn("Dropped {d} messages - consumer too slow", .{dropped});
    }

    processMessage(msg);
}
```

### Tuning for High Throughput

```zig
const client = try nats.Client.connect(allocator, io, url, .{
    .async_queue_size = 1024,      // Larger per-subscription queue
    .tcp_rcvbuf = 512 * 1024,      // 512KB TCP buffer
    .buffer_size = 1024 * 1024,    // 1MB read/write buffer
});
```

---

## Error Handling

```zig
client.publish(subject, data) catch |err| switch (err) {
    error.NotConnected => {
        // Connection lost - wait for reconnect or handle
    },
    error.PayloadTooLarge => {
        // Message exceeds server max_payload (usually 1MB)
    },
    error.EncodingFailed => {
        // Protocol encoding error
    },
    else => return err,
};
```

### Common Errors

| Error | Meaning |
|-------|---------|
| `NotConnected` | Not connected to server |
| `ConnectionFailed` | Failed to establish connection |
| `AuthorizationViolation` | Authentication failed |
| `PayloadTooLarge` | Message exceeds max_payload |
| `TooManySubscriptions` | Subscription limit reached (256) |
| `Closed` | Connection was closed |
| `Canceled` | Operation was cancelled |
| `Timeout` | Operation timed out |

---

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

---

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Complete |
| Pub/Sub | Complete |
| Request/Reply | Complete |
| Reconnection | Complete |
| JetStream | Planned |
| Key-Value | Planned |
| Object Store | Planned |
| TLS | Planned |

## License

Apache 2.0

## Contributing

Contributions welcome! Please read CONTRIBUTING.md first.
