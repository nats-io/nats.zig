<p align="center">
  <img src="logo/logo.png" width="300">
</p>

# nats.zig

A [Zig](https://ziglang.org/) client for the [NATS messaging system](https://nats.io).

Native Zig. Built on `std.Io`.

## Requirements

- Zig 0.16.0-dev.2535 or later
- NATS server (for running examples and tests)

## Installation

```bash
zig fetch --save https://github.com/M64GitHub/nats.zig/archive/refs/tags/v0.1.0.tar.gz
```

Then in `build.zig`:

```zig
const nats_dep = b.dependency("nats", .{
    .target = target,
    .optimize = optimize,
});

const exe = b.addExecutable(.{
    .name = "my-app",
    .root_module = b.createModule(.{
        .root_source_file = b.path("src/main.zig"),
        .target = target,
        .optimize = optimize,
        .imports = &.{
            .{ .name = "nats", .module = nats_dep.module("nats") },
        },
    }),
});
b.installArtifact(exe);
```

## Quick Start

Zig does not have closures, so to pass state into a callback we use a
`MsgHandler` — a type-erased interface that pairs your handler struct
with its `onMessage` method:

```zig
const std = @import("std");
const nats = @import("nats");

const Handler = struct {
    pub fn onMessage(_: *@This(), msg: *const nats.Message) void {
        std.debug.print("Received: {s}\n", .{msg.data});
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

    var handler = Handler{};
    const sub = try client.subscribe(
        "greet.*",
        nats.MsgHandler.init(Handler, &handler),
    );
    defer sub.deinit();

    try client.publish("greet.hello", "Hello, NATS!");
    init.io.sleep(.fromSeconds(1), .awake) catch {};
}
```

When no state is needed, use `subscribeFn()` with a plain function:

```zig
const std = @import("std");
const nats = @import("nats");

pub fn main(init: std.process.Init) !void {
    const client = try nats.Client.connect(
        init.gpa,
        init.io,
        "nats://localhost:4222",
        .{},
    );
    defer client.deinit();

    const sub = try client.subscribeFn("greet.*", onMessage);
    defer sub.deinit();

    try client.publish("greet.hello", "Hello, NATS!");
    init.io.sleep(.fromSeconds(1), .awake) catch {};
}

fn onMessage(msg: *const nats.Message) void {
    std.debug.print("Received: {s}\n", .{msg.data});
}
```

## Examples

Run with `zig build run-<name>` (requires `nats-server` on localhost:4222).

| Example | Run | Description |
|---------|-----|-------------|
| simple | `run-simple` | Basic pub/sub - connect, `subscribeSync`, publish, receive |
| request_reply | `run-request-reply` | RPC pattern with automatic inbox handling |
| queue_groups | `run-queue-groups` | Load-balanced workers with `io.concurrent()` |
| polling_loop | `run-polling-loop` | Non-blocking `tryNext()` with priority scheduling |
| select | `run-select` | Race subscription against timeout with `io.select()` |
| batch_receiving | `run-batch-receiving` | `nextBatch()` for bulk receives, stats monitoring |
| reconnection | `run-reconnection` | Auto-reconnect, backoff, buffer during disconnect |
| events | `run-events` | EventHandler callbacks with external state |
| callback | `run-callback` | `subscribe()` and `subscribeFn()` callback subscriptions |
| request_reply_callback | `run-request-reply-callback` | Service responder via callback subscription |
| graceful_shutdown | `run-graceful-shutdown` | `drain()` lifecycle, pre-shutdown health checks |

Source: `src/examples/`

### NATS by Example

Ports of [natsbyexample.com](https://natsbyexample.com) examples.

| Example | Run |
|---------|-----|
| [Pub-Sub](doc/nats-by-example/messaging/pub-sub.zig) | `run-nbe-messaging-pub-sub` |
| [Request-Reply](doc/nats-by-example/messaging/request-reply.zig) | `run-nbe-messaging-request-reply` |
| [JSON](doc/nats-by-example/messaging/json.zig) | `run-nbe-messaging-json` |
| [Concurrent](doc/nats-by-example/messaging/concurrent.zig) | `run-nbe-messaging-concurrent` |
| [Multiple Subscriptions](doc/nats-by-example/messaging/iterating-multiple-subscriptions.zig) | `run-nbe-messaging-iterating-multiple-subscriptions` |
| [NKeys & JWTs](doc/nats-by-example/auth/nkeys-jwts.zig) | `run-nbe-auth-nkeys-jwts` |

---

## Memory Ownership

Messages returned by `next()`, `tryNext()`, and `nextWithTimeout()` are **owned**.
You **must** call `deinit()` to free memory:

```zig
const msg = try sub.next();
defer msg.deinit();

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
    headers: ?[]const u8,      // Raw NATS headers (use headers.parse())
};
```

---

## Publishing

### Auto-Flush Behavior

Messages are buffered and automatically flushed to the network:

```zig
// Write to buffer - auto-flushed by io_task
try client.publish("events.click", "button1");
try client.publish("events.click", "button2");
try client.publish("events.click", "button3");
```

**Buffer details:**
- Default size: 1MB (configurable via `writer_buffer_size` option)
- No allocator needed - `publish()` writes to a pre-allocated buffer
- Auto-flushes (via io_task background loop)
- Multiple rapid publishes are naturally batched for efficiency

### Confirmed Flush

For scenarios where you need confirmation that the server received your messages,
use `flush()`. It sends PING and waits for PONG (matches Go/C client behavior):

```zig
try client.publish("events.important", data);
try client.flush(5_000_000_000); // 5 second timeout
// Server has confirmed receipt of all buffered messages
```

**When to use:**
- Critical messages where delivery confirmation matters
- Before shutting down to ensure all messages were sent
- Synchronization points in your application

### When Does Data Hit the Network?

| Method | Network I/O |
|--------|-------------|
| `publish()` | Auto-flushed |
| `publishRequest()` | Auto-flushed |
| `publishWithHeaders()` | Auto-flushed |
| `publishRequestWithHeaders()` | Auto-flushed |
| `flushBuffer()` | Yes - sends buffer to socket immediately (used internally) |
| `flush()` | Yes - sends buffer + PING, waits for PONG |
| `request()` | Yes - flushes, waits for response |
| `requestWithHeaders()` | Yes - flushes, waits for response |

---

## Subscribing

### Subscribe (Callback)

Messages are dispatched automatically via callback.

**MsgHandler pattern** (handler struct with state):

```zig
const MyHandler = struct {
    counter: *u32,
    pub fn onMessage(self: *@This(), msg: *const nats.Message) void {
        self.counter.* += 1;
        std.debug.print("got: {s}\n", .{msg.data});
    }
};

var count: u32 = 0;
var handler = MyHandler{ .counter = &count };
const sub = try client.subscribe(
    "events.>",
    nats.MsgHandler.init(MyHandler, &handler),
);
defer sub.deinit();
```

**Plain function** (no state needed):

```zig
fn onAlert(msg: *const nats.Message) void {
    std.debug.print("alert: {s}\n", .{msg.data});
}

const sub = try client.subscribeFn(
    "alerts.>",
    onAlert,
);
defer sub.deinit();
```

**Queue group** (load balancing — only one subscriber in the group
receives each message):

```zig
const sub = try client.subscribeQueue(
    "tasks.*",
    "workers",
    handler,
);
```

| Method | Handler | Queue Group |
|--------|---------|-------------|
| `subscribe` | MsgHandler | No |
| `subscribeQueue` | MsgHandler | Yes |
| `subscribeFn` | plain fn | No |
| `subscribeFnQueue` | plain fn | Yes |

> **Warning:** Do not call `next()`, `tryNext()`, or other receive methods on
> a callback subscription. They assert `mode == .manual` and will trap.

### Subscribe Sync (Manual Receive)

For manual control over message receiving, use `subscribeSync()`. You call
`next()`, `tryNext()`, or `nextBatch()` yourself:

```zig
const sub = try client.subscribeSync("events.>");
defer sub.deinit();

// Wildcards:
// * matches single token: "events.*" matches "events.click" but not "events.user.login"
// > matches remainder: "events.>" matches "events.click" and "events.user.login"

while (true) {
    const msg = try sub.next();
    defer msg.deinit();
    std.debug.print("{s}: {s}\n", .{ msg.subject, msg.data });
}
```

**Queue group** variant:

```zig
const sub1 = try client.subscribeSyncQueue("tasks.*", "workers");
const sub2 = try client.subscribeSyncQueue("tasks.*", "workers");
// Message goes to either sub1 OR sub2, not both
```

### Subscription Registration

When subscribing, the SUB command is buffered and sent to the server asynchronously.
If you need to ensure the subscription is fully registered before publishing (especially
with separate publisher/subscriber clients), call `flush()` after subscribing:

```zig
const sub = try client.subscribeSync("events.>");
defer sub.deinit();

// Ensure subscription is registered on server before publishing
try client.flush(5_000_000_000);  // 5 second timeout

// Now safe to publish from another client
```

**When is this needed?**
- Multi-client scenarios where one client publishes and another subscribes
- Tests that need deterministic message delivery
- Any situation requiring subscription to be active before first publish

**When is this NOT needed?**
- Single client publishing to itself (same client does subscribe + publish)
- Using `request()` which handles synchronization internally

### Unsubscribing

**Zig deinit pattern (recommended):** Use `defer sub.deinit()` - it calls `unsubscribe()`
internally and handles errors gracefully:

```zig
const sub = try client.subscribeSync("events.>");
defer sub.deinit();  // Unsubscribes + frees memory

// ... use subscription ...
```

**Explicit unsubscribe:** For users who need to check if the server
received the UNSUB command, call `unsubscribe()` directly:

```zig
const sub = try client.subscribeSync("events.>");

// ... use subscription ...

// Explicit unsubscribe with error checking
sub.unsubscribe() catch |err| {
    std.log.warn("Unsubscribe failed: {}", .{err});
};
sub.deinit();  // Still needed to free memory
```

| Method | Returns | Purpose |
|--------|---------|---------|
| `sub.unsubscribe()` | `!void` | Sends UNSUB to server, removes from tracking |
| `sub.deinit()` | `void` | Calls unsubscribe (if needed) + frees memory |

**Note:** `unsubscribe()` is idempotent - calling it multiple times is safe.
`deinit()` always succeeds (errors are logged, not returned) making it safe for
`defer`.

### Receiving Messages

**Blocking:** `next()` blocks until a message arrives. For use in dedicated receiver loops:

```zig
while (true) {
    const msg = try sub.next();
    defer msg.deinit();  // ALWAYS defer deinit

    std.debug.print("Subject: {s}\n", .{msg.subject});
    std.debug.print("Data: {s}\n", .{msg.data});
    if (msg.reply_to) |rt| {
        std.debug.print("Reply-to: {s}\n", .{rt});
    }
}
```

**Non-Blocking:** `tryNext()` returns immediately. Use for event loops or polling:

```zig
// Process all available messages without waiting
while (sub.tryNext()) |msg| {
    defer msg.deinit();
    processMessage(msg);
}
// No more messages - continue with other work
```

**With Timeout:** `nextWithTimeout()` returns `null` on timeout:

```zig
if (try sub.nextWithTimeout(5000)) |msg| {
    defer msg.deinit();
    std.debug.print("Got: {s}\n", .{msg.data});
} else {
    std.debug.print("No message within 5 seconds\n", .{});
}
```

**Batch:** `nextBatch()` / `tryNextBatch()` receive multiple messages at once:

```zig
var buf: [64]Message = undefined;

// Blocking - waits for at least 1 message, returns up to 64
const count = try sub.nextBatch(io, &buf);
for (buf[0..count]) |*msg| {
    defer msg.deinit();
    processMessage(msg.*);
}

// Non-blocking - returns immediately with available messages
const available = sub.tryNextBatch(&buf);
for (buf[0..available]) |*msg| {
    defer msg.deinit();
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

### Subscription Control

**Auto-Unsubscribe:** Automatically unsubscribe after receiving a specific number of messages:

```zig
const sub = try client.subscribeSync("events.>");

// Auto-unsubscribe after 10 messages
try sub.autoUnsubscribe(10);

// Process messages (subscription closes after 10th)
while (sub.isValid()) {
    if (sub.tryNext()) |msg| {
        defer msg.deinit();
        processMessage(msg);
    }
}
```

**Statistics:**

```zig
// Messages waiting in queue
const pending = sub.pending();

// Messages delivered (only tracked if autoUnsubscribe was called)
const delivered = sub.delivered();

// Check if subscription is still valid
if (sub.isValid()) {
    // Can still receive messages
}
```

**Per-Subscription Drain:** Drain a single subscription while keeping others active:

```zig
try sub.drain();
// Subscription stops receiving new messages
// Already-queued messages can still be consumed
```

---

## Request/Reply

### Using `request()` (Recommended)

The simplest way - handles inbox creation, subscription, and timeout:

```zig
// Returns null on timeout
if (try client.request("math.double", "21", 5000)) |reply| {
    defer reply.deinit();
    std.debug.print("Result: {s}\n", .{reply.data});  // "42"
} else {
    std.debug.print("Service did not respond\n", .{});
}
```

### Building a Service

Respond to requests by publishing to the `reply_to` subject:

```zig
const service = try client.subscribeSync("math.double");
defer service.deinit();

while (true) {
    const req = try service.next();
    defer req.deinit();

    // Parse request
    const num = std.fmt.parseInt(i32, req.data, 10) catch 0;

    // Build response
    var buf: [32]u8 = undefined;
    const result = std.fmt.bufPrint(&buf, "{d}", .{num * 2}) catch "error";

    // Send reply (auto-flushed)
    if (req.reply_to) |reply_to| {
        try client.publish(reply_to, result);
    }
}
```

### Responding with `msg.respond()`

Convenience method for the request/reply pattern:

```zig
const msg = try sub.next();
defer msg.deinit();

// Respond using the message's reply_to (auto-flushed)
msg.respond(client, "response data") catch |err| {
    if (err == error.NoReplyTo) {
        // Message had no reply_to address
    }
};
```

### Manual Request/Reply Pattern

For more control, manage the inbox yourself:

```zig
// Create inbox subscription
const inbox = try client.newInbox();
defer allocator.free(inbox);

const reply_sub = try client.subscribeSync(inbox);
defer reply_sub.deinit();

// Send request with reply-to (auto-flushed)
try client.publishRequest("service", inbox, "request data");

// Wait for response with timeout
if (try reply_sub.nextWithTimeout(5000)) |reply| {
    defer reply.deinit();
    // Process reply
} else {
    // Timeout
}
```

### Check No-Responders Status

Detect when a request has no available responders (status 503):

```zig
const reply = try client.request("service.endpoint", "data", 1000);
if (reply) |msg| {
    defer msg.deinit();

    if (msg.isNoResponders()) {
        // No service available to handle request
        std.debug.print("No responders for request\n", .{});
    } else {
        // Normal response - check status code if needed
        if (msg.getStatus()) |status| {
            std.debug.print("Status: {d}\n", .{status});
        }
    }
}
```

---

## Headers

NATS headers allow attaching metadata to messages (similar to HTTP headers).
Headers are supported with NATS server 2.2+.

### Publishing with Headers

```zig
const nats = @import("nats");
const headers = nats.protocol.headers;

// Single header
const hdrs = [_]headers.Entry{
    .{ .key = "X-Request-Id", .value = "req-123" },
};
try client.publishWithHeaders("events.user", &hdrs, "user logged in");

// Multiple headers
const multi_hdrs = [_]headers.Entry{
    .{ .key = "Content-Type", .value = "application/json" },
    .{ .key = "X-Correlation-Id", .value = "corr-456" },
    .{ .key = "X-Timestamp", .value = "2026-01-21T10:30:00Z" },
};
try client.publishWithHeaders("events.order", &multi_hdrs, order_json);
```

### Publish with Headers and Reply-To

```zig
const hdrs = [_]headers.Entry{
    .{ .key = "X-Request-Id", .value = "req-789" },
};
try client.publishRequestWithHeaders("service.echo", "my.inbox", &hdrs, "ping");
```

### Request/Reply with Headers

```zig
const hdrs = [_]headers.Entry{
    .{ .key = headers.HeaderName.msg_id, .value = "unique-001" },
};

if (try client.requestWithHeaders("service.api", &hdrs, "data", 5000)) |reply| {
    defer reply.deinit();
    std.debug.print("Response: {s}\n", .{reply.data});
} else {
    std.debug.print("Timeout\n", .{});
}
```

### Receiving and Parsing Headers

```zig
const msg = try sub.next();
defer msg.deinit();

if (msg.headers) |raw_headers| {
    var parsed = headers.parse(allocator, raw_headers);
    defer parsed.deinit();  // MUST call deinit!

    if (parsed.err == null) {
        // Iterate all headers
        for (parsed.items()) |entry| {
            std.debug.print("{s}: {s}\n", .{ entry.key, entry.value });
        }

        // Lookup by name (case-insensitive)
        if (parsed.get("X-Request-Id")) |req_id| {
            std.debug.print("Request ID: {s}\n", .{req_id});
        }

        // Check for no-responders status
        if (parsed.isNoResponders()) {
            std.debug.print("No responders available\n", .{});
        }
    }
}
```

**Important**: `ParseResult` owns its data (copies strings to heap). This means
parsed headers remain valid even after `msg.deinit()` is called. Always call
`parsed.deinit()` to free memory.

### Well-Known Header Names

Use constants from `headers.HeaderName` for JetStream and NATS features:

```zig
const hdrs = [_]headers.Entry{
    // JetStream message deduplication
    .{ .key = headers.HeaderName.msg_id, .value = "unique-msg-001" },
    // Expected stream for publish
    .{ .key = headers.HeaderName.expected_stream, .value = "ORDERS" },
};
```

| Constant | Header Name | Purpose |
|----------|-------------|---------|
| `msg_id` | `Nats-Msg-Id` | JetStream deduplication |
| `expected_stream` | `Nats-Expected-Stream` | Verify target stream |
| `expected_last_msg_id` | `Nats-Expected-Last-Msg-Id` | Optimistic concurrency |
| `expected_last_seq` | `Nats-Expected-Last-Sequence` | Sequence verification |

### HeaderMap Builder

For programmatic header construction:

```zig
const nats = @import("nats");

var headers: nats.Client.HeaderMap = .{};
defer headers.deinit(allocator);

// Set headers (replaces existing)
try headers.set(allocator, "Content-Type", "application/json");
try headers.set(allocator, "X-Request-Id", "req-123");

// Add headers (allows multiple values for same key)
try headers.add(allocator, "X-Tag", "important");
try headers.add(allocator, "X-Tag", "urgent");

// Get values
if (headers.get("Content-Type")) |ct| {
    std.debug.print("Content-Type: {s}\n", .{ct});
}

// Get all values for a key
if (try headers.getAll(allocator, "X-Tag")) |tags| {
    defer allocator.free(tags);
    for (tags) |tag| {
        std.debug.print("Tag: {s}\n", .{tag});
    }
}

// Delete headers
headers.delete(allocator, "X-Tag");

// Publish with HeaderMap (auto-flushed)
try client.publishWithHeaderMap(allocator, "subject", &headers, "payload");
```

### Header Notes

- Header values can contain colons (URLs, timestamps work fine)
- Case-insensitive lookup for header names
- On parse error: `items()` returns empty slice, `get()` returns null

---

## Async Patterns with std.Io

### Cancellation Pattern

Always defer cancel when using `io.async()`:

```zig
var future = io.async(someFn, .{args});
defer future.cancel(io) catch {};  // defer cancel
const result = try future.await(io);
```

### Racing Operations with `io.select()`

Wait for the first of multiple operations to complete:

```zig
fn sleepMs(io_ctx: std.Io, ms: i64) void {
    io_ctx.sleep(.fromMilliseconds(ms), .awake) catch {};
}

var recv_future = io.async(nats.Client.Sub.next, .{sub});
var timeout_future = io.async(sleepMs, .{ io, 5000 });

const result = io.select(.{
    .message = &recv_future,
    .timeout = &timeout_future,
}) catch |err| {
    timeout_future.cancel(io);
    if (recv_future.cancel(io)) |m| m.deinit() else |_| {}
    return err;
};

switch (result) {
    .message => |msg_result| {
        timeout_future.cancel(io);
        const msg = try msg_result;
        defer msg.deinit();
        std.debug.print("Received: {s}\n", .{msg.data});
    },
    .timeout => |_| {
        if (recv_future.cancel(io)) |m| m.deinit() else |_| {}
        std.debug.print("Timeout!\n", .{});
    },
}
```

### Async Message Receive with Ownership

When using `io.async()` to receive messages, handle ownership carefully:

```zig
var future = io.async(nats.Client.Sub.next, .{sub});
defer if (future.cancel(io)) |m| m.deinit() else |_| {};

if (future.await(io)) |msg| {
    // Message ownership transferred - use it here
    // Do not add defer msg.deinit() - outer defer handles cleanup
    std.debug.print("Got: {s}\n", .{msg.data});
    return;  // outer defer runs, cancel() returns null
} else |err| {
    std.debug.print("Error: {}\n", .{err});
}
```

**Key points:**
- After `await()` succeeds, `cancel()` returns null (message already consumed)
- If function exits before `await()`, `cancel()` returns the pending message
- Adding a second `defer msg.deinit()` inside the if-block would cause double-free

### Io.Queue for Cross-Thread Communication

Use `Io.Queue(T)` for producer/consumer patterns across threads:

```zig
const WorkResult = struct {
    worker_id: u8,
    msg: nats.Message,

    fn deinit(self: WorkResult) void {
        self.msg.deinit();
    }
};

// Fixed-size buffer backing the queue
var queue_buf: [32]WorkResult = undefined;
var queue: Io.Queue(WorkResult) = .init(&queue_buf);

// Worker thread: push results
fn worker(io: Io, sub: *Sub, q: *Io.Queue(WorkResult)) void {
    while (true) {
        const msg = sub.next() catch return;
        q.putOne(io, .{ .worker_id = 1, .msg = msg }) catch return;
    }
}

// Main thread: consume results
while (true) {
    const result = queue.getOne(io) catch break;
    defer result.deinit();
    std.debug.print("Worker {d}: {s}\n", .{ result.worker_id, result.msg.data });
}
```

**Use cases:**
- Load-balanced workers reporting to main thread
- Aggregating results from `io.concurrent()` tasks
- Decoupling message producers from consumers

---

## Connections

### Connection Options

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    // Identity
    .name = "my-app",              // Client name (visible in server logs)

    // Buffers
    .reader_buffer_size = 256 * 1024,  // Read buffer (default 1MB)
    .writer_buffer_size = 256 * 1024,  // Write buffer (default 1MB)
    .sub_queue_size = 8192,            // Per-subscription queue size
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

    // Inbox prefix (for request/reply)
    .inbox_prefix = "_INBOX",      // Custom inbox prefix

    // Connection behavior
    .retry_on_failed_connect = false,     // Retry on initial failure
    .no_randomize = false,                // Don't randomize server order
    .ignore_discovered_servers = false,   // Only use explicit servers
    .drain_timeout_ms = 30_000,           // Default drain timeout
    .flush_timeout_ms = 10_000,           // Default flush timeout
});
```

### Event Callbacks

Handle connection lifecycle events using the `EventHandler` pattern - a type-safe,
Zig-idiomatic approach similar to `std.mem.Allocator`.

```zig
const MyHandler = struct {
    pub fn onConnect(self: *@This()) void {
        _ = self;
        std.log.info("Connected!", .{});
    }

    pub fn onDisconnect(self: *@This(), err: ?anyerror) void {
        _ = self;
        std.log.warn("Disconnected: {any}", .{err});
    }

    pub fn onReconnect(self: *@This()) void {
        _ = self;
        std.log.info("Reconnected!", .{});
    }
};

var handler = MyHandler{};
const client = try nats.Client.connect(allocator, io, url, .{
    .event_handler = nats.EventHandler.init(MyHandler, &handler),
});
```

**Accessing External State:** Handlers can reference external application state:

```zig
const AppState = struct {
    is_online: bool = false,
    reconnect_count: u32 = 0,
    last_error: ?anyerror = null,
};

const MyHandler = struct {
    app: *AppState,

    pub fn onConnect(self: *@This()) void {
        self.app.is_online = true;
    }

    pub fn onDisconnect(self: *@This(), err: ?anyerror) void {
        self.app.is_online = false;
        self.app.last_error = err;
    }

    pub fn onReconnect(self: *@This()) void {
        self.app.is_online = true;
        self.app.reconnect_count += 1;
    }
};

var app_state = AppState{};
var handler = MyHandler{ .app = &app_state };

const client = try nats.Client.connect(allocator, io, url, .{
    .event_handler = nats.EventHandler.init(MyHandler, &handler),
});
```

| Callback | When Fired |
|----------|------------|
| `onConnect()` | Initial connection established |
| `onDisconnect(?anyerror)` | Connection lost (error or clean close) |
| `onReconnect()` | Reconnection successful |
| `onClose()` | Connection permanently closed |
| `onError(anyerror)` | Async error (slow consumer, etc.) |
| `onLameDuck()` | Server entering shutdown mode |
| `onDiscoveredServers(u8)` | New server discovered in cluster |
| `onDraining()` | Drain process started |
| `onSubscriptionComplete(u64)` | Subscription drain finished (receives SID) |

All callbacks are **optional** - only implement the ones you need.

### Connection State

```zig
const State = @import("nats").connection.State;

const status = client.getStatus();
switch (status) {
    .connected => std.debug.print("Connected\n", .{}),
    .reconnecting => std.debug.print("Reconnecting...\n", .{}),
    .draining => std.debug.print("Draining\n", .{}),
    .closed => std.debug.print("Closed\n", .{}),
    else => {},
}

// Convenience checks
if (client.isClosed()) { /* permanently closed */ }
if (client.isDraining()) { /* draining subscriptions */ }
if (client.isReconnecting()) { /* attempting reconnect */ }

// Subscription count
const num_subs = client.numSubscriptions();
```

### Connection Information

```zig
// Server details (from INFO response)
if (client.getConnectedUrl()) |url| {
    std.debug.print("Connected to: {s}\n", .{url});
}
if (client.getConnectedServerId()) |id| {
    std.debug.print("Server ID: {s}\n", .{id});
}
if (client.getConnectedServerName()) |name| {
    std.debug.print("Server name: {s}\n", .{name});
}
if (client.getConnectedServerVersion()) |version| {
    std.debug.print("Server version: {s}\n", .{version});
}

// Payload and feature info
const max_payload = client.getMaxPayload();
const supports_headers = client.headersSupported();

// Server pool (for cluster connections)
const server_count = client.getServerCount();
for (0..server_count) |i| {
    if (client.getServerUrl(@intCast(i))) |url| {
        std.debug.print("Known server: {s}\n", .{url});
    }
}

// RTT measurement
const rtt_ns = try client.getRtt();
const rtt_ms = @as(f64, @floatFromInt(rtt_ns)) / 1_000_000.0;
std.debug.print("RTT: {d:.2}ms\n", .{rtt_ms});
```

### Connection Statistics

Monitor throughput and connection health:

```zig
const stats = client.getStats();
std.debug.print("Messages: in={d} out={d}\n", .{stats.msgs_in, stats.msgs_out});
std.debug.print("Bytes: in={d} out={d}\n", .{stats.bytes_in, stats.bytes_out});
std.debug.print("Reconnects: {d}\n", .{stats.reconnects});
```

| Field | Type | Description |
|-------|------|-------------|
| `msgs_in` | `u64` | Total messages received |
| `msgs_out` | `u64` | Total messages sent |
| `bytes_in` | `u64` | Total bytes received |
| `bytes_out` | `u64` | Total bytes sent |
| `reconnects` | `u32` | Number of reconnections |

### Connection Control

**Flush with Server Confirmation:**

```zig
// Sends PING and waits for PONG (confirms server received messages)
client.flush(5_000_000_000) catch |err| {
    if (err == error.Timeout) {
        std.debug.print("Flush timed out\n", .{});
    }
};
```

**Force Reconnect:**

```zig
try client.forceReconnect();
// Connection closes, io_task starts reconnection process
```

**Drain with Timeout:**

```zig
const result = client.drainTimeout(30_000_000_000) catch |err| {
    if (err == error.Timeout) {
        std.debug.print("Drain timed out\n", .{});
    }
    return err;
};
if (!result.isClean()) {
    std.debug.print("Drain had failures\n", .{});
}
```

### Handling Slow Consumers

When messages arrive faster than you process them, the queue fills up and messages are dropped:

```zig
while (true) {
    const msg = try sub.next();
    defer msg.deinit();

    // Check for dropped messages periodically
    const dropped = sub.getDroppedCount();
    if (dropped > 0) {
        std.log.warn("Dropped {d} messages - consumer too slow", .{dropped});
    }

    processMessage(msg);
}
```

**Tuning for High Throughput:**

```zig
const client = try nats.Client.connect(allocator, io, url, .{
    .sub_queue_size = 16384,          // Larger per-subscription queue
    .tcp_rcvbuf = 512 * 1024,         // 512KB TCP buffer
    .reader_buffer_size = 1024 * 1024, // 1MB read buffer
    .writer_buffer_size = 1024 * 1024, // 1MB write buffer
});
```

---

## Authentication

### Username/Password

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .user = "user",
    .pass = "pass",
});
```

### Token Authentication

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .auth_token = "my-secret-token",
});
```

### NKey Authentication

NKey authentication uses Ed25519 signatures for secure, password-less
authentication. NKeys are the recommended authentication method for production
NATS deployments.

**Using NKey Seed (Direct):**

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .nkey_seed = "SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY",
});
```

**Using NKey Seed File:**

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .nkey_seed_file = "/path/to/user.nk",
});
```

**Using Signing Callback (HSM/Hardware Keys):**

```zig
fn mySignCallback(nonce: []const u8, sig: *[64]u8) bool {
    // Sign nonce using HSM, hardware token, etc.
    return hsm.sign(nonce, sig);
}

const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .nkey_pubkey = "UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4",
    .nkey_sign_fn = &mySignCallback,
});
```

### JWT/Credentials Authentication

For NATS deployments using the account/user JWT model.

**Using Credentials File:**

```zig
const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{
    .creds_file = "/path/to/user.creds",
});
```

**Using Credentials Content:**

```zig
// From environment variable
const creds = std.posix.getenv("NATS_CREDS") orelse return error.MissingCreds;
const client = try nats.Client.connect(allocator, io, url, .{
    .creds = creds,
});

// Or embed at compile time
const client = try nats.Client.connect(allocator, io, url, .{
    .creds = @embedFile("user.creds"),
});
```

### NKey Generation & JWT Encoding

Generate NKey keypairs, encode JWTs, and format credentials files
programmatically. No allocator needed - all operations use
caller-provided stack buffers.

**Generate Keypairs:**

```zig
const nats = @import("nats");

// Generate operator, account, and user keypairs
var op_kp = nats.auth.KeyPair.generate(io, .operator);
defer op_kp.wipe();

var acct_kp = nats.auth.KeyPair.generate(io, .account);
defer acct_kp.wipe();

var user_kp = nats.auth.KeyPair.generate(io, .user);
defer user_kp.wipe();

// Get public key (base32-encoded, 56 chars)
var pk_buf: [56]u8 = undefined;
const pub_key = op_kp.publicKey(&pk_buf);  // "O..."

// Encode seed (base32-encoded, 58 chars)
var seed_buf: [58]u8 = undefined;
const seed = op_kp.encodeSeed(&seed_buf);  // "SO..."
```

**Encode JWTs:**

```zig
// Account JWT (signed by operator)
var acct_jwt_buf: [2048]u8 = undefined;
const acct_jwt = try nats.auth.jwt.encodeAccountClaims(
    &acct_jwt_buf,
    acct_pub,       // account public key (subject)
    "my-account",   // account name
    op_kp,          // operator keypair (signer)
    iat,            // issued-at (unix seconds)
    .{},            // AccountOptions (defaults: unlimited)
);

// User JWT with permissions (signed by account)
var user_jwt_buf: [2048]u8 = undefined;
const user_jwt = try nats.auth.jwt.encodeUserClaims(
    &user_jwt_buf,
    user_pub,       // user public key (subject)
    "my-user",      // user name
    acct_kp,        // account keypair (signer)
    iat,            // issued-at (unix seconds)
    .{
        .pub_allow = &.{"app.>"},
        .sub_allow = &.{ "app.>", "_INBOX.>" },
    },
);
```

**Format Credentials File:**

```zig
var creds_buf: [4096]u8 = undefined;
const creds = nats.auth.creds.format(
    &creds_buf,
    user_jwt,   // JWT string
    user_seed,  // NKey seed string
);
// creds contains the full .creds file content
```

**Account Options (limits):**

| Field | Default | Description |
|-------|---------|-------------|
| `subs` | `-1` | Max subscriptions (-1 = unlimited) |
| `conn` | `-1` | Max connections |
| `data` | `-1` | Max data bytes |
| `payload` | `-1` | Max message payload |
| `imports` | `-1` | Max imports |
| `exports` | `-1` | Max exports |
| `leaf` | `-1` | Max leaf node connections |
| `mem_storage` | `-1` | Max memory storage |
| `disk_storage` | `-1` | Max disk storage |
| `wildcards` | `true` | Allow wildcard subscriptions |

**User Options (permissions):**

| Field | Default | Description |
|-------|---------|-------------|
| `pub_allow` | `&.{}` | Subjects allowed to publish |
| `sub_allow` | `&.{}` | Subjects allowed to subscribe |
| `subs` | `-1` | Max subscriptions (-1 = unlimited) |
| `data` | `-1` | Max data bytes |
| `payload` | `-1` | Max message payload |

See the [NKeys & JWTs example](doc/nats-by-example/auth/nkeys-jwts.zig)
for a complete working example.

### TLS

**Enabling TLS:**

```zig
// 1. URL scheme (recommended)
const client = try nats.Client.connect(allocator, io, "tls://localhost:4443", .{});

// 2. Explicit option
const client = try nats.Client.connect(allocator, io, "nats://localhost:4443", .{
    .tls_required = true,
});

// 3. Automatic - if server requires TLS, client upgrades automatically
```

**TLS Options:**

```zig
const client = try nats.Client.connect(allocator, io, "tls://localhost:4443", .{
    // Server certificate verification (production)
    .tls_ca_file = "/path/to/ca.pem",

    // Skip verification (development only!)
    .tls_insecure_skip_verify = true,

    // TLS-first handshake (for TLS-terminating proxies)
    .tls_handshake_first = true,
});
```

**Mutual TLS (mTLS):**

```zig
const client = try nats.Client.connect(allocator, io, "tls://localhost:4443", .{
    .tls_ca_file = "/path/to/ca.pem",
    .tls_cert_file = "/path/to/client.pem",
    .tls_key_file = "/path/to/client-key.pem",
});
```

**Checking TLS Status:**

```zig
if (client.isTls()) {
    std.debug.print("Connection is encrypted\n", .{});
}
```

| Option | Type | Description |
|--------|------|-------------|
| `tls_required` | `bool` | Force TLS connection |
| `tls_ca_file` | `?[]const u8` | CA certificate file path (PEM) |
| `tls_cert_file` | `?[]const u8` | Client certificate for mTLS (PEM) |
| `tls_key_file` | `?[]const u8` | Client private key for mTLS (PEM) |
| `tls_insecure_skip_verify` | `bool` | Skip server certificate verification |
| `tls_handshake_first` | `bool` | TLS handshake before NATS protocol |

### Authentication Priority

When multiple auth options are set:

1. `creds_file` / `creds` - JWT + NKey from credentials
2. `nkey_seed` / `nkey_seed_file` - NKey only
3. `nkey_sign_fn` + `nkey_pubkey` - Custom signing
4. `user` / `pass` or `auth_token` - Basic auth

### Security Notes

- The library wipes seed data from memory after use (best effort)

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
| `ConnectionClosed` | Connection closed unexpectedly |
| `ConnectionTimeout` | Connection attempt timed out |
| `ConnectionRefused` | Server refused connection |
| `AuthenticationFailed` | Authentication failed |
| `PayloadTooLarge` | Message exceeds max_payload |
| `TooManySubscriptions` | Subscription limit reached (256) |
| `Closed` | Connection was closed |
| `Canceled` | Operation was cancelled |
| `Timeout` | Operation timed out |

---

## Server Compatibility

Verify the server meets minimum version requirements:

```zig
// Check for NATS 2.10.0 or later (required for some features)
if (client.checkCompatibility(2, 10, 0)) {
    // Server supports NATS 2.10+ features
} else {
    std.debug.print("Server version too old\n", .{});
}

// Get the actual version string
if (client.getConnectedServerVersion()) |version| {
    std.debug.print("Connected to NATS {s}\n", .{version});
}
```

---

## API Quick Reference

| Want to... | Method | Returns |
|------------|--------|---------|
| Publish message | `client.publish(subject, data)` | `!void` |
| Publish with reply-to | `client.publishRequest(subject, reply_to, data)` | `!void` |
| Flush buffer to socket | `client.flushBuffer()` | `!void` |
| Subscribe | `client.subscribeSync(subject)` | `!*Sub` |
| Queue subscribe (sync) | `client.subscribeSyncQueue(subject, group)` | `!*Sub` |
| Callback subscribe | `client.subscribe(subject, handler)` | `!*Sub` |
| Callback queue sub | `client.subscribeQueue(subject, group, handler)` | `!*Sub` |
| Callback fn sub | `client.subscribeFn(subject, fn)` | `!*Sub` |
| Callback fn queue sub | `client.subscribeFnQueue(subject, group, fn)` | `!*Sub` |
| Unsubscribe | `sub.unsubscribe()` | `!void` |
| Free subscription | `sub.deinit()` | `void` |
| Request/reply | `client.request(subject, data, timeout_ms)` | `!?Message` |
| Publish with headers | `client.publishWithHeaders(subject, hdrs, data)` | `!void` |
| Publish+reply+headers | `client.publishRequestWithHeaders(subject, reply, hdrs, data)` | `!void` |
| Request with headers | `client.requestWithHeaders(subject, hdrs, data, timeout_ms)` | `!?Message` |
| Receive (blocking) | `sub.next()` | `!Message` |
| Receive (non-blocking) | `sub.tryNext()` | `?Message` |
| Receive (with timeout) | `sub.nextWithTimeout(timeout_ms)` | `!?Message` |
| Batch receive | `sub.nextBatch(io, buf)` | `!usize` |
| Check drops | `sub.getDroppedCount()` | `u64` |
| Auto-unsubscribe | `sub.autoUnsubscribe(max_msgs)` | `!void` |
| Check pending | `sub.pending()` | `usize` |
| Check pending bytes | `sub.pendingBytes()` | `u64` |
| Check delivered | `sub.delivered()` | `u64` |
| Check valid | `sub.isValid()` | `bool` |
| Check sub draining | `sub.isDraining()` | `bool` |
| Get subscription ID | `sub.getSid()` | `u64` |
| Get subject | `sub.getSubject()` | `[]const u8` |
| Get queue group | `sub.getQueueGroup()` | `?[]const u8` |
| Set pending limits | `sub.setPendingLimits(msg_limit)` | `void` |
| Get pending limits | `sub.getPendingLimits()` | `usize` |
| Subscription stats | `sub.getSubStats()` | `SubStats` |
| Respond to message | `msg.respond(client, data)` | `!void` |
| Message size | `msg.size()` | `usize` |
| No-responders check | `msg.isNoResponders()` | `bool` |
| Status code | `msg.getStatus()` | `?u16` |
| Create inbox | `client.newInbox()` | `![]u8` |
| Connection status | `client.getStatus()` | `State` |
| Connection stats | `client.getStats()` | `Stats` |
| Server RTT | `client.getRtt()` | `!u64` |
| Server URL | `client.getConnectedUrl()` | `?[]const u8` |
| Server ID | `client.getConnectedServerId()` | `?[]const u8` |
| Flush confirmed | `client.flush(timeout_ns)` | `!void` |
| Force reconnect | `client.forceReconnect()` | `!void` |
| Generate NKey keypair | `auth.KeyPair.generate(io, key_type)` | `KeyPair` |
| Encode NKey seed | `kp.encodeSeed(&buf)` | `[]const u8` |
| Parse NKey seed | `auth.KeyPair.fromSeed(seed)` | `!KeyPair` |
| Get public key | `kp.publicKey(&buf)` | `[]const u8` |
| Wipe keypair | `kp.wipe()` | `void` |
| Encode account JWT | `auth.jwt.encodeAccountClaims(buf, sub, name, signer, iat, opts)` | `![]const u8` |
| Encode user JWT | `auth.jwt.encodeUserClaims(buf, sub, name, signer, iat, opts)` | `![]const u8` |
| Format credentials | `auth.creds.format(buf, jwt, seed)` | `[]const u8` |
| Parse credentials | `auth.creds.parse(content)` | `!Credentials` |

---

## Building

```bash
# Build library
zig build

# Run unit tests
zig build test

# Run integration tests (requires nats-server)
zig build test-integration

# Format code
zig build fmt
```
---

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Implemented |
| Pub/Sub | Implemented |
| Request/Reply | Implemented |
| Headers | Implemented |
| Reconnection | Implemented |
| Event Callbacks | Implemented |
| NKey Authentication | Implemented |
| JWT/Credentials | Implemented |
| TLS | Implemented |
| JetStream | Planned |
| Key-Value | Planned |
| Object Store | Planned |

## License

Apache 2.0

## Contributing

Contributions welcome!
