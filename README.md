[![CI](https://github.com/nats-io/nats.zig/actions/workflows/ci.yml/badge.svg)](https://github.com/nats-io/nats.zig/actions/workflows/ci.yml)
[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](LICENSE)
![Zig](https://img.shields.io/badge/Zig-0.16.0-orange)

<p align="center">
  <img src="logo/logo.png">
</p>

<p align="center">
    A <a href="https://www.ziglang.org/">Zig</a> client for the <a href="https://nats.io">NATS messaging system</a>.
</p>

# nats.zig

A [Zig](https://ziglang.org/) client for the [NATS messaging system](https://nats.io).

Built on `std.Io`.

> **Pre-1.0** - This library is under active development.
> Core pub/sub, server-authenticated TLS, JetStream (pull + push
> consumers), Key-Value Store, and the Micro Services API are
> supported and covered by integration tests. Object Store and
> mTLS are not yet implemented. The API may change before 1.0.

Check out [NATS by Example](https://natsbyexample.com) for
runnable, cross-client NATS examples. This repository includes
Zig ports in [doc/nats-by-example](doc/nats-by-example/README.md).

## Contents

- [Requirements](#requirements)
- [Documentation](#documentation)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
- [Memory Ownership](#memory-ownership)
- [Publishing](#publishing)
- [Subscribing](#subscribing)
- [Request/Reply](#requestreply)
- [Headers](#headers)
- [JetStream](#jetstream)
- [Micro Services](#micro-services)
- [Async Patterns with std.Io](#async-patterns-with-stdio)
- [Connections](#connections)
- [Authentication](#authentication)
- [Error Handling](#error-handling)
- [Server Compatibility](#server-compatibility)
- [Building](#building)
- [Status](#status)

## Documentation

- [Examples](src/examples/README.md) - runnable examples built by `zig build`
- [JetStream guide](doc/JetStream.md) - stream, consumer, publish,
  pull-consume, ack, and error-handling details
- [NATS by Example ports](doc/nats-by-example/README.md) - Zig ports of
  selected cross-client examples from natsbyexample.com
- [Integration tests](src/testing/README.md) - local test layout,
  fixtures, and focused test targets

## Requirements

- Zig 0.16.0 or later
- NATS server (for running examples and tests)

## Installation

```bash
zig fetch --save https://github.com/nats-io/nats.zig/archive/refs/tags/v0.1.0.tar.gz
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

Subscriptions use callbacks - messages are dispatched automatically,
no manual receive loop needed. There are three ways to subscribe:

**`subscribe()` with a MsgHandler** - captures state, like a closure:

```zig
const std = @import("std");
const nats = @import("nats");

// Handler struct captures external state via pointer
const Handler = struct {
    counter: *u32,
    pub fn onMessage(self: *@This(), msg: *const nats.Message) void {
        // Modify captured state from within the callback
        self.counter.* += 1;
        std.debug.print("[{d}] {s}\n", .{ self.counter.*, msg.data });
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

    // State lives in main - handler captures a pointer to it
    var count: u32 = 0;
    var handler = Handler{ .counter = &count };
    const sub = try client.subscribe(
        "greet.*",
        nats.MsgHandler.init(Handler, &handler),
    );
    defer sub.deinit();

    try client.publish("greet.hello", "Hello, NATS!");
    init.io.sleep(.fromSeconds(1), .awake) catch {};

    // Main sees the mutations made by the callback
    std.debug.print("Total: {d}\n", .{count});
}
```

**`subscribeFn()` with a plain function** - when no state is needed:

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

> **Note:** Callback messages are freed automatically after your handler
> returns. No `msg.deinit()` needed.

**`subscribeSync()` for manual receive** - you control the receive loop:

```zig
const sub = try client.subscribeSync("greet.*");
defer sub.deinit();

try client.publish("greet.hello", "Hello, NATS!");

if (try sub.nextMsgTimeout(5000)) |msg| {
    defer msg.deinit();
    std.debug.print("Received: {s}\n", .{msg.data});
}
```

See [Examples](#examples) below for more patterns including
request/reply, queue groups, headers, and async I/O.

## Examples

Run with `zig build run-<name>` (requires `nats-server` on localhost:4222).

| Example | Run | Description |
|---------|-----|-------------|
| simple | `run-simple` | Basic pub/sub - connect, `subscribeSync`, publish, receive |
| request_reply | `run-request-reply` | RPC pattern with automatic inbox handling |
| headers | `run-headers` | Publish, receive, and parse NATS headers |
| queue_groups | `run-queue-groups` | Load-balanced workers with `io.concurrent()` |
| polling_loop | `run-polling-loop` | Non-blocking `tryNextMsg()` with priority scheduling |
| select | `run-select` | Race subscription against timeout with `Io.Select` |
| batch_receiving | `run-batch-receiving` | `nextMsgBatch()` for bulk receives, stats monitoring |
| reconnection | `run-reconnection` | Auto-reconnect, backoff, buffer during disconnect |
| events | `run-events` | EventHandler callbacks with external state |
| callback | `run-callback` | `subscribe()` and `subscribeFn()` callback subscriptions |
| request_reply_callback | `run-request-reply-callback` | Service responder via callback subscription |
| graceful_shutdown | `run-graceful-shutdown` | `drain()` lifecycle, pre-shutdown health checks |
| jetstream_publish | `run-jetstream-publish` | Create a stream and publish with ack confirmation |
| jetstream_consume | `run-jetstream-consume` | Pull consumer fetch and acknowledgement |
| jetstream_push | `run-jetstream-push` | Push consumer callback delivery |
| jetstream_async_publish | `run-jetstream-async-publish` | Async JetStream publishing |
| kv | `run-kv` | Key-Value bucket operations |
| kv_watch | `run-kv-watch` | Watch Key-Value updates |
| micro_echo | `run-micro-echo` | NATS service API echo service |

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

Messages returned by `nextMsg()`, `tryNextMsg()`, and `nextMsgTimeout()` are **owned**.
You **must** call `deinit()` to free memory:

```zig
const msg = try sub.nextMsg();
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

**How it works:**
- `publish()` encodes into a lock-free ring buffer (no mutex)
- The io_task background thread drains the ring to the socket
- Multiple rapid publishes are naturally batched for efficiency
- Works at full speed even in tight loops (100K+ msgs/sec)
- Ring size: 2MB minimum (auto-sized, power-of-2)

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

**Queue group** (load balancing - only one subscriber in the group
receives each message):

```zig
const sub = try client.queueSubscribe(
    "tasks.*",
    "workers",
    handler,
);
```

| Method | Handler | Queue Group |
|--------|---------|-------------|
| `subscribe` | MsgHandler | No |
| `queueSubscribe` | MsgHandler | Yes |
| `subscribeFn` | plain fn | No |
| `queueSubscribeFn` | plain fn | Yes |

> **Warning:** Do not call `nextMsg()`, `tryNextMsg()`, or other receive methods on
> a callback subscription. They assert `mode == .manual` and will trap.

### Subscribe Sync (Manual Receive)

For manual control over message receiving, use `subscribeSync()`. You call
`nextMsg()`, `tryNextMsg()`, or `nextMsgBatch()` yourself:

```zig
const sub = try client.subscribeSync("events.>");
defer sub.deinit();

// Wildcards:
// * matches single token: "events.*" matches "events.click" but not "events.user.login"
// > matches remainder: "events.>" matches "events.click" and "events.user.login"

while (true) {
    const msg = try sub.nextMsg();
    defer msg.deinit();
    std.debug.print("{s}: {s}\n", .{ msg.subject, msg.data });
}
```

**Queue group** variant:

```zig
const sub1 = try client.queueSubscribeSync("tasks.*", "workers");
const sub2 = try client.queueSubscribeSync("tasks.*", "workers");
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

**Blocking:** `nextMsg()` blocks until a message arrives. For use in dedicated receiver loops:

```zig
while (true) {
    const msg = try sub.nextMsg();
    defer msg.deinit();  // ALWAYS defer deinit

    std.debug.print("Subject: {s}\n", .{msg.subject});
    std.debug.print("Data: {s}\n", .{msg.data});
    if (msg.reply_to) |rt| {
        std.debug.print("Reply-to: {s}\n", .{rt});
    }
}
```

**Non-Blocking:** `tryNextMsg()` returns immediately. Use for event loops or polling:

```zig
// Process all available messages without waiting
while (sub.tryNextMsg()) |msg| {
    defer msg.deinit();
    processMessage(msg);
}
// No more messages - continue with other work
```

**With Timeout:** `nextMsgTimeout()` returns `null` on timeout:

```zig
if (try sub.nextMsgTimeout(5000)) |msg| {
    defer msg.deinit();
    std.debug.print("Got: {s}\n", .{msg.data});
} else {
    std.debug.print("No message within 5 seconds\n", .{});
}
```

**Batch:** `nextMsgBatch()` / `tryNextMsgBatch()` receive multiple messages at once:

```zig
var buf: [64]Message = undefined;

// Blocking - waits for at least 1 message, returns up to 64
const count = try sub.nextMsgBatch(io, &buf);
for (buf[0..count]) |*msg| {
    defer msg.deinit();
    processMessage(msg.*);
}

// Non-blocking - returns immediately with available messages
const available = sub.tryNextMsgBatch(&buf);
for (buf[0..available]) |*msg| {
    defer msg.deinit();
    processMessage(msg.*);
}
```

### Receive Method Comparison

| Method | Blocks | Returns | Use Case |
|--------|--------|---------|----------|
| `nextMsg()` | Yes | `!Message` | Dedicated receiver loop |
| `tryNextMsg()` | No | `?Message` | Polling, event loops |
| `nextMsgTimeout()` | Yes (bounded) | `!?Message` | Request/reply, timed waits |
| `nextMsgBatch()` | Yes | `!usize` | High-throughput batching |
| `tryNextMsgBatch()` | No | `usize` | Drain queue without blocking |

### Subscription Control

**Auto-Unsubscribe:** Automatically unsubscribe after receiving a specific number of messages:

```zig
const sub = try client.subscribeSync("events.>");

// Auto-unsubscribe after 10 messages
try sub.autoUnsubscribe(10);

// Process messages (subscription closes after 10th)
while (sub.isValid()) {
    if (sub.tryNextMsg()) |msg| {
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
    const req = try service.nextMsg();
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
const msg = try sub.nextMsg();
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
if (try reply_sub.nextMsgTimeout(5000)) |reply| {
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
        if (msg.status()) |status| {
            std.debug.print("Status: {d}\n", .{status});
        }
    }
}
```

### Implementation Note: Response Multiplexer

`request()`, `requestMsg()`, and `requestWithHeaders()` use a shared
*response multiplexer* internally - the same pattern as the Go
client's `respMux`. The first call lazily subscribes once to a
wildcard inbox `_INBOX.<connNUID>.*` and does a PING/PONG round-trip
to confirm server registration. Every subsequent call reuses that
single subscription and just registers a per-request waiter in a
token-keyed map. The dispatcher routes incoming replies back to the
matching waiter.

Benefits over the naive per-request subscription approach:

- **No SUB/UNSUB protocol churn** - the server (and any clustered
  gateways/leaf nodes) sees one wildcard subscription per connection
  instead of one SUB+UNSUB pair per request.
- **No per-request allocations** for the subscription struct, queue
  buffer, or owned subject string.
- **No latency floor** - the old implementation burned a hardcoded
  5ms sleep on every request to give the server time to process the
  per-request SUB. The muxer pays one PING/PONG round-trip *once* on
  the first request and amortizes it to zero across subsequent calls.
- **Better concurrent throughput** - relevant for JetStream and KV
  workloads, which are RPC-heavy internally.

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
const msg = try sub.nextMsg();
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

var headers = nats.Client.HeaderMap.init(allocator);
defer headers.deinit();

// Set headers (replaces existing)
try headers.set("Content-Type", "application/json");
try headers.set("X-Request-Id", "req-123");

// Add headers (allows multiple values for same key)
try headers.add("X-Tag", "important");
try headers.add("X-Tag", "urgent");

// Get values
if (headers.get("Content-Type")) |ct| {
    std.debug.print("Content-Type: {s}\n", .{ct});
}

// Get all values for a key
if (try headers.getAll("X-Tag")) |tags| {
    defer allocator.free(tags);
    for (tags) |tag| {
        std.debug.print("Tag: {s}\n", .{tag});
    }
}

// Delete headers
headers.delete("X-Tag");

// Publish with HeaderMap (auto-flushed)
try client.publishWithHeaderMap("subject", &headers, "payload");
```

### Header Notes

- Header values can contain colons (URLs, timestamps work fine)
- Case-insensitive lookup for header names
- Header names must be non-empty and cannot contain whitespace, control
  characters, DEL, or `:`. Header values cannot contain control characters
  or DEL. Invalid headers return `error.InvalidHeader`.
- On parse error: `items()` returns empty slice, `get()` returns null

---

## JetStream

JetStream is NATS' persistence and streaming layer. It provides
at-least-once delivery, message replay, and durable consumers --
all through a JSON request/reply API on `$JS.API.*` subjects.

For runnable examples, see `src/examples/jetstream_*.zig`,
`src/examples/kv*.zig`, the focused [JetStream guide](doc/JetStream.md),
and the feature coverage summary below.

### JetStream Example

```zig
const nats = @import("nats");
const js_mod = nats.jetstream;

// Create a JetStream context (stack-allocated, no heap)
var js = try js_mod.JetStream.init(client, .{});

// Create a stream
var stream = try js.createStream(.{
    .name = "ORDERS",
    .subjects = &.{"orders.>"},
    .storage = .memory,
});
defer stream.deinit();

// Publish with ack confirmation
var ack = try js.publish("orders.new", "order-1");
defer ack.deinit();
// ack.value.seq, ack.value.stream

// Create a pull consumer and fetch messages
var cons = try js.createConsumer("ORDERS", .{
    .name = "processor",
    .durable_name = "processor",
    .ack_policy = .explicit,
});
defer cons.deinit();

var pull = js_mod.PullSubscription{
    .js = &js,
    .stream = "ORDERS",
};
try pull.setConsumer("processor");
var result = try pull.fetch(.{
    .max_messages = 10,
    .timeout_ms = 5000,
});
defer result.deinit();

for (result.messages) |*msg| {
    try msg.ack();
}
```

### Key-Value Store Example

```zig
const js_mod = nats.jetstream;

var js = try js_mod.JetStream.init(client, .{});

// Create a KV bucket
var kv = try js.createKeyValue(.{
    .bucket = "config",
    .storage = .memory,
    .history = 5,
});

// Put and get
const rev = try kv.put("db.host", "localhost:5432");
var entry = (try kv.get("db.host")).?;
defer entry.deinit();
// entry.revision == rev, entry.operation == .put

// Optimistic concurrency
const rev2 = try kv.update("db.host", "newhost:5432", rev);

// Create only if key doesn't exist
_ = try kv.create("db.port", "5432");
_ = kv.create("db.port", "9999") catch |err| {
    // err == error.ApiError (key exists)
};

// List all keys
const keys = try kv.keys(allocator);
defer {
    for (keys) |k| allocator.free(k);
    allocator.free(keys);
}

// Watch for real-time updates
var watcher = try kv.watchAll();
defer watcher.deinit();
while (try watcher.next(5000)) |*update| {
    defer update.deinit();
    // update.key, update.revision, update.operation
}
```

Bucket names and keys are validated client-side before API requests are sent.
Bucket names may not be empty, exceed 64 bytes, or contain wildcards,
separators, whitespace, control characters, or DEL. KV keys must be non-empty
NATS subject tokens without wildcards; watch patterns may use `*` and a
terminal `>`.

### Supported JetStream Features

| Area | Supported APIs | Notes |
|------|----------------|-------|
| Streams | `createStream()`, `updateStream()`, `deleteStream()`, `streamInfo()`, `purgeStream()`, `purgeStreamSubject()` | Includes stream listing and subject-filtered purge. |
| Consumers | `createConsumer()`, `updateConsumer()`, `deleteConsumer()`, `consumerInfo()` | Pull, push, and ordered consumer workflows. |
| Listing | `streamNames()`, `streams()`, `consumerNames()`, `consumers()`, `accountInfo()` | Paginated listing APIs are available for streams and consumers. |
| Publishing | `publish()`, `publishWithOpts()`, `publishMsg()` | Publish acknowledgments, deduplication headers, optimistic concurrency, and publish TTL. |
| Pull Consumers | `fetch()`, `fetchNoWait()`, `fetchBytes()`, `next()`, `messages()`, `consume()` | Batch fetch, single-message fetch, continuous pull iteration, callbacks, heartbeat monitoring, and ordered delivery. |
| Push Consumers | `createPushConsumer()`, `PushSubscription.consume()` | Callback delivery uses `JsMsgHandler`; callback messages are borrowed and valid only during the callback. |
| Acknowledgment | `ack()`, `doubleAck()`, `nak()`, `nakWithDelay()`, `inProgress()`, `term()`, `termWithReason()` | Metadata can be parsed from JetStream reply subjects. |
| Key-Value Store | `createKeyValue()`, `keyValue()`, `deleteKeyValue()`, `put()`, `get()`, `create()`, `update()`, `delete()`, `purge()`, `keys()`, `history()`, `watch()`, `watchAll()` | Bucket management, optimistic concurrency by revision, history, filtered key listing, and live watches. |
| Error Handling | `lastApiError()` | JetStream API errors expose server status, error code, and description. |
| Domains | `try JetStream.init(client, .{ .domain = ... })` | Supports multi-tenant JetStream domains. |

### Current Limitations

| Feature | Status |
|---------|--------|
| Object Store | Not implemented |

---

## Micro Services

The `nats.micro` module implements the NATS service API for
discoverable request/reply services. Services automatically register
monitoring endpoints under `$SRV.PING`, `$SRV.INFO`, and `$SRV.STATS`
including name- and id-specific variants.

```zig
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
        .description = "Echo service",
        .endpoint = .{
            .subject = "echo",
            .handler = nats.micro.Handler.init(Echo, &echo),
        },
    });
    defer service.deinit();

    while (true) {
        init.io.sleep(.fromSeconds(1), .awake) catch {};
    }
}
```

Handlers can be comptime vtable handlers with `Handler.init(T, &value)`
or plain functions with `Handler.fromFn(fn)`. A request handler can
read `req.subject()`, `req.data()`, `req.headers()`, and reply with
`req.respond()`, `req.respondJson()`, or `req.respondError()`.

Services support endpoint groups, queue groups, metadata, stats reset,
and graceful stop/drain:

```zig
var api = try service.addGroup("api");
_ = try api.addEndpoint(.{
    .subject = "v1.echo",
    .handler = nats.micro.Handler.init(Echo, &echo),
});

try service.stop(null);
try service.waitStopped();
```

Run the complete example with:

```bash
zig build run-micro-echo
```

---

## Async Patterns with std.Io

### Cancellation Pattern

Always defer cancel when using `io.async()`:

```zig
var future = io.async(someFn, .{args});
defer future.cancel(io) catch {};  // defer cancel
const result = try future.await(io);
```

### Racing Operations with `Io.Select`

Wait for the first of multiple operations to complete:

```zig
fn sleepMs(io_ctx: std.Io, ms: i64) void {
    io_ctx.sleep(.fromMilliseconds(ms), .awake) catch {};
}

const Sel = std.Io.Select(union(enum) {
    message: anyerror!nats.Message,
    timeout: void,
});
var buf: [2]Sel.Union = undefined;
var sel = Sel.init(io, &buf);
sel.async(.message, nats.Client.Sub.nextMsg, .{sub});
sel.async(.timeout, sleepMs, .{ io, 5000 });

const result = sel.await() catch |err| {
    while (sel.cancel()) |remaining| {
        switch (remaining) {
            .message => |r| {
                if (r) |m| m.deinit() else |_| {}
            },
            .timeout => {},
        }
    }
    return err;
};
while (sel.cancel()) |remaining| {
    switch (remaining) {
        .message => |r| {
            if (r) |m| m.deinit() else |_| {}
        },
        .timeout => {},
    }
}

switch (result) {
    .message => |msg_result| {
        const msg = try msg_result;
        defer msg.deinit();
        std.debug.print("Received: {s}\n", .{msg.data});
    },
    .timeout => {
        std.debug.print("Timeout!\n", .{});
    },
}
```

### Async Message Receive with Ownership

When using `io.async()` to receive messages, handle ownership carefully:

```zig
var future = io.async(nats.Client.Sub.nextMsg, .{sub});
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
        const msg = sub.nextMsg() catch return;
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
    .reader_buffer_size = 1024 * 1024 + 8 * 1024, // Read buffer default
    .writer_buffer_size = 1024 * 1024 + 8 * 1024, // Write buffer default
    .sub_queue_size = 8192,            // Per-subscription queue size
    .tcp_rcvbuf = 1024 * 1024,         // TCP receive buffer hint default

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

const status = client.status();
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
if (client.connectedUrl()) |url| {
    std.debug.print("Connected to: {s}\n", .{url});
}
if (client.connectedServerId()) |id| {
    std.debug.print("Server ID: {s}\n", .{id});
}
if (client.connectedServerName()) |name| {
    std.debug.print("Server name: {s}\n", .{name});
}
if (client.connectedServerVersion()) |version| {
    std.debug.print("Server version: {s}\n", .{version});
}

// Payload and feature info
const max_payload = client.maxPayload();
const supports_headers = client.headersSupported();

// Server pool (for cluster connections)
const server_count = client.serverCount();
for (0..server_count) |i| {
    if (client.serverUrl(@intCast(i))) |url| {
        std.debug.print("Known server: {s}\n", .{url});
    }
}

// RTT measurement
const rtt_ns = try client.rtt();
const rtt_ms = @as(f64, @floatFromInt(rtt_ns)) / 1_000_000.0;
std.debug.print("RTT: {d:.2}ms\n", .{rtt_ms});
```

### Connection Statistics

Monitor throughput and connection health:

```zig
const stats = client.stats();
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
| `connects` | `u32` | Total successful connections |

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
    const msg = try sub.nextMsg();
    defer msg.deinit();

    // Check for dropped messages periodically
    const dropped = sub.dropped();
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
    .reader_buffer_size = 2 * 1024 * 1024, // 2MB read buffer
    .writer_buffer_size = 2 * 1024 * 1024, // 2MB write buffer
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

**Mutual TLS (mTLS):** client certificates are planned but not
implemented yet. Setting `tls_cert_file` or `tls_key_file` currently
returns `error.MtlsNotImplemented`.

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
| `tls_cert_file` | `?[]const u8` | Reserved for mTLS; currently returns `error.MtlsNotImplemented` |
| `tls_key_file` | `?[]const u8` | Reserved for mTLS; currently returns `error.MtlsNotImplemented` |
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
| `TooManySubscriptions` | Subscription limit reached (16,384) |
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
if (client.connectedServerVersion()) |version| {
    std.debug.print("Connected to NATS {s}\n", .{version});
}
```

---

## Building

```bash
# Build library
zig build

# Run unit tests
zig build test

# Run integration tests (requires nats-server and nats CLI)
zig build test-integration

# Format code
zig build fmt
```

See [src/testing/README.md](src/testing/README.md) for integration test
layout, fixtures, and focused test targets.

---

## Status

| Component | Status |
|-----------|--------|
| Core Protocol | Supported |
| Pub/Sub | Supported |
| Request/Reply | Supported |
| Headers | Supported |
| Reconnection | Supported |
| Event Callbacks | Supported |
| NKey Authentication | Supported |
| JWT/Credentials | Supported |
| Server-authenticated TLS | Supported |
| mTLS client certificates | Planned |
| JetStream Core | Supported |
| JetStream Pull Consumers | Supported |
| JetStream Push Consumers | Supported |
| JetStream Ordered Consumer | Supported |
| Key-Value Store | Supported |
| Micro Services API | Supported |
| Object Store | Planned |
| Async Publish | Supported |

## Related Projects

Other Zig-based NATS implementations from the community:

- [NATS C client library, packaged for Zig](https://github.com/allyourcodebase/nats.c)
- [Zig language bindings to the NATS.c library](https://github.com/epicyclic-dev/nats-client)
- [Zig client for NATS Core and JetStream](https://github.com/g41797/nats)
- [A Zig client library for NATS, the cloud-native messaging system](https://github.com/lalinsky/nats.zig)
- [Minimal synchronous NATS Zig client](https://github.com/ianic/nats.zig)
- [Work-in-progress NATS library for Zig](https://github.com/rutgerbrf/zig-nats)

## License

Apache 2.0

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup,
test commands, and contribution guidelines.
