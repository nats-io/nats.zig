# JetStream Guide for nats.zig

JetStream is NATS' persistence and streaming layer. It provides
at-least-once delivery, message replay, and durable consumers --
all through a JSON request/reply API layered on core NATS. No new
wire protocol; everything goes through `$JS.API.*` subjects.

This guide covers the nats.zig JetStream API with side-by-side
Go comparisons for developers familiar with nats.go.

It is a focused companion to the comprehensive root
[README](../README.md). For Key-Value Store coverage, see the
README JetStream section and `src/examples/kv*.zig`.

## Table of Contents

- [Quick Start](#quick-start)
- [JetStream Context](#jetstream-context)
- [Streams](#streams)
- [Consumers](#consumers)
- [Publishing](#publishing)
- [Pull Subscription](#pull-subscription)
- [Message Acknowledgment](#message-acknowledgment)
- [Error Handling](#error-handling)
- [Response Ownership](#response-ownership)
- [Type Reference](#type-reference)

---

## Quick Start

A complete example: create a stream, publish a message, create a
consumer, fetch the message, and acknowledge it.

**Zig:**

```zig
const nats = @import("nats");
const js_mod = nats.jetstream;

// Assumes `client` is already connected
var js = try js_mod.JetStream.init(client, .{});

// Create a stream
var stream = try js.createStream(.{
    .name = "ORDERS",
    .subjects = &.{"orders.>"},
    .storage = .memory,
});
defer stream.deinit();

// Publish a message
var ack = try js.publish("orders.new", "order-1");
defer ack.deinit();
// ack.value.seq == 1, ack.value.stream == "ORDERS"

// Create a consumer
var cons = try js.createConsumer("ORDERS", .{
    .name = "processor",
    .durable_name = "processor",
    .ack_policy = .explicit,
});
defer cons.deinit();

// Fetch messages
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
    // msg.data() returns the payload
    try msg.ack();
}
```

**Go:**

```go
js, _ := jetstream.New(nc)

// Create a stream
stream, _ := js.CreateStream(ctx, jetstream.StreamConfig{
    Name:     "ORDERS",
    Subjects: []string{"orders.>"},
    Storage:  jetstream.MemoryStorage,
})

// Publish a message
ack, _ := js.Publish(ctx, "orders.new", []byte("order-1"))
// ack.Stream == "ORDERS", ack.Sequence == 1

// Create a consumer
cons, _ := js.CreateConsumer(ctx, "ORDERS",
    jetstream.ConsumerConfig{
        Durable:   "processor",
        AckPolicy: jetstream.AckExplicitPolicy,
    })

// Fetch messages
batch, _ := cons.Fetch(10)
for msg := range batch.Messages() {
    msg.Ack()
}
```

---

## JetStream Context

The JetStream context is a lightweight struct (stack-allocated) that
holds a pointer to the NATS client, the API prefix, and timeout
settings. No heap allocation is needed. `JetStream.init()` is fallible because
it validates the API prefix or domain before storing it in the fixed-size
context buffer.

### Creating a Context

**Zig:**

```zig
const js_mod = nats.jetstream;

// Default settings
var js = try js_mod.JetStream.init(client, .{});

// Custom timeout
var js2 = try js_mod.JetStream.init(client, .{
    .timeout_ms = 10000,
});

// With domain (multi-tenant)
var js3 = try js_mod.JetStream.init(client, .{
    .domain = "hub",
});
// API prefix becomes: $JS.hub.API.
```

Stream, consumer, domain, and API-prefix names are validated at runtime.
Invalid names return `error.InvalidName`, `error.InvalidApiPrefix`, or
`error.NameTooLong` instead of relying on debug-only assertions.

**Go:**

```go
js, _ := jetstream.New(nc)

// With domain
js, _ = jetstream.NewWithDomain(nc, "hub")
```

### Options

| Field | Zig | Go | Default |
|-------|-----|-----|---------|
| API prefix | `.api_prefix` | `APIPrefix` | `$JS.API.` |
| Timeout | `.timeout_ms` | `DefaultTimeout` | 5000ms |
| Domain | `.domain` | via `NewWithDomain()` | none |

---

## Streams

Streams capture messages published to matching subjects.

### Create a Stream

**Zig:**

```zig
var resp = try js.createStream(.{
    .name = "EVENTS",
    .subjects = &.{"events.>"},
    .retention = .limits,
    .storage = .file,
    .max_msgs = 100000,
    .max_bytes = 1073741824, // 1GB
});
defer resp.deinit();

const info = resp.value;
// info.config.?.name == "EVENTS"
// info.state.?.messages == 0
```

**Go:**

```go
stream, _ := js.CreateStream(ctx, jetstream.StreamConfig{
    Name:      "EVENTS",
    Subjects:  []string{"events.>"},
    Retention: jetstream.LimitsPolicy,
    Storage:   jetstream.FileStorage,
    MaxMsgs:   100000,
    MaxBytes:  1073741824,
})
info, _ := stream.Info(ctx)
```

### Get Stream Info

**Zig:**

```zig
var info = try js.streamInfo("EVENTS");
defer info.deinit();

if (info.value.state) |state| {
    // state.messages, state.bytes, state.first_seq,
    // state.last_seq, state.consumer_count
}
```

**Go:**

```go
stream, _ := js.Stream(ctx, "EVENTS")
info, _ := stream.Info(ctx)
// info.State.Msgs, info.State.Bytes, etc.
```

### Update a Stream

**Zig:**

```zig
var resp = try js.updateStream(.{
    .name = "EVENTS",
    .subjects = &.{ "events.>", "logs.>" },
    .max_msgs = 200000,
});
defer resp.deinit();
```

**Go:**

```go
stream, _ := js.UpdateStream(ctx, jetstream.StreamConfig{
    Name:     "EVENTS",
    Subjects: []string{"events.>", "logs.>"},
    MaxMsgs:  200000,
})
```

### Purge a Stream

**Zig:**

```zig
var resp = try js.purgeStream("EVENTS");
defer resp.deinit();
// resp.value.purged == number of messages removed
```

**Go:**

```go
stream, _ := js.Stream(ctx, "EVENTS")
_ = stream.Purge(ctx)
```

### Delete a Stream

**Zig:**

```zig
var resp = try js.deleteStream("EVENTS");
defer resp.deinit();
// resp.value.success == true
```

**Go:**

```go
_ = js.DeleteStream(ctx, "EVENTS")
```

### StreamConfig Reference

| Field | Type | Description |
|-------|------|-------------|
| `name` | `[]const u8` | Stream name (required) |
| `subjects` | `?[]const []const u8` | Subjects to capture |
| `retention` | `?RetentionPolicy` | limits, interest, workqueue |
| `storage` | `?StorageType` | file, memory |
| `max_msgs` | `?i64` | Max messages in stream |
| `max_bytes` | `?i64` | Max total bytes |
| `max_age` | `?i64` | Max message age (nanoseconds) |
| `max_msg_size` | `?i32` | Max single message size |
| `max_msgs_per_subject` | `?i64` | Per-subject limit |
| `max_consumers` | `?i64` | Max consumers |
| `num_replicas` | `?i32` | Replica count |
| `discard` | `?DiscardPolicy` | old, new |
| `duplicate_window` | `?i64` | Dedup window (nanoseconds) |
| `no_ack` | `?bool` | Disable publish acks |
| `compression` | `?StoreCompression` | none, s2 |

All optional fields default to `null` and are omitted from the
JSON request (server applies its own defaults).

---

## Consumers

Consumers track read position in a stream and manage message
delivery.

### Create a Consumer

**Zig:**

```zig
var resp = try js.createConsumer("EVENTS", .{
    .name = "my-worker",
    .durable_name = "my-worker",
    .ack_policy = .explicit,
    .deliver_policy = .all,
    .filter_subject = "events.orders.>",
    .max_ack_pending = 1000,
});
defer resp.deinit();

if (resp.value.name) |name| {
    // name == "my-worker"
}
```

**Go:**

```go
cons, _ := js.CreateConsumer(ctx, "EVENTS",
    jetstream.ConsumerConfig{
        Durable:       "my-worker",
        AckPolicy:     jetstream.AckExplicitPolicy,
        DeliverPolicy: jetstream.DeliverAllPolicy,
        FilterSubject: "events.orders.>",
        MaxAckPending: 1000,
    })
```

### Get Consumer Info

**Zig:**

```zig
var info = try js.consumerInfo("EVENTS", "my-worker");
defer info.deinit();
// info.value.num_pending -- messages waiting
// info.value.num_ack_pending -- delivered but unacked
```

**Go:**

```go
cons, _ := js.Consumer(ctx, "EVENTS", "my-worker")
info, _ := cons.Info(ctx)
```

### Update a Consumer

**Zig:**

```zig
var resp = try js.updateConsumer("EVENTS", .{
    .name = "my-worker",
    .durable_name = "my-worker",
    .ack_policy = .explicit,
    .max_ack_pending = 2000,
});
defer resp.deinit();
```

**Go:**

```go
cons, _ := js.UpdateConsumer(ctx, "EVENTS",
    jetstream.ConsumerConfig{
        Durable:       "my-worker",
        AckPolicy:     jetstream.AckExplicitPolicy,
        MaxAckPending: 2000,
    })
```

### Delete a Consumer

**Zig:**

```zig
var resp = try js.deleteConsumer("EVENTS", "my-worker");
defer resp.deinit();
// resp.value.success == true
```

**Go:**

```go
_ = js.DeleteConsumer(ctx, "EVENTS", "my-worker")
```

### ConsumerConfig Reference

| Field | Type | Description |
|-------|------|-------------|
| `name` | `?[]const u8` | Consumer name |
| `durable_name` | `?[]const u8` | Durable name (survives restarts) |
| `ack_policy` | `?AckPolicy` | none, all, explicit |
| `deliver_policy` | `?DeliverPolicy` | all, last, new, ... |
| `ack_wait` | `?i64` | Ack timeout (nanoseconds) |
| `max_deliver` | `?i64` | Max redelivery attempts |
| `filter_subject` | `?[]const u8` | Subject filter |
| `filter_subjects` | `?[]const []const u8` | Multiple filters |
| `replay_policy` | `?ReplayPolicy` | instant, original |
| `max_waiting` | `?i64` | Max pull requests waiting |
| `max_ack_pending` | `?i64` | Max unacked messages |
| `inactive_threshold` | `?i64` | Idle cleanup (nanoseconds) |
| `headers_only` | `?bool` | Deliver headers only |

---

## Publishing

JetStream publish goes directly to the stream subject (not through
`$JS.API`). The server returns a `PubAck` confirming storage.

### Simple Publish

**Zig:**

```zig
var ack = try js.publish("orders.new", payload);
defer ack.deinit();

// Check the ack
if (ack.value.stream) |stream| {
    // stream name that stored the message
}
const seq = ack.value.seq; // sequence number
```

**Go:**

```go
ack, _ := js.Publish(ctx, "orders.new", payload)
// ack.Stream, ack.Sequence
```

### Publish with Options

Use `publishWithOpts` for idempotency and optimistic concurrency.

**Zig:**

```zig
var ack = try js.publishWithOpts(
    "orders.new",
    payload,
    .{
        .msg_id = "order-123",
        .expected_stream = "ORDERS",
        .expected_last_seq = 41,
    },
);
defer ack.deinit();

// Check for duplicate
if (ack.value.duplicate) |dup| {
    if (dup) {
        // Message was already stored (idempotent)
    }
}
```

**Go:**

```go
ack, _ := js.Publish(ctx, "orders.new", payload,
    jetstream.WithMsgID("order-123"),
    jetstream.WithExpectStream("ORDERS"),
    jetstream.WithExpectLastSequence(41),
)
```

### Publish Option Headers

| Zig field | Header sent | Purpose |
|-----------|------------|---------|
| `msg_id` | `Nats-Msg-Id` | Deduplication key |
| `expected_stream` | `Nats-Expected-Stream` | Verify target stream |
| `expected_last_seq` | `Nats-Expected-Last-Sequence` | Optimistic concurrency |
| `expected_last_msg_id` | `Nats-Expected-Last-Msg-Id` | Sequence by msg ID |
| `expected_last_subj_seq` | `Nats-Expected-Last-Subject-Sequence` | Per-subject sequence |

---

## Pull Subscription

Pull consumers fetch messages on demand. Create a
`PullSubscription`, then call `fetch()` to get a batch.

### Setup and Fetch

**Zig:**

```zig
var pull = nats.jetstream.PullSubscription{
    .js = &js,
    .stream = "ORDERS",
};
try pull.setConsumer("processor");

var result = try pull.fetch(.{
    .max_messages = 100,
    .timeout_ms = 5000,
});
defer result.deinit();

for (result.messages) |*msg| {
    const data = msg.data();
    // process data...
    try msg.ack();
}
```

**Go:**

```go
cons, _ := js.Consumer(ctx, "ORDERS", "processor")

batch, _ := cons.Fetch(100)
for msg := range batch.Messages() {
    data := msg.Data()
    // process data...
    msg.Ack()
}
```

### FetchOpts

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `max_messages` | `u32` | 1 | Batch size |
| `timeout_ms` | `u32` | 5000 | Timeout in milliseconds |

### How Fetch Works

1. Subscribes to a temporary inbox
2. Publishes a pull request JSON to
   `$JS.API.CONSUMER.MSG.NEXT.{stream}.{consumer}`
3. Collects messages until batch is full or a status signal
   arrives:
   - **404** -- no messages available (stop)
   - **408** -- request expired (stop)
   - **409** -- leadership change (stop)
   - **100** -- idle heartbeat (skip, continue)
4. Returns `FetchResult` with collected messages

### FetchResult

```zig
const FetchResult = struct {
    messages: []JsMsg,
    allocator: Allocator,

    pub fn count(self: *const FetchResult) usize;
    pub fn deinit(self: *FetchResult) void;
};
```

Call `deinit()` to free all messages and the backing slice.

---

## Message Acknowledgment

JetStream messages must be acknowledged to confirm processing.
All ack methods publish a protocol token to the message's
reply-to subject.

### Ack Methods

| Method | Zig | Go | Payload | Repeatable |
|--------|-----|-----|---------|------------|
| Acknowledge | `msg.ack()` | `msg.Ack()` | `+ACK` | No |
| Negative ack | `msg.nak()` | `msg.Nak()` | `-NAK` | No |
| NAK with delay | `msg.nakWithDelay(ns)` | `msg.NakWithDelay(d)` | `-NAK {"delay":N}` | No |
| In progress | `msg.inProgress()` | `msg.InProgress()` | `+WPI` | Yes |
| Terminate | `msg.term()` | `msg.Term()` | `+TERM` | No |
| Terminate + reason | `msg.termWithReason(r)` | `msg.TermWithReason(r)` | `+TERM reason` | No |

### Examples

**Zig:**

```zig
for (result.messages) |*msg| {
    const data = msg.data();

    if (isValid(data)) {
        try msg.ack();
    } else if (isRetryable(data)) {
        // Retry after 5 seconds
        try msg.nakWithDelay(5_000_000_000);
    } else {
        try msg.termWithReason("invalid payload");
    }
}
```

**Go:**

```go
for msg := range batch.Messages() {
    data := msg.Data()

    if isValid(data) {
        msg.Ack()
    } else if isRetryable(data) {
        msg.NakWithDelay(5 * time.Second)
    } else {
        msg.TermWithReason("invalid payload")
    }
}
```

### Extending the Ack Deadline

For long-running processing, send periodic `inProgress()` signals
to prevent redelivery:

**Zig:**

```zig
try msg.inProgress(); // Reset ack timer
// ... do work ...
try msg.inProgress(); // Reset again
// ... finish work ...
try msg.ack();
```

### JsMsg Accessors

| Method | Returns | Description |
|--------|---------|-------------|
| `data()` | `[]const u8` | Message payload |
| `subject()` | `[]const u8` | Original subject |
| `headers()` | `?[]const u8` | Raw headers |
| `replyTo()` | `?[]const u8` | Ack reply subject |
| `deinit()` | `void` | Free message memory |

---

## Error Handling

nats.zig uses a two-layer error system for JetStream:

1. **Zig error unions** -- transport/protocol failures
2. **ApiError struct** -- server-side JetStream errors

### Layer 1: Zig Errors

```zig
pub const Error = error{
    Timeout,
    NoResponders,
    ApiError,
    JsonParseError,
    SubjectTooLong,
    NoHeartbeat,
    ConsumerDeleted,
    OrderedReset,
    InvalidKey,
    InvalidData,
    KeyNotFound,
    WrongLastRevision,
    ThreadSpawnFailed,
};
```

### Layer 2: API Errors

When `error.ApiError` is returned, call `js.lastApiError()` to
get the server-side error details:

**Zig:**

```zig
var info = js.streamInfo("NONEXISTENT");
if (info) |*r| {
    defer r.deinit();
    // use r.value...
} else |err| {
    if (err == error.ApiError) {
        if (js.lastApiError()) |api_err| {
            // api_err.code       -- HTTP-like status (404)
            // api_err.err_code   -- JetStream error code
            // api_err.description() -- error message
        }
    }
}
```

**Go:**

```go
_, err := js.Stream(ctx, "NONEXISTENT")
if err != nil {
    var jsErr jetstream.JetStreamError
    if errors.As(err, &jsErr) {
        apiErr := jsErr.APIError()
        // apiErr.Code, apiErr.ErrorCode, apiErr.Description
    }
}
```

### Common Error Codes

| Constant | Code | Meaning |
|----------|------|---------|
| `ErrCode.stream_not_found` | 10059 | Stream does not exist |
| `ErrCode.stream_name_in_use` | 10058 | Stream name taken |
| `ErrCode.consumer_not_found` | 10014 | Consumer does not exist |
| `ErrCode.consumer_already_exists` | 10105 | Consumer name taken |
| `ErrCode.js_not_enabled` | 10076 | JetStream not enabled |
| `ErrCode.bad_request` | 10003 | Invalid request |
| `ErrCode.stream_wrong_last_seq` | 10071 | Sequence mismatch |
| `ErrCode.message_not_found` | 10037 | Message not in stream |

Full list in `src/jetstream/errors.zig`.

### Checking Specific Errors

**Zig:**

```zig
const ErrCode = nats.jetstream.errors.ErrCode;

if (js.lastApiError()) |api_err| {
    if (api_err.err_code == ErrCode.stream_not_found) {
        // Handle missing stream
    }
}
```

**Go:**

```go
if errors.Is(err, jetstream.ErrStreamNotFound) {
    // Handle missing stream
}
```

---

## Response Ownership

Every JetStream operation that returns a `Response(T)` owns
parsed JSON memory. All string slices in `resp.value` point
into the parsed arena.

**You must call `deinit()` when done:**

```zig
var resp = try js.createStream(.{ .name = "TEST" });
defer resp.deinit(); // Frees parsed JSON arena

// Access data through resp.value
if (resp.value.config) |cfg| {
    // cfg.name is valid until resp.deinit()
}
```

If you need to keep data beyond `deinit()`, copy it first:

```zig
var resp = try js.streamInfo("TEST");
const msg_count = resp.value.state.?.messages;
resp.deinit(); // Safe -- msg_count is a u64 (copied)
```

String data requires explicit copying:

```zig
var resp = try js.streamInfo("TEST");
const name = try allocator.dupe(
    u8,
    resp.value.config.?.name,
);
resp.deinit(); // Safe -- name is independently owned
defer allocator.free(name);
```

---

## Type Reference

### Enums

| Zig Enum | Values | Go Equivalent |
|----------|--------|---------------|
| `RetentionPolicy` | limits, interest, workqueue | `LimitsPolicy`, `InterestPolicy`, `WorkQueuePolicy` |
| `StorageType` | file, memory | `FileStorage`, `MemoryStorage` |
| `DiscardPolicy` | old, new | `DiscardOld`, `DiscardNew` |
| `StoreCompression` | none, s2 | `NoCompression`, `S2Compression` |
| `DeliverPolicy` | all, last, new, by_start_sequence, by_start_time, last_per_subject | `DeliverAllPolicy`, `DeliverLastPolicy`, ... |
| `AckPolicy` | none, all, explicit | `AckNonePolicy`, `AckAllPolicy`, `AckExplicitPolicy` |
| `ReplayPolicy` | instant, original | `ReplayInstantPolicy`, `ReplayOriginalPolicy` |

### Key Differences from Go

| Aspect | Zig (nats.zig) | Go (nats.go) |
|--------|----------------|--------------|
| Context | Stack struct, `try JetStream.init()` | Interface, `jetstream.New()` |
| Timeout | `timeout_ms: u32` on JetStream | `context.Context` per call |
| Responses | `Response(T)` with `defer deinit()` | Go GC handles memory |
| Errors | `error.ApiError` + `lastApiError()` | `JetStreamError` interface |
| Pull | `PullSubscription.fetch()` | `consumer.Fetch()` |
| Options | Struct fields with `?T = null` | Functional options pattern |
| Enums | Lowercase tags (`.file`) | PascalCase constants (`FileStorage`) |
| Durations | Nanoseconds (`i64`) | `time.Duration` |

### Duration Conversion

JetStream JSON uses nanoseconds for all duration fields:

```zig
// 30 seconds
const thirty_sec: i64 = 30 * std.time.ns_per_s;

// 5 minutes
const five_min: i64 = 5 * 60 * std.time.ns_per_s;

// Use in config
var stream = try js.createStream(.{
    .name = "TEST",
    .max_age = five_min,
    .duplicate_window = thirty_sec,
});
```
