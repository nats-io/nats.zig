# Messaging

Examples based on the [natsbyexample.com](https://natsbyexample.com/)
**Messaging** category, implemented in Zig using the nats.zig client.

## Building and Running

Prerequisites: a `nats-server` running on `localhost:4222`.

```sh
nats-server &
```

Each example is a standalone executable. Build and run with:

```sh
zig build run-nbe-messaging-<example-name>
```

For example:

```sh
zig build run-nbe-messaging-pub-sub
zig build run-nbe-messaging-request-reply
```

## Examples

| Example | Description | Source |
|---------|-------------|--------|
| [Publish-Subscribe](Pub-Sub.md) | Subject-based pub/sub with wildcard routing and at-most-once delivery | [pub-sub.zig](pub-sub.zig) |
| [Request-Reply](Request-Reply.md) | RPC-style communication using temporary inbox subjects | [request-reply.zig](request-reply.zig) |
| [JSON for Message Payloads](Json.md) | Type-safe JSON serialization/deserialization with `std.json` | [json.zig](json.zig) |
| [Concurrent Message Processing](Concurrent.md) | Parallel message processing with `io.concurrent()` worker threads | [concurrent.zig](concurrent.zig) |
| [Iterating Over Multiple Subscriptions](Iterating-Multiple-Subscriptions.md) | Polling multiple subscriptions in a unified round-robin loop | [iterating-multiple-subscriptions.zig](iterating-multiple-subscriptions.zig) |
