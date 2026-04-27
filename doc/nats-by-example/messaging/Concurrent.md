# Concurrent Message Processing

By default, messages from a subscription are processed sequentially -
each message must finish before the next one starts. For workloads
where message processing takes variable time (API calls, database
queries, computation), concurrent processing can significantly improve
throughput.

This example uses `io.concurrent()` to spawn worker threads that
process messages in parallel. Each worker simulates variable
processing time with a random delay, causing messages to complete
out of their original order.

## Running

Prerequisites: `nats-server` running on `localhost:4222`.

```sh
nats-server &
zig build run-nbe-messaging-concurrent
```

## Output (order varies per run)

```
received message: "hello 3"
received message: "hello 0"
received message: "hello 7"
received message: "hello 1"
received message: "hello 5"
received message: "hello 8"
received message: "hello 4"
received message: "hello 9"
received message: "hello 6"
received message: "hello 2"

processed 10 messages concurrently
```

**Note**: The message order is non-deterministic. Each run produces
a different sequence because workers process with random delays.

## What's Happening

1. 10 messages are published to `greet.joe`.
2. All 10 are received on the main thread and copied into work items.
3. Three concurrent workers are spawned via `io.concurrent()`.
4. Each worker processes its assigned messages with a random delay
   (0-100ms) to simulate variable work.
5. Workers write directly to stdout using `writeStreamingAll` (atomic
   per-line writes avoid interleaved output).
6. The main thread waits for all workers to complete.

## Source

See [concurrent.zig](concurrent.zig) for the full example.

Based on [natsbyexample.com/examples/messaging/concurrent](https://natsbyexample.com/examples/messaging/concurrent/rust).
