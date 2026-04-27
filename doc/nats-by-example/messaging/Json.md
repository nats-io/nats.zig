# JSON for Message Payloads

NATS message payloads are opaque byte sequences. It is up to the
application to define serialization. JSON is a natural choice for
cross-language compatibility.

Zig's `std.json` provides compile-time type-safe serialization and
deserialization:

- **Serialize**: `std.json.Stringify.value(struct, options, writer)`
  writes JSON to any `Io.Writer` (including fixed-buffer writers).
- **Deserialize**: `std.json.parseFromSlice(T, allocator, data, options)`
  parses JSON bytes into a typed struct, returning an error for
  invalid input.

## Running

Prerequisites: `nats-server` running on `localhost:4222`.

```sh
nats-server &
zig build run-nbe-messaging-json
```

## Output

```
received valid payload: foo=bar, bar=27
received invalid payload: not json
```

## What's Happening

1. A `Payload` struct is defined with `foo` (string) and `bar` (int)
   fields.
2. An instance is serialized to JSON using `Stringify.value` into a
   stack-allocated buffer - no heap allocation needed.
3. The JSON bytes are published to the `greet` subject.
4. A second message with invalid content (`"not json"`) is published.
5. The receiver tries `parseFromSlice` on each message. The first
   succeeds and prints the deserialized fields. The second fails
   gracefully and prints the raw payload.

## Source

See [json.zig](json.zig) for the full example.

Based on [natsbyexample.com/examples/messaging/json](https://natsbyexample.com/examples/messaging/json/go).
