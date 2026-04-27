# Publish-Subscribe

NATS implements publish-subscribe message distribution through subject-based
routing. Publishers send messages to named subjects. Subscribers express
interest in subjects (including wildcards) and receive matching messages.

The core guarantee is **at-most-once delivery**: if there is no subscriber
listening when a message is published, the message is silently discarded.
This is similar to UDP or MQTT QoS 0. For stronger delivery guarantees, see
JetStream.

## Wildcard Subscriptions

NATS supports two wildcard tokens in subscriptions:

- `*` matches a single token: `greet.*` matches `greet.joe`, `greet.pam`
- `>` matches one or more tokens: `greet.>` matches `greet.joe`,
  `greet.joe.hello`

## Running

Prerequisites: `nats-server` running on `localhost:4222`.

```sh
nats-server &
zig build run-nbe-messaging-pub-sub
```

## Output

```
subscribed after a publish...
msg is null? true
msg data: "hello" on subject "greet.joe"
msg data: "hello" on subject "greet.pam"
msg data: "hello" on subject "greet.bob"
```

## What's Happening

1. A message is published to `greet.joe` **before** any subscription exists.
   This message is lost - at-most-once delivery means no buffering.
2. A wildcard subscription on `greet.*` is created.
3. Attempting to receive returns `null` - the earlier message is gone.
4. Two messages are published to `greet.joe` and `greet.pam`. Both are
   received because the subscription is now active and the wildcard matches.
5. A third message to `greet.bob` is also received via the same wildcard.

## Source

See [pub-sub.zig](pub-sub.zig) for the full example.

Based on [natsbyexample.com/examples/messaging/pub-sub](https://natsbyexample.com/examples/messaging/pub-sub/go).
