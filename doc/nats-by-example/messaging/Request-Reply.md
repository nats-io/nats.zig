# Request-Reply

The request-reply pattern enables RPC-style communication over NATS.
Under the hood, NATS implements this as an optimized pair of
publish-subscribe operations: the requester creates a temporary inbox
subject, subscribes to it, and publishes the request with a `reply_to`
header pointing at that inbox.

Unlike strict point-to-point protocols, multiple subscribers can
potentially respond to a request. The client receives the first reply
and discards the rest.

When no handler is subscribed, the server sends a "no responders"
notification (status 503) instead of silently timing out.

## Running

Prerequisites: `nats-server` running on `localhost:4222`.

```sh
nats-server &
zig build run-nbe-messaging-request-reply
```

## Output

```
hello, joe
hello, sue
hello, bob
no responders
```

## What's Happening

1. A subscription on `greet.*` handles incoming requests in a
   background async task.
2. The handler extracts the name from the subject (`greet.joe` ->
   `joe`) and responds with `"hello, joe"`.
3. Three requests are made - `greet.joe`, `greet.sue`, `greet.bob` -
   each receiving a personalized greeting.
4. The handler subscription is unsubscribed.
5. A fourth request to `greet.joe` returns "no responders" because
   no handler is listening anymore.

## Source

See [request-reply.zig](request-reply.zig) for the full example.

Based on [natsbyexample.com/examples/messaging/request-reply](https://natsbyexample.com/examples/messaging/request-reply/go).
