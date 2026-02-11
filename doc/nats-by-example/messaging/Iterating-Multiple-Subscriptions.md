# Iterating Over Multiple Subscriptions

NATS wildcards cover many routing cases, but sometimes you need
separate subscriptions. For example, you want `transport.cars`,
`transport.planes`, and `transport.ships` but not
`transport.spaceships`.

This example shows how to poll multiple subscriptions in a unified
loop using `tryNext()` - Zig's equivalent of merging async streams
into one iteration. Messages from all subscriptions are processed
in round-robin fashion without blocking.

## Running

Prerequisites: `nats-server` running on `localhost:4222`.

```sh
nats-server &
zig build run-nbe-messaging-iterating-multiple-subscriptions
```

## Output

```
received on cars.0: car number 0
received on planes.0: plane number 0
received on ships.0: ship number 0
received on cars.1: car number 1
received on planes.1: plane number 1
received on ships.1: ship number 1
...
received on cars.9: car number 9
received on planes.9: plane number 9
received on ships.9: ship number 9

received 30 messages from 3 subscriptions
```

## What's Happening

1. Three separate subscriptions are created: `cars.>`, `planes.>`,
   and `ships.>`.
2. 10 messages are published to each category (30 total).
3. All three subscriptions are polled in round-robin using
   `tryNext()` which returns instantly if no message is available.
4. Each message's subject and payload are printed as received.
5. A short sleep avoids busy-spinning when no messages are ready.

## Source

See [iterating-multiple-subscriptions.zig](iterating-multiple-subscriptions.zig)
for the full example.

Based on [natsbyexample.com/examples/messaging/iterating-multiple-subscriptions](https://natsbyexample.com/examples/messaging/iterating-multiple-subscriptions/rust).
