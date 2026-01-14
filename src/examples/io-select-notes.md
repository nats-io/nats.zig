# io.select() with NATS Subscriptions - Lessons Learned

## The Problem

We attempted to use `io.select()` to race multiple NATS queue group workers:

```zig
// BROKEN PATTERN - do not use with queue groups
while (total_received < message_count) {
    var f1 = io.async(Sub.next, .{ worker1, allocator, io });
    var f2 = io.async(Sub.next, .{ worker2, allocator, io });
    var f3 = io.async(Sub.next, .{ worker3, allocator, io });

    const result = io.select(.{ .w1 = &f1, .w2 = &f2, .w3 = &f3 });

    switch (result) {
        .w1 => { /* process */ f1 = io.async(...); },  // BUG!
        ...
    }
}
```

This caused **intermittent hangs** and **message loss**.

## Root Causes

### 1. io.select() is ONE-SHOT

After `io.select()` returns:
- The **winning future is consumed** (destroyed internally)
- **Non-selected futures remain in `.pending_awaited` state**
- These MUST be cancelled before calling select() again
- Overwriting a future variable without cancelling corrupts the state machine

### 2. Cancel Returns Same Result (Idempotent)

```zig
// After select returns .w1:
const msg = result.w1;        // Got the message here
defer msg.deinit(allocator);

// WRONG - double free!
if (f1.cancel(io)) |m| m.deinit(allocator);  // Returns SAME message!
```

Cancel on a completed future returns the same result. Must track winner and skip its cleanup.

### 3. Queue Groups + Select = Message Loss

With NATS queue groups, each message goes to exactly ONE subscription (server-side distribution).

When we cancel non-winning futures:
- Their subscriptions may have received messages
- These messages are discarded during cancel cleanup
- Result: only 2-4 messages received out of 9

## The Correct Pattern: Winner-Tracking with Defer

The key insight is that `cancel()` on an already-awaited future returns the
**same result** (idempotent). We can still use defer by tracking which future won:

```zig
// CORRECT - Winner-tracking pattern
var f1 = io.async(Sub.next, .{ sub1, allocator, io });
var f2 = io.async(Sub.next, .{ sub2, allocator, io });

// Track winner to skip its cancel (avoids double-free)
var winner: enum { none, f1, f2 } = .none;

// Defer cancel for non-winners only
defer if (winner != .f1) {
    if (f1.cancel(io)) |m| m.deinit(allocator) else |_| {}
};
defer if (winner != .f2) {
    if (f2.cancel(io)) |m| m.deinit(allocator) else |_| {}
};

const result = io.select(.{ .f1 = &f1, .f2 = &f2 }) catch break;

switch (result) {
    .f1 => |msg_result| {
        winner = .f1;  // Skip f1 cancel in defer
        const msg = msg_result catch continue;
        defer msg.deinit(allocator);
        // process message
    },
    .f2 => |msg_result| {
        winner = .f2;  // Skip f2 cancel in defer
        const msg = msg_result catch continue;
        defer msg.deinit(allocator);
        // process message
    },
}
```

This pattern:
- Uses defer for cleanup ✓
- Properly cancels losers ✓
- Avoids double-free by skipping winner's cancel ✓
- Works in loops ✓

## Summary

| Use Case | Pattern | Works? |
|----------|---------|--------|
| Multiple queue group workers | `io.select()` | NO - message loss |
| Multiple queue group workers | Round-robin polling | YES |
| Multiple independent subscriptions | `io.select()` with winner-tracking | YES |
| Single subscription with timeout | `io.select()` with winner-tracking | YES |
| Race unrelated operations | `io.select()` with winner-tracking | YES |

## Key Rules for io.select()

1. **Winner-tracking**: Track which future won to skip its cancel
2. **Defer cancel losers**: `defer if (winner != .x) { cancel and cleanup }`
3. **Set winner before processing**: `winner = .x;` as first line in switch arm
4. **Works in loops**: Pattern handles break/continue correctly
5. **Avoid with queue groups**: Use polling instead for load-balanced workers
