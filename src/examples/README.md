# Examples

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
