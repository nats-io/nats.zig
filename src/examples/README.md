# Examples

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
