[![CI](https://github.com/mutech/nats.zig/actions/workflows/ci.yml/badge.svg)](https://github.com/mutech/nats.zig/actions/workflows/ci.yml)
[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](LICENSE)
![Zig](https://img.shields.io/badge/Zig-0.16.0-orange)

<p align="center">
  <img src="logo/logo.png">
</p>

# nats.zig (mutech fork)

A fork of [`nats-io/nats.zig`](https://github.com/nats-io/nats.zig) — a
[Zig](https://ziglang.org/) client for the [NATS messaging system](https://nats.io).

> **This README documents only what the fork changes.** For the base library —
> installation, quick start, pub/sub, request/reply, JetStream, KV, Micro,
> authentication, TLS, memory ownership, error handling, the full API reference
> and the examples — see the **[upstream `nats-io/nats.zig`
> README](https://github.com/nats-io/nats.zig#readme)**. Everything there applies
> unless noted below.

## What's different in this fork

1. **UNIX domain socket (UDS) transport** for the
   [`mutech/nats-server`](https://github.com/mutech/nats-server) UDS fork —
   connect with `nats+uds:///path/to/socket` for local, zero-TCP messaging.
2. **CPU / memory fixes** over the original — no idle busy-spin (idle CPU ~0 %)
   and runtime-sized buffers instead of oversized fixed preallocation, so many
   small, mostly-idle clients are affordable.
3. **`std.Io`-agnostic refactor** — the client does *all* socket I/O through the
   injected `io: Io`, so it runs unchanged on `std.Io.Threaded` (the default)
   **or** an evented `std.Io` backend such as
   [zio](https://github.com/lalinsky/zio). The caller chooses the execution
   model; the library no longer pins a threading model or hand-rolls
   `poll(2)`/eventfd. The public API stays `std.Io`-idiomatic (`io.async`/futures,
   `Io.Select`, `Io.Queue`, cancellation, mutexes, timers).

> **Intent & status.** My preference is to land these changes upstream, but the
> third item is a large enough refactor that upstreaming it is unlikely — so this
> fork is probably best treated as an **alternative NATS client** rather than a
> staging branch. Feedback on the design is very welcome.

## Using the evented (zio) backend

By default the library runs on `std.Io.Threaded`. To run on zio's evented
`std.Io` (io_uring on Linux), construct a `zio.Runtime` and pass its `io()` to
`Client.connect` — the client is backend-agnostic, nothing else changes. In this
repo the test/bench harness selects the backend at build time with
`-Dio_backend=zio` (zio is a *lazy* `build.zig.zon` dependency, fetched only for
that flag, so consumers of the `nats` module never pull it in); see
[`src/io_backend.zig`](src/io_backend.zig) for how the runtime is constructed.

**What to pay attention to** — these apply to any evented `std.Io`, and to
`std.Io.Threaded` on machines with few cores:

- **Use `io.concurrent`, not `io.async`, for anything that must run
  concurrently** (a responder loop, a background drain, a task the caller later
  depends on). `io.async` may run *inline* when the async pool is saturated, and
  a connected client already occupies pool slots — so on a ≤2-core host such an
  `io.async` call (or an `Io.Select` arm, which is dispatched via `io.async`) can
  run inline and deadlock. Use `io.concurrent`, or size the runtime's
  `async_limit` accordingly.
- **Put timeouts on a *wait*, never a racing `sleep` task.** Use
  `Batch.awaitConcurrent(timeout)` for supported operations, or a
  `futexWaitTimeout` deadline loop — never `Io.Select(op, sleep(T))`, whose sleep
  arm can run inline and block for the full timeout.
- **Executors.** zio's `Runtime` defaults to a multi-threaded executor pool. For
  many small runtimes (e.g. one per connection) prefer one executor per runtime
  (`.executors = .exact(1)`) so you don't spawn a whole pool per client.

## License

Apache 2.0, same as upstream — see [LICENSE](LICENSE).
