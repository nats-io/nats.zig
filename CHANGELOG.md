# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

> The library is pre-1.0. Public API may change between minor
> versions; any such change is listed under **Changed** with the
> migration in the entry itself.

## [Unreleased]

### Added

- **Scatter/gather `requestMany` (ADR-47).** New
  `Client.requestMany(subject, payload, opts)` returns an iterator
  over multiple replies to a single request, and
  `Client.requestManyCallback(...)` delivers them through a
  `MsgHandler` and returns a summary. Stop conditions: total
  deadline (`max_wait_ms`, default 1000 ms), inter-reply stall
  timer (`stall_ms`), delivered count (`max_messages`), and
  user-supplied `Sentinel` predicate. A server-side 503 ("no
  responders") is detected and surfaced as
  `termination = .no_responders`; draining the client or losing
  the connection for good ends the call with `.closed`. The
  implementation uses a dedicated
  two-token inbox subscription that does not collide with the
  shared `request()` response multiplexer.
- **`RequestManyOptions`, `RequestManyIter`, `RequestManyResult`,
  `RequestManyTermination`, `Sentinel` and
  `emptyPayloadSentinel()`** are re-exported from the `nats`
  module. `emptyPayloadSentinel()` provides the
  ADR-47 standard "empty payload terminates the stream" marker;
  user code can also build custom predicates over message
  payload, subject, or headers.
- **`example-request-many`** demonstrates both the iterator and
  callback forms against an in-process 3-worker scatter service.
  Run with `zig build run-request-many`.

### Changed

- **Requires Zig 0.17.0** (`minimum_zig_version = "0.17.0"`).
  Zig 0.16 can no longer build the library. Migrated off APIs
  that 0.17 removed or deprecated (`**` array repeat,
  `std.fmt.bufPrint`, `std.mem.indexOf*`, `std.fmt.allocPrint`,
  and others); no public API changes.
- **`-Dio_backend=evented` is a compile error** instead of a
  silently unsupported option. Only `threaded` is supported until
  `std.Io.Evented` gains network read/write. The README now
  scopes the `std.Io` claim to the public API: the internal
  io_task uses `poll(2)` and requires `std.Io.Threaded` as host.

### Fixed

- **BADF panic during disconnect/deinit.** `deinit()`, `drain()`
  and `forceReconnect()` could race io_task's reconnect path and
  shut down or close an fd it had already closed or replaced,
  which panics in Debug (seen on macOS). The stream is now
  guarded by a mutex. The `fcntl`-based fd-validity check, which
  misreported closed fds on libc platforms, is gone.
- **FreeBSD and other BSDs build.** `TCP_NODELAY` now comes from
  `defaults.Posix.tcp_nodelay` instead of `std.posix.TCP`, which
  is `void` on those targets.

## [0.1.0] - 2026-04-29

Initial public release.

### Highlights

- Core NATS protocol over `std.Io` (built with Zig 0.16.0), no
  external C dependencies.
- Pub/sub with three subscription styles: `subscribe()` /
  `subscribeFn()` (callback) and `subscribeSync()` (manual
  receive). Queue group variants for each.
- Request/reply with a shared response multiplexer
  (`_INBOX.<NUID>.*`) that amortizes inbox SUB/UNSUB across
  requests, and `msg.respond()` convenience.
- NATS headers (HPUB/HMSG) with structured publish, parsing,
  and a `HeaderMap` builder; well-known header constants for
  JetStream features.
- Reconnection with backoff, server pool, auto-discovery, and
  lifecycle events through an `EventHandler` vtable.
- Authentication: username/password, token, NKey seed (string
  or file), NKey signing callback for HSM use, and JWT/creds.
  Programmatic NKey generation and JWT/creds encoding helpers.
- Server-authenticated TLS via URL scheme or explicit options.
  mTLS client certificates are reserved but not yet implemented.
- JetStream: stream and consumer CRUD, publish with ack and
  deduplication, pull consumers (`fetch`, `fetchNoWait`,
  `fetchBytes`, `next`, `messages`, `consume`), push consumers
  with callback delivery, ordered consumers, full ack/nak/term
  set, JetStream domains, and async publish.
- Key-Value store: create/update/delete buckets, put/get,
  create-only and optimistic update by revision, delete and
  purge, key listing, history, and live watches.
- Micro services API: discoverable services with `$SRV.PING`,
  `$SRV.INFO`, `$SRV.STATS`; endpoint groups, queue groups,
  metadata, stats reset, and drain.
- Integration test suite covering core, reconnect, JetStream,
  KV, micro, headers, auth, and TLS scenarios.

[Unreleased]: https://github.com/nats-io/nats.zig/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nats-io/nats.zig/releases/tag/v0.1.0
