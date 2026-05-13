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
  deadline (`max_wait_ms`), inter-reply stall timer (`stall_ms`),
  delivered count (`max_messages`), and user-supplied `Sentinel`
  predicate. A server-side 503 ("no responders") is detected and
  surfaced as `termination = .no_responders`. The implementation
  uses a dedicated two-token inbox subscription that does not
  collide with the shared `request()` response multiplexer.
- **`Sentinel` and `emptyPayloadSentinel()`** are re-exported
  from the `nats` module. `emptyPayloadSentinel()` provides the
  ADR-47 standard "empty payload terminates the stream" marker;
  user code can also build custom predicates over message
  payload, subject, or headers.
- **`example-request-many`** demonstrates both the iterator and
  callback forms against an in-process 3-worker scatter service.
  Run with `zig build run-request-many`.

### Fixed

- **TLS handshake state machine.** `tls://` URLs now fail closed
  when TLS cannot be negotiated, and reconnect honors each server
  URL's scheme rather than only the first URL's scheme.
- **Header validation.** Structured header publish paths
  (`publishWithHeaders`, `publishRequestWithHeaders`, the
  micro `respondError` path) reject CR/LF/control bytes, DEL, and
  colon in keys, blocking header-injection vectors.
- **Authenticated connects can no longer hang.** A successful
  auth-required connection no longer blocks on a non-existent
  server acknowledgement.
- **KV slice lifetimes.** `KeyLister.next()` and
  `KvWatcher.next()` previously returned slices into a
  deinitialized message. The watcher/lister now duplicates the
  key onto its own allocator and frees it on the next call /
  deinit.
- **`PullSubscription.consume()` lifetime.** The consume context
  is now heap-owned; the background task no longer dereferences
  a pointer to a stack-local that disappeared when
  `consume()` returned.
- **`AsyncPublisher.publishWithOpts()` leak on failure.** A
  failed publish no longer leaves pending futures in the map or
  inflates the in-flight count.
- **`Service.addService()` cleanup on partial failure.** A
  failure between subscription creation and the final flush no
  longer leaves orphaned monitor subscriptions alive.
- **Runtime validation before fixed-buffer copies.** Stream
  names, consumer names, KV bucket names, and the JetStream API
  prefix are validated with runtime errors instead of
  debug-only assertions, so release builds reject malformed
  input safely.
- **KV scan error propagation.** `keys()`, `history()`,
  `historyWithOpts()`, and `purgeDeletes()` no longer silently
  return partial results when the pull errors mid-iteration.
- **Connection edge cases.** `rtt()` no longer misses the PONG
  it triggered; the muxer's PONG race is fixed; subscription
  routing is preserved during concurrent unsubscribe; the
  initial-connect leak on overlong TLS host is closed.

### Changed

- Several JetStream and Micro public setters that previously
  relied on `std.debug.assert` for input length now return errors
  at runtime. Callers that fed validated input see no change;
  callers that relied on the assert in Debug builds will now see
  the same condition raised as an error in Release builds.
- The API quick-reference document was removed in favor of the
  in-code documentation and the JetStream guide.

### Removed

- `doc/api-quick-reference.md` (superseded).

## [0.1.0] - 2026-04-29

Initial public release.

### Highlights

- Core NATS protocol over `std.Io` (Zig 0.16+), no external C
  dependencies.
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
