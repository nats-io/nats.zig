# Contributing

Thanks for helping improve `nats.zig`.

## Development Setup

Required tools:

- Zig 0.16.0 or later
- `nats-server` on `PATH` for integration tests
- `nats` CLI on `PATH` for JetStream/KV cross-verification tests

Common commands:

```sh
zig build
zig build test
zig build fmt
zig build fmt-check
zig build test-integration
```

Focused integration targets are also available:

```sh
zig build test-integration-tls
zig build test-integration-micro
```

See `src/testing/README.md` for integration test layout and fixtures.

## Before Opening a Pull Request

- Run `zig build`.
- Run `zig build test`.
- Run the relevant integration target when changing connection,
  authentication, TLS, JetStream, KV, or service behavior.
- Keep examples compiling when public APIs change.
- Update `README.md` or `src/examples/README.md` when adding or
  changing user-facing examples.

## Style

- Prefer existing module patterns over new abstractions.
- Keep ownership rules explicit in public APIs and examples.
- Avoid unrelated refactors in bug-fix changes.
- Format changed Zig files with `zig build fmt`.
