# Integration Tests

This directory contains the integration test harness and fixtures for the
NATS Zig client. These tests exercise the client against real `nats-server`
processes instead of mocks.

## Layout

- `integration_test.zig` runs the main end-to-end integration suite.
- `micro_integration_test.zig` runs the micro service integration suite.
- `tls_integration_test.zig` runs the focused JWT/TLS integration suite.
- `client/` contains the grouped test cases used by the runners.
- `configs/` contains NATS server configurations used by the harness.
- `certs/` contains TLS certificates used only by the test servers.
- `server_manager.zig` and `test_utils.zig` provide shared test
  infrastructure.

## Commands

Run from the repository root:

```sh
zig build test-integration
zig build test-integration-micro
zig build test-integration-tls
```

The tests require `nats-server` to be available on `PATH`. The main
integration suite also uses the `nats` CLI for JetStream/KV
cross-verification tests.

## Scope

This directory is for correctness and interoperability tests. Performance
benchmarking is intentionally kept out of the main client repository; use the
separate benchmark repository for cross-client performance comparisons.
