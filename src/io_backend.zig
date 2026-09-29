//! Constructs the std.Io backend used by the fork's own entry points — the
//! examples and integration tests. The library is backend-agnostic:
//! `Client.connect` takes an `io: std.Io`, so this indirection exists only so
//! those executables (and the test harness) can switch backends via
//! `-Dio_backend` without each constructing a runtime by hand.
//!
//! 'threaded' (default) uses `std.Io.Threaded`; 'zio' uses zio's evented Io.
//! Threaded stays the default until the evented driver is complete, because the
//! current io_task still blocks its thread in `poll(2)` and would starve a
//! single-threaded zio loop.
//!
//! Usage:
//! ```
//! const io_backend = @import("io_backend");
//! var backend: io_backend.Backend = undefined;
//! try io_backend.init(&backend, gpa);
//! defer backend.deinit();
//! const io = backend.io();
//! var client = try nats.Client.connect(gpa, io, url, .{});
//! defer client.deinit();
//! ```

const std = @import("std");
const build_options = @import("build_options");

const want_zio = std.mem.eql(u8, build_options.io_backend, "zio");

/// The selected backend's name ("zio" or "threaded"), for diagnostics/labels.
pub const name = build_options.io_backend;

comptime {
    if (!want_zio and !std.mem.eql(u8, build_options.io_backend, "threaded")) {
        @compileError(
            "unknown -Dio_backend='" ++ build_options.io_backend ++
                "'; expected 'threaded' or 'zio'.",
        );
    }
}

// Imported only in the zio configuration so a threaded build does not require
// the (lazy, test-only) zio dependency to be present.
const zio = if (want_zio) @import("zio") else void;

/// The selected Io backend. `std.Io.Threaded` is a value type; `zio.Runtime` is
/// heap-managed and held by pointer (its `init` returns `*Runtime`). Both expose
/// `io()` and `deinit()`, so callers use `Backend` uniformly.
pub const Backend = if (want_zio) *zio.Runtime else std.Io.Threaded;

/// Initialize the selected backend in place with default options. Caller owns
/// the result and must call `Backend.deinit()`. `out` may be undefined on entry.
pub fn init(out: *Backend, gpa: std.mem.Allocator) !void {
    return initWithEnviron(out, gpa, .empty);
}

/// Initialize with a process environment. `std.Io.Threaded` uses it to resolve
/// child-process lookups (e.g. spawning the test server) through the runner's
/// PATH. zio's `Runtime` has no environment option, so the parameter is unused
/// there; zio still spawns child processes via an internal Threaded fallback, so
/// process-spawning integration tests do run on zio — they just cannot customize
/// the environment through this seam.
pub fn initWithEnviron(
    out: *Backend,
    gpa: std.mem.Allocator,
    environ: std.process.Environ,
) !void {
    if (want_zio) {
        // Run the event loop on a background worker thread rather than the
        // calling thread. This mirrors the Threaded backend (io ops run off a
        // pool, not the caller) so the test/example harness -- which drives
        // blocking io from a plain OS thread and often uses SEVERAL runtimes at
        // once (e.g. a separate publisher + subscriber client) -- makes progress
        // on every runtime concurrently. With the default (main executor on the
        // calling thread) a single-threaded loop can only advance the one
        // runtime whose loop the caller is currently driving, so two-client
        // tests deadlock.
        //
        // Exactly one executor: the harness creates a fresh runtime per test
        // (hundreds over a run). `.auto` sizes the pool to the core count, so
        // each runtime would spawn 10-30 threads and the churn exhausts the
        // process thread/FD limit partway through the suite (Runtime.init then
        // fails). One executor per runtime is plenty -- the reader and writer
        // fibers multiplex on it cooperatively -- and the driver code is
        // nonetheless verified race-clean under a multi-threaded executor
        // (a `.auto` run clears every functional test before hitting that
        // resource ceiling).
        out.* = try zio.Runtime.init(gpa, .{
            .enable_main_executor = false,
            .executors = .exact(1),
        });
    } else {
        // async_limit = .unlimited (default is cpu_count-1). The test/example
        // harness spawns responders and racing Io.Select arms via io.async, and
        // a connected client already occupies the shared busy-count with its
        // reader+writer (io.concurrent). With the default limit, on a ≤2-core
        // host every such io.async/Select arm overflows to running INLINE on the
        // caller (async_limit=1) -- responders never run, Select races deadlock.
        // Real consumers bring their own Io; this only sizes the fork's harness
        // so its concurrency scaffolding runs regardless of host core count.
        // (Must-be-concurrent work still uses io.concurrent, never io.async.)
        out.* = std.Io.Threaded.init(gpa, .{
            .environ = environ,
            .async_limit = .unlimited,
        });
    }
}

test "Backend exposes io() and deinit()" {
    const Runtime = if (want_zio) zio.Runtime else std.Io.Threaded;
    try std.testing.expect(@hasDecl(Runtime, "io"));
    try std.testing.expect(@hasDecl(Runtime, "deinit"));
}
