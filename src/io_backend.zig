//! Comptime selector for the std.Io backend used by entry points.
//!
//! Picks between `std.Io.Threaded` (default) and `std.Io.Evented`
//! at compile time, based on the `-Dio_backend=threaded|evented`
//! build option. The library itself is backend-agnostic and
//! accepts any `std.Io` via `Client.connect`; this module exists
//! purely so applications, examples, and integration tests can
//! flip backends without code changes.
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

const want_evented = std.mem.eql(
    u8,
    build_options.io_backend,
    "evented",
);

/// The selected Io backend type, chosen at compile time from the
/// `-Dio_backend=...` build option. Defaults to `std.Io.Threaded`.
pub const Backend = if (want_evented) blk: {
    if (std.Io.Evented == void) @compileError(
        "std.Io.Evented is not supported on this target. " ++
            "Build with -Dio_backend=threaded.",
    );
    break :blk std.Io.Evented;
} else std.Io.Threaded;

comptime {
    std.debug.assert(@sizeOf(Backend) > 0);
}

/// Initialize the selected backend in place with default options.
/// Caller owns the result and must call `Backend.deinit()`.
///
/// `out` may be undefined on entry; it is fully initialized on
/// successful return.
///
/// Threaded init cannot fail and Uring/Kqueue/Dispatch init can,
/// so the wrapper is uniformly errorable.
pub fn init(out: *Backend, gpa: std.mem.Allocator) !void {
    return initWithEnviron(out, gpa, .empty);
}

/// Initialize the selected backend with a process environment.
///
/// This matters for entry points that spawn child processes: std.Io resolves
/// `argv[0]` through the environment stored in the Io backend, not through
/// later shell state.
pub fn initWithEnviron(
    out: *Backend,
    gpa: std.mem.Allocator,
    environ: std.process.Environ,
) !void {
    std.debug.assert(@sizeOf(Backend) > 0);
    if (Backend == std.Io.Threaded) {
        out.* = std.Io.Threaded.init(gpa, .{ .environ = environ });
    } else {
        try Backend.init(out, gpa, .{ .environ = environ });
    }
}

test "Backend type is selectable at comptime" {
    try std.testing.expect(@sizeOf(Backend) > 0);
    try std.testing.expect(@hasDecl(Backend, "io"));
    try std.testing.expect(@hasDecl(Backend, "deinit"));
}
