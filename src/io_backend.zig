//! Comptime selector for the std.Io backend used by entry points.
//!
//! Currently only `std.Io.Threaded` is supported. The internal
//! `io_task` background reader uses direct `poll(2)` for low-latency
//! read/write interleaving and cannot be hosted by `std.Io.Evented`
//! today; the build option and module are kept so the wiring is in
//! place when evented support lands, but selecting
//! `-Dio_backend=evented` is a compile error.
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

/// The selected Io backend type. Always `std.Io.Threaded` today —
/// see the module doc for why `evented` is not yet supported.
pub const Backend = if (want_evented) @compileError(
    "io_backend=evented is not supported yet. The internal io_task " ++
        "uses direct posix.poll() for low-latency read/write " ++
        "interleaving and requires std.Io.Threaded as the host " ++
        "runtime. Re-run with -Dio_backend=threaded (the default).",
) else std.Io.Threaded;

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
