//! Debug printing utilities for NATS client.
//!
//! Compile with -DEnableDebug=true to enable debug output.
//! When disabled, all debug calls are eliminated by dead code elimination.

const std = @import("std");
const build_options = @import("build_options");

/// Debug printing enabled at compile time.
pub const enabled = build_options.enable_debug;

/// Print debug message if debug is enabled.
/// Dead code eliminated when disabled.
pub inline fn print(comptime fmt: []const u8, args: anytype) void {
    if (enabled) {
        std.debug.print("[NATS] " ++ fmt ++ "\n", args);
    }
}

/// Print reconnection event.
pub inline fn reconnectEvent(
    comptime event: []const u8,
    attempt: u32,
    server: []const u8,
) void {
    if (enabled) {
        std.debug.print(
            "[NATS:RECONNECT] {s} attempt={d} server={s}\n",
            .{ event, attempt, server },
        );
    }
}

/// Print connection state change.
pub inline fn stateChange(
    comptime from: []const u8,
    comptime to: []const u8,
) void {
    if (enabled) {
        std.debug.print("[NATS:STATE] {s} -> {s}\n", .{ from, to });
    }
}

/// Print PING/PONG event.
pub inline fn pingPong(comptime event: []const u8, outstanding: u8) void {
    if (enabled) {
        std.debug.print(
            "[NATS:HEALTH] {s} outstanding={d}\n",
            .{ event, outstanding },
        );
    }
}

/// Print subscription event.
pub inline fn subscription(
    comptime event: []const u8,
    sid: u64,
    subject: []const u8,
) void {
    if (enabled) {
        std.debug.print(
            "[NATS:SUB] {s} sid={d} subject={s}\n",
            .{ event, sid, subject },
        );
    }
}

/// Print pending buffer event.
pub inline fn pendingBuffer(
    comptime event: []const u8,
    pos: usize,
    capacity: usize,
) void {
    if (enabled) {
        std.debug.print(
            "[NATS:BUFFER] {s} pos={d} capacity={d}\n",
            .{ event, pos, capacity },
        );
    }
}
