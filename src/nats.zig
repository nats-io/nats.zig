//! NATS Client Library for Zig
//!
//! A Zig implementation of the NATS messaging protocol with native
//! async I/O using std.Io, zero external C dependencies.
//!
//! ## Quick Start
//!
//! ```zig
//! const nats = @import("nats");
//! const std = @import("std");
//!
//! pub fn main() !void {
//!     var gpa: std.heap.GeneralPurposeAllocator(.{}) = .init;
//!     defer _ = gpa.deinit();
//!     const allocator = gpa.allocator();
//!
//!     var threaded: std.Io.Threaded = .init(allocator);
//!     defer threaded.deinit();
//!     const io = threaded.io();
//!
//!     const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{});
//!     defer client.deinit(allocator);
//!
//!     try client.publish("hello", "world");
//!     try client.flush();
//! }
//! ```

const std = @import("std");

// Module exports
pub const defaults = @import("defaults.zig");
pub const protocol = @import("protocol.zig");
pub const connection = @import("connection.zig");
pub const pubsub = @import("pubsub.zig");
pub const memory = @import("memory.zig");
pub const auth = @import("auth.zig");

// Configuration types
pub const QueueSize = defaults.QueueSize;

// Client module
pub const Client = @import("Client.zig");

// Primary types (nested in Client)
pub const Subscription = Client.Subscription;
pub const Message = Client.Message;
pub const Options = Client.Options;
pub const Stats = Client.Stats;

// Event callback types (nested in Client)
pub const Event = Client.Event;
pub const EventHandler = Client.EventHandler;

// Convenience exports
pub const newInbox = pubsub.newInbox;
pub const validateSubject = pubsub.validatePublish;

// Protocol types
pub const ServerInfo = protocol.ServerInfo;
pub const ConnectOptions = protocol.ConnectOptions;

/// Library version
pub const version = defaults.Protocol.version;

/// Default NATS port
pub const default_port: u16 = defaults.Protocol.port;

/// Default maximum payload size (1MB)
pub const default_max_payload: u32 = defaults.Protocol.max_payload;

test {
    std.testing.refAllDecls(@This());
}
