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
//! pub fn main(init: std.process.Init) !void {
//!     const allocator = init.gpa;
//!     const io = init.io;
//!
//!     const client = try nats.Client.connect(allocator, io, "nats://localhost:4222", .{});
//!     defer client.deinit();
//!
//!     try client.publish("hello", "world");
//!     try client.flush(std.time.ns_per_s * 10);
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
pub const Statistics = Client.Statistics;

// Event callback types (nested in Client)
pub const Event = Client.Event;
pub const EventHandler = Client.EventHandler;

// Message callback type (nested in Client)
pub const MsgHandler = Client.MsgHandler;

// Events module exports
const events = @import("events.zig");
pub const EventError = events.Error;
pub const statusText = events.statusText;

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
