//! NATS Client Library for Zig
//!
//! A pure Zig implementation of the NATS messaging protocol with native
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
pub const protocol = @import("protocol.zig");
pub const connection = @import("connection.zig");
pub const pubsub = @import("pubsub.zig");
pub const memory = @import("memory.zig");

// Client module
pub const client = @import("client.zig");

// Primary types
pub const Client = client.Client;
pub const Subscription = client.Subscription;
pub const Message = client.Message;
pub const MessageRef = client.MessageRef;
pub const Options = client.Options;
pub const Stats = client.Stats;

// Connection types
pub const Status = connection.State;

// Convenience exports
pub const newInbox = pubsub.newInbox;
pub const validateSubject = pubsub.validatePublish;

// Protocol types
pub const ServerInfo = protocol.OwnedServerInfo;
pub const ConnectOptions = protocol.ConnectOptions;

/// Library version
pub const version = "0.1.0";

/// Default NATS port
pub const default_port: u16 = 4222;

/// Default maximum payload size (1MB)
pub const default_max_payload: u32 = 1048576;

test {
    std.testing.refAllDecls(@This());
}
