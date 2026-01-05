//! NATS Client Library for Zig
//!
//! A pure Zig implementation of the NATS messaging protocol with native
//! async I/O, zero external C dependencies, and JetStream support.
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
//!     const url = "nats://localhost:4222";
//!     var client = try nats.Client.connect(allocator, io, url, .{});
//!     defer client.deinit(allocator, io);
//!
//!     try client.publish(allocator, "hello", "world");
//! }

const std = @import("std");

// Re-export protocol types
pub const protocol = @import("protocol.zig");

// Re-export core types
pub const types = @import("types.zig");

// Re-export connection types
pub const connection = @import("connection.zig");

// Re-export pub/sub types
pub const pubsub = @import("pubsub.zig");

// Re-export memory management
pub const memory = @import("memory.zig");

// Re-export client
pub const client = @import("client.zig");
pub const Client = client.Client;
pub const Stats = client.Stats;

// Convenience re-exports for common types
pub const Message = types.Message;
pub const Options = types.Options;
pub const Status = connection.State;
pub const Error = types.Error;
pub const Subscription = pubsub.Subscription;

// Pub/Sub convenience exports
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
    // Run tests from all submodules
    std.testing.refAllDecls(@This());
}
