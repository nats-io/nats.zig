//! NATS Protocol Implementation
//!
//! This module handles the NATS wire protocol including parsing server
//! commands and encoding client commands.
//!
//! - Server commands: INFO, MSG, HMSG, PING, PONG, +OK, -ERR
//! - Client commands: CONNECT, PUB, HPUB, SUB, UNSUB, PING, PONG

const std = @import("std");

pub const commands = @import("protocol/commands.zig");
pub const parser = @import("protocol/parser.zig");
pub const encoder = @import("protocol/encoder.zig");
pub const headers = @import("protocol/headers.zig");
pub const header_map = @import("protocol/header_map.zig");
pub const errors = @import("protocol/errors.zig");

// Re-export common types
pub const ServerInfo = commands.ServerInfo;
pub const RawServerInfo = commands.RawServerInfo;
pub const ConnectOptions = commands.ConnectOptions;
pub const ServerCommand = commands.ServerCommand;
pub const ClientCommand = commands.ClientCommand;
pub const MsgArgs = commands.MsgArgs;
pub const HMsgArgs = commands.HMsgArgs;
pub const PubArgs = commands.PubArgs;
pub const SubArgs = commands.SubArgs;

pub const Parser = parser.Parser;
pub const Encoder = encoder.Encoder;
pub const HeaderMap = header_map.HeaderMap;

// Protocol errors
pub const Error = errors.Error;
pub const parseServerError = errors.parseServerError;
pub const isAuthError = errors.isAuthError;

test {
    std.testing.refAllDecls(@This());
}
