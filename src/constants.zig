const std = @import("std");
const time = @import("std").time;

// Default Constants
pub const Version: *const [5:0]u8 = "0.0.1";
pub const DefaultURL: *const [21:0]u8 = "nats://127.0.0.1:4222";
pub const DefaultPort = 4222;
pub const DefaultMaxReconnect = 60;
pub const DefaultReconnectWait = 2 * time.Second;
pub const DefaultReconnectJitter = 100 * time.Millisecond;
pub const DefaultReconnectJitterTLS = time.Second;
pub const DefaultTimeout = 2 * time.Second;
pub const DefaultPingInterval = 2 * time.Minute;
pub const DefaultMaxPingOut = 2;
pub const DefaultMaxChanLen = 64 * 1024; // 64k
pub const DefaultReconnectBufSize = 8 * 1024 * 1024; // 8MB
pub const RequestChanLen = 8;
pub const DefaultDrainTimeout = 30 * time.Second;
pub const DefaultFlusherTimeout = time.Minute;
pub const LangString: *const [3:0]u8 = "zig";
