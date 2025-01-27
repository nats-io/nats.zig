const std = @import("std");
const time = std.time;


// Default Constants
// const (
// Version                   = "1.38.0"
// DefaultURL                = "nats://127.0.0.1:4222"
// DefaultPort               = 4222
// DefaultMaxReconnect       = 60
// DefaultReconnectWait      = 2 * time.Second
// DefaultReconnectJitter    = 100 * time.Millisecond
// DefaultReconnectJitterTLS = time.Second
// DefaultTimeout            = 2 * time.Second
// DefaultPingInterval       = 2 * time.Minute
// DefaultMaxPingOut         = 2
// DefaultMaxChanLen         = 64 * 1024       // 64k
// 	DefaultReconnectBufSize   = 8 * 1024 * 1024 // 8MB
// 	RequestChanLen            = 8
// DefaultDrainTimeout       = 30 * time.Second
// DefaultFlusherTimeout     = time.Minute
// LangString                = "go"
// )



// Connection States


// STALE_CONNECTION is for detection and proper handling of stale connections.
const STALE_CONNECTION = "stale connection";

// PERMISSIONS_ERR is for when nats server subject authorization has failed.
const PERMISSIONS_ERR = "permissions violation";

// AUTHORIZATION_ERR is for when nats server user authorization has failed.
const AUTHORIZATION_ERR = "authorization violation";

// AUTHENTICATION_EXPIRED_ERR is for when nats server user authorization has expired.
const AUTHENTICATION_EXPIRED_ERR = "user authentication expired";

// AUTHENTICATION_REVOKED_ERR is for when user authorization has been revoked.
const AUTHENTICATION_REVOKED_ERR = "user authentication revoked";

// ACCOUNT_AUTHENTICATION_EXPIRED_ERR is for when nats server account authorization has expired.
const ACCOUNT_AUTHENTICATION_EXPIRED_ERR = "account authentication expired";

// MAX_CONNECTIONS_ERR is for when nats server denies the connection due to server max_connections limit
const MAX_CONNECTIONS_ERR = "maximum connections exceeded";

// MAX_SUBSCRIPTIONS_ERR is for when nats server denies the connection due to server subscriptions limit
const MAX_SUBSCRIPTIONS_ERR = "maximum subscriptions exceeded";


pub fn main() !void {
    // std.debug.print("JSON: {s}\n", .{jsonString});
    //
    std.debug.print("Hello World", .{});
}
