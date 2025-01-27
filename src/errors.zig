const std = @import("std");

// Errors from go client example
// var (
// ErrClientCertOrRootCAsRequired = errors.New("nats: at least one of certCB or rootCAsCB must be set")
// ErrNoInfoReceived              = errors.New("nats: protocol exception, INFO not received")
// ErrReconnectBufExceeded        = errors.New("nats: outbound buffer limit exceeded")
// ErrInvalidConnection           = errors.New("nats: invalid connection")
// ErrInvalidMsg                  = errors.New("nats: invalid message or message nil")
// ErrInvalidArg                  = errors.New("nats: invalid argument")
// ErrInvalidContext              = errors.New("nats: invalid context")
// ErrNoDeadlineContext           = errors.New("nats: context requires a deadline")
// ErrNoEchoNotSupported          = errors.New("nats: no echo option not supported by this server")
// ErrClientIDNotSupported        = errors.New("nats: client ID not supported by this server")
// ErrUserButNoSigCB              = errors.New("nats: user callback defined without a signature handler")
// ErrNkeyButNoSigCB              = errors.New("nats: nkey defined without a signature handler")
// ErrNoUserCB                    = errors.New("nats: user callback not defined")
// ErrNkeyAndUser                 = errors.New("nats: user callback and nkey defined")
// ErrNkeysNotSupported           = errors.New("nats: nkeys not supported by the server")
// ErrStaleConnection             = errors.New("nats: " + STALE_CONNECTION)
// ErrTokenAlreadySet             = errors.New("nats: token and token handler both set")
// ErrUserInfoAlreadySet          = errors.New("nats: cannot set user info callback and user/pass")
// ErrMsgNotBound                 = errors.New("nats: message is not bound to subscription/connection")
// ErrMsgNoReply                  = errors.New("nats: message does not have a reply")
// ErrClientIPNotSupported        = errors.New("nats: client IP not supported by this server")
// ErrDisconnected                = errors.New("nats: server is disconnected")
// ErrHeadersNotSupported         = errors.New("nats: headers not supported by this server")
// ErrBadHeaderMsg                = errors.New("nats: message could not decode headers")
// ErrNoResponders                = errors.New("nats: no responders available for request")
// ErrMaxConnectionsExceeded      = errors.New("nats: server maximum connections exceeded")
// ErrConnectionNotTLS            = errors.New("nats: connection is not tls")
// ErrMaxSubscriptionsExceeded    = errors.New("nats: server maximum subscriptions exceeded")
// )

pub const NatsError = error{
    ConnectionClosed,
    ConnectionDraining,
    DrainTimeout,
    ConnectionReconnecting,
    SecureConnRequired,
    SecureConnWanted,
    BadSubscription,
    TypeSubscription,
    BadSubject,
    BadQueueName,
    SlowConsumer,
    Timeout,
    BadTimeout,
    Authorization,
    AuthExpired,
    AuthRevoked,
    PermissionViolation,
    AccountAuthExpired,
    NoServers,
    JsonParse,
    ChanArg,
    MaxPayload,
    MaxMessages,
    SyncSubRequired,
    MultipleTLSConfigs,
    ClientCertOrRootCAsRequired,
    NoInfoReceived,
    ReconnectBufExceeded,
    InvalidConnection,
    InvalidMsg,
    InvalidArg,
    InvalidContext,
    NoDeadlineContext,
    NoEchoNotSupported,
    ClientIDNotSupported,
    UserButNoSigCB,
    NkeyButNoSigCB,
    NoUserCB,
    NkeysNotSupported,
    StaleConnection,
};

pub fn natsErrToString(err: NatsError) []const u8 {
    return switch (err) {
        NatsError.ConnectionClosed => return wrapError("connection closed"),
        NatsError.ConnectionDraining => return wrapError("connection draining"),
        NatsError.DrainTimeout => return wrapError("draining connection timed out"),
        NatsError.ConnectionReconnecting => return wrapError("connection reconnecting"),
        NatsError.SecureConnRequired => return wrapError("secure connection required"),
        NatsError.SecureConnWanted => return wrapError("secure connection not available"),
        NatsError.BadSubscription => return wrapError("invalid subscription"),
        NatsError.TypeSubscription => return wrapError("invalid subscription type"),
        NatsError.BadSubject => return wrapError("invalid subject"),
        NatsError.BadQueueName => return wrapError("invalid queue name"),
        NatsError.SlowConsumer => return wrapError("slow consumer, message dropped"),
        NatsError.Timeout => return wrapError("timeout"),
        NatsError.BadTimeout => return wrapError("timeout invalid"),
        NatsError.Authorization => return wrapError("authorization violation"),
        NatsError.AuthExpired => return wrapError("authorization expired"),
        NatsError.AuthRevoked => return wrapError("authorization revoked"),
        NatsError.PermissionViolation => return wrapError("persmssion violation"),
        NatsError.AccountAuthExpired => return wrapError("account authentication expired"),
        NatsError.NoServers => return wrapError("no servers available for connection"),
        NatsError.JsonParse => return wrapError("connect message, json parse error"),
        NatsError.ChanArg => return wrapError("argument needs to be a channel type"),
        NatsError.MaxPayload => return wrapError("maximum payload exceeded"),
        NatsError.MaxMessages => return wrapError("maximum messages delivered"),
        NatsError.SyncSubRequired => return wrapError("illegal call on an async subscription"),
        NatsError.MultipleTLSConfigs => return wrapError("multiple tls.Configs not allowed"),
        NatsError.ClientCertOrRootCAsRequired => return wrapError("at least one of certCB or rootCAsCB must be set"),
        NatsError.NoInfoReceived => return wrapError("protocol exception, INFO not received"),
        NatsError.ReconnectBufExceeded => return wrapError("outbound buffer limit exceeded"),
        NatsError.InvalidConnection => return wrapError("invalid connection"),
        NatsError.InvalidMsg => return wrapError("invalid message or message nil"),
        NatsError.InvalidArg => return wrapError("invalid argument"),
        NatsError.InvalidContext => return wrapError("invalid context"),
        NatsError.NoDeadlineContext => return wrapError("context requires a deadline"),
        NatsError.NoEchoNotSupported => return wrapError("no echo option not supported by this server"),
        NatsError.ClientIDNotSupported => return wrapError("client ID not supported by this server"),
        NatsError.UserButNoSigCB => return wrapError("user callback defined without a signature handler"),
        NatsError.NkeyButNoSigCB => return wrapError("nkey defined without a signature handler"),
        NatsError.NoUserCB => return wrapError("user callback not defined"),
        NatsError.NkeyAndUser => return wrapError("user callback and nkey defined"),
        NatsError.NkeysNotSupported => return wrapError("nkeys not supported by the server"),
        NatsError.StaleConnection => return wrapError(" + STALE_CONNECTION"),
        else => return "nats: unknown error",
    };
}

fn wrapError(str: []const u8) []const u8 {
    return std.fmt.bufPrint("nats: {}", .{str});
}
