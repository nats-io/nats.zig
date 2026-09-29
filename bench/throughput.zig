//! Throughput/latency baseline.
//!
//! (a) pub/sub throughput: publish N small messages to a subject this client is
//!     subscribed to (echo on) while a background task drains them; report the
//!     end-to-end msgs/sec (publish + server + deliver).
//! (b) request/reply RTT: a background responder echoes replies; the requester
//!     times synchronous `request()` calls and reports the distribution.
//!
//! Both phases run several internal rounds after a single connect, so the
//! reported figure is a distribution (min/median/max), not a single sample
//! dominated by connect/scheduling jitter. Counts are env-tunable so the window
//! can be sized without a rebuild:
//!   NATS_URL              server url (default TCP localhost)
//!   BENCH_TP_MSGS         messages per throughput round (default 1_000_000)
//!   BENCH_TP_ROUNDS       throughput rounds (default 5)
//!   BENCH_RTT_CALLS       timed request() calls per RTT round (default 5_000)
//!   BENCH_RTT_WARMUP      untimed warmup calls before each RTT round (default 500)
//!   BENCH_RTT_ROUNDS      RTT rounds (default 5)
//!
//! Run via `zig build bench-throughput`.

const std = @import("std");
const nats = @import("nats");
const io_backend = @import("io_backend");

const Io = std.Io;

const default_url = "nats://127.0.0.1:4222";
const throughput_payload = "0123456789abcdef"; // 16 bytes

fn envUsize(env: anytype, key: []const u8, default: usize) usize {
    const raw = env.get(key) orelse return default;
    return std.fmt.parseInt(usize, std.mem.trim(u8, raw, " \t\r\n"), 10) catch default;
}

const Stats = struct {
    min: f64,
    median: f64,
    max: f64,
    mean: f64,

    fn of(samples: []f64) Stats {
        std.mem.sort(f64, samples, {}, std.sort.asc(f64));
        var sum: f64 = 0;
        for (samples) |s| sum += s;
        return .{
            .min = samples[0],
            .median = samples[samples.len / 2],
            .max = samples[samples.len - 1],
            .mean = sum / @as(f64, @floatFromInt(samples.len)),
        };
    }
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const env = init.environ_map;
    const url = env.get("NATS_URL") orelse default_url;

    try benchThroughput(allocator, env, url);
    try benchRtt(allocator, env, url);
}

const DrainState = struct {
    sub: *nats.Client.Sub,
    target: usize,
    received: usize = 0,
};

fn drainTask(state: *DrainState) void {
    while (state.received < state.target) {
        const maybe = state.sub.nextMsgTimeout(5000) catch return;
        const msg = maybe orelse return; // timeout: leaves received < target
        msg.deinit();
        state.received += 1;
    }
}

fn benchThroughput(allocator: std.mem.Allocator, env: anytype, url: []const u8) !void {
    const msgs = envUsize(env, "BENCH_TP_MSGS", 1_000_000);
    const rounds = envUsize(env, "BENCH_TP_ROUNDS", 5);
    // Queue depth for the drain subscription. On a single executor the publish
    // loop and the in-process drain share one OS thread, so the reader can burst
    // ahead of the drain; the queue must absorb that burst or the sub drops
    // (slow-consumer). Env-tunable so the burst headroom is explicit.
    const queue: u32 = @intCast(envUsize(env, "BENCH_TP_QUEUE", 16_384));
    const batch = envUsize(env, "BENCH_TP_BATCH", 4_096);

    var backend: io_backend.Backend = undefined;
    try io_backend.init(&backend, allocator);
    defer backend.deinit();
    const io = backend.io();

    const client = try nats.Client.connect(allocator, io, url, .{ .name = "bench-tp", .sub_queue_size = queue });
    defer client.deinit();

    const sub = try client.subscribeSync("bench.tp");
    defer sub.deinit();
    try client.flush(1_000_000_000);

    const rates = try allocator.alloc(f64, rounds);
    defer allocator.free(rates);

    for (0..rounds) |round| {
        var state: DrainState = .{ .sub = sub, .target = msgs };
        // `concurrent`, not `async`: the drain must make progress *while* the
        // publish loop runs, so it keeps the subscription queue empty. A lazy
        // `async` future may not run until awaited, letting the queue overflow
        // (slow-consumer drops) on an evented backend.
        var drain = try io.concurrent(drainTask, .{&state});

        const t0 = Io.Timestamp.now(io, .awake).nanoseconds;
        var i: usize = 0;
        while (i < msgs) : (i += 1) {
            try client.publish("bench.tp", throughput_payload);
            // Pace the publisher: flush every `batch` messages so the writer
            // fiber drains the publish ring and the drain fiber pops the sub
            // queue. Without this, a tight publish loop on a single executor
            // never yields — the ring fills (PublishBufferFull) or the sub queue
            // overflows (slow-consumer). This is how a real high-rate producer
            // applies backpressure; it is not backend-specific.
            if ((i + 1) % batch == 0) try client.flush(5_000_000_000);
        }
        try client.flush(5_000_000_000);
        drain.await(io);
        const t1 = Io.Timestamp.now(io, .awake).nanoseconds;

        if (state.received != msgs) {
            std.debug.print(
                "throughput: INCOMPLETE round {d} — sent {d}, received {d} (slow-consumer drops?)\n",
                .{ round, msgs, state.received },
            );
            return error.ThroughputIncomplete;
        }

        const secs = @as(f64, @floatFromInt(@as(u64, @intCast(t1 - t0)))) / std.time.ns_per_s;
        rates[round] = @as(f64, @floatFromInt(msgs)) / secs;
    }

    const s = Stats.of(rates);
    std.debug.print(
        \\throughput (pub/sub end-to-end, backend={s}, url={s})
        \\  per round: {d} x {d}B, {d} rounds
        \\  msgs/s   : median {d:.0}  (min {d:.0}, max {d:.0}, mean {d:.0})
        \\
    , .{ io_backend.name, url, msgs, throughput_payload.len, rounds, s.median, s.min, s.max, s.mean });
}

const Responder = struct {
    client: *nats.Client,
    sub: *nats.Client.Sub,
    stop: std.atomic.Value(bool) = .init(false),
};

fn responderTask(r: *Responder) void {
    while (!r.stop.load(.acquire)) {
        const maybe = r.sub.nextMsgTimeout(200) catch return;
        const msg = maybe orelse continue;
        defer msg.deinit();
        if (msg.reply_to) |reply_to| {
            r.client.publish(reply_to, msg.data) catch {};
        }
    }
}

fn benchRtt(allocator: std.mem.Allocator, env: anytype, url: []const u8) !void {
    const calls = envUsize(env, "BENCH_RTT_CALLS", 5_000);
    const warmup = envUsize(env, "BENCH_RTT_WARMUP", 500);
    const rounds = envUsize(env, "BENCH_RTT_ROUNDS", 5);

    var svc_backend: io_backend.Backend = undefined;
    try io_backend.init(&svc_backend, allocator);
    defer svc_backend.deinit();
    const svc_io = svc_backend.io();

    var req_backend: io_backend.Backend = undefined;
    try io_backend.init(&req_backend, allocator);
    defer req_backend.deinit();
    const req_io = req_backend.io();

    const svc = try nats.Client.connect(allocator, svc_io, url, .{ .name = "bench-rtt-svc" });
    defer svc.deinit();
    const req = try nats.Client.connect(allocator, req_io, url, .{ .name = "bench-rtt-req" });
    defer req.deinit();

    const svc_sub = try svc.subscribeSync("bench.rpc");
    defer svc_sub.deinit();
    try svc.flush(1_000_000_000);

    var responder: Responder = .{ .client = svc, .sub = svc_sub };
    var resp_future = svc_io.async(responderTask, .{&responder});
    defer {
        responder.stop.store(true, .release);
        resp_future.await(svc_io);
    }

    // All timed samples across every round, plus each round's median so we can
    // report between-round stability (the axis that single-shot runs hid).
    const all = try allocator.alloc(u64, calls * rounds);
    defer allocator.free(all);
    const round_medians = try allocator.alloc(f64, rounds);
    defer allocator.free(round_medians);
    const scratch = try allocator.alloc(u64, calls);
    defer allocator.free(scratch);

    for (0..rounds) |round| {
        var n: usize = 0;
        while (n < warmup + calls) : (n += 1) {
            const start = Io.Timestamp.now(req_io, .awake).nanoseconds;
            const reply = try req.request("bench.rpc", "ping", 1000);
            const end = Io.Timestamp.now(req_io, .awake).nanoseconds;
            const r = reply orelse return error.RequestTimedOut;
            r.deinit();
            if (n >= warmup) {
                const sample: u64 = @intCast(end - start);
                all[round * calls + (n - warmup)] = sample;
                scratch[n - warmup] = sample;
            }
        }
        std.mem.sort(u64, scratch, {}, std.sort.asc(u64));
        round_medians[round] = @as(f64, @floatFromInt(scratch[calls / 2])) / 1000.0;
    }

    std.mem.sort(u64, all, {}, std.sort.asc(u64));
    const rm = Stats.of(round_medians);
    const pct = struct {
        fn at(a: []const u64, num: usize, den: usize) f64 {
            const idx = @min(a.len - 1, a.len * num / den);
            return @as(f64, @floatFromInt(a[idx])) / 1000.0;
        }
    }.at;

    std.debug.print(
        \\request/reply RTT (backend={s}, url={s})
        \\  {d} calls x {d} rounds ({d} warmup/round)
        \\  round-median: median {d:.1}  (min {d:.1}, max {d:.1}) us
        \\  aggregate   : min {d:.1}  p50 {d:.1}  p90 {d:.1}  p99 {d:.1}  p99.9 {d:.1} us
        \\
    , .{
        io_backend.name, url,               calls,               rounds,                                   warmup,
        rm.median,       rm.min,            rm.max,              @as(f64, @floatFromInt(all[0])) / 1000.0, pct(all, 1, 2),
        pct(all, 9, 10), pct(all, 99, 100), pct(all, 999, 1000),
    });
}
