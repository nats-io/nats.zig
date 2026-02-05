const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Debug option for reconnection events (default: false)
    const enable_debug = b.option(
        bool,
        "EnableDebug",
        "Enable debug prints for reconnection events (default: false)",
    ) orelse false;

    // Create build options module
    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_debug", enable_debug);

    const nats = b.addModule("nats", .{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "build_options", .module = build_options.createModule() },
        },
    });

    const mod_tests = b.addTest(.{ .root_module = nats });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // 1. Simple example (hello world - entry point)
    const simple_exe = b.addExecutable(.{
        .name = "example-simple",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/simple.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(simple_exe);

    const run_simple = b.step("run-simple", "Run simple hello world example");
    const simple_cmd = b.addRunArtifact(simple_exe);
    run_simple.dependOn(&simple_cmd.step);
    simple_cmd.step.dependOn(b.getInstallStep());

    // 2. Request/Reply example (RPC pattern)
    const request_reply_exe = b.addExecutable(.{
        .name = "example-request-reply",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/request_reply.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(request_reply_exe);

    const run_request_reply = b.step(
        "run-request-reply",
        "Run request/reply RPC example",
    );
    const request_reply_cmd = b.addRunArtifact(request_reply_exe);
    run_request_reply.dependOn(&request_reply_cmd.step);
    request_reply_cmd.step.dependOn(b.getInstallStep());

    // 3. Queue Groups example (load balancing with workers)
    const queue_groups_exe = b.addExecutable(.{
        .name = "example-queue-groups",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/queue_groups.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(queue_groups_exe);

    const run_queue_groups = b.step(
        "run-queue-groups",
        "Run queue groups (load balancing) example",
    );
    const queue_groups_cmd = b.addRunArtifact(queue_groups_exe);
    run_queue_groups.dependOn(&queue_groups_cmd.step);
    queue_groups_cmd.step.dependOn(b.getInstallStep());

    // 4. Select example (io.select timeout pattern)
    const select_exe = b.addExecutable(.{
        .name = "example-select",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/select.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(select_exe);

    const run_select = b.step(
        "run-select",
        "Run io.select() async timeout example",
    );
    const select_cmd = b.addRunArtifact(select_exe);
    run_select.dependOn(&select_cmd.step);
    select_cmd.step.dependOn(b.getInstallStep());

    // 5. Batch Receiving example (efficient batch message retrieval)
    const batch_exe = b.addExecutable(.{
        .name = "example-batch-receiving",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/batch_receiving.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(batch_exe);

    const run_batch = b.step(
        "run-batch-receiving",
        "Run batch receiving patterns example",
    );
    const batch_cmd = b.addRunArtifact(batch_exe);
    run_batch.dependOn(&batch_cmd.step);
    batch_cmd.step.dependOn(b.getInstallStep());

    // 6. Graceful Shutdown example (drain and lifecycle)
    const shutdown_exe = b.addExecutable(.{
        .name = "example-graceful-shutdown",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/graceful_shutdown.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(shutdown_exe);

    const run_shutdown = b.step(
        "run-graceful-shutdown",
        "Run graceful shutdown (drain) example",
    );
    const shutdown_cmd = b.addRunArtifact(shutdown_exe);
    run_shutdown.dependOn(&shutdown_cmd.step);
    shutdown_cmd.step.dependOn(b.getInstallStep());

    // 7. Reconnection example (resilience patterns)
    const reconnect_exe = b.addExecutable(.{
        .name = "example-reconnection",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/reconnection.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(reconnect_exe);

    const run_reconnect = b.step(
        "run-reconnection",
        "Run reconnection resilience example",
    );
    const reconnect_cmd = b.addRunArtifact(reconnect_exe);
    run_reconnect.dependOn(&reconnect_cmd.step);
    reconnect_cmd.step.dependOn(b.getInstallStep());

    // 8. Polling Loop example (non-blocking patterns)
    const polling_exe = b.addExecutable(.{
        .name = "example-polling-loop",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/polling_loop.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(polling_exe);

    const run_polling = b.step(
        "run-polling-loop",
        "Run non-blocking polling loop example",
    );
    const polling_cmd = b.addRunArtifact(polling_exe);
    run_polling.dependOn(&polling_cmd.step);
    polling_cmd.step.dependOn(b.getInstallStep());

    // 9. Event Callbacks example (connection lifecycle)
    const events_exe = b.addExecutable(.{
        .name = "example-events",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/events.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(events_exe);

    const run_events = b.step(
        "run-events",
        "Run event callbacks (connection lifecycle) example",
    );
    const events_cmd = b.addRunArtifact(events_exe);
    run_events.dependOn(&events_cmd.step);
    events_cmd.step.dependOn(b.getInstallStep());

    const fmt = b.addFmt(.{
        .paths = &.{ "src", "build.zig" },
        .check = false,
    });
    const fmt_step = b.step("fmt", "Format source code");
    fmt_step.dependOn(&fmt.step);

    const fmt_check = b.addFmt(.{
        .paths = &.{ "src", "build.zig" },
        .check = true,
    });
    const fmt_check_step = b.step("fmt-check", "Check formatting");
    fmt_check_step.dependOn(&fmt_check.step);

    // Integration tests (requires nats-server)
    const integration_exe = b.addExecutable(.{
        .name = "integration-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/testing/integration_test.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(integration_exe);

    const run_integration = b.step(
        "test-integration",
        "Run integration tests (requires nats-server)",
    );
    const integration_cmd = b.addRunArtifact(integration_exe);
    run_integration.dependOn(&integration_cmd.step);
    integration_cmd.step.dependOn(b.getInstallStep());

    // Publisher benchmark
    const bench_pub_exe = b.addExecutable(.{
        .name = "bench-pub",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tools/bench-pub.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(bench_pub_exe);

    const run_bench_pub = b.step(
        "run-bench-pub",
        "Run publisher benchmark (pass args after --)",
    );
    const bench_pub_cmd = b.addRunArtifact(bench_pub_exe);
    if (b.args) |args| bench_pub_cmd.addArgs(args);
    run_bench_pub.dependOn(&bench_pub_cmd.step);
    bench_pub_cmd.step.dependOn(b.getInstallStep());

    // Subscriber benchmark
    const bench_sub_exe = b.addExecutable(.{
        .name = "bench-sub",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/tools/bench-sub.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(bench_sub_exe);

    const run_bench_sub = b.step(
        "run-bench-sub",
        "Run subscriber benchmark (pass args after --)",
    );
    const bench_sub_cmd = b.addRunArtifact(bench_sub_exe);
    if (b.args) |args| bench_sub_cmd.addArgs(args);
    run_bench_sub.dependOn(&bench_sub_cmd.step);
    bench_sub_cmd.step.dependOn(b.getInstallStep());

    // Performance benchmark orchestrator (multi-client comparison)
    const perf_bench_exe = b.addExecutable(.{
        .name = "perf-bench",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/testing/performance_bench.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(perf_bench_exe);

    const run_perf_bench = b.step(
        "run-perf-bench",
        "Run multi-client performance benchmarks (Zig, C, Rust, Go)",
    );
    const perf_bench_cmd = b.addRunArtifact(perf_bench_exe);
    if (b.args) |args| perf_bench_cmd.addArgs(args);
    run_perf_bench.dependOn(&perf_bench_cmd.step);
    perf_bench_cmd.step.dependOn(b.getInstallStep());

    // Quick performance benchmark (Zig io_u, Zig, Go only - faster)
    const perf_bench_quick_exe = b.addExecutable(.{
        .name = "perf-bench-quick",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/testing/performance_bench_quick.zig",
            ),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(perf_bench_quick_exe);

    const run_perf_bench_quick = b.step(
        "run-perf-bench-quick",
        "Run quick performance benchmarks (Zig io_u, Zig, Go only)",
    );
    const perf_bench_quick_cmd = b.addRunArtifact(perf_bench_quick_exe);
    if (b.args) |args| perf_bench_quick_cmd.addArgs(args);
    run_perf_bench_quick.dependOn(&perf_bench_quick_cmd.step);
    perf_bench_quick_cmd.step.dependOn(b.getInstallStep());
}
