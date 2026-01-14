const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    const nats = b.addModule("nats", .{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
    });

    const mod_tests = b.addTest(.{ .root_module = nats });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // Simple example (quickstart)
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

    const run_simple = b.step("example-simple", "Run simple example");
    const simple_cmd = b.addRunArtifact(simple_exe);
    run_simple.dependOn(&simple_cmd.step);
    simple_cmd.step.dependOn(b.getInstallStep());

    // Pub/Sub example
    const pubsub_exe = b.addExecutable(.{
        .name = "example-pubsub",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/pubsub.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(pubsub_exe);

    const run_pubsub = b.step("example-pubsub", "Run pub/sub example");
    const pubsub_cmd = b.addRunArtifact(pubsub_exe);
    run_pubsub.dependOn(&pubsub_cmd.step);
    pubsub_cmd.step.dependOn(b.getInstallStep());

    // Request/Reply example
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
        "example-request-reply",
        "Run request/reply example",
    );
    const request_reply_cmd = b.addRunArtifact(request_reply_exe);
    run_request_reply.dependOn(&request_reply_cmd.step);
    request_reply_cmd.step.dependOn(b.getInstallStep());

    // Workers (queue groups) example
    const workers_exe = b.addExecutable(.{
        .name = "example-workers",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/workers.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(workers_exe);

    const run_workers = b.step("example-workers", "Run workers (queue groups) example");
    const workers_cmd = b.addRunArtifact(workers_exe);
    run_workers.dependOn(&workers_cmd.step);
    workers_cmd.step.dependOn(b.getInstallStep());

    // Workers polling example
    const workers_polling_exe = b.addExecutable(.{
        .name = "example-workers-polling",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/workers_polling.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(workers_polling_exe);

    const run_workers_polling = b.step(
        "example-workers-polling",
        "Run workers polling example",
    );
    const workers_polling_cmd = b.addRunArtifact(workers_polling_exe);
    run_workers_polling.dependOn(&workers_polling_cmd.step);
    workers_polling_cmd.step.dependOn(b.getInstallStep());

    // Workers concurrent example (io.concurrent + Io.Queue)
    const workers_concurrent_exe = b.addExecutable(.{
        .name = "example-workers-concurrent",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/workers_concurrent.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(workers_concurrent_exe);

    const run_workers_concurrent = b.step(
        "example-workers-concurrent",
        "Run workers concurrent example (io.concurrent + Io.Queue)",
    );
    const workers_concurrent_cmd = b.addRunArtifact(workers_concurrent_exe);
    run_workers_concurrent.dependOn(&workers_concurrent_cmd.step);
    workers_concurrent_cmd.step.dependOn(b.getInstallStep());

    // Select example (io.select pattern)
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
        "example-select",
        "Run io.select() pattern example",
    );
    const select_cmd = b.addRunArtifact(select_exe);
    run_select.dependOn(&select_cmd.step);
    select_cmd.step.dependOn(b.getInstallStep());

    // Multiple subscriptions example
    const multi_sub_exe = b.addExecutable(.{
        .name = "example-multi-sub",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/multi_sub.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(multi_sub_exe);

    const run_multi_sub = b.step(
        "example-multi-sub",
        "Run multiple subscriptions example (polling)",
    );
    const multi_sub_cmd = b.addRunArtifact(multi_sub_exe);
    run_multi_sub.dependOn(&multi_sub_cmd.step);
    multi_sub_cmd.step.dependOn(b.getInstallStep());

    // Multiple subscriptions async example (io.concurrent + Io.Queue)
    const multi_sub_async_exe = b.addExecutable(.{
        .name = "example-multi-sub-async",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/multi_sub_async.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(multi_sub_async_exe);

    const run_multi_sub_async = b.step(
        "example-multi-sub-async",
        "Run multiple subscriptions async example (io.concurrent)",
    );
    const multi_sub_async_cmd = b.addRunArtifact(multi_sub_async_exe);
    run_multi_sub_async.dependOn(&multi_sub_async_cmd.step);
    multi_sub_async_cmd.step.dependOn(b.getInstallStep());

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
}
