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

    // Connect example
    const connect_exe = b.addExecutable(.{
        .name = "connect",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/connect.zig"),
            .target = target,
            .optimize = optimize,
        }),
    });
    b.installArtifact(connect_exe);

    const run_connect = b.step("run-connect", "Run connect example");
    const connect_cmd = b.addRunArtifact(connect_exe);
    run_connect.dependOn(&connect_cmd.step);
    connect_cmd.step.dependOn(b.getInstallStep());

    // Pub/Sub example
    const pubsub_exe = b.addExecutable(.{
        .name = "pubsub",
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

    const run_pubsub = b.step("run-pubsub", "Run pub/sub example");
    const pubsub_cmd = b.addRunArtifact(pubsub_exe);
    run_pubsub.dependOn(&pubsub_cmd.step);
    pubsub_cmd.step.dependOn(b.getInstallStep());

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

    // Performance test orchestrator
    const perf_test_exe = b.addExecutable(.{
        .name = "perf-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/testing/performance_tests.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(perf_test_exe);

    const run_perf_test = b.step(
        "run-perf-test",
        "Run performance comparison tests",
    );
    const perf_test_cmd = b.addRunArtifact(perf_test_exe);
    if (b.args) |args| perf_test_cmd.addArgs(args);
    run_perf_test.dependOn(&perf_test_cmd.step);
    perf_test_cmd.step.dependOn(b.getInstallStep());
}
