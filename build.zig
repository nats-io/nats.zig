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

    // Io backend selector.
    // 'threaded' = std.Io.Threaded (default, OS threads).
    // 'evented'  = std.Io.Evented (Linux: Uring, BSD: Kqueue, Apple: Dispatch).
    const io_backend_choice = b.option(
        []const u8,
        "io_backend",
        "Io backend: 'threaded' (default) or 'evented'",
    ) orelse "threaded";

    // Create build options module. Share a single Module instance
    // across all consumers (nats, io_backend, ...) — calling
    // createModule() twice would generate two distinct Modules
    // pointing at the same options.zig file, which Zig rejects
    // when both end up in the same compile graph.
    const build_options = b.addOptions();
    build_options.addOption(bool, "enable_debug", enable_debug);
    build_options.addOption([]const u8, "io_backend", io_backend_choice);
    const build_options_mod = build_options.createModule();

    const nats = b.addModule("nats", .{
        .root_source_file = b.path("src/nats.zig"),
        .target = target,
        .imports = &.{
            .{ .name = "build_options", .module = build_options_mod },
        },
    });

    const mod_tests = b.addTest(.{ .root_module = nats });
    const run_mod_tests = b.addRunArtifact(mod_tests);

    const test_step = b.step("test", "Run tests");
    test_step.dependOn(&run_mod_tests.step);

    // Backend selector module. Used by entry points (examples,
    // integration tests) so they can flip between
    // std.Io.Threaded and std.Io.Evented via -Dio_backend=...
    // The library module itself does NOT depend on this; only
    // application code chooses a backend.
    const io_backend_mod = b.createModule(.{
        .root_source_file = b.path("src/io_backend.zig"),
        .target = target,
        .imports = &.{
            .{
                .name = "build_options",
                .module = build_options_mod,
            },
        },
    });

    // Standalone test for the io_backend selector module. Ensures
    // src/io_backend.zig compiles under -Dio_backend=threaded and
    // -Dio_backend=evented.
    const io_backend_tests = b.addTest(.{ .root_module = io_backend_mod });
    const run_io_backend_tests = b.addRunArtifact(io_backend_tests);
    test_step.dependOn(&run_io_backend_tests.step);

    // 1. Simple example (hello world - entry point)
    const simple_exe = b.addExecutable(.{
        .name = "example-simple",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/simple.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
                .{ .name = "io_backend", .module = io_backend_mod },
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
                .{ .name = "io_backend", .module = io_backend_mod },
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

    // Headers example (metadata with HPUB/HMSG)
    const headers_exe = b.addExecutable(.{
        .name = "example-headers",
        .root_module = b.createModule(.{
            .root_source_file = b.path("src/examples/headers.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
                .{ .name = "io_backend", .module = io_backend_mod },
            },
        }),
    });
    b.installArtifact(headers_exe);

    const run_headers = b.step(
        "run-headers",
        "Run headers example",
    );
    const headers_cmd = b.addRunArtifact(headers_exe);
    run_headers.dependOn(&headers_cmd.step);
    headers_cmd.step.dependOn(b.getInstallStep());

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
                .{ .name = "io_backend", .module = io_backend_mod },
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

    // 10. Callback Subscriptions example
    const callback_exe = b.addExecutable(.{
        .name = "example-callback",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/callback.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(callback_exe);

    const run_callback = b.step(
        "run-callback",
        "Run callback subscriptions example",
    );
    const callback_cmd = b.addRunArtifact(callback_exe);
    run_callback.dependOn(&callback_cmd.step);
    callback_cmd.step.dependOn(b.getInstallStep());

    // 11. Request/Reply with Callback example
    const req_rep_cb_exe = b.addExecutable(.{
        .name = "example-request-reply-callback",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/request_reply_callback.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
                .{ .name = "io_backend", .module = io_backend_mod },
            },
        }),
    });
    b.installArtifact(req_rep_cb_exe);

    const run_req_rep_cb = b.step(
        "run-request-reply-callback",
        "Run request/reply callback example",
    );
    const req_rep_cb_cmd = b.addRunArtifact(req_rep_cb_exe);
    run_req_rep_cb.dependOn(&req_rep_cb_cmd.step);
    req_rep_cb_cmd.step.dependOn(b.getInstallStep());

    // 12. JetStream Publish example
    const js_pub_exe = b.addExecutable(.{
        .name = "example-jetstream-publish",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/jetstream_publish.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(js_pub_exe);

    const run_js_pub = b.step(
        "run-jetstream-publish",
        "Run JetStream publish example",
    );
    const js_pub_cmd = b.addRunArtifact(js_pub_exe);
    run_js_pub.dependOn(&js_pub_cmd.step);
    js_pub_cmd.step.dependOn(b.getInstallStep());

    // 13. JetStream Consume example
    const js_consume_exe = b.addExecutable(.{
        .name = "example-jetstream-consume",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/jetstream_consume.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(js_consume_exe);

    const run_js_consume = b.step(
        "run-jetstream-consume",
        "Run JetStream pull consumer example",
    );
    const js_consume_cmd = b.addRunArtifact(
        js_consume_exe,
    );
    run_js_consume.dependOn(&js_consume_cmd.step);
    js_consume_cmd.step.dependOn(b.getInstallStep());

    // 14. JetStream Push Consumer example
    const js_push_exe = b.addExecutable(.{
        .name = "example-jetstream-push",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/jetstream_push.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(js_push_exe);

    const run_js_push = b.step(
        "run-jetstream-push",
        "Run JetStream push consumer example",
    );
    const js_push_cmd = b.addRunArtifact(js_push_exe);
    run_js_push.dependOn(&js_push_cmd.step);
    js_push_cmd.step.dependOn(b.getInstallStep());

    // 15. JetStream Async Publish example
    const js_async_exe = b.addExecutable(.{
        .name = "example-jetstream-async-publish",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/jetstream_async_publish.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(js_async_exe);

    const run_js_async = b.step(
        "run-jetstream-async-publish",
        "Run JetStream async publish example",
    );
    const js_async_cmd = b.addRunArtifact(
        js_async_exe,
    );
    run_js_async.dependOn(&js_async_cmd.step);
    js_async_cmd.step.dependOn(b.getInstallStep());

    // 16. Key-Value Store example
    const kv_exe = b.addExecutable(.{
        .name = "example-kv",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/kv.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(kv_exe);

    const run_kv = b.step(
        "run-kv",
        "Run KV store example",
    );
    const kv_cmd = b.addRunArtifact(kv_exe);
    run_kv.dependOn(&kv_cmd.step);
    kv_cmd.step.dependOn(b.getInstallStep());

    // 17. Key-Value Watch example
    const kv_watch_exe = b.addExecutable(.{
        .name = "example-kv-watch",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/kv_watch.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(kv_watch_exe);

    const run_kv_watch = b.step(
        "run-kv-watch",
        "Run KV watch example",
    );
    const kv_watch_cmd = b.addRunArtifact(
        kv_watch_exe,
    );
    run_kv_watch.dependOn(&kv_watch_cmd.step);
    kv_watch_cmd.step.dependOn(b.getInstallStep());

    // 18. Microservices echo example
    const micro_echo_exe = b.addExecutable(.{
        .name = "example-micro-echo",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/examples/micro_echo.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(micro_echo_exe);

    const run_micro_echo = b.step(
        "run-micro-echo",
        "Run microservices echo example",
    );
    const micro_echo_cmd = b.addRunArtifact(
        micro_echo_exe,
    );
    run_micro_echo.dependOn(&micro_echo_cmd.step);
    micro_echo_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: Pub-Sub messaging
    const nbe_pubsub_exe = b.addExecutable(.{
        .name = "nbe-messaging-pub-sub",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/messaging/pub-sub.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_pubsub_exe);

    const run_nbe_pubsub = b.step(
        "run-nbe-messaging-pub-sub",
        "Run NATS by Example: Pub-Sub messaging",
    );
    const nbe_pubsub_cmd = b.addRunArtifact(nbe_pubsub_exe);
    run_nbe_pubsub.dependOn(&nbe_pubsub_cmd.step);
    nbe_pubsub_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: Request-Reply
    const nbe_reqrep_exe = b.addExecutable(.{
        .name = "nbe-messaging-request-reply",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/messaging/request-reply.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_reqrep_exe);

    const run_nbe_reqrep = b.step(
        "run-nbe-messaging-request-reply",
        "Run NATS by Example: Request-Reply",
    );
    const nbe_reqrep_cmd = b.addRunArtifact(nbe_reqrep_exe);
    run_nbe_reqrep.dependOn(&nbe_reqrep_cmd.step);
    nbe_reqrep_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: JSON payloads
    const nbe_json_exe = b.addExecutable(.{
        .name = "nbe-messaging-json",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/messaging/json.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_json_exe);

    const run_nbe_json = b.step(
        "run-nbe-messaging-json",
        "Run NATS by Example: JSON payloads",
    );
    const nbe_json_cmd = b.addRunArtifact(nbe_json_exe);
    run_nbe_json.dependOn(&nbe_json_cmd.step);
    nbe_json_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: Concurrent processing
    const nbe_concurrent_exe = b.addExecutable(.{
        .name = "nbe-messaging-concurrent",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/messaging/concurrent.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_concurrent_exe);

    const run_nbe_concurrent = b.step(
        "run-nbe-messaging-concurrent",
        "Run NATS by Example: Concurrent processing",
    );
    const nbe_concurrent_cmd = b.addRunArtifact(
        nbe_concurrent_exe,
    );
    run_nbe_concurrent.dependOn(&nbe_concurrent_cmd.step);
    nbe_concurrent_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: Iterating multiple subscriptions
    const nbe_multisub_exe = b.addExecutable(.{
        .name = "nbe-messaging-iterating-multiple-subscriptions",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/messaging/" ++
                    "iterating-multiple-subscriptions.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_multisub_exe);

    const run_nbe_multisub = b.step(
        "run-nbe-messaging-iterating-multiple-subscriptions",
        "Run NATS by Example: Multiple subscriptions",
    );
    const nbe_multisub_cmd = b.addRunArtifact(
        nbe_multisub_exe,
    );
    run_nbe_multisub.dependOn(&nbe_multisub_cmd.step);
    nbe_multisub_cmd.step.dependOn(b.getInstallStep());

    // NATS by Example: NKeys and JWTs (auth)
    const nbe_nkeys_jwts_exe = b.addExecutable(.{
        .name = "nbe-auth-nkeys-jwts",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "doc/nats-by-example/auth/nkeys-jwts.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
            },
        }),
    });
    b.installArtifact(nbe_nkeys_jwts_exe);

    const run_nbe_nkeys_jwts = b.step(
        "run-nbe-auth-nkeys-jwts",
        "Run NATS by Example: NKeys and JWTs",
    );
    const nbe_nkeys_jwts_cmd = b.addRunArtifact(
        nbe_nkeys_jwts_exe,
    );
    run_nbe_nkeys_jwts.dependOn(&nbe_nkeys_jwts_cmd.step);
    nbe_nkeys_jwts_cmd.step.dependOn(b.getInstallStep());

    const fmt = b.addFmt(.{
        .paths = &.{ "src", "doc", "build.zig" },
        .check = false,
    });
    const fmt_step = b.step("fmt", "Format source code");
    fmt_step.dependOn(&fmt.step);

    const fmt_check = b.addFmt(.{
        .paths = &.{ "src", "doc", "build.zig" },
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
                .{ .name = "io_backend", .module = io_backend_mod },
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

    // Micro-only integration tests (faster; just the micro suite).
    const micro_integration_exe = b.addExecutable(.{
        .name = "micro-integration-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/testing/micro_integration_test.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
                .{ .name = "io_backend", .module = io_backend_mod },
            },
        }),
    });
    b.installArtifact(micro_integration_exe);

    const run_micro_integration = b.step(
        "test-integration-micro",
        "Run only the micro integration tests",
    );
    const micro_integration_cmd = b.addRunArtifact(
        micro_integration_exe,
    );
    run_micro_integration.dependOn(&micro_integration_cmd.step);

    // Focused JWT/TLS integration tests.
    const tls_integration_exe = b.addExecutable(.{
        .name = "tls-integration-test",
        .root_module = b.createModule(.{
            .root_source_file = b.path(
                "src/testing/tls_integration_test.zig",
            ),
            .target = target,
            .optimize = optimize,
            .imports = &.{
                .{ .name = "nats", .module = nats },
                .{ .name = "io_backend", .module = io_backend_mod },
            },
        }),
    });
    b.installArtifact(tls_integration_exe);

    const run_tls_integration = b.step(
        "test-integration-tls",
        "Run only the TLS integration tests",
    );
    const tls_integration_cmd = b.addRunArtifact(
        tls_integration_exe,
    );
    run_tls_integration.dependOn(&tls_integration_cmd.step);
}
