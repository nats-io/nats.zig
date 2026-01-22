//! Subscription Module Tests
//!
//! Comprehensive edge case tests for FixedQueue and FixedSubscription.
//! Tests ring buffer wraparound, capacity limits, state transitions,
//! and subject validation edge cases.

const std = @import("std");
const testing = std.testing;
const subscription = @import("subscription.zig");
const FixedQueue = subscription.FixedQueue;
const FixedSubscription = subscription.FixedSubscription;
const State = subscription.State;

// Test types for FixedSubscription testing
const TestClient = struct {
    dummy: u32 = 0,
};

const TestMessage = struct {
    data: u32,
};

const TestConfig = subscription.FixedSubConfig{
    .max_subject_len = 64,
    .max_queue_group_len = 32,
    .queue_capacity = 8,
};

const TestSub = FixedSubscription(TestClient, TestMessage, TestConfig);

// Section 1: FixedQueue Basic Operations

test "FixedQueue push and pop basic" {
    var q: FixedQueue(u32, 4) = .{};
    try testing.expectEqual(@as(u16, 0), q.count);

    try q.push(1);
    try q.push(2);
    try q.push(3);

    try testing.expectEqual(@as(u16, 3), q.count);
    try testing.expectEqual(@as(?u32, 1), q.tryPop());
    try testing.expectEqual(@as(?u32, 2), q.tryPop());
    try testing.expectEqual(@as(?u32, 3), q.tryPop());
    try testing.expectEqual(@as(u16, 0), q.count);
}

test "FixedQueue full returns error" {
    var q: FixedQueue(u32, 2) = .{};

    try q.push(1);
    try q.push(2);
    try testing.expectError(error.QueueFull, q.push(3));
    try testing.expectEqual(@as(u16, 2), q.count);
}

test "FixedQueue pop from empty returns null" {
    var q: FixedQueue(u32, 4) = .{};
    try testing.expectEqual(@as(?u32, null), q.tryPop());
    try testing.expectEqual(@as(?u32, null), q.tryPop());
    try testing.expectEqual(@as(u16, 0), q.count);
}

// Section 2: FixedQueue Wraparound Behavior

test "FixedQueue wraparound single cycle" {
    // Capacity 4: fill, empty, refill to test wraparound
    var q: FixedQueue(u32, 4) = .{};

    // Fill completely
    try q.push(1);
    try q.push(2);
    try q.push(3);
    try q.push(4);
    try testing.expectError(error.QueueFull, q.push(5));

    // Empty completely
    try testing.expectEqual(@as(?u32, 1), q.tryPop());
    try testing.expectEqual(@as(?u32, 2), q.tryPop());
    try testing.expectEqual(@as(?u32, 3), q.tryPop());
    try testing.expectEqual(@as(?u32, 4), q.tryPop());
    try testing.expectEqual(@as(?u32, null), q.tryPop());

    // Refill - now head/tail have wrapped
    try q.push(10);
    try q.push(20);
    try q.push(30);
    try q.push(40);

    // Verify FIFO order maintained
    try testing.expectEqual(@as(?u32, 10), q.tryPop());
    try testing.expectEqual(@as(?u32, 20), q.tryPop());
    try testing.expectEqual(@as(?u32, 30), q.tryPop());
    try testing.expectEqual(@as(?u32, 40), q.tryPop());
}

test "FixedQueue wraparound interleaved push pop" {
    var q: FixedQueue(u32, 4) = .{};

    // Push 2, pop 1, repeat - causes gradual wraparound
    try q.push(1);
    try q.push(2);
    try testing.expectEqual(@as(?u32, 1), q.tryPop());

    try q.push(3);
    try q.push(4);
    try testing.expectEqual(@as(?u32, 2), q.tryPop());

    try q.push(5);
    try q.push(6);
    try testing.expectEqual(@as(?u32, 3), q.tryPop());

    // Continue until wrapped multiple times
    try q.push(7);
    try testing.expectEqual(@as(?u32, 4), q.tryPop());
    try q.push(8);
    try testing.expectEqual(@as(?u32, 5), q.tryPop());

    // Verify remaining
    try testing.expectEqual(@as(?u32, 6), q.tryPop());
    try testing.expectEqual(@as(?u32, 7), q.tryPop());
    try testing.expectEqual(@as(?u32, 8), q.tryPop());
    try testing.expectEqual(@as(?u32, null), q.tryPop());
}

test "FixedQueue wraparound stress" {
    var q: FixedQueue(u32, 8) = .{};

    // 100 push/pop cycles to stress wraparound
    var i: u32 = 0;
    while (i < 100) : (i += 1) {
        try q.push(i);
        const val = q.tryPop();
        try testing.expectEqual(@as(?u32, i), val);
    }

    try testing.expectEqual(@as(u16, 0), q.count);
}

// Section 3: FixedQueue Clear Operations

test "FixedQueue clear non-empty" {
    var q: FixedQueue(u32, 4) = .{};

    try q.push(1);
    try q.push(2);
    try q.push(3);
    try testing.expectEqual(@as(u16, 3), q.count);

    q.clear();

    try testing.expectEqual(@as(u16, 0), q.count);
    try testing.expectEqual(@as(?u32, null), q.tryPop());

    // Should be able to push again
    try q.push(100);
    try testing.expectEqual(@as(?u32, 100), q.tryPop());
}

test "FixedQueue clear empty" {
    var q: FixedQueue(u32, 4) = .{};

    q.clear();
    q.clear(); // Double clear
    q.clear();

    try testing.expectEqual(@as(u16, 0), q.count);
    try testing.expectEqual(@as(?u32, null), q.tryPop());
}

test "FixedQueue clear after wraparound" {
    var q: FixedQueue(u32, 4) = .{};

    // Cause wraparound
    try q.push(1);
    try q.push(2);
    _ = q.tryPop();
    _ = q.tryPop();
    try q.push(3);
    try q.push(4);

    q.clear();

    try testing.expectEqual(@as(u16, 0), q.count);
    try testing.expectEqual(@as(u16, 0), q.head);
    try testing.expectEqual(@as(u16, 0), q.tail);
}

// Section 4: FixedQueue Edge Capacities

test "FixedQueue capacity 1" {
    var q: FixedQueue(u32, 1) = .{};

    try q.push(42);
    try testing.expectError(error.QueueFull, q.push(43));
    try testing.expectEqual(@as(?u32, 42), q.tryPop());
    try testing.expectEqual(@as(?u32, null), q.tryPop());

    // Can push again
    try q.push(100);
    try testing.expectEqual(@as(?u32, 100), q.tryPop());
}

test "FixedQueue capacity 2" {
    var q: FixedQueue(u32, 2) = .{};

    try q.push(1);
    try q.push(2);
    try testing.expectError(error.QueueFull, q.push(3));

    try testing.expectEqual(@as(?u32, 1), q.tryPop());
    try q.push(3);
    try testing.expectEqual(@as(?u32, 2), q.tryPop());
    try testing.expectEqual(@as(?u32, 3), q.tryPop());
}

test "FixedQueue capacity 256" {
    var q: FixedQueue(u32, 256) = .{};

    // Fill to capacity
    var i: u32 = 0;
    while (i < 256) : (i += 1) {
        try q.push(i);
    }

    try testing.expectError(error.QueueFull, q.push(256));
    try testing.expectEqual(@as(u16, 256), q.count);

    // Verify FIFO order
    i = 0;
    while (i < 256) : (i += 1) {
        try testing.expectEqual(@as(?u32, i), q.tryPop());
    }
}

// Section 5: FixedQueue Count Accuracy

test "FixedQueue count accuracy through operations" {
    var q: FixedQueue(u32, 8) = .{};

    try testing.expectEqual(@as(u16, 0), q.count);

    try q.push(1);
    try testing.expectEqual(@as(u16, 1), q.count);

    try q.push(2);
    try q.push(3);
    try testing.expectEqual(@as(u16, 3), q.count);

    _ = q.tryPop();
    try testing.expectEqual(@as(u16, 2), q.count);

    _ = q.tryPop();
    _ = q.tryPop();
    try testing.expectEqual(@as(u16, 0), q.count);

    // Pop from empty doesn't affect count
    _ = q.tryPop();
    try testing.expectEqual(@as(u16, 0), q.count);
}

test "FixedQueue count with failed push" {
    var q: FixedQueue(u32, 2) = .{};

    try q.push(1);
    try q.push(2);
    try testing.expectEqual(@as(u16, 2), q.count);

    // Failed push shouldn't change count
    try testing.expectError(error.QueueFull, q.push(3));
    try testing.expectEqual(@as(u16, 2), q.count);
}

// Section 6: FixedQueue FIFO Order

test "FixedQueue strict FIFO order" {
    var q: FixedQueue(u32, 16) = .{};

    const values = [_]u32{ 100, 200, 300, 400, 500, 600, 700, 800 };
    for (values) |v| {
        try q.push(v);
    }

    for (values) |expected| {
        try testing.expectEqual(@as(?u32, expected), q.tryPop());
    }
}

test "FixedQueue FIFO with interleaved operations" {
    var q: FixedQueue(u32, 4) = .{};

    try q.push(1);
    try q.push(2);
    try testing.expectEqual(@as(?u32, 1), q.tryPop());
    try q.push(3);
    try testing.expectEqual(@as(?u32, 2), q.tryPop());
    try q.push(4);
    try q.push(5);
    try testing.expectEqual(@as(?u32, 3), q.tryPop());
    try testing.expectEqual(@as(?u32, 4), q.tryPop());
    try testing.expectEqual(@as(?u32, 5), q.tryPop());
}

// Section 7: FixedQueue Different Types

test "FixedQueue with struct type" {
    const Item = struct { id: u32, value: u64 };
    var q: FixedQueue(Item, 4) = .{};

    try q.push(.{ .id = 1, .value = 100 });
    try q.push(.{ .id = 2, .value = 200 });

    const item1 = q.tryPop().?;
    try testing.expectEqual(@as(u32, 1), item1.id);
    try testing.expectEqual(@as(u64, 100), item1.value);

    const item2 = q.tryPop().?;
    try testing.expectEqual(@as(u32, 2), item2.id);
    try testing.expectEqual(@as(u64, 200), item2.value);
}

test "FixedQueue with pointer type" {
    var values = [_]u32{ 10, 20, 30 };
    var q: FixedQueue(*u32, 4) = .{};

    try q.push(&values[0]);
    try q.push(&values[1]);
    try q.push(&values[2]);

    const p1 = q.tryPop().?;
    try testing.expectEqual(@as(u32, 10), p1.*);

    const p2 = q.tryPop().?;
    try testing.expectEqual(@as(u32, 20), p2.*);
}

// Section 8: FixedSubscription initEmpty

test "FixedSubscription initEmpty defaults" {
    const sub = TestSub.initEmpty();

    try testing.expectEqual(@as(u64, 0), sub.sid);
    try testing.expectEqual(@as(u8, 0), sub.subject_len);
    try testing.expectEqual(@as(u8, 0), sub.queue_group_len);
    try testing.expectEqual(State.unsubscribed, sub.state);
    try testing.expectEqual(@as(u64, 0), sub.max_msgs);
    try testing.expectEqual(@as(u64, 0), sub.received_msgs);
    try testing.expectEqual(false, sub.active);
}

test "FixedSubscription initEmpty subject accessor" {
    const sub = TestSub.initEmpty();

    // subject() returns empty slice when inactive
    const s = sub.subject();
    try testing.expectEqual(@as(usize, 0), s.len);
}

test "FixedSubscription initEmpty queueGroup accessor" {
    const sub = TestSub.initEmpty();

    // queueGroup() returns null when not set
    try testing.expect(sub.queueGroup() == null);
}

// Section 9: FixedSubscription activate Valid Cases

test "FixedSubscription activate basic" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 123, "test.subject", null);

    try testing.expectEqual(@as(u64, 123), sub.sid);
    try testing.expectEqualStrings("test.subject", sub.subject());
    try testing.expect(sub.queueGroup() == null);
    try testing.expectEqual(State.active, sub.state);
    try testing.expectEqual(true, sub.active);
    try testing.expectEqual(true, sub.isActive());
}

test "FixedSubscription activate with queue group" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 456, "orders", "workers");

    try testing.expectEqualStrings("orders", sub.subject());
    try testing.expectEqualStrings("workers", sub.queueGroup().?);
    try testing.expectEqual(true, sub.isActive());
}

test "FixedSubscription activate subject at max length" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Config has max_subject_len = 64
    const max_subject = "a" ** 64;
    try sub.activate(&client, 1, max_subject, null);

    try testing.expectEqual(@as(usize, 64), sub.subject().len);
    try testing.expectEqualStrings(max_subject, sub.subject());
}

test "FixedSubscription activate queue_group at max length" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Config has max_queue_group_len = 32
    const max_qg = "q" ** 32;
    try sub.activate(&client, 1, "test", max_qg);

    try testing.expectEqual(@as(usize, 32), sub.queueGroup().?.len);
    try testing.expectEqualStrings(max_qg, sub.queueGroup().?);
}

// Section 10: FixedSubscription activate Error Cases

test "FixedSubscription activate subject too long" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Config has max_subject_len = 64
    const long_subject = "a" ** 65;
    try testing.expectError(
        error.SubjectTooLong,
        sub.activate(&client, 1, long_subject, null),
    );

    // Should remain inactive
    try testing.expectEqual(false, sub.active);
}

test "FixedSubscription activate queue_group too long" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Config has max_queue_group_len = 32
    const long_qg = "q" ** 33;
    try testing.expectError(
        error.QueueGroupTooLong,
        sub.activate(&client, 1, "test", long_qg),
    );

    try testing.expectEqual(false, sub.active);
}

test "FixedSubscription activate empty subject returns error" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try testing.expectError(
        error.EmptySubject,
        sub.activate(&client, 1, "", null),
    );

    try testing.expectEqual(false, sub.active);
}

// Section 11: FixedSubscription deactivate

test "FixedSubscription deactivate resets state" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 123, "test", null);
    try testing.expectEqual(true, sub.isActive());

    sub.deactivate();

    try testing.expectEqual(false, sub.active);
    try testing.expectEqual(State.unsubscribed, sub.state);
    try testing.expectEqual(@as(u64, 0), sub.sid);
    try testing.expectEqual(false, sub.isActive());
}

test "FixedSubscription deactivate on inactive" {
    var sub = TestSub.initEmpty();

    // Should be safe to deactivate an inactive slot
    sub.deactivate();
    sub.deactivate();

    try testing.expectEqual(false, sub.active);
}

test "FixedSubscription reactivate after deactivate" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "first.subject", null);
    sub.deactivate();

    try sub.activate(&client, 2, "second.subject", "queue");

    try testing.expectEqual(@as(u64, 2), sub.sid);
    try testing.expectEqualStrings("second.subject", sub.subject());
    try testing.expectEqualStrings("queue", sub.queueGroup().?);
    try testing.expectEqual(true, sub.isActive());
}

// Section 12: FixedSubscription State Transitions

test "FixedSubscription drain from active" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "test", null);
    try testing.expectEqual(State.active, sub.state);

    sub.drain();

    try testing.expectEqual(State.draining, sub.state);
    // Note: isActive() returns false when draining
    try testing.expectEqual(false, sub.isActive());
}

test "FixedSubscription drain from non-active is noop" {
    var sub = TestSub.initEmpty();

    sub.drain();
    try testing.expectEqual(State.unsubscribed, sub.state);

    // Set to draining, drain again should be noop
    var client = TestClient{};
    try sub.activate(&client, 1, "test", null);
    sub.drain();
    try testing.expectEqual(State.draining, sub.state);

    sub.drain(); // Should not change state
    try testing.expectEqual(State.draining, sub.state);
}

test "FixedSubscription isActive requires both flags" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Initially: state=unsubscribed, active=false -> isActive=false
    try testing.expectEqual(false, sub.isActive());

    // After activate: state=active, active=true -> isActive=true
    try sub.activate(&client, 1, "test", null);
    try testing.expectEqual(true, sub.isActive());

    // After drain: state=draining, active=true -> isActive=false
    sub.drain();
    try testing.expectEqual(false, sub.isActive());

    // After deactivate: state=unsubscribed, active=false -> isActive=false
    sub.deactivate();
    try testing.expectEqual(false, sub.isActive());
}

// Section 13: FixedSubscription pending count

test "FixedSubscription pending initial" {
    const sub = TestSub.initEmpty();
    try testing.expectEqual(@as(u16, 0), sub.pending());
}

test "FixedSubscription pending after activate" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "test", null);
    try testing.expectEqual(@as(u16, 0), sub.pending());
}

test "FixedSubscription pending after message push" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "test", null);

    try sub.messages.push(.{ .data = 1 });
    try testing.expectEqual(@as(u16, 1), sub.pending());

    try sub.messages.push(.{ .data = 2 });
    try testing.expectEqual(@as(u16, 2), sub.pending());

    _ = sub.messages.tryPop();
    try testing.expectEqual(@as(u16, 1), sub.pending());
}

// Section 14: FixedSubscription matches

test "FixedSubscription matches exact" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "foo.bar", null);

    try testing.expect(sub.matches("foo.bar"));
    try testing.expect(!sub.matches("foo.baz"));
    try testing.expect(!sub.matches("foo"));
    try testing.expect(!sub.matches("foo.bar.baz"));
}

test "FixedSubscription matches wildcard single" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "foo.*", null);

    try testing.expect(sub.matches("foo.bar"));
    try testing.expect(sub.matches("foo.baz"));
    try testing.expect(!sub.matches("foo.bar.baz"));
    try testing.expect(!sub.matches("foo"));
}

test "FixedSubscription matches wildcard full" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "foo.>", null);

    try testing.expect(sub.matches("foo.bar"));
    try testing.expect(sub.matches("foo.bar.baz"));
    try testing.expect(sub.matches("foo.a.b.c.d"));
    try testing.expect(!sub.matches("foo"));
    try testing.expect(!sub.matches("bar.foo"));
}

// Section 15: FixedSubscription Multiple Cycles

test "FixedSubscription multiple activate deactivate cycles" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    var i: u64 = 0;
    while (i < 10) : (i += 1) {
        try sub.activate(&client, i, "test", null);
        try testing.expectEqual(i, sub.sid);
        try testing.expectEqual(true, sub.isActive());

        sub.deactivate();
        try testing.expectEqual(false, sub.isActive());
    }
}

test "FixedSubscription activate clears message queue" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "test", null);

    // Push some messages
    try sub.messages.push(.{ .data = 1 });
    try sub.messages.push(.{ .data = 2 });
    try testing.expectEqual(@as(u16, 2), sub.pending());

    // Deactivate and reactivate
    sub.deactivate();
    try sub.activate(&client, 2, "other", null);

    // Queue should be cleared
    try testing.expectEqual(@as(u16, 0), sub.pending());
}

// Section 16: State Enum

test "State enum values distinct" {
    try testing.expect(State.active != State.draining);
    try testing.expect(State.active != State.unsubscribed);
    try testing.expect(State.draining != State.unsubscribed);
}

// Section 17: Edge Case Configs

test "FixedSubscription minimal config" {
    const MinConfig = subscription.FixedSubConfig{
        .max_subject_len = 1,
        .max_queue_group_len = 1,
        .queue_capacity = 1,
    };

    const MinSub = FixedSubscription(TestClient, TestMessage, MinConfig);
    var client = TestClient{};
    var sub = MinSub.initEmpty();

    try sub.activate(&client, 1, "a", "b");
    try testing.expectEqualStrings("a", sub.subject());
    try testing.expectEqualStrings("b", sub.queueGroup().?);

    // Queue capacity 1
    try sub.messages.push(.{ .data = 1 });
    try testing.expectError(error.QueueFull, sub.messages.push(.{ .data = 2 }));
}

test "FixedSubscription large config" {
    const LargeConfig = subscription.FixedSubConfig{
        .max_subject_len = 1024,
        .max_queue_group_len = 512,
        .queue_capacity = 1024,
    };

    const LargeSub = FixedSubscription(TestClient, TestMessage, LargeConfig);
    var client = TestClient{};
    var sub = LargeSub.initEmpty();

    const long_subject = "a" ** 1024;
    try sub.activate(&client, 1, long_subject, null);
    try testing.expectEqual(@as(usize, 1024), sub.subject().len);
}

test "FixedSubscription large queue group" {
    const LargeConfig = subscription.FixedSubConfig{
        .max_subject_len = 256,
        .max_queue_group_len = 512,
        .queue_capacity = 256,
    };

    const LargeSub = FixedSubscription(TestClient, TestMessage, LargeConfig);
    var client = TestClient{};
    var sub = LargeSub.initEmpty();

    const long_qg = "q" ** 512;
    try sub.activate(&client, 1, "test", long_qg);
    try testing.expectEqual(@as(usize, 512), sub.queueGroup().?.len);
}

// Section 18: SID Edge Values

test "FixedSubscription SID zero" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 0, "test", null);
    try testing.expectEqual(@as(u64, 0), sub.sid);
}

test "FixedSubscription SID max u64" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, std.math.maxInt(u64), "test", null);
    try testing.expectEqual(std.math.maxInt(u64), sub.sid);
}

// Section 19: Received/Max Messages

test "FixedSubscription received msgs initial" {
    const sub = TestSub.initEmpty();
    try testing.expectEqual(@as(u64, 0), sub.received_msgs);
}

test "FixedSubscription max msgs initial" {
    const sub = TestSub.initEmpty();
    try testing.expectEqual(@as(u64, 0), sub.max_msgs);
}

test "FixedSubscription activate resets counters" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Manually set counters
    sub.received_msgs = 100;
    sub.max_msgs = 50;

    try sub.activate(&client, 1, "test", null);

    try testing.expectEqual(@as(u64, 0), sub.received_msgs);
    try testing.expectEqual(@as(u64, 0), sub.max_msgs);
}

// Section 20: Subject/QueueGroup with Dots

test "FixedSubscription subject with dots" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "a.b.c.d.e", null);
    try testing.expectEqualStrings("a.b.c.d.e", sub.subject());
}

test "FixedSubscription subject single char" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "x", null);
    try testing.expectEqualStrings("x", sub.subject());
}

test "FixedSubscription queue group with dots" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // NATS queue groups typically don't have dots but are accepted
    try sub.activate(&client, 1, "test", "group.name");
    try testing.expectEqualStrings("group.name", sub.queueGroup().?);
}

// Section 21: Subject/QueueGroup Special Characters

test "FixedSubscription subject with hyphens and underscores" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "my-subject_name", null);
    try testing.expectEqualStrings("my-subject_name", sub.subject());
}

test "FixedSubscription subject with numbers" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "topic123.sub456", null);
    try testing.expectEqualStrings("topic123.sub456", sub.subject());
}

test "FixedSubscription queue group with hyphens" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    try sub.activate(&client, 1, "test", "worker-pool-1");
    try testing.expectEqualStrings("worker-pool-1", sub.queueGroup().?);
}

test "FixedSubscription subject all printable ASCII" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // Most printable ASCII chars (excluding space and control chars)
    try sub.activate(&client, 1, "!#$%&'()+,-./0123456789:;<=>?@ABC", null);
    try testing.expectEqualStrings("!#$%&'()+,-./0123456789:;<=>?@ABC", sub.subject());
}

// Section 22: Subject/QueueGroup Unicode
// NOTE: FixedSubscription stores raw bytes - it does NOT validate subject content.
// Unicode validation (if needed) should be done at the NATS protocol encoder level.
// These tests verify that arbitrary bytes are stored/retrieved correctly.

test "FixedSubscription subject with unicode bytes" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // UTF-8 encoded Japanese: "テスト" (tesuto = test)
    const unicode_subject = "\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88";
    try sub.activate(&client, 1, unicode_subject, null);
    try testing.expectEqualStrings(unicode_subject, sub.subject());
}

test "FixedSubscription subject with emoji bytes" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // UTF-8 encoded emoji: 🚀 (rocket)
    const emoji_subject = "\xf0\x9f\x9a\x80.launch";
    try sub.activate(&client, 1, emoji_subject, null);
    try testing.expectEqualStrings(emoji_subject, sub.subject());
}

test "FixedSubscription queue group with unicode bytes" {
    var client = TestClient{};
    var sub = TestSub.initEmpty();

    // UTF-8 encoded: "队列" (duìliè = queue in Chinese)
    const unicode_qg = "\xe9\x98\x9f\xe5\x88\x97";
    try sub.activate(&client, 1, "test", unicode_qg);
    try testing.expectEqualStrings(unicode_qg, sub.queueGroup().?);
}
