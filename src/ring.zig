const std = @import("std");

// Value-only messages: copying a pointer would leave lifetime and ownership
// outside the queue's publication protocol. Padding is not inspected.
fn copyable(comptime T: type) bool {
    return switch (@typeInfo(T)) {
        .void, .bool, .int, .float, .@"enum" => true,
        .array => |a| copyable(a.child),
        .vector => |v| copyable(v.child),
        .optional => |o| copyable(o.child),
        .@"struct" => |s| blk: {
            inline for (s.field_types) |F| if (!copyable(F)) break :blk false;
            break :blk true;
        },
        .@"union" => |u| blk: {
            if (u.tag_type == null) break :blk false;
            inline for (u.field_types) |F| if (!copyable(F)) break :blk false;
            break :blk true;
        },
        else => false,
    };
}

// Exactly one producer calls push and exactly one consumer calls pop. Neither
// may move or reset the queue while either endpoint is using it. All capacity
// slots are usable; unsigned wrapping subtraction preserves occupancy across
// counter overflow because capacity is less than half the counter range.
pub fn Ring(comptime T: type, comptime capacity: usize) type {
    if (!copyable(T)) @compileError("ring messages must contain only copyable values, without pointers");
    if (capacity == 0 or capacity & (capacity - 1) != 0 or capacity > std.math.maxInt(usize) / 2)
        @compileError("ring capacity must be a power of two below half the usize range");

    return struct {
        const Self = @This();
        // std/atomic.zig: cache_line is the target CPU's cache-line estimate.
        head: std.atomic.Value(usize) align(std.atomic.cache_line) = .init(0),
        tail: std.atomic.Value(usize) align(std.atomic.cache_line) = .init(0),
        slots: [capacity]T align(std.atomic.cache_line) = undefined,

        pub fn push(self: *Self, value: T) bool {
            const head = self.head.load(.monotonic);
            // Acquire pairs with pop's release: the consumer has finished
            // reading this slot before the producer can overwrite it.
            if (head -% self.tail.load(.acquire) == capacity) return false;
            self.slots[head & (capacity - 1)] = value;
            // Publish the initialized slot before making it visible.
            self.head.store(head +% 1, .release);
            return true;
        }

        pub fn pop(self: *Self) ?T {
            const tail = self.tail.load(.monotonic);
            // Acquire pairs with push's release, making the slot write visible.
            if (tail == self.head.load(.acquire)) return null;
            const value = self.slots[tail & (capacity - 1)];
            self.tail.store(tail +% 1, .release);
            return value;
        }
    };
}

test "FIFO, all slots usable, and full/empty leave endpoints unchanged" {
    var queue: Ring(u32, 4) = .{};
    try std.testing.expectEqual(null, queue.pop());
    for (0..4) |i| try std.testing.expect(queue.push(@intCast(i)));
    try std.testing.expect(!queue.push(99));
    for (0..4) |i| try std.testing.expectEqual(@as(u32, @intCast(i)), queue.pop());
    try std.testing.expectEqual(null, queue.pop());
    try std.testing.expect(queue.push(42));
    try std.testing.expectEqual(42, queue.pop());
    var single: Ring(u8, 1) = .{};
    try std.testing.expect(single.push(7));
    try std.testing.expect(!single.push(8));
    try std.testing.expectEqual(7, single.pop());
    try std.testing.expectEqual(null, single.pop());
}

test "indices wrap through usize maximum without losing FIFO order" {
    var queue: Ring(u32, 4) = .{};
    const start = std.math.maxInt(usize) - 1;
    queue.head.store(start, .monotonic);
    queue.tail.store(start, .monotonic);
    for (0..20) |batch| {
        for (0..4) |i| try std.testing.expect(queue.push(@intCast(batch * 4 + i)));
        try std.testing.expect(!queue.push(99));
        for (0..4) |i| try std.testing.expectEqual(@as(u32, @intCast(batch * 4 + i)), queue.pop());
        try std.testing.expectEqual(null, queue.pop());
    }
    try std.testing.expect(queue.head.load(.monotonic) < start);
}

test "message restriction and endpoint cache-line separation" {
    const Message = struct { sequence: u64, values: [3]f32, kind: enum { meter, command } };
    try std.testing.expect(copyable(Message));
    try std.testing.expect(copyable(?Message));
    try std.testing.expect(copyable(union(enum) { integer: u32, real: f32 }));
    try std.testing.expect(!copyable(struct { samples: []f32 }));
    try std.testing.expect(!copyable(*u8));
    var queue: Ring(Message, 2) = .{};
    try std.testing.expect(@intFromPtr(&queue.head) / std.atomic.cache_line != @intFromPtr(&queue.tail) / std.atomic.cache_line);
    try std.testing.expect(@intFromPtr(&queue.tail) / std.atomic.cache_line != @intFromPtr(&queue.slots) / std.atomic.cache_line);
    const message: Message = .{ .sequence = 5, .values = .{ 1, 2, 3 }, .kind = .meter };
    try std.testing.expect(queue.push(message));
    try std.testing.expectEqualDeep(message, queue.pop().?);
}

test "two threads transfer three million sequence numbers" {
    const Harness = struct {
        const count = 3_000_000;
        queue: Ring(u64, 1024) = .{},
        abort: std.atomic.Value(bool) = .init(false),
        valid: bool = true,

        fn produce(self: *@This()) void {
            for (0..count) |sequence| {
                while (!self.queue.push(sequence)) {
                    if (self.abort.load(.monotonic)) return;
                    std.atomic.spinLoopHint();
                }
            }
        }

        fn consume(self: *@This()) void {
            for (0..count) |sequence| {
                while (true) {
                    if (self.queue.pop()) |value| {
                        if (value != sequence) self.valid = false;
                        break;
                    }
                    std.atomic.spinLoopHint();
                }
            }
        }
    };
    var harness: Harness = .{};
    const producer = try std.Thread.spawn(.{}, Harness.produce, .{&harness});
    errdefer {
        harness.abort.store(true, .monotonic);
        producer.join();
    }
    const consumer = try std.Thread.spawn(.{}, Harness.consume, .{&harness});
    producer.join();
    consumer.join();
    // Thread.join synchronizes consumer's write to valid (std/Thread.zig).
    try std.testing.expect(harness.valid);
    try std.testing.expectEqual(null, harness.queue.pop());
}
