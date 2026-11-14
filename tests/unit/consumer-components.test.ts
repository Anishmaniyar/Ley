import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Mutex } from "../../examples/consumer/shared/async-mutex.js";
import { PriorityQueue } from "../../examples/consumer/shared/min-heap.js";
import { WakeUpSignal } from "../../examples/consumer/shared/wake-up_signal.js";
import { RetryQueue } from "../../examples/consumer/notification-service/retry/retry-queue.js";
import { RetryScheduler } from "../../examples/consumer/notification-service/retry/retry-scheduler.js";
import { calculateDelay } from "../../examples/consumer/notification-service/retry/retry-delay.js";
import type { RetryEntry } from "../../examples/consumer/notification-service/retry/retry-entry.js";
import { processMessage } from "../../examples/consumer/notification-service/processing/message-processor.js";
import {
  BASE_RETRY_DELAY_MS,
  BACKOFF_MULTIPLIER,
  JITTER_FACTOR,
  MAX_RETRY_DELAY_MS,
  MAX_RETRIES,
} from "../../examples/consumer/notification-service/config.js";
import type { Message } from "../../src/broker/modules/message/message.js";

function message(key: string): Message {
  return {
    key,
    value: {
      event_type: "order.created",
      payload: { order_id: key, user_id: "user-1", amount: 100 },
    },
    timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
    headers: { source: "test", content_type: "application/json" },
  };
}

function retryEntry(key: string, nextRetryAt: number): RetryEntry {
  return {
    message: message(key),
    retryCount: 0,
    nextRetryAt,
    lastError: { name: "Error", message: "initial failure" },
  };
}

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("Mutex", () => {
  it("executes tasks sequentially", async () => {
    const mutex = new Mutex();
    const order: string[] = [];
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });

    const first = mutex.runExclusive(async () => {
      order.push("first:start");
      await firstGate;
      order.push("first:end");
    });

    const second = mutex.runExclusive(async () => {
      order.push("second:start");
      order.push("second:end");
    });

    await Promise.resolve();
    expect(order).toEqual(["first:start"]);

    releaseFirst();
    await Promise.all([first, second]);

    expect(order).toEqual([
      "first:start",
      "first:end",
      "second:start",
      "second:end",
    ]);
  });

  it("does not let a failed task block later tasks", async () => {
    const mutex = new Mutex();
    let secondRan = false;

    const first = mutex.runExclusive(async () => {
      throw new Error("task failed");
    });

    const second = mutex.runExclusive(async () => {
      secondRan = true;
    });

    await expect(first).rejects.toThrow("task failed");
    await second;

    expect(secondRan).toBe(true);
  });

  it("rejects with the original error", async () => {
    const mutex = new Mutex();
    const original = new Error("original failure");

    await expect(
      mutex.runExclusive(async () => {
        throw original;
      }),
    ).rejects.toBe(original);
  });

  it("cancels a task that is still waiting when the signal aborts", async () => {
    const mutex = new Mutex();
    const controller = new AbortController();
    let cancelledTaskRan = false;

    const first = mutex.runExclusive(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20));
    });

    const second = mutex.runExclusive(async () => {
      cancelledTaskRan = true;
    }, controller.signal);

    controller.abort();

    await expect(second).rejects.toThrow("cancelled");
    await first;
    expect(cancelledTaskRan).toBe(false);
  });
});

describe("PriorityQueue", () => {
  it("orders elements by the provided comparator", () => {
    const queue = new PriorityQueue<number>((a, b) => a - b);

    [5, 1, 4, 2, 3].forEach((value) => queue.push(value));

    expect(queue.peek()).toBe(1);
    expect([queue.pop(), queue.pop(), queue.pop(), queue.pop(), queue.pop()])
      .toEqual([1, 2, 3, 4, 5]);
  });

  it("returns undefined when empty", () => {
    const queue = new PriorityQueue<number>((a, b) => a - b);

    expect(queue.isEmpty()).toBe(true);
    expect(queue.peek()).toBeUndefined();
    expect(queue.pop()).toBeUndefined();
  });

  it("keeps the smallest element after removals", () => {
    const queue = new PriorityQueue<number>((a, b) => a - b);

    [9, 3, 7, 1, 8, 2].forEach((value) => queue.push(value));

    expect(queue.pop()).toBe(1);
    expect(queue.peek()).toBe(2);
    expect(queue.pop()).toBe(2);
    expect(queue.peek()).toBe(3);
  });

  it("supports duplicate priorities", () => {
    const queue = new PriorityQueue<number>((a, b) => a - b);

    [2, 2, 1, 1].forEach((value) => queue.push(value));

    expect([queue.pop(), queue.pop(), queue.pop(), queue.pop()]).toEqual([
      1, 1, 2, 2,
    ]);
  });
});

describe("RetryQueue", () => {
  it("dequeues retries in nextRetryAt order", () => {
    const queue = new RetryQueue();

    queue.enqueue(retryEntry("late", 30));
    queue.enqueue(retryEntry("early", 10));
    queue.enqueue(retryEntry("middle", 20));

    expect(queue.peek()?.message.key).toBe("early");
    expect(queue.dequeue()?.message.key).toBe("early");
    expect(queue.dequeue()?.message.key).toBe("middle");
    expect(queue.dequeue()?.message.key).toBe("late");
    expect(queue.isEmpty()).toBe(true);
  });
});

describe("WakeUpSignal", () => {
  it("resolves immediately when notified before waiting", async () => {
    const signal = new WakeUpSignal();

    signal.notify();

    await expect(signal.wait()).resolves.toBeUndefined();
  });

  it("resolves when notified while waiting", async () => {
    const signal = new WakeUpSignal();
    let resolved = false;

    const waiting = signal.wait().then(() => {
      resolved = true;
    });

    await Promise.resolve();
    expect(resolved).toBe(false);

    signal.notify();
    await waiting;

    expect(resolved).toBe(true);
  });

  it("resolves on timeout without a notification", async () => {
    vi.useFakeTimers();
    const signal = new WakeUpSignal();
    let resolved = false;

    const waiting = signal.wait(1000).then(() => {
      resolved = true;
    });

    await vi.advanceTimersByTimeAsync(999);
    expect(resolved).toBe(false);

    await vi.advanceTimersByTimeAsync(1);
    await waiting;

    expect(resolved).toBe(true);
  });

  it("clears a pending timeout when notified", async () => {
    vi.useFakeTimers();
    const signal = new WakeUpSignal();
    let resolved = false;

    const waiting = signal.wait(1000).then(() => {
      resolved = true;
    });

    signal.notify();
    await waiting;

    expect(resolved).toBe(true);
    expect(vi.getTimerCount()).toBe(0);
  });
});

describe("calculateDelay", () => {
  it("applies exponential backoff", () => {
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    expect(calculateDelay(0)).toBe(BASE_RETRY_DELAY_MS);
    expect(calculateDelay(1)).toBe(BASE_RETRY_DELAY_MS * BACKOFF_MULTIPLIER);
    expect(calculateDelay(2)).toBe(
      BASE_RETRY_DELAY_MS * BACKOFF_MULTIPLIER ** 2,
    );
  });

  it("applies jitter below and above the base delay", () => {
    const random = vi.spyOn(Math, "random");

    random.mockReturnValue(0);
    expect(calculateDelay(0)).toBe(BASE_RETRY_DELAY_MS * (1 - JITTER_FACTOR));

    random.mockReturnValue(1);
    expect(calculateDelay(0)).toBe(BASE_RETRY_DELAY_MS * (1 + JITTER_FACTOR));
  });

  it("never exceeds the maximum retry delay", () => {
    const random = vi.spyOn(Math, "random");

    random.mockReturnValue(0.5);
    expect(calculateDelay(10)).toBe(MAX_RETRY_DELAY_MS);

    random.mockReturnValue(1);
    expect(calculateDelay(10)).toBe(MAX_RETRY_DELAY_MS);
  });

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])(
    "rejects an invalid retry count: %s",
    (retryCount) => {
      expect(() => calculateDelay(retryCount)).toThrow(RangeError);
    },
  );
});

describe("RetryScheduler", () => {
  it("waits until the retry deadline", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const queue = new RetryQueue();
    const processed: string[] = [];
    const scheduler = new RetryScheduler(queue, new WakeUpSignal(), async (due) => {
      processed.push(due.key);
    });

    queue.enqueue(retryEntry("late", Date.now() + 2000));
    scheduler.start();

    await vi.advanceTimersByTimeAsync(1999);
    expect(processed).toEqual([]);

    await vi.advanceTimersByTimeAsync(1);
    expect(processed).toEqual(["late"]);
    expect(queue.isEmpty()).toBe(true);

    await scheduler.stop();
  });

  it("retries failed messages", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const queue = new RetryQueue();
    let attempts = 0;
    const scheduler = new RetryScheduler(queue, new WakeUpSignal(), async () => {
      attempts++;
      if (attempts === 1) {
        throw new Error("temporary failure");
      }
    });

    queue.enqueue(retryEntry("flaky", Date.now()));
    scheduler.start();

    await vi.advanceTimersByTimeAsync(0);
    expect(attempts).toBe(1);
    expect(queue.isEmpty()).toBe(false);
    expect(queue.peek()?.retryCount).toBe(1);
    expect(queue.peek()?.lastError.message).toBe("temporary failure");

    await vi.advanceTimersByTimeAsync(
      BASE_RETRY_DELAY_MS * BACKOFF_MULTIPLIER,
    );
    expect(attempts).toBe(2);
    expect(queue.isEmpty()).toBe(true);

    await scheduler.stop();
  });

  it("stops after the maximum number of retries", async () => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0.5);

    const queue = new RetryQueue();
    let attempts = 0;
    const scheduler = new RetryScheduler(queue, new WakeUpSignal(), async () => {
      attempts++;
      throw new Error("permanent failure");
    });

    queue.enqueue(retryEntry("doomed", Date.now()));
    scheduler.start();

    await vi.advanceTimersByTimeAsync(60_000);

    expect(attempts).toBe(MAX_RETRIES);
    expect(queue.isEmpty()).toBe(true);

    const deadLetters = scheduler.getDeadLetters();
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0].message.key).toBe("doomed");
    expect(deadLetters[0].retryCount).toBe(MAX_RETRIES);
    expect(deadLetters[0].lastError.message).toBe("permanent failure");

    await scheduler.stop();
  });

  it("refuses new retries before starting and after stopping", async () => {
    const queue = new RetryQueue();
    const scheduler = new RetryScheduler(queue, new WakeUpSignal(), async () => {});
    const entry = retryEntry("entry", Date.now());

    expect(scheduler.scheduleRetry(entry)).toBe(false);

    scheduler.start();
    expect(scheduler.scheduleRetry(entry)).toBe(true);

    await scheduler.stop();
    expect(scheduler.scheduleRetry(retryEntry("after-stop", Date.now()))).toBe(
      false,
    );
  });

  it("does not re-enqueue a retry that fails during shutdown", async () => {
    const queue = new RetryQueue();
    let started!: () => void;
    const processorStarted = new Promise<void>((resolve) => {
      started = resolve;
    });
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });

    const scheduler = new RetryScheduler(queue, new WakeUpSignal(), async () => {
      started();
      await gate;
      throw new Error("failed while shutting down");
    });

    queue.enqueue(retryEntry("shutdown", Date.now()));
    scheduler.start();

    await processorStarted;

    const stopping = scheduler.stop();
    release();
    await stopping;

    expect(queue.isEmpty()).toBe(true);
    expect(scheduler.getDeadLetters()).toHaveLength(0);
  });
});

describe("processMessage failure simulation", () => {
  beforeEach(() => {
    vi.spyOn(console, "log").mockImplementation(() => {});
  });

  it("succeeds on the 60% branch", async () => {
    await expect(
      processMessage(message("ok"), { delayMs: 0, random: () => 0.6 }),
    ).resolves.toBeUndefined();
  });

  it("throws on the 40% branch", async () => {
    await expect(
      processMessage(message("failed"), { delayMs: 0, random: () => 0.2 }),
    ).rejects.toThrow("Notification delivery failed");
  });

  it("treats the failure rate boundary as success", async () => {
    await expect(
      processMessage(message("boundary"), { delayMs: 0, random: () => 0.4 }),
    ).resolves.toBeUndefined();
  });
});
