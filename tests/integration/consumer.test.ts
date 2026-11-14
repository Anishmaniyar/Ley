import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message } from "../../src/broker/modules/message/message.js";
import { Mutex } from "../../examples/consumer/shared/async-mutex.js";
import { WakeUpSignal } from "../../examples/consumer/shared/wake-up_signal.js";
import { OffsetManager } from "../../examples/consumer/notification-service/offset/offset-manager.js";
import { RetryQueue } from "../../examples/consumer/notification-service/retry/retry-queue.js";
import { RetryScheduler } from "../../examples/consumer/notification-service/retry/retry-scheduler.js";
import { calculateDelay } from "../../examples/consumer/notification-service/retry/retry-delay.js";
import type { RetryEntry } from "../../examples/consumer/notification-service/retry/retry-entry.js";
import {
  BATCH_SIZE,
  POLL_INTERVAL_MS,
} from "../../examples/consumer/notification-service/config.js";

// Keep retry timings small so integration tests stay fast. Jitter is disabled
// so the number and order of attempts is deterministic.
vi.mock("../../examples/consumer/notification-service/config.js", () => ({
  BATCH_SIZE: 10,
  POLL_INTERVAL_MS: 5,
  MAX_RETRIES: 3,
  BASE_RETRY_DELAY_MS: 50,
  MAX_RETRY_DELAY_MS: 200,
  BACKOFF_MULTIPLIER: 2,
  JITTER_FACTOR: 0,
}));

type BrokerModule = typeof import("../../src/broker/modules/broker/broker.service.js");
type FetchBatch = (
  offset: number,
  limit: number,
) => Message[] | Promise<Message[]>;
type Processor = (message: Message) => Promise<void>;

let broker: BrokerModule;

function message(key: string): Message {
  return {
    key,
    value: {
      event_type: "order.created",
      payload: { order_id: key, user_id: "user-1", amount: 100 },
    },
    timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
    headers: { source: "integration-test", content_type: "application/json" },
  };
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 5000,
  intervalMs = 5,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;

  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await delay(intervalMs);
  }

  if (!predicate()) {
    throw new Error("Timed out waiting for condition");
  }
}

// Mirrors the wiring in notification-service/index.ts: one mutex shared by the
// fetch loop and the retry scheduler, one offset manager, one retry queue.
class ConsumerHarness {
  readonly offsetManager = new OffsetManager();
  readonly retryQueue = new RetryQueue();
  readonly wakeUpSignal = new WakeUpSignal();
  readonly mutex = new Mutex();
  readonly retryScheduler: RetryScheduler;
  readonly processedKeys: string[] = [];
  readonly attemptCounts = new Map<string, number>();

  active = 0;
  maxActive = 0;

  private readonly controller = new AbortController();
  private readonly processMessageSafely: (message: Message) => Promise<void>;
  private running = false;
  private loopPromise: Promise<void> | null = null;
  private loopError: unknown = null;
  private stopPromise: Promise<void> | null = null;

  constructor(
    private readonly fetchBatch: FetchBatch,
    private readonly processor: Processor,
  ) {
    this.processMessageSafely = (message) =>
      this.mutex.runExclusive(async () => {
        this.active++;
        this.maxActive = Math.max(this.maxActive, this.active);
        this.attemptCounts.set(
          message.key,
          (this.attemptCounts.get(message.key) ?? 0) + 1,
        );

        try {
          await this.processor(message);
          this.processedKeys.push(message.key);
        } finally {
          this.active--;
        }
      }, this.controller.signal);

    this.retryScheduler = new RetryScheduler(
      this.retryQueue,
      this.wakeUpSignal,
      this.processMessageSafely,
    );
  }

  get offset(): number {
    return this.offsetManager.getOffset();
  }

  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.retryScheduler.start();

    this.loopPromise = this.consume().catch((error) => {
      this.loopError = error;
    });
  }

  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    this.stopPromise = (async () => {
      this.running = false;
      this.controller.abort();

      const loopDone = this.loopPromise ?? Promise.resolve();

      await Promise.all([loopDone, this.retryScheduler.stop()]);

      if (this.loopError) {
        throw this.loopError;
      }
    })();

    return this.stopPromise;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => {
      if (!this.running) {
        resolve();
        return;
      }

      const timer = setTimeout(() => {
        this.controller.signal.removeEventListener("abort", onAbort);
        resolve();
      }, ms);

      const onAbort = () => {
        clearTimeout(timer);
        this.controller.signal.removeEventListener("abort", onAbort);
        resolve();
      };

      this.controller.signal.addEventListener("abort", onAbort, {
        once: true,
      });
    });
  }

  private async consume(): Promise<void> {
    while (this.running) {
      let messages: Message[];

      try {
        messages = await this.fetchBatch(
          this.offsetManager.getOffset(),
          BATCH_SIZE,
        );
      } catch (error) {
        if (!this.running) {
          break;
        }
        await this.sleep(POLL_INTERVAL_MS);
        continue;
      }

      if (!this.running) {
        break;
      }

      if (messages.length === 0) {
        await this.sleep(POLL_INTERVAL_MS);
        continue;
      }

      for (const failedMessage of messages) {
        if (!this.running) {
          break;
        }

        try {
          await this.processMessageSafely(failedMessage);
          this.offsetManager.advance();
        } catch (error) {
          if (!this.running) {
            break;
          }

          const entry: RetryEntry = {
            message: failedMessage,
            retryCount: 0,
            nextRetryAt: Date.now() + calculateDelay(0),
            lastError: {
              name: error instanceof Error ? error.name : "Error",
              message: error instanceof Error ? error.message : String(error),
            },
          };

          const scheduled = this.retryScheduler.scheduleRetry(entry);

          if (scheduled) {
            this.offsetManager.advance();
          } else if (!this.running) {
            break;
          } else {
            throw new Error("Retry scheduler is not running");
          }
        }
      }
    }
  }
}

const harnesses: ConsumerHarness[] = [];

function createHarness(processor: Processor): ConsumerHarness {
  const harness = new ConsumerHarness(
    (offset, limit) => broker.fetchMessages(offset, limit),
    processor,
  );

  harnesses.push(harness);

  return harness;
}

function publish(key: string): number {
  return broker.publishMessage(message(key));
}

beforeEach(async () => {
  vi.resetModules();
  broker = await import(
    "../../src/broker/modules/broker/broker.service.js"
  );
  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(async () => {
  await Promise.allSettled(harnesses.map((harness) => harness.stop()));
  harnesses.length = 0;
  vi.restoreAllMocks();
});

describe("consumer integration", () => {
  it("lets multiple producers publish and multiple consumers fetch independently", async () => {
    const published = await Promise.all(
      [0, 1, 2].map(async (producer) => {
        const keys: string[] = [];

        for (let index = 0; index < 5; index++) {
          const key = `producer-${producer}-${index}`;
          publish(key);
          keys.push(key);
        }

        return keys;
      }),
    );

    const allKeys = published.flat();
    const first = createHarness(async () => {});
    const second = createHarness(async () => {});

    first.start();
    second.start();

    await waitFor(
      () =>
        first.processedKeys.length === allKeys.length &&
        second.processedKeys.length === allKeys.length,
    );

    await Promise.all([first.stop(), second.stop()]);

    expect(new Set(first.processedKeys)).toEqual(new Set(allKeys));
    expect(new Set(second.processedKeys)).toEqual(new Set(allKeys));
    expect(first.offset).toBe(allKeys.length);
    expect(second.offset).toBe(allKeys.length);
  });

  it("schedules a failed message for retry and removes it when the retry succeeds", async () => {
    publish("retry-me");

    let attempts = 0;
    const harness = createHarness(async () => {
      attempts++;
      if (attempts === 1) {
        throw new Error("first attempt failed");
      }
    });

    harness.start();

    await waitFor(() => attempts === 1);
    await waitFor(() => !harness.retryQueue.isEmpty());

    const scheduled = harness.retryQueue.peek();
    expect(scheduled?.message.key).toBe("retry-me");
    expect(scheduled?.retryCount).toBe(0);
    expect(scheduled?.lastError.message).toBe("first attempt failed");

    await waitFor(() => harness.processedKeys.includes("retry-me"));

    expect(harness.retryQueue.isEmpty()).toBe(true);
    expect(harness.processedKeys).toEqual(["retry-me"]);

    await harness.stop();
  });

  it("keeps processing later messages after a message fails", async () => {
    publish("always-fails");
    publish("later-message");

    const harness = createHarness(async (failedMessage) => {
      if (failedMessage.key === "always-fails") {
        throw new Error("delivery failed");
      }
    });

    harness.start();

    await waitFor(() => harness.processedKeys.includes("later-message"));

    expect(harness.processedKeys).toEqual(["later-message"]);
    expect(harness.offset).toBe(2);

    await harness.stop();
  });

  it("reschedules a retry that fails again", async () => {
    publish("reschedule-me");

    let attempts = 0;
    const harness = createHarness(async () => {
      attempts++;
      throw new Error(`failure-${attempts}`);
    });

    harness.start();

    await waitFor(() => attempts >= 2);
    await waitFor(() => !harness.retryQueue.isEmpty());

    const pending = harness.retryQueue.peek();
    expect(pending?.message.key).toBe("reschedule-me");
    expect(pending?.retryCount).toBeGreaterThanOrEqual(1);
    expect(pending?.lastError.message).toMatch(/^failure-\d+$/);

    await harness.stop();
  });

  it("never processes a new message and a due retry at the same time", async () => {
    publish("retry-overlap");

    for (let index = 0; index < 8; index++) {
      publish(`new-message-${index}`);
    }

    let failOnce = true;
    const harness = createHarness(async (candidate) => {
      await delay(15);

      if (candidate.key === "retry-overlap" && failOnce) {
        failOnce = false;
        throw new Error("overlap failure");
      }
    });

    harness.start();

    await waitFor(
      () =>
        harness.processedKeys.filter((key) => key.startsWith("new-message-"))
          .length === 8 &&
        harness.attemptCounts.get("retry-overlap") !== undefined &&
        harness.attemptCounts.get("retry-overlap")! >= 2,
    );

    expect(harness.maxActive).toBe(1);

    await harness.stop();
  });

  it("continues processing after a retry fails", async () => {
    publish("bad-retry");

    const goodKeys = Array.from(
      { length: 5 },
      (_, index) => `good-message-${index}`,
    );
    goodKeys.forEach(publish);

    let badAttempts = 0;
    const harness = createHarness(async (candidate) => {
      if (candidate.key === "bad-retry") {
        badAttempts++;
        throw new Error("keeps failing");
      }
    });

    harness.start();

    await waitFor(
      () =>
        badAttempts >= 2 &&
        goodKeys.every((key) => harness.processedKeys.includes(key)),
    );

    expect(harness.processedKeys).toHaveLength(goodKeys.length);

    await harness.stop();
  });

  it("shuts down gracefully during normal processing", async () => {
    publish("slow-message-1");
    publish("slow-message-2");

    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    let started = 0;

    const harness = createHarness(async () => {
      started++;
      await gate;
    });

    harness.start();

    await waitFor(() => started === 1);

    let stopped = false;
    const stopping = harness.stop().then(() => {
      stopped = true;
    });

    await delay(20);
    expect(stopped).toBe(false);

    release();
    await stopping;

    expect(harness.processedKeys).toEqual(["slow-message-1"]);
    expect(harness.offset).toBe(1);
    expect(harness.retryQueue.isEmpty()).toBe(true);
    expect(harness.retryScheduler.getDeadLetters()).toHaveLength(0);
  });

  it("shuts down gracefully during retry processing", async () => {
    publish("slow-retry");

    let attempts = 0;
    let retryStarted!: () => void;
    const retryStartedPromise = new Promise<void>((resolve) => {
      retryStarted = resolve;
    });
    let releaseRetry!: () => void;
    const retryGate = new Promise<void>((resolve) => {
      releaseRetry = resolve;
    });

    const harness = createHarness(async () => {
      attempts++;

      if (attempts === 1) {
        throw new Error("first attempt failed");
      }

      retryStarted();
      await retryGate;
    });

    harness.start();

    await retryStartedPromise;

    let stopped = false;
    const stopping = harness.stop().then(() => {
      stopped = true;
    });

    await delay(20);
    expect(stopped).toBe(false);

    releaseRetry();
    await stopping;

    expect(harness.retryQueue.isEmpty()).toBe(true);
    expect(harness.retryScheduler.getDeadLetters()).toHaveLength(0);
  });

  it("cancels a retry queued behind the mutex without counting it", async () => {
    publish("retry-cancelled");
    publish("blocker");

    let blockerStarted!: () => void;
    const blockerStartedPromise = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });
    let releaseBlocker!: () => void;
    const blockerGate = new Promise<void>((resolve) => {
      releaseBlocker = resolve;
    });

    let retryAttempts = 0;
    const harness = createHarness(async (candidate) => {
      if (candidate.key === "retry-cancelled") {
        retryAttempts++;
        if (retryAttempts === 1) {
          throw new Error("first attempt failed");
        }
        return;
      }

      if (candidate.key === "blocker") {
        blockerStarted();
        await blockerGate;
      }
    });

    harness.start();

    await blockerStartedPromise;

    // The retry is due while the blocker holds the mutex, so the scheduler
    // dequeues it and queues it behind the blocker.
    await waitFor(() => harness.retryQueue.isEmpty());

    let stopped = false;
    const stopping = harness.stop().then(() => {
      stopped = true;
    });

    await delay(10);
    expect(stopped).toBe(false);

    releaseBlocker();
    await stopping;

    // The queued retry was cancelled before it ever ran, so it was not counted
    // as a failed attempt and was not rescheduled.
    expect(retryAttempts).toBe(1);
    expect(harness.processedKeys).toEqual(["blocker"]);
    expect(harness.retryQueue.isEmpty()).toBe(true);
    expect(harness.retryScheduler.getDeadLetters()).toHaveLength(0);
  });

  it("restarts with a fresh offset and empty retry state", async () => {
    publish("restart-1");
    publish("restart-2");

    const failing = createHarness(async () => {
      throw new Error("broker unavailable");
    });

    failing.start();

    await waitFor(() => failing.offset === 2);
    expect(failing.retryQueue.isEmpty()).toBe(false);

    await failing.stop();

    const restarted = createHarness(async () => {});

    // Offsets and retry state are volatile: a new consumer starts from scratch.
    expect(restarted.offset).toBe(0);
    expect(restarted.retryQueue.isEmpty()).toBe(true);

    restarted.start();

    await waitFor(() => restarted.processedKeys.length === 2);
    expect(new Set(restarted.processedKeys)).toEqual(
      new Set(["restart-1", "restart-2"]),
    );
    expect(restarted.offset).toBe(2);

    await restarted.stop();
  });
});
