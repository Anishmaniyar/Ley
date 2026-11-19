import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { Message } from "../../src/broker/modules/message/message.js";
import { Consumer } from "../../examples/consumer/core/consumer.js";

// The real Consumer class fetches over HTTP from a hardcoded broker URL.
// In tests we stub global fetch to route into the real in-process broker
// service, so the whole path (publish -> memory log -> fetch -> consumer)
// is exercised without opening a socket.
type BrokerModule = typeof import("../../src/broker/modules/broker/broker.service.js");

let broker: BrokerModule;

const consumers: Consumer[] = [];

function message(key: string): Message {
  return {
    key,
    value: {
      event_type: "order.created",
      payload: { order_id: key, user_id: "user-1", amount: 100 },
    },
    timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
    headers: { source: "bulk-test", content_type: "application/json" },
  };
}

function publishBulk(keys: string[]): number[] {
  return keys.map((key) => broker.publishMessage(message(key)));
}

function createConsumer(
  name: string,
  processor: (message: Message) => Promise<void>,
): { consumer: Consumer; processed: string[]; active: () => number; maxActive: () => number } {
  const processed: string[] = [];
  let active = 0;
  let maxActive = 0;

  const consumer = new Consumer({
    destination: name,
    processMessage: async (msg) => {
      active++;
      maxActive = Math.max(maxActive, active);
      try {
        await processor(msg);
        processed.push(`${name}:${msg.key}`);
      } finally {
        active--;
      }
    },
    config: { pollIntervalMs: 5, jitterFactor: 0, baseRetryDelayMs: 20, maxRetryDelayMs: 50 },
  });

  consumers.push(consumer);

  return { consumer, processed, active: () => active, maxActive: () => maxActive };
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

beforeEach(async () => {
  vi.resetModules();
  broker = await import(
    "../../src/broker/modules/broker/broker.service.js"
  );

  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string | URL) => {
      const url = new URL(String(input));
      const offset = Number(url.searchParams.get("offset") ?? "0");
      const limit = Number(url.searchParams.get("limit") ?? "10");

      return {
        ok: true,
        status: 200,
        json: async () => ({ messages: broker.fetchMessages(offset, limit) }),
      } as Response;
    }),
  );

  vi.spyOn(console, "log").mockImplementation(() => {});
  vi.spyOn(console, "error").mockImplementation(() => {});
});

afterEach(async () => {
  await Promise.allSettled(consumers.map((consumer) => consumer.stop()));
  consumers.length = 0;
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("bulk publish and sequential consumption", () => {
  it("stores a bulk publish in order and each consumer drains it sequentially", async () => {
    const keys = Array.from({ length: 25 }, (_, index) => `bulk-order-${index}`);

    const offsets = publishBulk(keys);

    // Broker assigned sequential offsets and stored every message.
    expect(offsets).toEqual(keys.map((_, index) => index));
    expect(broker.fetchMessages(0, 100).map((msg) => msg.key)).toEqual(keys);

    const notification = createConsumer("notification", async () => {});
    const analytics = createConsumer("analytics", async () => {});
    const reporting = createConsumer("reporting", async () => {});

    notification.consumer.start();
    analytics.consumer.start();
    reporting.consumer.start();

    await waitFor(
      () =>
        notification.processed.length === keys.length &&
        analytics.processed.length === keys.length &&
        reporting.processed.length === keys.length,
    );

    // Each consumer processed every message, in published order, one at a time.
    for (const consumer of [notification, analytics, reporting]) {
      expect(consumer.processed.map((entry) => entry.split(":")[1])).toEqual(keys);
      expect(consumer.maxActive()).toBe(1);
    }

    await Promise.all([
      notification.consumer.stop(),
      analytics.consumer.stop(),
      reporting.consumer.stop(),
    ]);
  });

  it("keeps other consumers unaffected when one consumer's processor fails", async () => {
    const keys = Array.from({ length: 10 }, (_, index) => `bulk-fail-${index}`);
    publishBulk(keys);

    const notification = createConsumer("notification", async (msg) => {
      if (msg.key === "bulk-fail-3") {
        throw new Error("notification delivery failed");
      }
    });
    const analytics = createConsumer("analytics", async () => {});
    const reporting = createConsumer("reporting", async () => {});

    notification.consumer.start();
    analytics.consumer.start();
    reporting.consumer.start();

    await waitFor(
      () =>
        analytics.processed.length === keys.length &&
        reporting.processed.length === keys.length,
    );

    // The healthy consumers processed everything; the failing one skipped only its poison message.
    expect(analytics.processed.map((entry) => entry.split(":")[1])).toEqual(keys);
    expect(reporting.processed.map((entry) => entry.split(":")[1])).toEqual(keys);
    expect(notification.processed).not.toContain("notification:bulk-fail-3");
    expect(notification.processed.length).toBe(keys.length - 1);

    await Promise.all([
      notification.consumer.stop(),
      analytics.consumer.stop(),
      reporting.consumer.stop(),
    ]);
  });
});
