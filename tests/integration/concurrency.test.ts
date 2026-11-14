import { once } from "node:events";
import type { Server } from "node:http";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import request from "supertest";
import type { Express } from "express";
import type { Message } from "../../src/broker/modules/message/message.js";

const CONCURRENT_REQUESTS = 100;

type PublishResult = {
  status: number;
  body: { offset: number; message: string };
};

let server: Server;
let originals: Message[];
let publishResults: PublishResult[];
let storedMessages: Message[];

function concurrentMessage(index: number): Message {
  return {
    key: `concurrent-${index}`,
    value: {
      event_type: "order.created",
      payload: {
        order_id: `order-${index}`,
        sequence: index,
        amount: index * 10,
      },
    },
    timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
    headers: {
      source: "concurrency-test",
      content_type: "application/json",
    },
  };
}

beforeAll(async () => {
  vi.resetModules();
  const app = (await import("../../src/broker/app.js")).default as Express;

  server = app.listen(0);
  await once(server, "listening");

  originals = Array.from({ length: CONCURRENT_REQUESTS }, (_, index) =>
    concurrentMessage(index),
  );

  // Fire every publish at once so the broker must handle overlapping requests.
  publishResults = await Promise.all(
    originals.map(async (message) => {
      const response = await request(server)
        .post("/api/v1/messages")
        .send(message);

      return { status: response.status, body: response.body };
    }),
  );

  const fetchResponse = await request(server)
    .get("/api/v1/messages")
    .query({ offset: "0", limit: String(CONCURRENT_REQUESTS) });

  storedMessages = fetchResponse.body.messages as Message[];
}, 30_000);

afterAll(async () => {
  if (server) {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
});

describe("concurrent publishing", () => {
  it("handles 100 overlapping publish requests", () => {
    expect(publishResults).toHaveLength(CONCURRENT_REQUESTS);

    for (const result of publishResults) {
      expect(result.status).toBe(201);
      expect(result.body.message).toBe("Message published successfully");
      expect(Number.isInteger(result.body.offset)).toBe(true);
    }
  });

  it("accepts every valid request", () => {
    const accepted = publishResults.filter((result) => result.status === 201);

    expect(accepted).toHaveLength(CONCURRENT_REQUESTS);
  });

  it("assigns a unique offset to every accepted message", () => {
    const offsets = publishResults.map((result) => result.body.offset);

    expect(new Set(offsets).size).toBe(CONCURRENT_REQUESTS);
  });

  it("assigns a continuous offset sequence without gaps", () => {
    const offsets = publishResults
      .map((result) => result.body.offset)
      .sort((left, right) => left - right);

    expect(offsets).toEqual(
      Array.from({ length: CONCURRENT_REQUESTS }, (_, index) => index),
    );
  });

  it("stores every accepted message in the log", () => {
    expect(storedMessages).toHaveLength(CONCURRENT_REQUESTS);
  });

  it("stores each message at the offset returned by its publish response", () => {
    for (const [index, result] of publishResults.entries()) {
      expect(storedMessages[result.body.offset]).toEqual(originals[index]);
    }
  });

  it("stores every message with its original content intact", () => {
    const storedByKey = new Map(
      storedMessages.map((message) => [message.key, message]),
    );

    expect(storedByKey.size).toBe(CONCURRENT_REQUESTS);

    for (const original of originals) {
      expect(storedByKey.get(original.key)).toEqual(original);
    }
  });
});

// Creates an isolated broker instance so ordering tests do not share the log.
async function withFreshBroker<T>(
  run: (freshServer: Server) => Promise<T>,
): Promise<T> {
  vi.resetModules();
  const app = (await import("../../src/broker/app.js")).default as Express;
  const freshServer = app.listen(0);
  await once(freshServer, "listening");

  try {
    return await run(freshServer);
  } finally {
    await new Promise<void>((resolve, reject) => {
      freshServer.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

describe("ordering guarantees", () => {
  it("returns the same total order on every read", async () => {
    const secondRead = await request(server)
      .get("/api/v1/messages")
      .query({ offset: "0", limit: String(CONCURRENT_REQUESTS) });

    expect(secondRead.body.messages).toEqual(storedMessages);
  });

  it("orders by append order, not event creation time", async () => {
    await withFreshBroker(async (freshServer) => {
      const newerEvent = {
        ...concurrentMessage(0),
        key: "newer-event",
        timestamp: { created_at: "2026-01-02T00:00:00.000Z" },
      };
      const olderEvent = {
        ...concurrentMessage(1),
        key: "older-event",
        timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
      };

      // The newer event is appended first, so it receives the earlier offset.
      const first = await request(freshServer)
        .post("/api/v1/messages")
        .send(newerEvent);
      const second = await request(freshServer)
        .post("/api/v1/messages")
        .send(olderEvent);

      expect([first.body.offset, second.body.offset]).toEqual([0, 1]);

      const fetched = await request(freshServer)
        .get("/api/v1/messages")
        .query({ offset: "0", limit: "10" });

      expect(
        (fetched.body.messages as Message[]).map((message) => message.key),
      ).toEqual(["newer-event", "older-event"]);
    });
  });

  it("never exposes partial or reordered entries to readers during publishing", async () => {
    await withFreshBroker(async (freshServer) => {
      const total = 60;
      const snapshots: Message[][] = [];

      const publishes = Array.from({ length: total }, (_, index) =>
        request(freshServer)
          .post("/api/v1/messages")
          .send(concurrentMessage(index)),
      );

      const reads = (async () => {
        for (let index = 0; index < 25; index++) {
          const response = await request(freshServer)
            .get("/api/v1/messages")
            .query({ offset: "0", limit: String(total) });

          snapshots.push(response.body.messages as Message[]);
          await new Promise((resolve) => setImmediate(resolve));
        }
      })();

      await Promise.all(publishes);
      await reads;

      const finalResponse = await request(freshServer)
        .get("/api/v1/messages")
        .query({ offset: "0", limit: String(total) });
      const finalMessages = finalResponse.body.messages as Message[];

      expect(finalMessages).toHaveLength(total);

      // Every read must be a prefix of the final log: no gaps, no reordering.
      for (const snapshot of snapshots) {
        expect(snapshot).toEqual(finalMessages.slice(0, snapshot.length));
      }
    });
  });

  it("keeps per-key order as the projection of append order", async () => {
    await withFreshBroker(async (freshServer) => {
      const events = [
        { key: "shared-key", sequence: "first" },
        { key: "other-key", sequence: "other" },
        { key: "shared-key", sequence: "second" },
      ];

      for (const event of events) {
        const message = {
          ...concurrentMessage(0),
          key: event.key,
          value: {
            event_type: "order.created",
            payload: { order_id: event.key, sequence: event.sequence },
          },
        };

        await request(freshServer).post("/api/v1/messages").send(message);
      }

      const fetched = await request(freshServer)
        .get("/api/v1/messages")
        .query({ offset: "0", limit: "10" });

      const sharedKeyEvents = (fetched.body.messages as Message[]).filter(
        (message) => message.key === "shared-key",
      );

      expect(
        sharedKeyEvents.map((message) => message.value.payload.sequence),
      ).toEqual(["first", "second"]);
    });
  });
});
