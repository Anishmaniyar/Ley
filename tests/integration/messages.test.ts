import { beforeEach, describe, expect, it, vi } from "vitest";
import request from "supertest";
import type { Express } from "express";
import type { Message } from "../../src/broker/modules/message/message.js";

let app: Express;

// Reset modules so each test gets a fresh app and a fresh in-memory log.
beforeEach(async () => {
  vi.resetModules();
  app = (await import("../../src/broker/app.js")).default;
});

function validMessage(overrides: Partial<Message> = {}): Message {
  return {
    key: "order-1",
    value: {
      event_type: "order.created",
      payload: { order_id: "order-1", amount: 100 },
    },
    timestamp: { created_at: "2026-01-01T00:00:00.000Z" },
    headers: {
      source: "order-service",
      content_type: "application/json",
    },
    ...overrides,
  };
}

function publish(message: Message) {
  return request(app).post("/api/v1/messages").send(message);
}

function fetchMessages(query: Record<string, string> = {}) {
  return request(app).get("/api/v1/messages").query(query);
}

describe("HTTP message API", () => {
  it("publishes a message and fetches it back through HTTP", async () => {
    const message = validMessage();

    const publishResponse = await publish(message);

    expect(publishResponse.status).toBe(201);
    expect(publishResponse.body).toEqual({
      offset: 0,
      message: "Message published successfully",
    });

    const fetchResponse = await fetchMessages({ offset: "0", limit: "10" });

    expect(fetchResponse.status).toBe(200);
    expect(fetchResponse.body).toEqual({
      messages: [message],
      offset: 0,
      limit: 10,
    });
  });

  it("assigns sequential offsets and preserves insertion order", async () => {
    const messages = [
      validMessage({ key: "a" }),
      validMessage({ key: "b" }),
      validMessage({ key: "c" }),
    ];

    const offsets: number[] = [];
    for (const message of messages) {
      const response = await publish(message);
      offsets.push(response.body.offset);
    }

    expect(offsets).toEqual([0, 1, 2]);

    const fetchResponse = await fetchMessages({ offset: "0", limit: "10" });
    expect(
      (fetchResponse.body.messages as Message[]).map(
        (message) => message.key,
      ),
    ).toEqual(["a", "b", "c"]);
  });

  it("fetches from a specific offset with different limits", async () => {
    for (const key of ["a", "b", "c", "d", "e"]) {
      await publish(validMessage({ key }));
    }

    const fromOffset = await fetchMessages({ offset: "1", limit: "2" });
    expect(
      (fromOffset.body.messages as Message[]).map((message) => message.key),
    ).toEqual(["b", "c"]);

    const all = await fetchMessages({ offset: "0", limit: "10" });
    expect(all.body.messages).toHaveLength(5);

    const tail = await fetchMessages({ offset: "3", limit: "10" });
    expect(
      (tail.body.messages as Message[]).map((message) => message.key),
    ).toEqual(["d", "e"]);
  });

  it("applies default offset and limit when omitted", async () => {
    for (let index = 0; index < 12; index++) {
      await publish(validMessage({ key: `order-${index}` }));
    }

    const response = await fetchMessages();

    expect(response.status).toBe(200);
    expect(response.body.offset).toBe(0);
    expect(response.body.limit).toBe(10);
    expect(response.body.messages).toHaveLength(10);
  });

  it("returns an empty list for an empty log and an offset beyond the log", async () => {
    const empty = await fetchMessages();
    expect(empty.status).toBe(200);
    expect(empty.body).toEqual({ messages: [], offset: 0, limit: 10 });

    await publish(validMessage({ key: "a" }));
    await publish(validMessage({ key: "b" }));

    const beyond = await fetchMessages({ offset: "5", limit: "10" });
    expect(beyond.status).toBe(200);
    expect(beyond.body).toEqual({ messages: [], offset: 5, limit: 10 });
  });

  it("rejects malformed JSON with HTTP 400", async () => {
    const response = await request(app)
      .post("/api/v1/messages")
      .set("Content-Type", "application/json")
      .send("{ not valid json");

    expect(response.status).toBe(400);
    expect(response.body).toEqual({
      status: "fail",
      message: "Invalid JSON request body",
    });
  });

  it.each([
    ["missing fields", { key: "only-key" }],
    [
      "empty payload",
      {
        ...validMessage(),
        value: { event_type: "order.created", payload: {} },
      },
    ],
    [
      "invalid timestamp",
      { ...validMessage(), timestamp: { created_at: "not-a-date" } },
    ],
  ])("rejects messages with %s", async (_label, message) => {
    const response = await request(app)
      .post("/api/v1/messages")
      .send(message);

    expect(response.status).toBe(400);
    expect(response.body).toEqual({
      status: "fail",
      message: "Invalid message structure",
    });
  });

  it.each([
    ["negative offset", { offset: "-1" }, "Offset must be a non-negative integer"],
    ["non-numeric offset", { offset: "abc" }, "Offset must be a non-negative integer"],
    ["fractional offset", { offset: "1.5" }, "Offset must be a non-negative integer"],
    ["zero limit", { limit: "0" }, "Limit must be a positive integer"],
    ["negative limit", { limit: "-1" }, "Limit must be a positive integer"],
    ["non-numeric limit", { limit: "abc" }, "Limit must be a positive integer"],
    ["fractional limit", { limit: "1.5" }, "Limit must be a positive integer"],
  ])("rejects invalid query parameters: %s", async (_label, query, message) => {
    const response = await fetchMessages(query);

    expect(response.status).toBe(400);
    expect(response.body).toEqual({ status: "fail", message });
  });

  it("returns HTTP 404 for unknown endpoints", async () => {
    const response = await request(app).get("/api/v1/unknown");

    expect(response.status).toBe(404);
    expect(response.body).toEqual({
      status: "fail",
      message: "Route GET /api/v1/unknown not found",
    });
  });

  it("does not expose stored messages through mutable external references", async () => {
    const message = validMessage();
    await publish(message);

    const firstRead = await fetchMessages();
    firstRead.body.messages[0].key = "mutated";
    firstRead.body.messages[0].value.payload.amount = 999;

    const secondRead = await fetchMessages();
    expect(secondRead.body.messages[0]).toEqual(message);
  });

  it("does not delete messages", async () => {
    await publish(validMessage());

    const firstRead = await fetchMessages();
    const secondRead = await fetchMessages();

    expect(secondRead.body.messages).toEqual(firstRead.body.messages);
    expect((await request(app).delete("/api/v1/messages")).status).toBe(404);
  });

  it("loses all messages when the broker restarts", async () => {
    await publish(validMessage({ key: "before-restart" }));

    vi.resetModules();
    const restartedApp = (await import("../../src/broker/app.js")).default;
    const response = await request(restartedApp)
      .get("/api/v1/messages")
      .query({ offset: "0", limit: "10" });

    expect(response.status).toBe(200);
    expect(response.body.messages).toEqual([]);
  });

  it("supports a pull-based producer-consumer lifecycle", async () => {
    await publish(validMessage({ key: "a" }));
    await publish(validMessage({ key: "b" }));

    let consumerOffset = 0;
    const batch = await fetchMessages({
      offset: String(consumerOffset),
      limit: "10",
    });

    expect(
      (batch.body.messages as Message[]).map((message) => message.key),
    ).toEqual(["a", "b"]);

    for (const message of batch.body.messages as Message[]) {
      try {
        if (message.key === "b") {
          throw new Error("processing failed");
        }
        consumerOffset++;
      } catch {
        break;
      }
    }

    // The failed message is not acknowledged, so the consumer keeps its offset.
    expect(consumerOffset).toBe(1);

    const retry = await fetchMessages({
      offset: String(consumerOffset),
      limit: "10",
    });

    expect(
      (retry.body.messages as Message[]).map((message) => message.key),
    ).toEqual(["b"]);
  });

  it("keeps consumer progress on the consumer, not on the broker", async () => {
    await publish(validMessage({ key: "a" }));
    await publish(validMessage({ key: "b" }));

    let consumerOffset = 1;
    const afterFirstRun = await fetchMessages({
      offset: String(consumerOffset),
      limit: "1",
    });
    expect(afterFirstRun.body.messages[0].key).toBe("b");

    // A consumer restart resets only its local offset.
    consumerOffset = 0;
    const afterRestart = await fetchMessages({
      offset: String(consumerOffset),
      limit: "1",
    });
    expect(afterRestart.body.messages[0].key).toBe("a");
  });
});
