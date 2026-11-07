import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Request, Response } from "express";
import type AppError from "../../src/shared/errors/appError.js";
import type { Message } from "../../src/broker/modules/message/message.js";

type AppErrorModule = typeof import("../../src/shared/errors/appError.js");
type StorageModule = typeof import("../../src/broker/modules/storage/memory-log.js");
type BrokerModule = typeof import("../../src/broker/modules/broker/broker.service.js");
type SchemaModule = typeof import("../../src/broker/modules/message/message.schema.js");
type ControllerModule = typeof import("../../src/broker/modules/message/message.controller.js");

let AppErrorClass: AppErrorModule["default"];
let storage: StorageModule;
let broker: BrokerModule;
let schema: SchemaModule;
let controller: ControllerModule;

// Reset modules so each test gets a fresh in-memory log.
beforeEach(async () => {
  vi.resetModules();
  AppErrorClass = (await import("../../src/shared/errors/appError.js")).default;
  storage = await import("../../src/broker/modules/storage/memory-log.js");
  broker = await import("../../src/broker/modules/broker/broker.service.js");
  schema = await import("../../src/broker/modules/message/message.schema.js");
  controller = await import(
    "../../src/broker/modules/message/message.controller.js"
  );
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

function messageWithout(field: keyof Message): Record<string, unknown> {
  const message: Record<string, unknown> = { ...validMessage() };
  delete message[field];
  return message;
}

function createResponseStub() {
  return {
    statusCode: 200,
    body: undefined as unknown,
    status(code: number) {
      this.statusCode = code;
      return this;
    },
    json(payload: unknown) {
      this.body = payload;
      return this;
    },
  };
}

describe("message schema", () => {
  it("accepts a valid message", () => {
    expect(schema.messageSchema.safeParse(validMessage()).success).toBe(true);
  });

  it.each([
    ["missing key", messageWithout("key")],
    ["missing value", messageWithout("value")],
    ["missing timestamp", messageWithout("timestamp")],
    ["missing headers", messageWithout("headers")],
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
    [
      "missing header field",
      { ...validMessage(), headers: { source: "order-service" } },
    ],
    ["non-object input", "not-a-message"],
    ["null input", null],
  ])("rejects invalid messages: %s", (_label, input) => {
    expect(schema.messageSchema.safeParse(input).success).toBe(false);
  });
});

describe("memory storage", () => {
  it("starts with an empty log", () => {
    expect(storage.length()).toBe(0);
    expect(storage.read(0, 10)).toEqual([]);
  });

  it("appends and reads messages in insertion order", () => {
    storage.append(validMessage({ key: "first" }));
    storage.append(validMessage({ key: "second" }));

    expect(storage.length()).toBe(2);
    expect(storage.read(0, 10).map((message) => message.key)).toEqual([
      "first",
      "second",
    ]);
  });

  it("reads from an offset with a limit", () => {
    ["a", "b", "c", "d"].forEach((key) =>
      storage.append(validMessage({ key })),
    );

    expect(storage.read(1, 2).map((message) => message.key)).toEqual([
      "b",
      "c",
    ]);
    expect(storage.read(0, 10)).toHaveLength(4);
  });

  it("returns an empty array when the offset is beyond the log", () => {
    storage.append(validMessage());

    expect(storage.read(5, 10)).toEqual([]);
  });

  it.each([
    ["negative offset", -1, 10, "Offset must be a non-negative integer"],
    ["non-integer offset", 1.5, 10, "Offset must be a non-negative integer"],
    ["zero limit", 0, 0, "Limit must be a positive integer"],
    ["negative limit", 0, -1, "Limit must be a positive integer"],
    ["non-integer limit", 0, 1.5, "Limit must be a positive integer"],
  ])("rejects %s", (_label, offset, limit, expectedMessage) => {
    expect(() => storage.read(offset, limit)).toThrowError(expectedMessage);
  });

  it("throws AppError with HTTP 400 for invalid reads", () => {
    try {
      storage.read(-1, 10);
      throw new Error("Expected read to throw");
    } catch (error) {
      expect(error).toBeInstanceOf(AppErrorClass);
      expect((error as AppError).statusCode).toBe(400);
    }
  });

  it("does not expose internal messages through external references", () => {
    const original = validMessage();
    storage.append(original);

    original.key = "mutated-after-append";
    original.value.payload.amount = 999;

    expect(storage.read(0, 1)[0].key).toBe("order-1");
    expect(storage.read(0, 1)[0].value.payload.amount).toBe(100);

    const fetched = storage.read(0, 1);
    fetched[0].key = "mutated-after-read";
    fetched[0].value.payload.amount = 0;

    expect(storage.read(0, 1)[0].key).toBe("order-1");
    expect(storage.read(0, 1)[0].value.payload.amount).toBe(100);
  });
});

describe("broker service", () => {
  it("assigns sequential offsets", () => {
    expect(broker.publishMessage(validMessage({ key: "a" }))).toBe(0);
    expect(broker.publishMessage(validMessage({ key: "b" }))).toBe(1);
    expect(broker.publishMessage(validMessage({ key: "c" }))).toBe(2);

    expect(storage.length()).toBe(3);
  });

  it("rejects invalid messages without appending", () => {
    expect(() =>
      broker.publishMessage({ key: "missing-fields" }),
    ).toThrowError("Invalid message structure");

    expect(storage.length()).toBe(0);
  });

  it("fetches messages using offset and limit", () => {
    ["a", "b", "c", "d"].forEach((key) =>
      broker.publishMessage(validMessage({ key })),
    );

    expect(
      broker.fetchMessages(1, 2).map((message) => message.key),
    ).toEqual(["b", "c"]);
    expect(broker.fetchMessages(10, 10)).toEqual([]);
  });

  it("rejects invalid fetch arguments", () => {
    expect(() => broker.fetchMessages(-1, 10)).toThrowError(AppErrorClass);
    expect(() => broker.fetchMessages(0, 0)).toThrowError(AppErrorClass);
  });
});

describe("query validation", () => {
  it("uses default offset and limit when omitted", () => {
    broker.publishMessage(validMessage());
    const req = { query: {} } as unknown as Request;
    const res = createResponseStub();

    controller.fetchMessagesController(req, res as unknown as Response);

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({
      messages: [validMessage()],
      offset: 0,
      limit: 10,
    });
  });

  it("passes valid offset and limit to the broker", () => {
    broker.publishMessage(validMessage({ key: "a" }));
    broker.publishMessage(validMessage({ key: "b" }));
    const req = {
      query: { offset: "1", limit: "1" },
    } as unknown as Request;
    const res = createResponseStub();

    controller.fetchMessagesController(req, res as unknown as Response);

    expect(res.statusCode).toBe(200);
    expect(res.body).toMatchObject({ offset: 1, limit: 1 });
    expect(
      (res.body as { messages: Message[] }).messages.map(
        (message) => message.key,
      ),
    ).toEqual(["b"]);
  });

  it.each([
    ["negative offset", { offset: "-1" }, "Offset must be a non-negative integer"],
    ["non-numeric offset", { offset: "abc" }, "Offset must be a non-negative integer"],
    ["fractional offset", { offset: "1.5" }, "Offset must be a non-negative integer"],
    ["zero limit", { limit: "0" }, "Limit must be a positive integer"],
    ["negative limit", { limit: "-3" }, "Limit must be a positive integer"],
    ["non-numeric limit", { limit: "abc" }, "Limit must be a positive integer"],
    ["fractional limit", { limit: "1.5" }, "Limit must be a positive integer"],
  ])("rejects %s", (_label, query, expectedMessage) => {
    const req = { query } as unknown as Request;
    const res = createResponseStub();

    expect(() =>
      controller.fetchMessagesController(req, res as unknown as Response),
    ).toThrowError(expectedMessage);
  });
});
