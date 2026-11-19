import type { Message } from "../../../src/broker/modules/message/message.js";

const BROKER_BASE_URL = "http://localhost:3000/api/v1/";

interface ConsumerRegistrationResponse {
  message: string;
  destination: string;
  status: "active";
  registeredAt: string;
}

// Fetches one batch of messages from the broker.
export async function fetchMessages(
  offset: number,
  limit: number,
  signal: AbortSignal,
): Promise<Message[]> {
  const url = new URL("messages", BROKER_BASE_URL);

  url.searchParams.set("offset", String(offset));
  url.searchParams.set("limit", String(limit));

  const response = await fetch(url, { signal });

  if (!response.ok) {
    throw new Error(`Broker returned HTTP ${response.status}`);
  }

  const data = (await response.json()) as { messages: Message[] };

  return data.messages;
}

// Registers a consumer destination with the broker.
export async function registerConsumer(
  destination: string,
): Promise<ConsumerRegistrationResponse> {
  const url = new URL("consumers", BROKER_BASE_URL);

  const response = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ destination }),
  });

  if (!response.ok) {
    throw new Error(`Broker returned HTTP ${response.status}`);
  }

  return (await response.json()) as ConsumerRegistrationResponse;
}

// Unregisters a consumer destination from the broker.
export async function unregisterConsumer(destination: string): Promise<void> {
  const url = new URL(
    `consumers/${encodeURIComponent(destination)}`,
    BROKER_BASE_URL,
  );

  const response = await fetch(url, {
    method: "DELETE",
  });

  if (!response.ok) {
    throw new Error(`Broker returned HTTP ${response.status}`);
  }
}
