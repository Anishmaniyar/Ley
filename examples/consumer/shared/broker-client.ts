import type { Message } from "../../../src/broker/modules/message/message.js";

const BROKER_URL = "http://localhost:3000/api/v1/messages";

// Fetches one batch of messages from the broker.
// offset is the log position to start from; limit caps how many messages are returned.
// The AbortSignal cancels an in-flight HTTP request when shutdown is requested.
// Non-2xx responses throw so the caller decides whether to retry.

export async function fetchMessages(
  offset: number,
  limit: number,
  signal: AbortSignal,
): Promise<Message[]> {
  const url = new URL(BROKER_URL);

  url.searchParams.set("offset", String(offset));
  url.searchParams.set("limit", String(limit));

  const response = await fetch(url, { signal });

  if (!response.ok) {
    throw new Error(`Broker returned HTTP ${response.status}`);
  }

  const data = (await response.json()) as { messages: Message[] };

  return data.messages;
}
