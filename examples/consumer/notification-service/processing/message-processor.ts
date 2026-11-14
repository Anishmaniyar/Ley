import { Message } from "../../../../src/broker/modules/message/message";

export interface ProcessMessageOptions {
  delayMs?: number;
  failureRate?: number;
  random?: () => number;
}

// Handles notification processing.
// 60% of attempts succeed and 40% throw so retry, backoff, and shutdown paths
// can be exercised end to end. Both values are injectable for deterministic tests.
export async function processMessage(
  message: Message,
  options: ProcessMessageOptions = {},
): Promise<void> {
  const {
    delayMs = 5000,
    failureRate = 0.4,
    random = Math.random,
  } = options;

  console.log("New message received:", message.key);
  console.log("Event:", message.value.event_type);
  console.log("Order ID:", message.value.payload.order_id);
  console.log("User ID:", message.value.payload.user_id);

  console.log("Processing notification...");

  // Simulate asynchronous notification delivery.
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });

  // The delivery attempt fails for the configured share of messages.
  if (random() < failureRate) {
    throw new Error(`Notification delivery failed for ${message.key}`);
  }

  console.log("Notification sent");
  console.log(
    `Your order ${message.value.payload.order_id} has been successfully placed`,
  );
}
