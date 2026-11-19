import { Message } from "../../../../src/broker/modules/message/message";
import {
  FAILURE_OPTION_NOTIFICATION_CONFIG,
  type NotificationFailureOptionConfig,
} from "./config";

// Handles notification processing.
// 60% of attempts succeed and 40% throw so retry, backoff, and shutdown paths
// can be exercised end to end. Both values are injectable for deterministic tests.
export async function notificationProcessor(
  message: Message,
  overrides: Partial<NotificationFailureOptionConfig> = {},
): Promise<void> {
  const config = { ...FAILURE_OPTION_NOTIFICATION_CONFIG, ...overrides };

  console.log("New message received:", message.key);
  console.log("Event:", message.value.event_type);
  console.log("Order ID:", message.value.payload.order_id);
  console.log("User ID:", message.value.payload.user_id);

  console.log("Processing notification...");

  const randomVal = (config.random ?? Math.random)();

  // Simulate asynchronous notification delivery.
  await new Promise<void>((resolve) => {
    setTimeout(resolve, config.delayMs);
  });

  // The delivery attempt fails for the configured share of messages.
  if (randomVal < config.failureRate) {
    throw new Error(`Notification delivery failed for ${message.key}`);
  }

  console.log("Notification sent");
  console.log(
    `Your order ${message.value.payload.order_id} has been successfully placed`,
  );
}
