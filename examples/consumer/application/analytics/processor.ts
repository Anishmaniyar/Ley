import { Message } from "../../../../src/broker/modules/message/message";
import { FAILURE_OPTION_ANALYTICS_CONFIG } from "./config";

// Handles analytics processing.
// 80% of attempts succeed and 20% throw so retry, backoff, and shutdown paths
// can be exercised end to end. Both values are injectable for deterministic tests.
export async function analyticsProcessor(message: Message): Promise<void> {
  console.log("New message received:", message.key);
  console.log("Event:", message.value.event_type);
  console.log("Order ID:", message.value.payload.order_id);
  console.log("User ID:", message.value.payload.user_id);

  console.log("Processing analytics...");

  const randomVal = (FAILURE_OPTION_ANALYTICS_CONFIG.random ?? Math.random)();

  // Simulate asynchronous analytics delivery.
  await new Promise<void>((resolve) => {
    setTimeout(resolve, FAILURE_OPTION_ANALYTICS_CONFIG.delayMs);
  });

  // The delivery attempt fails for the configured share of messages.
  if (randomVal < FAILURE_OPTION_ANALYTICS_CONFIG.failureRate) {
    throw new Error(`Analytics delivery failed for ${message.key}`);
  }

  console.log("Analytics sent");
  console.log(
    `Your order ${message.value.payload.order_id} has been successfully recorded`,
  );
}
