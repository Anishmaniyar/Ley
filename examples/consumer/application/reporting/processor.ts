import { Message } from "../../../../src/broker/modules/message/message";
import { FAILURE_OPTION_REPORTING_CONFIG } from "./config";

// Handles reporting processing.
// 50% of attempts succeed and 50% throw to exercise retry,
// backoff, and shutdown paths.
export async function reportingProcessor(message: Message): Promise<void> {
  console.log("New message received:", message.key);
  console.log("Event:", message.value.event_type);
  console.log("Order ID:", message.value.payload.order_id);
  console.log("User ID:", message.value.payload.user_id);

  console.log("Processing report...");

  const randomVal = (FAILURE_OPTION_REPORTING_CONFIG.random ?? Math.random)();

  // Simulate asynchronous report processing.
  await new Promise<void>((resolve) => {
    setTimeout(resolve, FAILURE_OPTION_REPORTING_CONFIG.delayMs);
  });

  // Fail according to the configured failure rate.
  if (randomVal < FAILURE_OPTION_REPORTING_CONFIG.failureRate) {
    throw new Error(`Reporting processing failed for ${message.key}`);
  }

  console.log("Report generated successfully");
  console.log(`Report generated for order ${message.value.payload.order_id}`);
}
