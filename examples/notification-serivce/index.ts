import type { Message } from "../../src/broker/modules/message/message.js";

// Consumer tracks its own progress; offset is not stored by the broker.
let currentOffset = 0;
const batchSize = 10;

// Fetch a batch starting from the consumer's current offset.
async function fetchMessages(): Promise<Message[] | null> {
  const url = new URL("http://localhost:3000/api/v1/messages");

  url.searchParams.set("offset", String(currentOffset));
  url.searchParams.set("limit", String(batchSize));

  try {
    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`Broker returned HTTP ${response.status}`);
    }

    const data = (await response.json()) as { messages: Message[] };

    console.log("Consumer received the messages");

    return data.messages;
  } catch (error) {
    console.error("Failed to fetch messages from broker:", error);
    return null;
  }
}

// Process one message by simulating a notification.
async function processMessage(message: Message): Promise<void> {
  console.log("New message received:", message.key);
  console.log("Event:", message.value.event_type);
  console.log("Order ID:", message.value.payload.order_id);
  console.log("User ID:", message.value.payload.user_id);

  console.log("Processing notification...");

  // Simulate asynchronous notification delivery.
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 5000);
  });

  console.log("Notification sent");
  console.log(
    `Your order ${message.value.payload.order_id} has been successfully placed`,
  );
}

// Fetches and processes messages sequentially.
async function consumeMessages() {
  const messages = await fetchMessages();

  // Stop if fetching failed.
  if (messages === null) {
    return;
  }

  // Process each message before moving to the next.
  for (const message of messages) {
    try {
      await processMessage(message);

      // Advance only after successful processing.
      currentOffset++;
    } catch (error) {
      console.error("Message processing failed:", error);
      break;
    }
  }
}

consumeMessages();
