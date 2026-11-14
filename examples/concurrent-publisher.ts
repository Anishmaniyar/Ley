const BROKER_URL = "http://localhost:3000/api/v1/messages";
const TOTAL_MESSAGES = 100;
const REQUEST_TIMEOUT_MS = 10_000;

interface Message {
  key: string;
  value: {
    event_type: string;
    payload: Record<string, unknown>;
  };
  timestamp: {
    created_at: string;
  };
  headers: {
    source: string;
    content_type: string;
  };
}

interface PublishResponse {
  status: string;
  offset: number;
}

function createMessage(index: number): Message {
  const producerIndex = index % 3;
  const messageId = index + 1;

  if (producerIndex === 0) {
    const orderId = `order_${messageId}`;

    return {
      key: orderId,
      value: {
        event_type: "order.created",
        payload: {
          order_id: orderId,
          user_id: `user_${messageId}`,
          amount: 1500,
          currency: "INR",
        },
      },
      timestamp: {
        created_at: new Date().toISOString(),
      },
      headers: {
        source: "order-service",
        content_type: "application/json",
      },
    };
  }

  if (producerIndex === 1) {
    const productId = `product_${messageId}`;

    return {
      key: productId,
      value: {
        event_type: "inventory.reserved",
        payload: {
          inventory_id: `inventory_${messageId}`,
          product_id: productId,
          quantity: 10,
          warehouse_id: `warehouse_${messageId}`,
        },
      },
      timestamp: {
        created_at: new Date().toISOString(),
      },
      headers: {
        source: "inventory-service",
        content_type: "application/json",
      },
    };
  }

  const orderId = `order_${messageId}`;

  return {
    key: orderId,
    value: {
      event_type: "payment.completed",
      payload: {
        payment_id: `payment_${messageId}`,
        order_id: orderId,
        user_id: `user_${messageId}`,
        amount: 1000,
        currency: "INR",
        status: "completed",
      },
    },
    timestamp: {
      created_at: new Date().toISOString(),
    },
    headers: {
      source: "payment-service",
      content_type: "application/json",
    },
  };
}

async function publishMessage(message: Message): Promise<PublishResponse> {
  const response = await fetch(BROKER_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(message),
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });

  const data: unknown = await response.json().catch(() => null);

  if (!response.ok) {
    throw new Error(
      `${message.headers.source}: HTTP ${response.status} - ${JSON.stringify(data)}`,
    );
  }

  if (
    !data ||
    typeof data !== "object" ||
    !("offset" in data) ||
    typeof data.offset !== "number" ||
    !Number.isInteger(data.offset)
  ) {
    throw new Error(`${message.headers.source}: Invalid broker response`);
  }

  return data as PublishResponse;
}

async function runConcurrentPublishing() {
  console.log(
    `Starting concurrent publishing of ${TOTAL_MESSAGES} messages...\n`,
  );

  const messages = Array.from({ length: TOTAL_MESSAGES }, (_, index) =>
    createMessage(index),
  );

  const results = await Promise.allSettled(
    messages.map((message) => publishMessage(message)),
  );

  const successfulResults: PublishResponse[] = [];
  const failedResults: string[] = [];

  results.forEach((result, index) => {
    if (result.status === "fulfilled") {
      successfulResults.push(result.value);
    } else {
      failedResults.push(`Message ${index + 1}: ${result.reason}`);
    }
  });

  const offsets = successfulResults.map((result) => result.offset);
  const uniqueOffsets = new Set(offsets);

  const sortedOffsets = [...offsets].sort((a, b) => a - b);

  const hasContinuousOffsets =
    sortedOffsets.length > 0 &&
    sortedOffsets[sortedOffsets.length - 1] - sortedOffsets[0] + 1 ===
      sortedOffsets.length &&
    uniqueOffsets.size === sortedOffsets.length;

  console.log("\n========== PUBLISHING RESULTS ==========\n");

  console.log(`Total messages sent:       ${TOTAL_MESSAGES}`);
  console.log(`Successfully published:    ${successfulResults.length}`);
  console.log(`Failed requests:            ${failedResults.length}`);
  console.log(`Unique assigned offsets:    ${uniqueOffsets.size}`);
  console.log(
    `Offset range:               ${
      sortedOffsets.length > 0
        ? `${sortedOffsets[0]} - ${sortedOffsets[sortedOffsets.length - 1]}`
        : "No successful offsets"
    }`,
  );
  console.log(`Offsets are continuous:     ${hasContinuousOffsets}`);

  if (failedResults.length > 0) {
    console.log("\n========== FAILED REQUESTS ==========\n");

    failedResults.forEach((error) => {
      console.error(error);
    });
  }

  console.log("\n========== PRODUCER BREAKDOWN ==========\n");

  for (const source of [
    "order-service",
    "inventory-service",
    "payment-service",
  ]) {
    const count = messages.filter(
      (message) => message.headers.source === source,
    ).length;

    console.log(`${source}: ${count} messages`);
  }

  console.log("\n========== FINAL STATUS ==========\n");

  if (
    successfulResults.length === TOTAL_MESSAGES &&
    uniqueOffsets.size === TOTAL_MESSAGES &&
    hasContinuousOffsets
  ) {
    console.log("SUCCESS: All messages received unique, continuous offsets.");
  } else {
    console.log("FAILURE: Check the publishing results above.");
  }
}

runConcurrentPublishing().catch((error) => {
  console.error("Concurrent publishing test failed:", error);
  process.exitCode = 1;
});
