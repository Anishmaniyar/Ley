// Creates a dummy order and publishes its event to the broker using HTTP.

const order = {
  order_id: "1234",
  user_id: "user_123",
  amount: 1000,
  currency: "INR",
};

const orderEvent = {
  event_type: "order.created",
  payload: order,
};

const message = {
  key: order.order_id,
  value: orderEvent,
  timestamp: {
    created_at: new Date().toISOString(),
  },
  headers: {
    source: "order-service",
    content_type: "application/json",
  },
};

// Producer order service sends the request to broker to store it right now we have dummy data and using fetch as it is low level and works fine
async function publishOrder() {
  const url = "http://localhost:3000/api/v1/messages";

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(message),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message: string };
      throw new Error(
        `Broker rejected Order message: ${response.status} - ${errorData.message}`,
      );
    }

    const data = (await response.json()) as { offset: number };

    console.log("Broker accepted Order message.");
    console.log("Assigned offset:", data.offset);
  } catch (error) {
    console.error("Failed to publish order:", error);
  }
}

publishOrder();
