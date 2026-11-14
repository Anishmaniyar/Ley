const payment = {
  payment_id: "payment_123",
  order_id: "order_456",
  user_id: "user_123",
  amount: 1000,
  currency: "INR",
  status: "completed",
};

const paymentEvent = {
  event_type: "payment.completed",
  payload: payment,
};

const messagePayment = {
  key: payment.order_id,
  value: paymentEvent,
  timestamp: {
    created_at: new Date().toISOString(),
  },
  headers: {
    source: "payment-service",
    content_type: "application/json",
  },
};

// Producer payment service sends the request to broker to store it right now we have dummy data and using fetch as it is low level and works fine
async function publishPayment() {
  const url = "http://localhost:3000/api/v1/messages";

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(messagePayment),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message: string };
      throw new Error(
        `Broker rejected Payment message: ${response.status} - ${errorData.message}`,
      );
    }

    const data = (await response.json()) as { offset: number };

    console.log("Broker accepted Payment message.");
    console.log("Assigned offset:", data.offset);
  } catch (error) {
    console.error("Failed to publish Payment message:", error);
  }
}

publishPayment();
