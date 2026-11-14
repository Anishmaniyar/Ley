// Creates a dummy inventory and publishes its event to the broker using HTTP.

const inventory = {
  inventory_id: "inventory_123",
  product_id: "product_456",
  quantity: 10,
  warehouse_id: "warehouse_789",
};

const inventoryEvent = {
  event_type: "inventory.reserved",
  payload: inventory,
};

const messageInventory = {
  key: inventory.product_id,
  value: inventoryEvent,
  timestamp: {
    created_at: new Date().toISOString(),
  },
  headers: {
    source: "inventory-service",
    content_type: "application/json",
  },
};

// Producer inventory service sends the request to broker to store it right now we have dummy data and using fetch as it is low level and works fine
async function publishInventory() {
  const url = "http://localhost:3000/api/v1/messages";

  try {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(messageInventory),
    });

    if (!response.ok) {
      const errorData = (await response.json()) as { message: string };
      throw new Error(
        `Broker rejected Inventory message: ${response.status} - ${errorData.message}`,
      );
    }

    const data = (await response.json()) as { offset: number };

    console.log("Broker accepted Inventory message.");
    console.log("Assigned offset:", data.offset);
  } catch (error) {
    console.error("Failed to publish Inventory message:", error);
  }
}

publishInventory();
