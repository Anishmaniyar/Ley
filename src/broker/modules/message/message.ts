// Represents event-specific information.
export interface MessageValue {
  event_type: string;
  payload: Record<string, unknown>;
}

// Represents the producer's event creation time.
export interface MessageTimestamp {
  created_at: string;
}

// Represents message metadata.
export interface MessageHeaders {
  source: string;
  content_type: string;
}

// Represents the complete message accepted by the broker.
export interface Message {
  key: string;
  value: MessageValue;
  timestamp: MessageTimestamp;
  headers: MessageHeaders;
}
