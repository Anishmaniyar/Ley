import type { Message } from "../../../../src/broker/modules/message/message.js";

export interface RetryEntry {
  message: Message;
  retryCount: number;
  nextRetryAt: number;
  lastError: {
    name: string;
    message: string;
    stack?: string;
  };
}
