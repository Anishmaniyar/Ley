import type { Message } from "../message/message";
import AppError from "../../../shared/errors/appError";

// Stores messages in insertion order. Array indexes represent offsets.
let messages: Message[] = [];

// Appends a new message to the end of the log.
export function append(message: Message): void {
  messages.push(structuredClone(message));
}

// Reads messages starting from the given offset.
export function read(offset: number, limit: number): Message[] {
  if (!Number.isInteger(offset) || offset < 0) {
    throw new AppError("Offset must be a non-negative integer", 400);
  }

  if (!Number.isInteger(limit) || limit <= 0) {
    throw new AppError("Limit must be a positive integer", 400);
  }

  return structuredClone(messages.slice(offset, offset + limit));
}

// Returns the total number of messages in the log.
export function length(): number {
  return messages.length;
}
