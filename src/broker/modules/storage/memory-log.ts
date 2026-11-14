import type { Message } from "../message/message";
import AppError from "../../../shared/errors/appError";

// Stores messages in insertion order. Array indexes represent offsets.
let messages: Message[] = [];

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

// Offset assignment and insertion must stay in one synchronous block.
// Node runs this function to completion before any other request handler, so
// concurrent publishers cannot interleave between reading the length and
// pushing. Adding an `await` here would break offset uniqueness and ordering.
export function append(message: Message): number {
  const offset = messages.length;

  messages.push(structuredClone(message));

  return offset;
}

// Returns the total number of messages in the log.
export function length(): number {
  return messages.length;
}
