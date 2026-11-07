import { messageSchema } from "../message/message.schema.js";
import { append, length, read } from "../storage/memory-log.js";
import AppError from "../../../shared/errors/appError.js";
import type { Message } from "../message/message.js";

// input is unknown type, as it comes from an external source (e.g., HTTP request body)
export function publishMessage(input: unknown): number {
  // validate the input against the message schema
  const result = messageSchema.safeParse(input);

  // If validation fails, throw error
  if (!result.success) {
    throw new AppError("Invalid message structure", 400);
  }

  // Determine next ofset and append the validated message to the log
  // THIS WORKS FOR NOW LATER WHEN WE DO CONCURRENT AND ASYNCHRONOURS STORAGE REVISIT
  const offset = length();

  // Append the validated message to the log
  append(result.data);

  return offset;
}

export function fetchMessages(offset: number, limit: number): Message[] {
  // fetch all the messages from the log starting from the given offset and up to the specified limit
  return read(offset, limit);
}
