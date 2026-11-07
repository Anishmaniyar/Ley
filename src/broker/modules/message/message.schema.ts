import { z } from "zod";

// Validates the structure and required fields of incoming broker messages
export const messageSchema = z.object({
  key: z.string(),

  value: z
    .object({
      event_type: z.string(),
      payload: z.record(z.string(), z.unknown()),
    })
    .refine((value) => Object.keys(value.payload).length > 0),

  timestamp: z.object({
    created_at: z.iso.datetime(),
  }),

  headers: z.object({
    source: z.string(),
    content_type: z.string(),
  }),
});

export type MessageInput = z.infer<typeof messageSchema>;
