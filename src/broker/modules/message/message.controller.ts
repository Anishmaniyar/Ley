import type { Request, Response } from "express";
import * as brokerService from "../broker/broker.service";
import AppError from "../../../shared/errors/appError";

export function publishMessageController(req: Request, res: Response) {
  // HTTP publishing logic
  const pubishMessageInput = req.body;

  const offset = brokerService.publishMessage(pubishMessageInput);

  res.status(201).json({ offset, message: "Message published successfully" });
}

export function fetchMessagesController(req: Request, res: Response) {
  // HTTP fetching logic

  const offsetParam = req.query.offset;
  const limitParam = req.query.limit;

  // Apply defaults only when parameters are missing.
  const offset = offsetParam === undefined ? 0 : Number(offsetParam);
  const limit = limitParam === undefined ? 10 : Number(limitParam);

  // Reject invalid query parameters.
  if (!Number.isInteger(offset) || offset < 0) {
    throw new AppError("Offset must be a non-negative integer", 400);
  }

  if (!Number.isInteger(limit) || limit <= 0) {
    throw new AppError("Limit must be a positive integer", 400);
  }

  const messages = brokerService.fetchMessages(offset, limit);

  res.status(200).json({ messages, offset, limit });
}
