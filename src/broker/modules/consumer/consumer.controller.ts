// Handles HTTP requests and response
import type { Request, Response } from "express";
import * as ConsumerRegistry from "./consumer-registry";
import AppError from "../../../shared/errors/appError";

export function registrationConsumerController(req: Request, res: Response) {
  const destination = req.body?.destination;

  if (typeof destination !== "string" || !destination.trim()) {
    throw new AppError("A valid destination is required", 400);
  }

  const response = ConsumerRegistry.register(destination.trim());

  return res.status(201).json({
    message: "Consumer registered successfully",
    destination: response.destination,
    status: response.status,
    registeredAt: response.registeredAt,
  });
}
export function unregistrationConsumerController(req: Request, res: Response) {
  const { destination } = req.params;

  if (typeof destination !== "string" || destination.trim().length === 0) {
    throw new AppError("Destination parameter is required", 400);
  }

  ConsumerRegistry.unregister(destination);

  return res.status(204).end();
}
