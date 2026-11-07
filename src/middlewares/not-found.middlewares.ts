import type { Request, Response, NextFunction } from "express";
import AppError from "../shared/errors/appError";

// Handles requests to routes that do not exist.
export function notFoundMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  next(new AppError(`Route ${req.method} ${req.originalUrl} not found`, 404));
}
