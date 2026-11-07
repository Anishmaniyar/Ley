import type { Request, Response, NextFunction } from "express";
import AppError from "../shared/errors/appError";

// Converts application errors into consistent HTTP responses.
export function errorMiddleware(
  err: Error,
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  let error = err;

  // Handle malformed JSON from Express JSON parser.
  if (err instanceof SyntaxError && "status" in err && err.status === 400) {
    error = new AppError("Invalid JSON request body", 400);
  }

  // Handle known operational errors.
  if (error instanceof AppError && error.isOperational) {
    res.status(error.statusCode).json({
      status: error.status,
      message: error.message,
    });
    return;
  }

  // Log unexpected errors internally, not in the API response.
  console.error(err);

  res.status(500).json({
    status: "error",
    message: "Something went wrong",
  });
}
