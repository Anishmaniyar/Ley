import type { ConsumerConfig } from "../config.js";

type RetryDelayConfig = Pick<
  ConsumerConfig,
  "baseRetryDelayMs" | "maxRetryDelayMs" | "backoffMultiplier" | "jitterFactor"
>;

// Calculate exponential backoff, apply jitter, and enforce the maximum delay.
export function calculateDelay(
  retryCount: number,
  config: RetryDelayConfig,
): number {
  if (
    !Number.isFinite(retryCount) ||
    !Number.isInteger(retryCount) ||
    retryCount < 0
  ) {
    throw new RangeError("Retry count must be a non-negative finite integer");
  }

  const delay = Math.min(
    config.baseRetryDelayMs * config.backoffMultiplier ** retryCount,
    config.maxRetryDelayMs,
  );

  const minDelay = delay * (1 - config.jitterFactor);
  const maxDelay = delay * (1 + config.jitterFactor);

  const randomDelay = minDelay + Math.random() * (maxDelay - minDelay);

  return Math.min(randomDelay, config.maxRetryDelayMs);
}
