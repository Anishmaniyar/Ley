import {
  BASE_RETRY_DELAY_MS,
  MAX_RETRY_DELAY_MS,
  BACKOFF_MULTIPLIER,
  JITTER_FACTOR,
} from "../config";

// Calculate exponential backoff, apply jitter, and enforce the maximum delay.
export function calculateDelay(retryCount: number): number {
  if (
    !Number.isFinite(retryCount) ||
    !Number.isInteger(retryCount) ||
    retryCount < 0
  ) {
    throw new RangeError("Retry count must be a non-negative finite integer");
  }

  // Calculate exponential backoff with an upper limit.
  const delay = Math.min(
    BASE_RETRY_DELAY_MS * BACKOFF_MULTIPLIER ** retryCount,
    MAX_RETRY_DELAY_MS,
  );

  // Define the allowed jitter range around the calculated delay.
  const minDelay = delay * (1 - JITTER_FACTOR);
  const maxDelay = delay * (1 + JITTER_FACTOR);

  // Randomize retry timing to prevent synchronized retries (thundering herd).
  const randomDelay = minDelay + Math.random() * (maxDelay - minDelay);

  // Ensure jitter never pushes the delay above the configured maximum.
  return Math.min(randomDelay, MAX_RETRY_DELAY_MS);
}
