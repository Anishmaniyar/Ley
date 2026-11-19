export interface ConsumerConfig {
  batchSize: number;
  pollIntervalMs: number;

  maxRetries: number;
  baseRetryDelayMs: number;
  maxRetryDelayMs: number;
  backoffMultiplier: number;
  jitterFactor: number;
}

export const DEFAULT_CONSUMER_CONFIG: ConsumerConfig = {
  batchSize: 10,
  pollIntervalMs: 1000,

  maxRetries: 5,
  baseRetryDelayMs: 2000,
  maxRetryDelayMs: 20000,
  backoffMultiplier: 2,
  jitterFactor: 0.3,
};

export function resolveConsumerConfig(
  overrides: Partial<ConsumerConfig> = {},
): ConsumerConfig {
  return {
    ...DEFAULT_CONSUMER_CONFIG,
    ...overrides,
  };
}

// Temporary exports to keep existing retry modules working.
// We will remove these after updating those modules.
export const BATCH_SIZE = DEFAULT_CONSUMER_CONFIG.batchSize;
export const POLL_INTERVAL_MS = DEFAULT_CONSUMER_CONFIG.pollIntervalMs;

export const MAX_RETRIES = DEFAULT_CONSUMER_CONFIG.maxRetries;
export const BASE_RETRY_DELAY_MS = DEFAULT_CONSUMER_CONFIG.baseRetryDelayMs;
export const MAX_RETRY_DELAY_MS = DEFAULT_CONSUMER_CONFIG.maxRetryDelayMs;
export const BACKOFF_MULTIPLIER = DEFAULT_CONSUMER_CONFIG.backoffMultiplier;
export const JITTER_FACTOR = DEFAULT_CONSUMER_CONFIG.jitterFactor;
