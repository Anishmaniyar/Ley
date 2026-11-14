import type { Message } from "../../../src/broker/modules/message/message.js";
import { fetchMessages } from "../shared/broker-client.js";
import { installGracefulShutdown } from "../shared/shutdown.js";
import { BATCH_SIZE, POLL_INTERVAL_MS } from "./config.js";
import { OffsetManager } from "./offset/offset-manager.js";
import { processMessage } from "./processing/message-processor.js";
import { calculateDelay } from "./retry/retry-delay.js";
import type { RetryEntry } from "./retry/retry-entry.js";
import { RetryQueue } from "./retry/retry-queue.js";
import { RetryScheduler } from "./retry/retry-scheduler.js";
import { WakeUpSignal } from "../shared/wake-up_signal.js";
import { Mutex } from "../shared/async-mutex.js";

// Consumer state lives in this process only; the broker stores no consumer progress.
let running = true;
let loopPromise: Promise<void> | null = null;
let stopPromise: Promise<void> | null = null;

const offsetManager = new OffsetManager();

const retryQueue = new RetryQueue();
const wakeUpSignal = new WakeUpSignal();
const messageMutex = new Mutex();

// Aborting this signal cancels an in-flight fetch or poll wait, and cancels
// tasks that are still queued behind someone else inside the mutex.
const shutdownController = new AbortController();

// New messages and retries share this function, so they can never process at
// the same time. Tasks already waiting inside the mutex are cancelled when
// shutdown begins; a task that is already executing is allowed to finish.
const processMessageSafely = (message: Message) =>
  messageMutex.runExclusive(
    () => processMessage(message),
    shutdownController.signal,
  );

const retryScheduler = new RetryScheduler(
  retryQueue,
  wakeUpSignal,
  processMessageSafely,
);

// Waits for the poll interval, but wakes immediately when shutdown is requested.
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) {
      resolve();
      return;
    }

    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);

    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      resolve();
    };

    signal.addEventListener("abort", onAbort, { once: true });
  });
}

// Fetches batches and processes them sequentially until shutdown is requested.
async function consumeMessages(): Promise<void> {
  while (running) {
    let messages: Message[];

    try {
      messages = await fetchMessages(
        offsetManager.getOffset(),
        BATCH_SIZE,
        shutdownController.signal,
      );
    } catch (error) {
      // An abort here is expected during shutdown, not a broker failure.
      if (!running) {
        break;
      }

      console.error("Failed to fetch messages from broker:", error);
      await sleep(POLL_INTERVAL_MS, shutdownController.signal);
      continue;
    }

    // Do not start new work after shutdown was requested.
    if (!running) {
      break;
    }

    if (messages.length === 0) {
      await sleep(POLL_INTERVAL_MS, shutdownController.signal);
      continue;
    }

    for (const message of messages) {
      if (!running) {
        break;
      }

      try {
        await processMessageSafely(message);
        // Advance only after successful processing.
        offsetManager.advance();
      } catch (error) {
        // A task cancelled because shutdown began was never actually tried, so
        // do not schedule a retry or advance the offset for it.
        if (!running) {
          break;
        }

        console.error("Message processing failed:", error);

        const entry: RetryEntry = {
          message,
          retryCount: 0,
          nextRetryAt: Date.now() + calculateDelay(0),
          lastError: {
            name: error instanceof Error ? error.name : "Error",
            message: error instanceof Error ? error.message : String(error),
          },
        };

        const scheduled = retryScheduler.scheduleRetry(entry);

        if (scheduled) {
          offsetManager.advance();
        } else {
          if (!running) {
            break;
          }

          throw new Error("Retry scheduler is not running");
        }
      }
    }
  }
}

function start(): void {
  console.log("Notification consumer started");
  retryScheduler.start();
  loopPromise = consumeMessages().catch((error) => {
    console.error("Consumer loop crashed:", error);
    process.exitCode = 1;
  });
}

// Idempotent: repeated calls share the same shutdown promise.
function stop(): Promise<void> {
  if (stopPromise) {
    return stopPromise;
  }

  stopPromise = (async () => {
    running = false;
    shutdownController.abort();

    // Stop the scheduler immediately so a retry that is still queued behind the
    // mutex is treated as cancellation, not as a failed attempt to reschedule.
    const loopDone = loopPromise ?? Promise.resolve();

    await Promise.all([loopDone, retryScheduler.stop()]);

    console.log("Notification consumer stopped");
  })();

  return stopPromise;
}

installGracefulShutdown([stop]);
start();
