import { RetryQueue } from "./retry-queue";
import { WakeUpSignal } from "../../shared/wake-up_signal";
import { RetryEntry } from "./retry-entry";
import { calculateDelay } from "./retry-delay";
import { MAX_RETRIES } from "../config";
type MessageProcessor = (message: RetryEntry["message"]) => Promise<void>;

export class RetryScheduler {
  // prevents duplicate start and reect new retry entries after shutdown
  private running = false;

  private loopPromise: Promise<void> | null = null;

  // Captures an unexpected crash so it can be surfaced by stop() instead of
  // becoming an unhandled Promise rejection.
  private loopError: unknown = null;

  // Messages that exhausted every retry attempt. A real deployment would route
  // these to a durable Dead Letter Queue instead of keeping them in memory.
  private readonly deadLetters: RetryEntry[] = [];

  constructor(
    private readonly retryQueue: RetryQueue,
    private readonly wakeUpSignal: WakeUpSignal,
    private readonly processMessage: MessageProcessor,
  ) {}

  // executes the scheduler
  private async run(): Promise<void> {
    while (this.running) {
      const retryEntry = this.retryQueue.peek();

      // No pending retries: wait for a notification.
      if (!retryEntry) {
        await this.wakeUpSignal.wait();
        continue;
      }

      // Calculate time remaining until the earliest retry.
      const delay = retryEntry.nextRetryAt - Date.now();

      // Retry isn't due yet: wait until deadline or notification.
      if (delay > 0) {
        await this.wakeUpSignal.wait(delay);
        continue;
      }

      // Retry is due: remove it from the queue.
      const dueEntry = this.retryQueue.dequeue();

      if (!dueEntry) {
        continue;
      }

      try {
        await this.processMessage(dueEntry.message);
      } catch (error) {
        // Shutdown began while this retry was in flight or waiting for the
        // mutex. Do not count it as a retry or enqueue it again.
        if (!this.running) {
          break;
        }

        dueEntry.retryCount++;

        // Always keep the most recent failure attached to the entry.
        dueEntry.lastError = {
          name: error instanceof Error ? error.name : "Error",
          message: error instanceof Error ? error.message : String(error),
          stack: error instanceof Error ? error.stack : undefined,
        };

        if (dueEntry.retryCount >= MAX_RETRIES) {
          this.deadLetters.push(dueEntry);
          console.error("Maximum retries exhausted:", dueEntry.message.key);
          continue;
        }

        dueEntry.nextRetryAt = Date.now() + calculateDelay(dueEntry.retryCount);

        // A shutdown may have started while the failure was being handled.
        if (!this.running) {
          break;
        }

        this.retryQueue.enqueue(dueEntry);
      }
    }
  }

  // will start the scheduler loop LAUNCHES and stores its Promise
  start(): void {
    if (this.running) {
      return;
    }

    this.running = true;
    this.loopPromise = this.run().catch((error) => {
      this.loopError = error;
      this.running = false;
      console.error("Retry scheduler crashed:", error);
    });
  }

  // will signal to shutdown and wait for the loop to finsih WILL AWAIT THAT PROMISE
  async stop(): Promise<void> {
    if (!this.running && !this.loopPromise) {
      return;
    }

    this.running = false;
    this.wakeUpSignal.notify();

    if (this.loopPromise) {
      await this.loopPromise;
      this.loopPromise = null;
    }

    if (this.loopError) {
      const error = this.loopError;
      this.loopError = null;
      throw error;
    }
  }

  scheduleRetry(entry: RetryEntry): boolean {
    if (!this.running) {
      return false;
    }

    this.retryQueue.enqueue(entry);

    this.wakeUpSignal.notify();

    return true;
  }

  // Returns a snapshot of messages that exhausted all retry attempts.
  getDeadLetters(): readonly RetryEntry[] {
    return this.deadLetters.slice();
  }
}
