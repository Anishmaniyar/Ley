import type { Message } from "../../../src/broker/modules/message/message.js";

import { resolveConsumerConfig, type ConsumerConfig } from "./config.js";

import {
  fetchMessages,
  registerConsumer,
  unregisterConsumer,
} from "./broker-client.js";
import { Mutex } from "./async-mutex.js";
import { OffsetManager } from "./offset/offset-management.js";

import { RetryQueue } from "./retry/queue.js";
import { RetryScheduler } from "./retry/scheduler.js";
import type { RetryEntry } from "./retry/entry.js";
import { calculateDelay } from "./retry/delay.js";

import { WakeUpSignal } from "../shared/wake-up_signal.js";

type MessageProcessor = (message: Message) => Promise<void>;

export interface ConsumerOptions {
  destination: string;
  processMessage: MessageProcessor;
  config?: Partial<ConsumerConfig>;
}

export class Consumer {
  private readonly config: ConsumerConfig;
  private readonly destination: string;

  // Independent runtime state for this consumer instance.
  private readonly offsetManager: OffsetManager;
  private readonly retryQueue: RetryQueue;
  private readonly wakeUpSignal: WakeUpSignal;
  private readonly messageMutex: Mutex;
  private readonly shutdownController = new AbortController();
  private readonly retryScheduler: RetryScheduler;

  private readonly processMessageSafely: MessageProcessor;

  private running = false;
  private started = false;
  private stopPromise: Promise<void> | null = null;
  private loopPromise: Promise<void> | null = null;

  private registrationState: "registered" | "unregistered" = "unregistered";
  private registrationPromise: Promise<void> | null = null;

  constructor(options: ConsumerOptions) {
    this.destination = options.destination;
    this.config = resolveConsumerConfig(options.config);

    this.offsetManager = new OffsetManager();
    this.retryQueue = new RetryQueue();
    this.wakeUpSignal = new WakeUpSignal();
    this.messageMutex = new Mutex();

    // Normal messages and retry messages share the same mutex.
    this.processMessageSafely = (message) =>
      this.messageMutex.runExclusive(
        () => options.processMessage(message),
        this.shutdownController.signal,
      );

    this.retryScheduler = new RetryScheduler(
      this.retryQueue,
      this.wakeUpSignal,
      this.processMessageSafely,
      this.config,
    );
  }

  // Waits for the polling interval but wakes immediately during shutdown.
  private sleep(ms: number, signal: AbortSignal): Promise<void> {
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

  // Fetches batches and processes messages sequentially.
  private async consumeMessages(): Promise<void> {
    while (this.running) {
      let messages: Message[];

      try {
        messages = await fetchMessages(
          this.offsetManager.getOffset(),
          this.config.batchSize,
          this.shutdownController.signal,
        );
      } catch (error) {
        // An abort during shutdown is expected.
        if (!this.running) {
          break;
        }

        console.error("Failed to fetch messages from broker:", error);

        await this.sleep(
          this.config.pollIntervalMs,
          this.shutdownController.signal,
        );

        continue;
      }

      // Do not start new work after shutdown begins.
      if (!this.running) {
        break;
      }

      if (messages.length === 0) {
        await this.sleep(
          this.config.pollIntervalMs,
          this.shutdownController.signal,
        );

        continue;
      }

      for (const message of messages) {
        if (!this.running) {
          break;
        }

        try {
          await this.processMessageSafely(message);

          // Advance only after successful processing.
          this.offsetManager.advance();
        } catch (error) {
          // Cancelled tasks during shutdown are not retried.
          if (!this.running) {
            break;
          }

          console.error("Message processing failed:", error);

          const entry: RetryEntry = {
            message,
            retryCount: 0,
            nextRetryAt: Date.now() + calculateDelay(0, this.config),
            lastError: {
              name: error instanceof Error ? error.name : "Error",
              message: error instanceof Error ? error.message : String(error),
            },
          };

          const scheduled = this.retryScheduler.scheduleRetry(entry);

          if (scheduled) {
            this.offsetManager.advance();
          } else {
            if (!this.running) {
              break;
            }

            throw new Error("Retry scheduler is not running");
          }
        }
      }
    }
  }

  async start(): Promise<void> {
    // A consumer instance can only be started once.
    if (this.started || this.stopPromise) {
      return;
    }

    this.started = true;

    this.registrationPromise = registerConsumer(this.destination).then(() => {
      this.registrationState = "registered";
    });

    await this.registrationPromise;

    // Shutdown may have started while registration was pending.
    if (this.stopPromise) {
      return;
    }

    this.running = true;

    console.log("Consumer started");

    this.retryScheduler.start();

    this.loopPromise = this.consumeMessages().catch((error) => {
      console.error("Consumer loop crashed:", error);
      process.exitCode = 1;
    });
  }

  // Idempotent: repeated calls share the same shutdown promise.
  stop(): Promise<void> {
    if (this.stopPromise) {
      return this.stopPromise;
    }

    this.stopPromise = (async () => {
      this.running = false;
      this.shutdownController.abort();

      // Stop the consumer loop and retry scheduler concurrently.
      const loopDone = this.loopPromise ?? Promise.resolve();

      const shutdownResults = await Promise.allSettled([
        loopDone,
        this.retryScheduler.stop(),
      ]);

      const shutdownErrors: unknown[] = [];

      // Collect errors without preventing the remaining shutdown steps.
      for (const result of shutdownResults) {
        if (result.status === "rejected") {
          shutdownErrors.push(result.reason);
        }
      }

      // Wait for any registration request already in progress.
      if (this.registrationPromise) {
        await this.registrationPromise.catch(() => {});
      }

      // Unregister only if registration succeeded.
      if (this.registrationState === "registered") {
        try {
          await unregisterConsumer(this.destination);
          this.registrationState = "unregistered";
        } catch (error) {
          shutdownErrors.push(error);
        }
      }

      console.log("Consumer stopped");

      // Report errors only after all shutdown steps have been attempted.
      if (shutdownErrors.length === 1) {
        throw shutdownErrors[0];
      }

      if (shutdownErrors.length > 1) {
        throw new AggregateError(
          shutdownErrors,
          "Consumer shutdown encountered multiple errors",
        );
      }
    })();

    return this.stopPromise;
  }
}
