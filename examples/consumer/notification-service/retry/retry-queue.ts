import { PriorityQueue } from "../../shared/min-heap.js";
import type { RetryEntry } from "./retry-entry.js";

// When consumer fails processin we try to store its value in retry queue which is an encapsulation of an priority queue and this is it
export class RetryQueue {
  private queue: PriorityQueue<RetryEntry>;

  constructor() {
    this.queue = new PriorityQueue<RetryEntry>(
      (a, b) => a.nextRetryAt - b.nextRetryAt,
    );
  }

  enqueue(entry: RetryEntry) {
    this.queue.push(entry);
  }

  peek(): RetryEntry | undefined {
    return this.queue.peek();
  }

  dequeue(): RetryEntry | undefined {
    return this.queue.pop();
  }

  isEmpty(): boolean {
    return this.queue.isEmpty();
  }
}
