export class Mutex {
  private queue: Promise<void> = Promise.resolve();

  // Runs tasks one at a time. If a signal is provided and aborts while a task
  // is still waiting for its turn, the task is cancelled instead of running.
  runExclusive<T>(task: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    const run = async (): Promise<T> => {
      if (signal?.aborted) {
        const error = new Error("Mutex task cancelled during shutdown");
        error.name = "AbortError";
        throw error;
      }

      return task();
    };

    const result = this.queue.then(run);

    this.queue = result.then(
      () => undefined,
      () => undefined,
    );

    return result;
  }
}
