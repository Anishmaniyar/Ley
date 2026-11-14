export class WakeUpSignal {
  private notified = false;

  private waiter: {
    resolve: () => void;
    timer: ReturnType<typeof setTimeout> | null;
  } | null = null;

  notify(): void {
    if (this.waiter !== null) {
      const waiter = this.waiter;

      if (waiter.timer !== null) {
        clearTimeout(waiter.timer);
      }

      this.waiter = null;
      waiter.resolve();
    } else {
      this.notified = true;
    }
  }

  wait(timeoutMs?: number): Promise<void> {
    if (this.notified) {
      this.notified = false;
      return Promise.resolve();
    }

    return new Promise<void>((resolve) => {
      let timer: ReturnType<typeof setTimeout> | null = null;

      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          this.waiter = null;
          resolve();
        }, timeoutMs);
      }

      this.waiter = {
        resolve,
        timer,
      };
    });
  }
}
