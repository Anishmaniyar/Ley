type StopFn = () => Promise<void> | void;

// Stops every consumer once, then exits. Repeated signals are ignored.
export function installGracefulShutdown(
  stops: StopFn[],
  timeoutMs = 10_000,
): void {
  let shuttingDown = false;

  const shutdown = async (signal: NodeJS.Signals) => {
    if (shuttingDown) {
      return;
    }

    shuttingDown = true;
    console.log(`Received ${signal}. Shutting down gracefully...`);

    const timer = setTimeout(() => {
      console.error(`Shutdown timed out after ${timeoutMs}ms. Forcing exit.`);
      process.exit(1);
    }, timeoutMs);

    const results = await Promise.allSettled(
      stops.map(async (stop) => await stop()),
    );

    clearTimeout(timer);

    let failed = false;
    for (const result of results) {
      if (result.status === "rejected") {
        failed = true;
        console.error("Consumer failed to stop:", result.reason);
      }
    }

    console.log(
      failed ? "Shutdown completed with errors." : "All consumers stopped.",
    );
    process.exit(failed ? 1 : 0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}
