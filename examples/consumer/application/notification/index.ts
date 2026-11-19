import { Consumer } from "../../core/consumer";
import { notificationProcessor } from "./processor";
import { installGracefulShutdown } from "../../shared/shutdown";

const notificationConsumer = new Consumer({
  destination: "notification",
  processMessage: notificationProcessor,
});

installGracefulShutdown([() => notificationConsumer.stop()]);

notificationConsumer.start();
