import { Consumer } from "../../core/consumer";
import { analyticsProcessor } from "./processor";
import { installGracefulShutdown } from "../../shared/shutdown";

const analyticsConsumer = new Consumer({
  destination: "analytics",
  processMessage: analyticsProcessor,
});

installGracefulShutdown([() => analyticsConsumer.stop()]);

analyticsConsumer.start();
