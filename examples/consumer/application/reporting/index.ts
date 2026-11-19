import { Consumer } from "../../core/consumer";
import { reportingProcessor } from "./processor";
import { installGracefulShutdown } from "../../shared/shutdown";

const reportingConsumer = new Consumer({
  destination: "reporting",
  processMessage: reportingProcessor,
});

installGracefulShutdown([() => reportingConsumer.stop()]);

reportingConsumer.start();
