// Defines POST and DELETE endpoints
import { Router } from "express";
import * as ConsumerController from "./consumer.controller";

const consumerRouter = Router();

consumerRouter.post(
  "/consumers",
  ConsumerController.registrationConsumerController,
);

consumerRouter.delete(
  "/consumers/:destination",
  ConsumerController.unregistrationConsumerController,
);

export default consumerRouter;
