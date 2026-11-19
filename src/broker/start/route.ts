import { Router } from "express";
import messageRouter from "../modules/message/message.routes";
import consumerRegistrationRouter from "../modules/consumer/consumer.routes";

const rootRouter = Router();

rootRouter.use("/messages", messageRouter);
rootRouter.use(consumerRegistrationRouter);

export default rootRouter;
