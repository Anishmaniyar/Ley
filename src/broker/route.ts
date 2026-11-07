import { Router } from "express";
import messageRouter from "./modules/message/message.routes";

const rootRouter = Router();

rootRouter.use("/messages", messageRouter);

export default rootRouter;
