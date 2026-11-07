import { Router } from "express";
import * as messageController from "./message.controller.js";

const messageRouter = Router();

messageRouter.post("", messageController.publishMessageController);

messageRouter.get("", messageController.fetchMessagesController);

export default messageRouter;
