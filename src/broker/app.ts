import express from "express";
import type { Request, Response } from "express";
import rootRouter from "./route.js";
import { notFoundMiddleware } from "../middlewares/not-found.middlewares.js";
import { errorMiddleware } from "../middlewares/error.middlewares.js";

const app = express();

app.use(express.json());

app.use("/api/v1", rootRouter);

app.get("/health", (req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.use(notFoundMiddleware);

app.use(errorMiddleware);

export default app;
