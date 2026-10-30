import dotenv from "dotenv";
import express from "express";
import { connectToDb } from "./src/db/postgres.js";
import { connectRedis } from "./src/config/redis.js";
import jobRouter from "./src/api/routes/job.route.js";

dotenv.config();
const app = express();

app.use(express.json());

app.use("/api/jobs", jobRouter);

app.get("/", (req, res) => {
  res.json({
    message: "Distributed Job System",
  });
});

const PORT = process.env.PORT || 4000;

const startServer = async () => {
  try {
    await connectRedis();
    await connectToDb();
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}`);
    });
  } catch (error) {
    console.log("Server startup failed: ", error);
    process.exit(1);
  }
};

startServer();
