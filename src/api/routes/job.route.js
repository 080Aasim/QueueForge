import express from "express";
import { createJob } from "../queues/producer.js";

const router = express.Router();

router.post("/", async (req, res) => {
  try {
    const { type, payload } = req.body;
    if (!type) {
      return res.status(400).json({
        message: "Job type is required",
      });
    }

    const job = await createJob(type, payload);
    return res.status(202).json({
      message: "Job added to queue",
      job,
    });
  } catch (error) {
    console.error(error);

    res.status(500).json({
      message: "Failed to create job",
    });
  }
});

export default router;
