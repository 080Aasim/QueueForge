import express from "express";
import { createJob } from "../queues/producer.js";
import {
  getJobById,
  getJobsPaginated,
} from "../queues/queue.js";

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

router.get("/", async (req, res) => {
  try {
    const { status, cursor = "0", limit = "10" } = req.query;
    const validStatuses = ["waiting", "processing", "completed", "failed"];

    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        message: "Invalid job status",
        validStatuses,
      });
    }
    if(!/^\d+$/.test(cursor)){
      return res.status(400).json({
        message: "Invalid cursor",
      })
    }
    const parsedLimit = Number(limit);
    if(!Number.isInteger(parsedLimit) || parsedLimit < 1 || parsedLimit > 100) {
      return res.status(400).json({
        message: "Limit must be between 1 and 100",
      })
    }
    const result = await getJobsPaginated(
      cursor, parsedLimit, status || null
    );
    return res.status(200).json(result);
  } catch (error) {
    console.error("Failed to retrieve jobs:", error);
    return res.status(500).json({
      message: "Failed to retrieve jobs",
    });
  }
});

router.get("/:id", async (req, res) => {
  try {
    const job = await getJobById(req.params.id);
    if (!job) {
      return res.status(404).json({
        message: "Job not found.",
      });
    }
    return res.status(200).json({ job });
  } catch (error) {
    console.error("Failed to retrieve jobs:", error);
    return res.status(500).json({
      message: "Failed to retrieve job",
    });
  }
});

export default router;
