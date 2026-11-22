import express from "express";
import { createJob } from "../queues/producer.js";
import { getAllJobs, getJobsByStatus, getJobById } from "../queues/queue.js";

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
    const { status } = req.query;
    const validStatuses = ["waiting", "processing", "completed", "failed"];

    if (status && !validStatuses.includes(status)) {
      return res.status(400).json({
        message: "Invalid job status",
        validStatuses,
      });
    }
    const jobs = status ? await getJobsByStatus(status) : await getAllJobs();
    return res.status(200).json({
      count: jobs.length,
      jobs,
    });
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
