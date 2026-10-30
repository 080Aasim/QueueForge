import {
  acknowledgeJob,
  claimJob,
  recoverExpiredJobs,
  renewJobLease,
} from "../queues/queue.js";
import { connectRedis } from "../../config/redis.js";

const HEARTBEAT_INTERVAL = 5 * 1000; // 5 seconds

async function startWorker() {
  try {
    await connectRedis();
    startRecoveryLoop();// Recover expired jobs before claiming a new job

    console.log("Worker started");

    while (true) {
      try {
        console.log("Waiting for job...");

        const job = await claimJob();

        if (!job) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
          continue;
        }

        console.log("Received job:", job);

        await processJob(job);
        await acknowledgeJob(job);
      } catch (error) {
        console.error("Worker error:", error);
      }
    }
  } catch (error) {
    console.error("Worker startup failed:", error);
    process.exit(1);
  }
}

async function processJob(job) {
  console.log(`Processing job ${job.id}`);

  let heartbeatRunning = false;

  const heartbeat = setInterval(async () => {
    // Prevent overlapping heartbeats requests
    if (heartbeatRunning) return;
    heartbeatRunning = true;

    try {
      const renewed = await renewJobLease(job);
      if (renewed) {
        console.log(`Lease renewed for job ${job.id}`);
      } else {
        console.warn(`Lease lost for job ${job.id}`);
      }
    } catch (error) {
      console.error(`Heartbeat error for job ${job.id}:`, error);
    } finally {
      heartbeatRunning = false;
    }
  }, HEARTBEAT_INTERVAL);

  // Simulate of job begin processed
  try {
    await new Promise((resolve) => setTimeout(resolve, 30000));
    console.log(`Job ${job.id} completed`);
  } finally {
    clearInterval(heartbeat);
  }
}

async function startRecoveryLoop() {
  while (true) {
    try {
      // console.log("Recovery loop checking expired jobs...");
      await recoverExpiredJobs();
    } catch (error) {
      console.error("Recovery error: ", error);
    }
    
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

startWorker();
