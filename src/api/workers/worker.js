import {
  acknowledgeJob,
  claimJob,
  recoverExpiredJobs,
  renewJobLease,
} from "../queues/queue.js";
import { connectRedis, disconnectRedis } from "../../config/redis.js";
import { pathToFileURL } from "node:url";

const HEARTBEAT_INTERVAL = Number(
  process.env.WORKER_HEARTBEAT_INTERVAL || 5000,
);

const JOB_PROCESSING_TIME = Number(process.env.JOB_PROCESSING_TIME || 30000);
// For grace full shutdown
let isShuttingDown = false;

process.on("SIGINT", () => {
  console.log("\nShutdown signal received (SIGINT)");
  isShuttingDown = true;
});
process.on("SIGTERM", () => {
  console.log("\nShutdown signal received (SIGTERM)");
  isShuttingDown = true;
});
process.on("message", (message) => {
  if (message === "shutdown") {
    console.log("Shutdown message received");
    isShuttingDown = true;
  }
});

export async function startWorker() {
  try {
    await connectRedis();
    const recoveryLoopPromise = startRecoveryLoop(); // Recover expired jobs before claiming a new job

    console.log("Worker started");

    while (!isShuttingDown) {
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
    await recoveryLoopPromise;
    await disconnectRedis();
    console.log("Worker Shutdown complete");
    if (process.connected) {
      process.disconnect();
    }
  } catch (error) {
    console.error("Worker startup failed:", error);
    process.exit(1);
  }
}

export async function processJob(job) {
  console.log(`Processing job ${job.id}`);

  let heartbeatRunning = false;
  let heartbeatPromise = Promise.resolve();

  const heartbeat = setInterval(() => {
    // Prevent overlapping heartbeats requests
    if (heartbeatRunning) return;

    heartbeatRunning = true;

    heartbeatPromise = (async () => {
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
    })();
  }, HEARTBEAT_INTERVAL);

  // Simulate of job begin processed
  try {
    await new Promise((resolve) => setTimeout(resolve, JOB_PROCESSING_TIME));
    console.log(`Job ${job.id} completed`);
  } finally {
    clearInterval(heartbeat);
    await heartbeatPromise;
  }
}

export async function startRecoveryLoop() {
  while (!isShuttingDown) {
    try {
      // console.log("Recovery loop checking expired jobs...");
      await recoverExpiredJobs();
    } catch (error) {
      console.error("Recovery error: ", error);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  console.log("Recovery loop stopped");
}

if (
  process.argv[1] &&
  import.meta.url === pathToFileURL(process.argv[1]).href
) {
  startWorker();
}
