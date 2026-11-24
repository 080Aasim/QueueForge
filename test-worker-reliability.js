import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

import {
  addJob,
  claimJob,
  recoverExpiredJobs,
  acknowledgeJob,
  renewJobLease
} from "./src/api/queues/queue.js";

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

// Short timings for automated tests.
process.env.WORKER_HEARTBEAT_INTERVAL = "50";
process.env.JOB_PROCESSING_TIME = "250";

// Import after setting test timings.
const { processJob } = await import("./src/api/workers/worker.js");

describe("Worker Reliability", () => {
  const jobIds = [];

  before(async () => {
    await connectRedis();
  });

  it("Should renew the lease while processing a long-running job", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "heartbeat-test@example.com",
      },
      status: "waiting",
    });

    const job = await claimJob();

    assert.equal(job.id, jobId);
    assert.equal(job.status, "processing");

    const jobData = JSON.stringify(job);

    // Capture the original lease deadline.
    const originalDeadline = Number(
      await redis.zScore("jobs:processing", jobData),
    );

    assert.ok(originalDeadline);

    // Simulate long-running processing.
    await processJob(job);

    // Check the lease after processing.
    const renewedDeadline = Number(
      await redis.zScore("jobs:processing", jobData),
    );

    assert.ok(renewedDeadline);
    assert.ok(renewedDeadline > originalDeadline);
    await acknowledgeJob(job);
  });
  it("Should recover a job after its worker crashes", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "crash-recovery@example.com",
      },
      status: "waiting",
    });

    // Worker claims the job.
    const job = await claimJob();

    assert.equal(job.id, jobId);
    assert.equal(job.status, "processing");
    assert.equal(job.attempts, 1);

    // Simulate worker crash by expiring its lease.
    await redis.zAdd("jobs:processing", {
      score: Date.now() - 1000,
      value: JSON.stringify(job),
    });

    // Recovery loop detects the expired lease.
    await recoverExpiredJobs();

    // Verify the job returned to waiting.
    const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

    const recoveredJob = waitingJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(recoveredJob);
    assert.equal(recoveredJob.status, "waiting");
    assert.equal(recoveredJob.attempts, 1);

    // Verify it is no longer processing.
    const processingJobs = await redis.lRange("jobs:processing:list", 0, -1);

    assert.equal(
      processingJobs.some((item) => JSON.parse(item).id === jobId),
      false,
    );
  });
  it(
    "Should finish the current job before shutting down",
    { timeout: 10000 },
    async () => {
      const jobId = randomUUID();
      jobIds.push(jobId);

      await addJob({
        id: jobId,
        type: "email",
        payload: { to: "shutdown-test@example.com" },
        status: "waiting",
      });

      const workerPath = fileURLToPath(
        new URL("./src/api/workers/worker.js", import.meta.url),
      );

      const child = spawn(process.execPath, [workerPath], {
        env: {
          ...process.env,
          WORKER_HEARTBEAT_INTERVAL: "50",
          JOB_PROCESSING_TIME: "250",
        },
        stdio: ["ignore", "pipe", "pipe", "ipc"],
      });
      const childExit = new Promise((resolve) => {
        child.once("exit", (code, signal) => {
          resolve({ code, signal });
        });
      });

      let output = "";

      const processingStarted = new Promise((resolve, reject) => {
        child.stdout.on("data", (data) => {
          output += data.toString();

          if (output.includes(`Processing job ${jobId}`)) {
            resolve();
          }
        });

        child.stderr.on("data", (data) => {
          output += data.toString();
        });

        child.once("error", reject);

        child.once("exit", (code) => {
          if (!output.includes(`Processing job ${jobId}`)) {
            reject(
              new Error(
                `Worker exited before processing the job. Code: ${code}\n${output}`,
              ),
            );
          }
        });
      });

      await processingStarted;

      child.send("shutdown");

      const result = await childExit;

      console.log("Worker exit result:", result);
      console.log("Worker output:", output);

      assert.equal(result.code, 0);

      const completedJob = JSON.parse(await redis.hGet("jobs:data", jobId));

      assert.equal(completedJob.status, "completed");
    },
  );
  it("Should reject stale worker renewal and acknowledgement after a job is reclaimed", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: { to: "stale-worker-test@example.com" },
      status: "waiting",
    });

    // Worker A claims the job.
    const workerAJob = await claimJob();

    assert.equal(workerAJob.id, jobId);
    assert.ok(workerAJob.claimToken);

    // Simulate Worker A's lease expiring.
    await redis.zAdd("jobs:processing", {
      score: Date.now() - 1000,
      value: JSON.stringify(workerAJob),
    });

    // Recover and let Worker B claim the same job.
    await recoverExpiredJobs();

    const workerBJob = await claimJob();

    assert.equal(workerBJob.id, jobId);
    assert.notEqual(workerBJob.claimToken, workerAJob.claimToken);

    // Worker A wakes up with its stale job object.
    const renewed = await renewJobLease(workerAJob);
    assert.equal(renewed, false);

    await acknowledgeJob(workerAJob);

    // Worker B must still own the processing job.
    const processingJobs = await redis.lRange("jobs:processing:list", 0, -1);

    const activeJob = processingJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(activeJob);
    assert.equal(activeJob.claimToken, workerBJob.claimToken);

    const storedJob = JSON.parse(await redis.hGet("jobs:data", jobId));

    assert.equal(storedJob.status, "processing");
    assert.equal(storedJob.claimToken, workerBJob.claimToken);
  });

  after(async () => {
    try {
      for (const jobId of jobIds) {
        const queues = ["jobs:waiting", "jobs:processing:list", "jobs:failed"];

        for (const queue of queues) {
          const jobs = await redis.lRange(queue, 0, -1);

          for (const item of jobs) {
            if (JSON.parse(item).id === jobId) {
              await redis.lRem(queue, 1, item);
            }
          }
        }

        const leasedJobs = await redis.zRange("jobs:processing", 0, -1);

        for (const item of leasedJobs) {
          if (JSON.parse(item).id === jobId) {
            await redis.zRem("jobs:processing", item);
          }
        }

        await redis.hDel("jobs:data", jobId);
        await redis.zRem("jobs:created", jobId);
      }
    } finally {
      await disconnectRedis();
    }
  });
});
