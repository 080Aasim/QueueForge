import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

import { addJob, claimJob, acknowledgeJob } from "./src/api/queues/queue.js";

describe("Job Acknowledgement", () => {
  const jobIds = [];

  before(async () => {
    await connectRedis();
  });
  it("Should acknowledge a processing job successfully", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "acknowledgement-test@example.com",
      },
      status: "waiting",
    });

    // Move the job from waiting to processing.
    const claimedJob = await claimJob();

    assert.equal(claimedJob.id, jobId);
    assert.equal(claimedJob.status, "processing");

    // Acknowledge the job.
    await acknowledgeJob(claimedJob);

    // Verify it was removed from the processing list.
    const processingJobs = await redis.lRange("jobs:processing:list", 0, -1);

    assert.equal(
      processingJobs.some((item) => JSON.parse(item).id === jobId),
      false,
    );

    // Verify it was removed from the processing sorted set.
    const leasedJobs = await redis.zRange("jobs:processing", 0, -1);

    assert.equal(
      leasedJobs.some((item) => JSON.parse(item).id === jobId),
      false,
    );

    // Verify it remains in the registry as completed.
    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.ok(storedJob);
    assert.equal(JSON.parse(storedJob).status, "completed");
  });
  it("Should not acknowledge a job that is not processing", async () => {
    const jobId = randomUUID();

    const fakeJob = {
      id: jobId,
      type: "email",
    };

    await acknowledgeJob(fakeJob);

    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.equal(storedJob, null);

    const processingJobs = await redis.lRange("jobs:processing:list", 0, -1);

    assert.equal(
      processingJobs.some((item) => JSON.parse(item).id === jobId),
      false,
    );
  });

  after(async () => {
    try {
      for (const jobId of jobIds) {
        const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

        for (const item of waitingJobs) {
          if (JSON.parse(item).id === jobId) {
            await redis.lRem("jobs:waiting", 1, item);
          }
        }

        const processingJobs = await redis.lRange(
          "jobs:processing:list",
          0,
          -1,
        );

        for (const item of processingJobs) {
          if (JSON.parse(item).id === jobId) {
            await redis.lRem("jobs:processing:list", 1, item);
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
