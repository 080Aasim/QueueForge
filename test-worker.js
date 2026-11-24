import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

import { addJob, claimJob } from "./src/api/queues/queue.js";

describe("Worker Job Claiming", () => {
  const jobIds = [];

  before(async () => {
    await connectRedis();
  });
  it("Should claim a waiting job successfully", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "worker-test@example.com",
      },
      status: "waiting",
    });

    const claimedJob = await claimJob();

    assert.ok(claimedJob);
    assert.equal(claimedJob.id, jobId);
    assert.equal(claimedJob.status, "processing");
    assert.equal(claimedJob.attempts, 1);
    // Verify processing list
    const processingJobs = await redis.lRange("jobs:processing:list", 0, -1);

    const processingJob = processingJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(processingJob);
    assert.equal(processingJob.status, "processing");

    // Verify lease sorted set
    const leasedJobs = await redis.zRange("jobs:processing", 0, -1);

    const leasedJob = leasedJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(leasedJob);

    // Verify persistent registry
    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.ok(storedJob);

    const parsedStoredJob = JSON.parse(storedJob);

    assert.equal(parsedStoredJob.status, "processing");
    assert.equal(parsedStoredJob.attempts, 1);
  });
  it("Should return null when the waiting queue is empty", async () => {
    // Temporarily preserve existing jobs
    const existingJobs = await redis.lRange("jobs:waiting", 0, -1);

    await redis.del("jobs:waiting");

    try {
      const claimedJob = await claimJob();

      assert.equal(claimedJob, null);
    } finally {
      // Restore the original waiting queue
      if (existingJobs.length > 0) {
        await redis.rPush("jobs:waiting", existingJobs);
      }
    }
  });
  it("Should increment attempts on every claim", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: { to: "retry-test@example.com" },
      status: "waiting",
    });

    const firstClaim = await claimJob();

    assert.equal(firstClaim.attempts, 1);

    // Simulate recovery by returning the job to waiting
    await redis.lRem("jobs:processing:list", 1, JSON.stringify(firstClaim));

    await redis.zRem("jobs:processing", JSON.stringify(firstClaim));

    const retryJob = {
      ...firstClaim,
      status: "waiting",
    };

    await redis.lPush("jobs:waiting", JSON.stringify(retryJob));
    await redis.hSet("jobs:data", jobId, JSON.stringify(retryJob));

    const secondClaim = await claimJob();

    assert.equal(secondClaim.id, jobId);
    assert.equal(secondClaim.attempts, 2);
  });
  it("Should prevent two workers from claiming the same job", async () => {
    const existingJobs = await redis.lRange("jobs:waiting", 0, -1);

    await redis.del("jobs:waiting");

    const jobId = randomUUID();
    jobIds.push(jobId);

    try {
      await addJob({
        id: jobId,
        type: "email",
        payload: { to: "concurrency-test@example.com" },
        status: "waiting",
      });

      const results = await Promise.all([claimJob(), claimJob()]);

      const successfulClaims = results.filter(
        (job) => job !== null && job.id === jobId,
      );

      assert.equal(successfulClaims.length, 1);

      assert.equal(results.filter((job) => job === null).length, 1);
    } finally {
      if (existingJobs.length > 0) {
        await redis.rPush("jobs:waiting", existingJobs);
      }
    }
  });

  after(async () => {
    try {
      for (const jobId of jobIds) {
        // Remove from waiting queue
        const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

        for (const item of waitingJobs) {
          if (JSON.parse(item).id === jobId) {
            await redis.lRem("jobs:waiting", 1, item);
          }
        }

        // Remove from processing list
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

        // Remove from processing sorted set
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
