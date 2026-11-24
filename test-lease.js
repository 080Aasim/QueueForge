import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

import {
  addJob,
  claimJob,
  renewJobLease,
  recoverExpiredJobs,
} from "./src/api/queues/queue.js";

describe("Job Lease Management", () => {
  const jobIds = [];

  before(async () => {
    await connectRedis();
  });

  it("Should renew the lease of a processing job", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "lease-test@example.com",
      },
      status: "waiting",
    });

    const claimedJob = await claimJob();

    assert.equal(claimedJob.id, jobId);

    // Get the original lease deadline.
    const originalDeadline = Number(
      await redis.zScore("jobs:processing", JSON.stringify(claimedJob)),
    );

    assert.ok(originalDeadline);

    // Renew the lease.
    const renewed = await renewJobLease(claimedJob);

    assert.equal(renewed, true);

    // Get the updated deadline.
    const updatedDeadline = Number(
      await redis.zScore("jobs:processing", JSON.stringify(claimedJob)),
    );

    assert.ok(updatedDeadline);
    assert.ok(updatedDeadline > originalDeadline);
  });
  it("Should not renew an already expired lease", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "expired-lease@example.com",
      },
      status: "waiting",
    });

    const claimedJob = await claimJob();

    assert.equal(claimedJob.id, jobId);

    // Force the lease deadline into the past.
    await redis.zAdd("jobs:processing", {
      score: Date.now() - 1000,
      value: JSON.stringify(claimedJob),
    });

    const renewed = await renewJobLease(claimedJob);

    assert.equal(renewed, false);
    // Remove the expired job from processing structures.
    await redis.zRem("jobs:processing", JSON.stringify(claimedJob));

    await redis.lRem("jobs:processing:list", 1, JSON.stringify(claimedJob));
  });
  it("Should recover an expired job back to the waiting queue", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "recovery-test@example.com",
      },
      status: "waiting",
    });

    const claimedJob = await claimJob();

    assert.equal(claimedJob.id, jobId);
    assert.equal(claimedJob.status, "processing");

    // Force the lease to expire.
    await redis.zAdd("jobs:processing", {
      score: Date.now() - 1000,
      value: JSON.stringify(claimedJob),
    });

    // Run recovery.
    await recoverExpiredJobs();

    // Verify the job is back in the waiting queue.
    const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

    const recoveredJob = waitingJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(recoveredJob);
    assert.equal(recoveredJob.status, "waiting");
    assert.equal(recoveredJob.attempts, 1);

    // Verify it was removed from processing.
    const lease = await redis.zRange("jobs:processing", 0, -1);

    assert.equal(
      lease.some((item) => JSON.parse(item).id === jobId),
      false,
    );

    // Verify the registry.
    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.equal(JSON.parse(storedJob).status, "waiting");
  });
  it("Should move a job to failed queue after maximum attempts", async () => {
    const jobId = randomUUID();
    jobIds.push(jobId);

    await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "max-retry-test@example.com",
      },
      status: "waiting",
    });

    let claimedJob;

    // Simulate three failed attempts.
    for (let attempt = 1; attempt <= 3; attempt++) {
      claimedJob = await claimJob();

      assert.equal(claimedJob.id, jobId);
      assert.equal(claimedJob.attempts, attempt);

      // Expire the current lease.
      await redis.zAdd("jobs:processing", {
        score: Date.now() - 1000,
        value: JSON.stringify(claimedJob),
      });

      await recoverExpiredJobs();

      if (attempt < 3) {
        // Job should be waiting for another attempt.
        const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

        assert.ok(waitingJobs.some((item) => JSON.parse(item).id === jobId));
      }
    }

    // After the third expiration, the job should be failed.
    const failedJobs = await redis.lRange("jobs:failed", 0, -1);

    const failedJob = failedJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(failedJob);
    assert.equal(failedJob.status, "failed");
    assert.equal(failedJob.attempts, 3);

    // It must no longer be waiting or processing.
    const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

    assert.equal(
      waitingJobs.some((item) => JSON.parse(item).id === jobId),
      false,
    );

    const lease = await redis.zRange("jobs:processing", 0, -1);

    assert.equal(
      lease.some((item) => JSON.parse(item).id === jobId),
      false,
    );

    // Registry should also show failed.
    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.equal(JSON.parse(storedJob).status, "failed");
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
