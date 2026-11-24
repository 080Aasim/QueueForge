import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";

import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

import { addJob } from "./src/api/queues/queue.js";

describe("Job Producer", () => {
  let jobId;

  before(async () => {
    await connectRedis();
  });

  after(async () => {
    if (jobId) {
      const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

      const matchingJob = waitingJobs.find(
        (item) => JSON.parse(item).id === jobId,
      );

      if (matchingJob) {
        await redis.lRem("jobs:waiting", 1, matchingJob);
      }

      await redis.hDel("jobs:data", jobId);
      await redis.zRem("jobs:created", jobId);
    }

    await disconnectRedis();
  });

  it("Should add a job to all required Redis structures", async () => {
    jobId = randomUUID();

    const job = await addJob({
      id: jobId,
      type: "email",
      payload: {
        to: "test@example.com",
      },
      status: "waiting",
    });

    // We'll add assertions here.
    // 1. Verify returned job
    assert.equal(job.id, jobId);
    assert.equal(job.type, "email");
    assert.equal(job.status, "waiting");
    assert.equal(job.attempts, 0);

    // 2. Verify waiting queue
    const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

    const queuedJob = waitingJobs
      .map((item) => JSON.parse(item))
      .find((item) => item.id === jobId);

    assert.ok(queuedJob);
    assert.equal(queuedJob.id, jobId);

    // 3. Verify job registry
    const storedJob = await redis.hGet("jobs:data", jobId);

    assert.ok(storedJob);

    const parsedStoredJob = JSON.parse(storedJob);

    assert.equal(parsedStoredJob.id, jobId);
    assert.equal(parsedStoredJob.type, "email");

    // 4. Verify creation index
    const score = await redis.zScore("jobs:created", jobId);

    assert.notEqual(score, null);
  });
  it("Should reject a job with an existing ID", async () => {
    const sequenceBefore = await redis.get("jobs:sequence");
    await assert.rejects(
      addJob({
        id: jobId,
        type: "email",
        payload: {
          to: "another@example.com",
        },
        status: "waiting",
      }),
      /Job ID already exists/,
    );

    const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

    const matchingJobs = waitingJobs
      .map((item) => JSON.parse(item))
      .filter((item) => item.id === jobId);

    assert.equal(matchingJobs.length, 1);
    const sequenceAfter = await redis.get("jobs:sequence");

    assert.equal(sequenceAfter, sequenceBefore);
  });
});
