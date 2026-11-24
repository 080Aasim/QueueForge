import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";
import { randomUUID } from "node:crypto";
import { addJob, getJobsPaginated } from "./src/api/queues/queue.js";

describe("Job Pagination", () => {
  let testJobsIds = [];
  let startingCursor;
  before(async () => {
    await connectRedis();
    startingCursor = String((await redis.get("jobs:sequence")) || "0");
    for (let i = 1; i <= 4; i++) {
      const jobId = randomUUID();
      testJobsIds.push(jobId);

      const job = await addJob({
        id: jobId,
        type: "test",
        payload: { number: i },
        status: "waiting",
      });

      if (i % 2 === 0) {
        job.status = "completed";

        await redis.hSet("jobs:data", jobId, JSON.stringify(job));
      }
    }
  });

  after(async () => {
    try {
      for (const jobId of testJobsIds) {
        const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);
        for (const jobData of waitingJobs) {
          const job = JSON.parse(jobData);
          if (job.id === jobId) {
            await redis.lRem("jobs:waiting", 1, jobData);
            break;
          }
        }
        await redis.hDel("jobs:data", jobId);
        await redis.zRem("jobs:created", jobId);
      }
    } finally {
      await disconnectRedis();
    }
  });

  it("Should return the requested number of jobs and a valid cursor", async () => {
    const firstBatch = await getJobsPaginated(startingCursor, 2);

    assert.equal(firstBatch.jobs.length, 2);
    assert.equal(firstBatch.count, 2);
    assert.equal(firstBatch.nextCursor, String(Number(startingCursor) + 2));
  });
  it("Should return the next batch without duplicated jobs", async () => {
    const firstBatch = await getJobsPaginated(startingCursor, 2);

    const secondBatch = await getJobsPaginated(firstBatch.nextCursor, 2);
    assert.equal(secondBatch.jobs.length, 2);
    assert.equal(secondBatch.count, 2);

    const firstBatchIds = firstBatch.jobs.map((job) => job.id);
    for (const job of secondBatch.jobs) {
      assert.ok(!firstBatchIds.includes(job.id));
    }
  });
  it("Should return an empty result when no more jobs exists", async () => {
    const lastCursor = String(Number(startingCursor) + 4);
    const result = await getJobsPaginated(lastCursor, 2);

    assert.equal(result.jobs.length, 0);
    assert.equal(result.count, 0);
    assert.equal(result.nextCursor, "0");
  });
  it("Should return only jobs matching the requested status", async () => {
    const result = await getJobsPaginated(startingCursor, 2, "waiting");

    assert.equal(result.jobs.length, 2);
    assert.equal(result.count, 2);

    assert.deepEqual(
      result.jobs.map((job) => job.id),
      [testJobsIds[0], testJobsIds[2]],
    );

    assert.equal(result.nextCursor, String(Number(startingCursor) + 3));
  });
  it("Should return a partial page when fewer matching jobs exist", async () => {
    const result = await getJobsPaginated(startingCursor, 3, "waiting");

    assert.equal(result.jobs.length, 2);
    assert.equal(result.count, 2);

    assert.ok(result.jobs.every((job) => job.status === "waiting"));

    assert.equal(result.nextCursor, "0");
  });
});
