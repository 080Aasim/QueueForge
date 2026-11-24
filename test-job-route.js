import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import express from "express";
import jobRouter from "./src/api/routes/job.route.js";
import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";

const app = express();

app.use(express.json());
app.use("/api/jobs", jobRouter);

let server;
let baseUrl;

before(async () => {
  await connectRedis();

  server = app.listen(0, "127.0.0.1");

  await new Promise((resolve) => {
    server.once("listening", resolve);
  });

  baseUrl = `http://127.0.0.1:${server.address().port}/api/jobs`;
});

describe("Job API Pagination Validation", () => {
  // We'll add individual tests here.
  let createdJobIds = [];
  it("Should reject invalid cursors", async () => {
    const invalidCursors = ["-1", "abc", "1.5", ""];

    for (const cursor of invalidCursors) {
      const response = await fetch(
        `${baseUrl}?cursor=${encodeURIComponent(cursor)}`,
      );

      assert.equal(response.status, 400);

      const data = await response.json();

      assert.equal(data.message, "Invalid cursor");
    }
  });
  it("Should reject invalid pagination limits", async () => {
    const invalidLimits = ["0", "-1", "101", "abc", "1.5"];

    for (const limit of invalidLimits) {
      const response = await fetch(
        `${baseUrl}?limit=${encodeURIComponent(limit)}`,
      );

      assert.equal(response.status, 400);

      const data = await response.json();

      assert.equal(data.message, "Limit must be between 1 and 100");
    }
  });
  it("Should reject invalid job statuses", async () => {
    const response = await fetch(`${baseUrl}?status=unknown`);

    assert.equal(response.status, 400);

    const data = await response.json();

    assert.equal(data.message, "Invalid job status");

    assert.deepEqual(data.validStatuses, [
      "waiting",
      "processing",
      "completed",
      "failed",
    ]);
  });
  it("Should accept a valid job status", async () => {
    const response = await fetch(`${baseUrl}?status=waiting`);

    assert.equal(response.status, 200);

    const data = await response.json();

    assert.ok(Array.isArray(data.jobs));
    assert.equal(typeof data.count, "number");
    assert.equal(typeof data.nextCursor, "string");

    assert.ok(data.jobs.every((job) => job.status === "waiting"));
  });
  it("Should return 404 when job does not exist", async () => {
    const response = await fetch(`${baseUrl}/non-existent-job-id`);

    assert.equal(response.status, 404);

    const data = await response.json();

    assert.equal(data.message, "Job not found.");
  });
  it("Should retrieve an existing job by ID", async () => {
    // Create a job through the API
    const createResponse = await fetch(baseUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        type: "test",
        payload: {
          message: "Test job retrieval",
        },
      }),
    });

    assert.equal(createResponse.status, 202);

    const createdData = await createResponse.json();
    const jobId = createdData.job.id;
    createdJobIds.push(jobId);

    // Retrieve the created job
    const response = await fetch(`${baseUrl}/${jobId}`);

    assert.equal(response.status, 200);

    const data = await response.json();

    assert.equal(data.job.id, jobId);
    assert.equal(data.job.type, "test");
    assert.equal(data.job.status, "waiting");
    assert.deepEqual(data.job.payload, {
      message: "Test job retrieval",
    });
  });
  it("Should create a job successfully", async () => {
    const response = await fetch(baseUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        type: "email",
        payload: {
          to: "test@example.com",
          subject: "Test email",
        },
      }),
    });

    assert.equal(response.status, 202);

    const data = await response.json();
    const job = data.job;

    createdJobIds.push(job.id);

    assert.equal(data.message, "Job added to queue");
    assert.ok(job.id);
    assert.equal(job.type, "email");
    assert.equal(job.status, "waiting");
    assert.equal(job.attempts, 0);

    assert.deepEqual(job.payload, {
      to: "test@example.com",
      subject: "Test email",
    });
  });
  after(async () => {
    for (const jobId of createdJobIds) {
      const jobData = await redis.hGet("jobs:data", jobId);

      if (jobData) {
        const job = JSON.parse(jobData);

        if (job.status === "waiting") {
          const waitingJobs = await redis.lRange("jobs:waiting", 0, -1);

          const matchingJob = waitingJobs.find(
            (item) => JSON.parse(item).id === jobId,
          );

          if (matchingJob) {
            await redis.lRem("jobs:waiting", 1, matchingJob);
          }
        }
      }

      await redis.hDel("jobs:data", jobId);
      await redis.zRem("jobs:created", jobId);
    }

    await new Promise((resolve, reject) => {
      server.close((error) => {
        if (error) reject(error);
        else resolve();
      });
    });

    await disconnectRedis();
  });
});
