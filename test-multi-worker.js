import test, { before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";

process.env.REDIS_URL = "redis://localhost:6379/1";

const { redis, connectRedis, disconnectRedis } =
  await import("./src/config/redis.js");

const WAITING_QUEUE = "jobs:waiting";
const PROCESSING_LIST = "jobs:processing:list";
const PROCESSING_QUEUE = "jobs:processing";
const FAILED_QUEUE = "jobs:failed";
const JOBS_DATA = "jobs:data";
const JOBS_CREATED = "jobs:created";

const WORKER_COUNT = 2;
const JOB_COUNT = 10;
const TEST_PROCESSING_TIME = 500;

const createdJobIds = [];
const workers = [];

before(async () => {
  await connectRedis();

  // Remove leftover test data from previous runs
  for (const jobId of createdJobIds) {
    await redis.hDel(JOBS_DATA, jobId);
    await redis.zRem(JOBS_CREATED, jobId);
  }
});

after(async () => {
  // Stop workers first
  for (const worker of workers) {
    if (worker.exitCode === null && worker.connected) {
      worker.send("shutdown");
    }
  }

  // Wait for workers to exit
  await Promise.all(
    workers.map(
      (worker) =>
        new Promise((resolve) => {
          if (worker.exitCode !== null) return resolve();
          worker.once("exit", resolve);
        }),
    ),
  );

  // Remove only our test jobs
  for (const jobId of createdJobIds) {
    const jobData = await redis.hGet(JOBS_DATA, jobId);

    if (jobData) {
      await redis.lRem(WAITING_QUEUE, 0, jobData);
      await redis.lRem(PROCESSING_LIST, 0, jobData);
      await redis.lRem(FAILED_QUEUE, 0, jobData);
      await redis.zRem(PROCESSING_QUEUE, jobData);
    }

    await redis.hDel(JOBS_DATA, jobId);
    await redis.zRem(JOBS_CREATED, jobId);
  }

  await disconnectRedis();
});

test("Two workers should process jobs without duplicate claims", async () => {
  const jobIds = [];

  // Create 10 jobs
  for (let i = 0; i < JOB_COUNT; i++) {
    const jobId = randomUUID();

    const job = {
      id: jobId,
      type: "integration-test",
      payload: { task: `Task ${i + 1}` },
      status: "waiting",
      attempts: 0,
      createdAt: new Date().toISOString(),
    };

    const jobData = JSON.stringify(job);

    await redis.lPush(WAITING_QUEUE, jobData);
    await redis.hSet(JOBS_DATA, jobId, jobData);
    await redis.zAdd(JOBS_CREATED, {
      score: i + 1,
      value: jobId,
    });

    createdJobIds.push(jobId);
    jobIds.push(jobId);
  }

  // Launch two independent workers
  const workerPath = "./src/api/workers/worker.js";

  for (let i = 0; i < WORKER_COUNT; i++) {
    const worker = spawn(process.execPath, [workerPath], {
      env: {
        ...process.env,
        WORKER_HEARTBEAT_INTERVAL: "100",
        JOB_PROCESSING_TIME: String(TEST_PROCESSING_TIME),
      },
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    });

    worker.processedJobIds = [];

    worker.stdout.on("data", (chunk) => {
      const output = chunk.toString();

      const matches = output.matchAll(/Processing job ([\w-]+)/g);

      for (const match of matches) {
        worker.processedJobIds.push(match[1]);
      }

      process.stdout.write(`[Worker ${i + 1}] ${output}`);
    });

    worker.stderr.on("data", (chunk) => {
      process.stderr.write(`[Worker ${i + 1}] ${chunk}`);
    });

    workers.push(worker);
  }

  // Wait for all jobs to complete
  const timeout = Date.now() + 15000;

  while (Date.now() < timeout) {
    const jobs = await Promise.all(
      jobIds.map((id) => redis.hGet(JOBS_DATA, id)),
    );

    const completedCount = jobs.filter((data) => {
      if (!data) return false;
      return JSON.parse(data).status === "completed";
    }).length;

    if (completedCount === JOB_COUNT) break;

    await new Promise((resolve) => setTimeout(resolve, 200));
  }

  // Verify final job statuses
  assert.equal(workers.length, WORKER_COUNT);

  for (const [index, worker] of workers.entries()) {
    assert.ok(
      worker.processedJobIds.length > 0,
      `Worker ${index + 1} should process at least one job`,
    );
  }
  const finalJobs = await Promise.all(
    jobIds.map((id) => redis.hGet(JOBS_DATA, id)),
  );

  const parsedJobs = finalJobs.map((data) => {
    assert.ok(data, "Job should exist in Redis");
    return JSON.parse(data);
  });

  assert.equal(parsedJobs.length, JOB_COUNT, "All 10 jobs should exist");

  assert.ok(
    parsedJobs.every((job) => job.status === "completed"),
    "All jobs should be completed",
  );

  assert.ok(
    parsedJobs.every((job) => job.attempts === 1),
    "Each job should be claimed exactly once",
  );
});
