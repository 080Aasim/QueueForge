import { redis } from "../../config/redis.js";

// const QUEUE_NAME = "jobs";
const WAITING_QUEUE = "jobs:waiting"; // List, Jobs waiting to be claimed
const PROCESSING_QUEUE = "jobs:processing"; // Sorted set, Lease deadlines
const PROCESSING_LIST = "jobs:processing:list"; // List, Jobs currently claimed by workers
const FAILED_QUEUE = "jobs:failed"; //List, Jobs that exhausted retries
const JOBS_DATA = "jobs:data"; // This preserve the job info. independently of the processing queues
const JOBS_CREATED = "jobs:created"; // Sorted set containing job IDs, ordered by creation
const JOBS_SEQUENCE = "jobs:sequence"; // Redis counter that generates uniques ordering scores

const CLAIMING_QUEUE = "jobs:claiming";
const CLAIM_GRACE_PERIOD = 5000;

// PROCESSING_LIST temporarily holds claimed jobs.
// PROCESSING_QUEUE remains your sorted set for lease deadlines.

const VISIBILITY_TIMEOUT = 30 * 1000;
const MAX_ATTEMPTS = 3;

// Create an async function to add to job to queue.
export async function addJob(job) {
  try {
    const jobData = JSON.stringify({
      ...job,
      attempts: job.attempts || 0,
      createdAt: job.createdAt || Date.now(),
    });

    const script = `
      local sequence = redis.call('INCR', KEYS[4])

      redis.call('LPUSH', KEYS[1], ARGV[1])
      redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
      redis.call('ZADD', KEYS[3], sequence, ARGV[2])

      return sequence
    `;

    await redis.eval(script, {
      keys: [
        WAITING_QUEUE,
        JOBS_DATA,
        JOBS_CREATED,
        JOBS_SEQUENCE,
      ],
      arguments: [jobData, String(job.id)],
    });

    console.log(`Job ${job.id} added to waiting queue`);

    return JSON.parse(jobData);
  } catch (error) {
    console.error("Failed to add job:", error);
    throw error;
  }
}

const CLAIM_JOB_SCRIPT = `
  local jobData = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')

  if not jobData then
    return nil
  end

  local job = cjson.decode(jobData)
  job.attempts = (job.attempts or 0) + 1
  job.status = "processing"

  local updatedJob = cjson.encode(job)
  local deadline = tonumber(ARGV[1])

  redis.call('LSET', KEYS[2], -1, updatedJob)
  redis.call('ZADD', KEYS[3], deadline, updatedJob)
  redis.call('HSET', KEYS[4], job.id, updatedJob)

  return updatedJob
`;

export async function claimJob() {
  const visibilityDeadline = Date.now() + VISIBILITY_TIMEOUT;

  // Atomically move job from waiting to processing list
  const result = await redis.eval(CLAIM_JOB_SCRIPT, {
    keys: [WAITING_QUEUE, PROCESSING_LIST, PROCESSING_QUEUE, JOBS_DATA],
    arguments: [visibilityDeadline.toString()],
  });

  if (!result) return null;

  const job = JSON.parse(result);

  console.log("Claimed job:", job.id);
  console.log("Lease deadline:", visibilityDeadline);

  return job;
}

export async function acknowledgeJob(job) {
  const script = `
    local processingJobs = redis.call('LRANGE', KEYS[1], 0, -1)

    for _, jobData in ipairs(processingJobs) do
      local currentJob = cjson.decode(jobData)

      if currentJob.id == ARGV[1] then
        currentJob.status = "completed"
        local updatedJob = cjson.encode(currentJob)
        redis.call('LREM', KEYS[1], 1, jobData)
        redis.call('ZREM', KEYS[2], jobData)
        redis.call('HSET', KEYS[3], currentJob.id, updatedJob)
        return 1
      end
    end

    return 0
  `;

  const result = await redis.eval(script, {
    keys: [PROCESSING_LIST, PROCESSING_QUEUE, JOBS_DATA],
    arguments: [String(job.id)],
  });

  if (result === 1) {
    console.log(`Job ${job.id} acknowledged`);
  } else {
    console.warn(`Job ${job.id} was not found for acknowledgement`);
  }
}

export async function recoverExpiredJobs() {
  const now = Date.now();

  const expiredJobs = await redis.zRangeByScore(PROCESSING_QUEUE, 0, now);

  const script = `
    local jobData = ARGV[1]
    local job = cjson.decode(jobData)

    -- Recheck lease because it may have been renewed
    -- after expiredJobs was initially fetched.
    local score = redis.call('ZSCORE', KEYS[1], jobData)

    if not score or tonumber(score) > tonumber(ARGV[2]) then
      return 0
    end

    redis.call('ZREM', KEYS[1], jobData)

    local processingJobs = redis.call('LRANGE', KEYS[4], 0, -1)

    for _, processingJobData in ipairs(processingJobs) do
      local processingJob = cjson.decode(processingJobData)

      if processingJob.id == job.id then
        redis.call('LREM', KEYS[4], 1, processingJobData)
        break
      end
    end

    if job.attempts >= tonumber(ARGV[3]) then
      job.status = "failed"
      local updatedJob = cjson.encode(job)

      redis.call('LPUSH', KEYS[3], updatedJob)
      redis.call('HSET', KEYS[5], job.id, updatedJob)

      return 2
    else
      job.status = "waiting"
      local updatedJob = cjson.encode(job)

      redis.call('LPUSH', KEYS[2], updatedJob)
      redis.call('HSET', KEYS[5], job.id, updatedJob)

      return 1
    end
  `;

  for (const jobData of expiredJobs) {
    const result = await redis.eval(script, {
      keys: [
        PROCESSING_QUEUE,
        WAITING_QUEUE,
        FAILED_QUEUE,
        PROCESSING_LIST,
        JOBS_DATA,
      ],
      arguments: [jobData, now.toString(), MAX_ATTEMPTS.toString()],
    });

    const job = JSON.parse(jobData);

    if (result === 1) {
      console.log(
        `Job ${job.id} recovered and re-queued. Attempt: ${job.attempts}`,
      );
    } else if (result === 2) {
      console.log(
        `Job ${job.id} exceeded max attempts and moved to failed queue`,
      );
    }
  }
}


export async function renewJobLease(job) {
  const now = Date.now();
  const newDeadline = now + VISIBILITY_TIMEOUT;

  const script = `
    local jobs = redis.call('ZRANGE', KEYS[1], 0, -1)

    for _, jobData in ipairs(jobs) do
      local currentJob = cjson.decode(jobData)

      if currentJob.id == ARGV[1] then
        local score = redis.call('ZSCORE', KEYS[1], jobData)

        if tonumber(score) <= tonumber(ARGV[2]) then
          return 0
        end

        redis.call('ZADD', KEYS[1], ARGV[3], jobData)
        return 1
      end
    end

    return 0
  `;

  const result = await redis.eval(script, {
    keys: [PROCESSING_QUEUE],
    arguments: [String(job.id), now.toString(), newDeadline.toString()],
  });

  return result === 1;
}

export async function getJobById(jobId) {
  const jobData = await redis.hGet(JOBS_DATA, jobId);
  if (!jobData) return null;

  return JSON.parse(jobData);
}

export async function getAllJobs() {
  const jobs = await redis.hGetAll(JOBS_DATA);

  return Object.values(jobs).map((jobData) => JSON.parse(jobData));
}

export async function getJobsByStatus(status) {
  const jobs = await getAllJobs();

  return jobs.filter((job) => job.status === status);
}


export async function getJobsPaginated(
  cursor = "0",
  limit = 10,
  status = null,
) {
  let nextCursor = String(cursor);
  const jobs = [];

  while (jobs.length < limit) {
    const remaining = limit - jobs.length;

    const ids = await redis.zRangeByScore(
      JOBS_CREATED,
      nextCursor === "0" ? "-inf" : `(${nextCursor}`,
      "+inf",
      {
        LIMIT: {
          offset: 0,
          count: remaining,
        },
      },
    );

    if (ids.length === 0) {
      nextCursor = "0";
      break;
    }

    for (const id of ids) {
      const jobData = await redis.hGet(JOBS_DATA, id);

      if (!jobData) continue;

      const job = JSON.parse(jobData);

      if (!status || job.status === status) {
        jobs.push(job);
      }
    }

    const lastId = ids[ids.length - 1];
    nextCursor = String(await redis.zScore(JOBS_CREATED, lastId));

    if (ids.length < remaining) {
      nextCursor = "0";
      break;
    }
  }

  return {
    jobs,
    count: jobs.length,
    nextCursor,
  };
}
