import { redis, blockingRedis } from "../../config/redis.js";

// const QUEUE_NAME = "jobs";
const WAITING_QUEUE = "jobs:waiting"; // List, Jobs waiting to be claimed
const PROCESSING_QUEUE = "jobs:processing"; // Sorted set, Lease deadlines
const PROCESSING_LIST = "jobs:processing:list"; // List, Jobs currently claimed by workers
const FAILED_QUEUE = "jobs:failed"; //List, Jobs that exhausted retries

const CLAIMING_QUEUE = "jobs:claiming";
const CLAIM_GRACE_PERIOD = 5000;

// PROCESSING_LIST temporarily holds claimed jobs.
// PROCESSING_QUEUE remains your sorted set for lease deadlines.

const VISIBILITY_TIMEOUT = 30 * 1000;
const MAX_ATTEMPTS = 3;

// Create an async function to add to job to queue using redis.lPush
export async function addJob(job) {
  try {
    const jobData = JSON.stringify({
      ...job,
      attempts: job.attempts || 0,
      createdAt: job.createdAt || Date.now(),
    });

    await redis.lPush(WAITING_QUEUE, jobData);

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

  local updatedJob = cjson.encode(job)
  local deadline = tonumber(ARGV[1])

  redis.call('LSET', KEYS[2], -1, updatedJob)
  redis.call('ZADD', KEYS[3], deadline, updatedJob)

  return updatedJob
`;

export async function claimJob() {
  const visibilityDeadline = Date.now() + VISIBILITY_TIMEOUT;

  // Atomically move job from waiting to processing list
  const result = await redis.eval(CLAIM_JOB_SCRIPT, {
    keys: [WAITING_QUEUE, PROCESSING_LIST, PROCESSING_QUEUE],
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
        redis.call('LREM', KEYS[1], 1, jobData)
        redis.call('ZREM', KEYS[2], jobData)
        return 1
      end
    end

    return 0
  `;

  const result = await redis.eval(script, {
    keys: [PROCESSING_LIST, PROCESSING_QUEUE],
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

  const expiredJobs = await redis.zRangeByScore(
    PROCESSING_QUEUE,
    0,
    now,
  );

  const script = `
    local jobData = ARGV[1]
    local job = cjson.decode(jobData)

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
      redis.call('LPUSH', KEYS[3], jobData)
      return 2
    else
      redis.call('LPUSH', KEYS[2], jobData)
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
      ],
      arguments: [
        jobData,
        now.toString(),
        MAX_ATTEMPTS.toString(),
      ],
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

// export async function recoverExpiredJobs() {
//   const now = Date.now();

//   const expiredJobs = await redis.zRangeByScore(PROCESSING_QUEUE, 0, now);

//   console.log("Recovery timestamp:", now);
//   console.log("Expired jobs found:", expiredJobs.length);

//   const script = `
//     local removed = redis.call('ZREM', KEYS[1], ARGV[1])

//     if removed == 1 then
//       local job = cjson.decode(ARGV[1])

//       if job.attempts >= tonumber(ARGV[2]) then
//         redis.call('LPUSH', KEYS[3], ARGV[1])
//         return 2
//       else
//         redis.call('LPUSH', KEYS[2], ARGV[1])
//         return 1
//       end
//     end

//     return 0
//   `;

//   for (const jobData of expiredJobs) {
//     const job = JSON.parse(jobData);

//     console.log("Attempting recovery for job:", job.id);
//     console.log("Calling recovery Lua script...");

//     const result = await redis.eval(script, {
//       keys: [PROCESSING_QUEUE, WAITING_QUEUE, FAILED_QUEUE],
//       arguments: [jobData, MAX_ATTEMPTS.toString()],
//     });

//     console.log("Lua script finished:", result);

//     try {
//       const result = await redis.eval(script, {
//         keys: [PROCESSING_QUEUE, WAITING_QUEUE, FAILED_QUEUE],
//         arguments: [jobData, MAX_ATTEMPTS.toString()],
//       });

//       console.log("Recovery Lua result:", result);

//       if (result === 1) {
//         console.log(
//           `Job ${job.id} recovered and re-queued. Attempt: ${job.attempts}`,
//         );
//       } else if (result === 2) {
//         console.log(
//           `Job ${job.id} exceeded max attempts and moved to failed queue`,
//         );
//       } else {
//         console.warn(`Job ${job.id} was not recovered. Result: ${result}`);
//       }
//     } catch (error) {
//       console.error(`Failed to recover job ${job.id}:`, error);
//     }
//   }
// }

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
    arguments: [
      String(job.id),
      now.toString(),
      newDeadline.toString(),
    ],
  });

  return result === 1;
}
