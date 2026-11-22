import { redis, connectRedis, disconnectRedis } from "./src/config/redis.js";

const JOBS_DATA = "jobs:data";
const JOBS_CREATED = "jobs:created";
const JOBS_SEQUENCE = "jobs:sequence";

async function migrateJobsIndex() {
  try {
    await connectRedis();

    // Read all existing jobs
    const jobsData = await redis.hGetAll(JOBS_DATA);

    // Convert hash entries into job objects
    const jobs = Object.entries(jobsData).map(([id, value]) => ({
      id,
      ...JSON.parse(value),
    }));

    // Sort jobs by creation time
    jobs.sort((a, b) => {
      return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
    });

    // Build Sorted Set entries
    const entries = jobs.map((job, index) => ({
      score: index + 1,
      value: job.id,
    }));

    const script = `
  if redis.call('EXISTS', KEYS[1]) == 1
    or redis.call('EXISTS', KEYS[2]) == 1 then
    return -1
  end

  for i = 2, #ARGV, 2 do
    redis.call('ZADD', KEYS[1], ARGV[i], ARGV[i + 1])
  end

  redis.call('SET', KEYS[2], ARGV[1])

  return 1
`;

    const args = [
      String(jobs.length),
      ...entries.flatMap((entry) => [String(entry.score), entry.value]),
    ];

    const result = await redis.eval(script, {
      keys: [JOBS_CREATED, JOBS_SEQUENCE],
      arguments: args,
    });

    if (result === -1) {
      throw new Error(
        "Migration already exists. Aborting to protect the index.",
      );
    }

    console.log(`Migration completed. Indexed ${jobs.length} jobs.`);
  } catch (error) {
    console.error("Migration failed:", error);
  } finally {
    await disconnectRedis();
  }
}

migrateJobsIndex();
