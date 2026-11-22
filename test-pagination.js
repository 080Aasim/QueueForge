import { connectRedis, disconnectRedis, redis } from "./src/config/redis.js";
import { getJobsPaginated } from "./src/api/queues/queue.js";

try {
  await connectRedis();

  const firstBatch = await getJobsPaginated("0", 2);

  console.log("First batch:", firstBatch.jobs.length);
  console.log("First cursor:", firstBatch.nextCursor);

  const secondBatch = await getJobsPaginated(
    firstBatch.nextCursor,
    2
  );

  console.log("Second batch:", secondBatch.jobs.length);
  console.log("Second cursor:", secondBatch.nextCursor);

} catch (error) {
  console.error(error);
} finally {
  await disconnectRedis();
}