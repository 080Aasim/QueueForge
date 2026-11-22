# Distributed Job System: Atomic Job Claiming, Lease Renewal, Crash Recovery & Job Status Tracking

## 1. Objective

Implement a reliable distributed job queue where multiple workers can process jobs concurrently without:

- Two workers claiming the same job.
- Losing jobs when a worker crashes.
- Processing jobs indefinitely without retry limits.
- Leaving completed jobs in processing queues.
- Losing track of jobs whose processing lease expires.
- Losing access to job information after a job leaves the active queue structures.

**Technology:** Node.js, Redis 6.2.1, JavaScript, Docker, Express.

---

## 2. Redis Queue Architecture

The system uses five Redis data structures.

| Redis Key | Data Structure | Purpose |
|---|---|---|
| `jobs:waiting` | List | Stores jobs waiting to be processed |
| `jobs:processing:list` | List | Stores jobs currently claimed by workers |
| `jobs:processing` | Sorted Set | Stores jobs with their lease expiration timestamps |
| `jobs:failed` | List | Stores jobs that exceeded maximum retries |
| `jobs:data` | Hash | Stores the latest job record, indexed by job ID, for status tracking |

### Why use both a List and a Sorted Set for processing?

**Processing List:**

- Keeps the actual claimed job data.
- Allows us to locate jobs by their ID.
- Helps maintain a record of jobs currently being processed.

**Processing Sorted Set:**

- Stores the job as its member.
- Stores its lease expiration timestamp as its score.
- Allows efficient identification of expired jobs using `ZRANGEBYSCORE`.

The two processing structures are maintained together.

### Why use a separate `jobs:data` Hash?

The active queue structures change as a job moves through its lifecycle. For example, a completed job is removed from the processing List and Sorted Set. The `jobs:data` Hash preserves its latest record so it can still be retrieved by ID.

- Hash key: `jobs:data`
- Hash field: job ID
- Hash value: serialized job JSON

This makes the registry the central source for job lookup and status queries.

---

## 3. Atomic Job Claiming

### Problem with the previous implementation

Initially, we used two separate Redis operations:

1. `BRPOP` removed a job from the waiting queue.
2. `ZADD` registered it in the processing sorted set.

If a worker crashed between these operations, the job could disappear from the waiting queue without being registered in processing.

This created a job-loss scenario.

### Solution: Lua Script + LMOVE

We replaced the separate operations with a single Lua script.

```lua
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
```

### How it works

1. `LMOVE` moves a job from the waiting list to the processing list.
2. Redis decodes the job JSON.
3. The attempt counter is incremented.
4. The job status is changed to `processing`.
5. A visibility deadline is calculated.
6. `LSET` updates the job inside the processing list.
7. `ZADD` registers the job and its lease deadline.
8. `HSET` updates the job record in the central registry.

All operations happen inside one Lua script.

**Important concept: Redis Lua atomicity**

Redis executes a Lua script atomically. Other Redis commands cannot interleave with the script's execution.

This closes the crash window between moving a job and registering its lease.

### JavaScript `claimJob()`

```js
export async function claimJob() {
  const visibilityDeadline = Date.now() + VISIBILITY_TIMEOUT;

  const result = await redis.eval(CLAIM_JOB_SCRIPT, {
    keys: [
      WAITING_QUEUE,
      PROCESSING_LIST,
      PROCESSING_QUEUE,
      JOBS_DATA,
    ],
    arguments: [visibilityDeadline.toString()],
  });

  if (!result) return null;

  const job = JSON.parse(result);

  console.log("Claimed job:", job.id);
  console.log("Lease deadline:", visibilityDeadline);

  return job;
}
```

We use non-blocking `LMOVE`, so `claimJob()` returns `null` when the queue is empty. The worker waits one second and tries again.

---

## 4. Visibility Timeout and Heartbeat

### What is a visibility timeout?

A visibility timeout is the period during which a worker owns a job before its lease expires.

Our configuration:

```js
const VISIBILITY_TIMEOUT = 30 * 1000;
const HEARTBEAT_INTERVAL = 5 * 1000;
```

- Visibility timeout: 30 seconds.
- Heartbeat interval: 5 seconds.

### Why do we need heartbeats?

Consider a job that takes longer than 30 seconds.

Without lease renewal, the recovery loop could assume that the worker crashed and requeue the job even though it is still processing.

To prevent this, the worker periodically renews its lease.

### Lease renewal implementation

We search the sorted set for the job using its unique ID.

The Lua script:

1. Finds the job in the sorted set.
2. Checks whether its lease still exists.
3. Checks whether the lease has already expired.
4. Updates the lease deadline if it is still valid.

The new deadline is:

```js
const newDeadline = Date.now() + VISIBILITY_TIMEOUT;
```

The renewal operation is atomic.

### Why check the old deadline?

A worker should not be allowed to renew a lease that has already expired.

If the deadline has passed, the renewal fails and the recovery mechanism can take over.

---

## 5. Job Acknowledgement

### What is acknowledgement?

Acknowledgement means that the worker has successfully completed the job.

After successful processing, the job must be removed from both processing structures. Its status must also be updated in the central registry.

### Implementation

We use the job ID instead of relying on matching serialized JSON.

```js
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
```

### Why use job ID?

Redis Lua's `cjson.encode()` and JavaScript's `JSON.stringify()` are not guaranteed to produce identical serialized JSON strings.

Searching by a unique job ID avoids depending on exact JSON serialization.

The processing removals and registry update happen atomically inside Lua.

---

## 6. Expired Job Recovery

### Problem

What happens when a worker crashes while processing a job?

The job remains in:

- `jobs:processing:list`
- `jobs:processing`

However, the worker stops renewing its lease.

Eventually, the visibility timeout expires.

### Recovery mechanism

We run a recovery loop approximately every second.

First, retrieve expired jobs:

```js
const now = Date.now();

const expiredJobs = await redis.zRangeByScore(
  PROCESSING_QUEUE,
  0,
  now
);
```

The sorted set stores the expiration timestamp as its score.

Therefore, jobs whose scores are less than or equal to the current timestamp are expired.

### Recovery decisions

For every expired job:

1. Verify that its lease is still expired.
2. Remove it from the processing sorted set.
3. Find and remove the corresponding job from the processing list using its ID.
4. Check the attempt counter.
5. Either requeue it or move it to the failed queue.
6. Update its status and central registry record.

These operations are executed inside a Lua script.

### Recovery rules

```text
If attempts < MAX_ATTEMPTS:
    Move job to jobs:waiting
    Set status to waiting

If attempts >= MAX_ATTEMPTS:
    Move job to jobs:failed
    Set status to failed
```

Our configuration:

```js
const MAX_ATTEMPTS = 3;
```

The recovery operation checks the lease again inside Lua to avoid acting on stale results from the initial expired-job lookup.

---

## 7. Retry Mechanism

Every time a job is claimed:

```lua
job.attempts = (job.attempts or 0) + 1
```

The attempt counter increases.

With `MAX_ATTEMPTS = 3`:

| Claim | Attempts | After lease expires |
|---|---:|---|
| First | 1 | Requeue |
| Second | 2 | Requeue |
| Third | 3 | Move to failed |

A job that completes successfully is acknowledged and removed instead of being retried.

---

## 8. Job Status Tracking API

### Why job status tracking?

The processing and waiting queues represent where a job currently resides. However, completed jobs are removed from the processing structures. The `jobs:data` Hash keeps the latest job record available after queue transitions.

Each job has one of four statuses:

| Status | Meaning |
|---|---|
| `waiting` | The job is waiting to be claimed |
| `processing` | A worker has claimed the job |
| `completed` | The job was successfully processed and acknowledged |
| `failed` | The job exhausted its maximum retry attempts |

### Status lifecycle

```text
waiting
   |
   v
processing
   |
   +---- successful acknowledgement ----> completed
   |
   +---- lease expires, attempts remain -> waiting
   |
   +---- lease expires, attempts maxed --> failed
```

### Updating the registry

When a job is created, the producer writes it to both the waiting List and the registry using a Redis transaction:

```js
const transaction = redis.multi();

transaction.lPush(WAITING_QUEUE, jobData);
transaction.hSet(JOBS_DATA, job.id, jobData);

await transaction.exec();
```

The Lua scripts then update the same registry as the job changes state:

- Claim: status becomes `processing`.
- Acknowledgement: status becomes `completed`.
- Recovery: status becomes `waiting` or `failed`.

This keeps the queue transition and the corresponding status update together within each Lua script.

### Available API endpoints

The jobs router is mounted at `/api/jobs`.

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/jobs` | Create a job |
| `GET` | `/api/jobs` | Retrieve all registered jobs |
| `GET` | `/api/jobs?status=waiting` | Filter jobs by status |
| `GET` | `/api/jobs/:id` | Retrieve one job by ID |

Supported status query values:

- `waiting`
- `processing`
- `completed`
- `failed`

Unsupported status values return `400 Bad Request` and include the valid status values.

A request for a job ID that does not exist returns `404 Not Found`.

### Redis data-access functions

Retrieve a job by ID:

```js
export async function getJobById(jobId) {
  const jobData = await redis.hGet(JOBS_DATA, jobId);

  if (!jobData) return null;

  return JSON.parse(jobData);
}
```

Retrieve all jobs:

```js
export async function getAllJobs() {
  const jobs = await redis.hGetAll(JOBS_DATA);

  return Object.values(jobs).map((jobData) => JSON.parse(jobData));
}
```

Filter jobs by status:

```js
export async function getJobsByStatus(status) {
  const jobs = await getAllJobs();

  return jobs.filter((job) => job.status === status);
}
```

### API testing

The following endpoints were tested using Postman:

- `POST /api/jobs`
- `GET /api/jobs`
- `GET /api/jobs?status=completed`
- `GET /api/jobs/:id`

The tests verified job creation, listing, status filtering, retrieval by ID, invalid status handling, and the not-found response for an unknown job ID.

**Result: PASSED.**

**Current implementation note:** `getAllJobs()` reads the entire Redis Hash, and `getJobsByStatus()` filters the retrieved jobs in application memory. Pagination and more scalable status indexing are future improvements.

---

## 9. Worker Implementation

### Main worker lifecycle

```text
Connect to Redis
      |
Start recovery loop
      |
Claim job
      |
Is a job available?
   /       \
 No         Yes
 |           |
Wait 1 sec  Process job
 |           |
Retry       Heartbeat renews lease
             |
         Job completed
             |
         Acknowledge
             |
         Claim next job
```

### Worker configuration

| Setting | Value |
|---|---:|
| Visibility timeout | 30 seconds |
| Heartbeat interval | 5 seconds |
| Simulated processing time | 30 seconds |
| Recovery interval | 1 second |
| Maximum attempts | 3 |
| Empty queue polling delay | 1 second |

The heartbeat continues while the job is being processed.

The worker acknowledges the job only after successful completion.

If processing fails or the worker crashes, the job remains unacknowledged and can eventually be recovered after its lease expires.

---

## 10. Graceful Worker Shutdown

### Why graceful shutdown?

A worker may receive a shutdown signal while it is processing a job. Exiting immediately could interrupt the job and leave it unacknowledged until its visibility timeout expires.

Graceful shutdown allows the worker to finish its current job before closing its Redis connections.

### Shutdown behavior

The worker listens for:

- `SIGINT` when the process receives Ctrl + C.
- `SIGTERM` when the process is asked to terminate.

When either signal is received, the worker sets `isShuttingDown` to `true`.

The shutdown sequence is:

1. Stop the worker from starting another claim cycle.
2. Allow the currently claimed job to finish.
3. Keep renewing the job lease while processing continues.
4. Acknowledge the job after successful completion.
5. Stop the recovery loop.
6. Close the Redis connections gracefully.
7. Finish the worker shutdown.

### Heartbeat during shutdown

The heartbeat must remain active while the current job is processing, even after a shutdown signal arrives. Otherwise, a long-running job could lose its lease and be recovered by another worker before it finishes.

The worker also waits for an in-progress heartbeat renewal to complete before returning from job processing.

### Redis connection cleanup

The Redis clients are closed using `quit()` after the worker and recovery loop have stopped. This gives pending Redis commands an opportunity to complete before the connections close.

### Shutdown test

We tested graceful shutdown by:

1. Starting the worker.
2. Submitting a job.
3. Waiting until the worker began processing it.
4. Pressing Ctrl + C while the job was running.
5. Observing that heartbeats continued until processing completed.

Observed behavior:

- The shutdown signal was received.
- The active job continued processing.
- Lease renewals continued.
- The job completed and was acknowledged.
- The recovery loop stopped.
- Redis connections closed.
- The worker exited cleanly.

**Result: PASSED.**

---

## 11. Testing and Results

We performed tests using Postman, Redis CLI, Docker, and multiple worker processes.

### Test 1: Multiple Workers

Submitted multiple jobs through the API and started two workers.

Results:

- Both workers claimed jobs.
- Workers processed different jobs concurrently.
- Jobs were processed and acknowledged.

**Result: PASSED.**

### Test 2: Heartbeat Renewal

Configured:

- Visibility timeout: 30 seconds.
- Heartbeat interval: 5 seconds.
- Processing duration: approximately 30 seconds.

Observed repeated successful lease renewals.

**Result: PASSED.**

### Test 3: Job Acknowledgement

After successful processing, the worker acknowledged the job.

Both processing structures were cleaned up, and the registry status became `completed`.

**Result: PASSED.**

### Test 4: Worker Crash Recovery

Crash recovery is different from graceful shutdown. The worker process was forcefully terminated while it owned a job.

Procedure:

1. Added a job.
2. Started Worker 1.
3. Let it claim the job.
4. Forcefully terminated Worker 1 while it was processing.
5. Waited for its visibility timeout to expire.
6. Verified that the recovery loop requeued the job.
7. Started another worker and verified that it could claim the recovered job.

The recovered job's attempt count increased on its next claim.

**Result: PASSED.**

### Test 5: Maximum Retry Handling

With `MAX_ATTEMPTS = 3`, repeatedly terminated workers while they were processing a job.

After the third attempt's lease expired, the recovery loop moved the job to the failed queue and updated its registry status to `failed`.

**Result: PASSED.**

### Test 6: Job Status Tracking API

Tested the following endpoints using Postman:

- `POST /api/jobs`
- `GET /api/jobs`
- `GET /api/jobs?status=completed`
- `GET /api/jobs/:id`

Also verified invalid status handling and the not-found response for missing job IDs.

**Result: PASSED.**

### Redis verification commands

```redis
LLEN jobs:waiting
LLEN jobs:processing:list
ZCARD jobs:processing
LLEN jobs:failed
HGETALL jobs:data
HGET jobs:data <JOB_ID>
```

`jobs:data` is a Hash, so use `HGET` or `HGETALL` to inspect it. Do not use List commands such as `LLEN` on this key.

---

## 12. Important Redis Commands Learned

| Command | Purpose |
|---|---|
| `LPUSH` | Add an item to the beginning of a List |
| `LMOVE` | Atomically move an item between Lists |
| `LSET` | Update an item at a List index |
| `LREM` | Remove matching items from a List |
| `LRANGE` | Read items from a List |
| `LLEN` | Get List length |
| `ZADD` | Add a member with a score to a Sorted Set |
| `ZREM` | Remove a member from a Sorted Set |
| `ZSCORE` | Retrieve a member's score |
| `ZRANGE` | Retrieve Sorted Set members |
| `ZRANGEBYSCORE` | Retrieve members within a score range |
| `ZCARD` | Get Sorted Set cardinality |
| `HSET` | Set a field in a Hash |
| `HGET` | Retrieve a field from a Hash |
| `HGETALL` | Retrieve all fields and values from a Hash |
| `MULTI` / `EXEC` | Execute a Redis transaction |
| `EVAL` | Execute a Lua script in Redis |

---

## 13. Key Engineering Concepts Learned

**1. Atomicity**

Multiple Redis operations can be executed as one atomic Lua script, preventing other commands from interleaving.

**2. Visibility Timeout**

A lease gives a worker temporary ownership of a job.

**3. Heartbeat**

A periodic signal that tells the system the worker is still alive and processing.

**4. At-least-once Processing**

A job may be processed again if a worker crashes before acknowledgement. Therefore, job handlers should ideally be idempotent.

**5. Crash Recovery**

Expired leases allow the system to detect abandoned jobs and make them available again.

**6. Retry Limit**

A maximum attempt count prevents jobs from retrying forever.

**7. Dead-Letter Queue**

The failed queue stores jobs that have exhausted their allowed retries.

**8. Stable Job Identity**

Using a unique job ID is more reliable than depending on exact JSON serialization.

**9. Central Job Registry**

A Redis Hash makes job records retrievable independently of their queue location and supports status lookup.

**10. Graceful Shutdown**

A worker finishes and acknowledges its active job before closing its connections.

---

## 14. Future Improvements

- Pagination for job listing endpoints.
- More scalable status filtering and indexing.
- Better empty-queue polling or blocking notification.
- Monitoring and metrics for workers and queues.
- Job priorities.
- Job scheduling and delayed execution.
- Better consistency checks between processing structures.
- Idempotent job execution.
- Redis Streams and Consumer Groups as a possible future architecture.

---

## Final Outcome

Implemented and tested a distributed job processing system using Node.js and Redis Lists, Sorted Sets, and Hashes.

The system supports:

- Concurrent job processing.
- Atomic job creation and claiming.
- Lease-based job ownership.
- Heartbeat-based lease renewal.
- Atomic acknowledgement.
- Automatic recovery of expired jobs.
- Retry limits and failed-job handling.
- Graceful worker shutdown.
- Centralized job status tracking.
- REST endpoints to create, list, filter, and retrieve jobs.

**Core job lifecycle, crash-recovery, graceful-shutdown, and job-status API tests passed.**
