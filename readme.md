# QueueForge: Distributed Job System with Atomic Processing, Crash Recovery, Cursor Pagination & Load Testing

## 1. Objective

Implement a reliable distributed job queue where multiple workers can process jobs concurrently without:

- Two workers claiming the same job.
- Losing jobs when a worker crashes.
- Processing jobs indefinitely without retry limits.
- Leaving completed jobs in processing queues.
- Losing track of jobs whose processing lease expires.
- Losing access to job information after a job leaves the active queue structures.

**Technology:** Node.js, Express 5, Redis 6.2.1, JavaScript (ES modules), Docker, PostgreSQL, Artillery, and Node.js `node:test`.

The queue coordination and job lifecycle are implemented with Redis Lists, Sorted Sets, Hashes, and Lua scripts. PostgreSQL is also configured in the API application.

---

## 2. Redis Queue Architecture

The system uses Redis Lists, Sorted Sets, and Hashes. The queue lifecycle uses five primary structures, while two additional keys provide a stable creation-order index for paginated job listing.

| Redis Key | Data Structure | Purpose |
|---|---|---|
| `jobs:waiting` | List | Stores jobs waiting to be processed |
| `jobs:processing:list` | List | Stores jobs currently claimed by workers |
| `jobs:processing` | Sorted Set | Stores jobs with their lease expiration timestamps |
| `jobs:failed` | List | Stores jobs that exceeded maximum retries |
| `jobs:data` | Hash | Stores the latest job record, indexed by job ID, for lookup and status tracking |
| `jobs:created` | Sorted Set | Stores job IDs ordered by a unique creation sequence score for pagination |
| `jobs:sequence` | String (integer counter) | Generates monotonically increasing scores for newly created jobs |

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

When a job is created, the producer uses a Lua script to update the waiting List, central registry, creation index, and sequence counter atomically:

```lua
local sequence = redis.call('INCR', KEYS[4])

redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[2], ARGV[1])
redis.call('ZADD', KEYS[3], sequence, ARGV[2])

return sequence
```

The keys are `jobs:waiting`, `jobs:data`, `jobs:created`, and `jobs:sequence`. The generated sequence is the Sorted Set score, and the job ID is its member. Using one Lua script keeps the queue, registry, and creation index in sync during job creation.

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
| `GET` | `/api/jobs` | Retrieve a page of registered jobs |
| `GET` | `/api/jobs?status=waiting` | Retrieve a page filtered by status |
| `GET` | `/api/jobs?limit=10&cursor=2` | Retrieve the next page after a creation-sequence cursor |
| `GET` | `/api/jobs/:id` | Retrieve one job by ID |

Supported status query values:

- `waiting`
- `processing`
- `completed`
- `failed`

Unsupported status values return `400 Bad Request` and include the valid status values.

A request for a job ID that does not exist returns `404 Not Found`.

### Cursor-based pagination with a Sorted Set

The job-listing endpoint uses `jobs:created` instead of scanning the `jobs:data` Hash with `HSCAN`. `HSCAN` is an iteration command, not a stable page API: its `COUNT` value is a hint, and scan results are not suitable for predictable page boundaries.

The creation index contains:

- Member: job ID.
- Score: monotonically increasing sequence from `jobs:sequence`.

The canonical job JSON remains in `jobs:data`. For each page, the application:

1. Reads job IDs from `jobs:created` in ascending score order, starting strictly after the supplied cursor.
2. Fetches each corresponding job record from `jobs:data`.
3. Applies the optional status filter.
4. Continues scanning subsequent IDs when status filtering leaves the page short.
5. Returns the matching jobs and the last scanned sequence score as `nextCursor`.

Example requests:

```http
GET /api/jobs?limit=2
GET /api/jobs?limit=2&cursor=2
GET /api/jobs?limit=2&status=completed
GET /api/jobs?limit=2&status=completed&cursor=2
```

Query parameters:

| Parameter | Purpose | Validation / behavior |
|---|---|---|
| `limit` | Maximum number of matching jobs in the response | Integer from 1 to 100; defaults to 10 |
| `cursor` | Last scanned creation-sequence score from the previous response | Non-negative integer; defaults to `0` |
| `status` | Optional job status filter | `waiting`, `processing`, `completed`, or `failed` |

A cursor of `0` starts from the beginning. A non-zero cursor is exclusive, so the next page starts after that score and does not repeat the previous page's last job. `nextCursor: "0"` indicates that the scan reached the end of the creation index. When a page exactly fills its limit at the end of the index, the client may need one additional request to receive an empty page and the terminal cursor.

Status filtering is performed while scanning the creation index, not through separate status-specific Sorted Sets. A filtered page may therefore need to scan more than `limit` IDs to collect enough matching jobs.

### Pagination test results

The test dataset contained 9 jobs: 8 completed and 1 waiting. With `limit=2`, the completed-job query returned all 8 completed jobs across four pages. The tests verified:

- Pages contained no more than 2 matching jobs.
- Job IDs did not repeat between pages.
- Results followed creation-sequence order.
- Status filtering returned only matching jobs.
- Requesting beyond the final completed-job cursor returned an empty `jobs` array, `count: 0`, and `nextCursor: "0"`.

**Result: PASSED for the tested dataset.**

Pagination is cursor-based, but it is not a snapshot across multiple HTTP requests. If a job's status changes between page requests, later pages can reflect the updated status.

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
- `GET /api/jobs?limit=2`
- `GET /api/jobs?limit=2&cursor=2`
- `GET /api/jobs?limit=2&status=completed`
- `GET /api/jobs/:id`

The tests verified job creation, paginated listing, status filtering, retrieval by ID, invalid status handling, cursor and limit validation, and the not-found response for an unknown job ID.

**Result: PASSED.**

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

### Automated test suite

The project uses Node.js' built-in test runner. Tests run sequentially because they share Redis keys and fixtures:

```powershell
npm test
```

Latest full-suite result: **28 tests passed, 0 failed**.

| Test file | Tests | Coverage |
|---|---:|---|
| `test-acknowledgement.js` | 2 | Successful acknowledgement, processing cleanup, and unknown job handling |
| `test-job-route.js` | 7 | Job creation, lookup, validation, status handling, and 404 behavior |
| `test-lease.js` | 4 | Lease renewal, expired lease rejection, requeue, and retry exhaustion |
| `test-pagination.js` | 5 | Cursor validation, page boundaries, no duplicates, status filtering, and terminal page |
| `test-producer.js` | 2 | Job creation and atomic duplicate-ID rejection |
| `test-worker-reliability.js` | 3 | Heartbeat renewal, crash recovery, and graceful shutdown |
| `test-worker.js` | 4 | Claiming, empty queue, attempt increments, and concurrent claim safety |
| `test-multi-worker.js` | 1 | Two-worker integration test processing 10 jobs |
| **Total** | **28** | **All passed** |

The multi-worker integration test uses an isolated Redis database (`DB 1`) so it does not consume jobs from the normal development queue. Both workers processed jobs, all 10 jobs reached `completed`, and each job had one claim in that test run. This is a test observation, not an exactly-once delivery guarantee; the system uses at-least-once processing semantics.

### Redis verification commands

```redis
LLEN jobs:waiting
LLEN jobs:processing:list
ZCARD jobs:processing
LLEN jobs:failed
HGETALL jobs:data
HGET jobs:data <JOB_ID>
ZCARD jobs:created
GET jobs:sequence
ZRANGE jobs:created 0 -1 WITHSCORES
```

`jobs:data` is a Hash, so use `HGET` or `HGETALL` to inspect it. Do not use List commands such as `LLEN` on this key. `jobs:created` is a Sorted Set, so use `ZCARD` and `ZRANGE` to inspect the pagination index; `jobs:sequence` is an integer counter, so use `GET`.

---

## 12. Load Testing and Performance Results

Load testing was performed with Artillery against the job-creation API. The API server was running locally, and each scenario submitted jobs through `POST /api/jobs`.

### Artillery configuration

The baseline scenario used a 30-second phase and a fixed arrival rate. The arrival rate was increased between runs while keeping the duration and job payload consistent.

```yaml
config:
  target: "http://localhost:4000"
  phases:
    - duration: 30
      arrivalRate: 50
      name: "Load test"

scenarios:
  - name: "Create jobs"
    flow:
      - post:
          url: "/api/jobs"
          json:
            type: "load-test"
            payload:
              message: "Artillery test job"
```

### API enqueue results

| Arrival rate | Duration | Requests | HTTP 202 accepted | Failed requests | Mean latency | p95 | p99 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 req/sec | 30 sec | 600 | 600 | 0 | 3.1 ms | 6 ms | 10.9 ms | 31 ms |
| 50 req/sec | 30 sec | 1,500 | 1,500 | 0 | 5.5 ms | 13.1 ms | 50.9 ms | 196 ms |

All measured requests in these two runs received `202 Accepted`. The 50 req/sec run showed higher tail latency, including a 196 ms maximum, but no failed requests. These numbers describe API job-enqueue performance, not worker execution throughput.

### Four-worker processing throughput

A separate live benchmark measured job consumption with four worker processes. Each worker used a simulated processing duration of 2 seconds. No new jobs were submitted during the 60-second measurement window.

| Metric | Result |
|---|---:|
| Worker processes | 4 |
| Simulated processing time per job | 2 seconds |
| Theoretical aggregate throughput | 2 jobs/sec |
| Measurement duration | 60.45 seconds |
| Waiting queue at start | 1,426 jobs |
| Waiting queue at end | 1,306 jobs |
| Jobs consumed during measurement | 120 |
| Measured throughput | **1.99 jobs/sec** |

The measured rate was approximately 99.5% of the simple theoretical rate of four workers divided by two seconds per job. This is a local benchmark using simulated work; real throughput will depend on job type, CPU, network, Redis latency, and external services.

### Post-benchmark Redis consistency check

After all four workers were stopped gracefully, the final Redis snapshot was:

| Redis key | Value | Interpretation |
|---|---:|---|
| `jobs:waiting` | 684 | Remaining backlog; workers were stopped before draining it |
| `jobs:processing` (`ZCARD`) | 0 | No active lease entries |
| `jobs:processing:list` (`LLEN`) | 0 | No jobs left in the processing list |
| `jobs:failed` (`LLEN`) | 0 | No failed entries in the final test state |

The non-empty waiting queue is expected because the benchmark was stopped with work still queued. The important consistency result is that both processing structures were empty after shutdown.

**Benchmark conclusion:** The API accepted the measured 20 and 50 requests/sec workloads without failed requests, the four-worker benchmark sustained 1.99 simulated jobs/sec, and the final processing structures were clean after graceful shutdown.

---

## 13. Challenges Encountered and Solutions Implemented

This section records the main engineering problems encountered while building and testing the queue, along with the changes used to solve them.

### Challenge 1: A job could be lost between claiming and registering it

**Problem:** The initial flow used separate Redis commands to remove a job from the waiting queue and register it in the processing Sorted Set. If a worker stopped between those commands, the job could be removed from the waiting queue without being tracked as processing.

**Solution:** Replaced the multi-command claim flow with one Lua script using `LMOVE`. The script moves the job, increments its attempt count, changes its status, registers its lease deadline, and updates `jobs:data` atomically.

**Outcome:** The claim transition and its Redis bookkeeping now execute as one atomic operation.

### Challenge 2: The processing List and lease Sorted Set needed matching job records

**Problem:** Claiming a job changes its JSON by updating `attempts` and `status`. The original JSON in the processing List therefore needed to be replaced, and the same updated record needed to be stored in the lease Sorted Set and central registry.

**Solution:** The claim Lua script uses `LSET` to replace the last item moved into the processing List, then writes the updated job to `jobs:processing` and `jobs:data`.

**Outcome:** The processing structures and registry receive the same logical job state during a claim.

### Challenge 3: JSON string equality was unreliable during acknowledgement and recovery

**Problem:** JavaScript `JSON.stringify()` and Redis Lua `cjson.encode()` can serialize the same object with different property ordering. Comparing the serialized strings directly could fail even when they represented the same job.

**Solution:** Locate the job by its unique `id`. Acknowledgement and recovery decode the stored JSON and compare IDs before removing the corresponding processing-list entry.

**Outcome:** Queue operations rely on stable job identity instead of serialized-string equality.

### Challenge 4: A worker crash could leave a job stuck in processing

**Problem:** When a worker is forcefully terminated, it stops renewing its lease. The job remains in the processing structures until the lease expires.

**Solution:** Added a recovery loop that checks `jobs:processing` for expired lease scores. A Lua script rechecks the lease deadline before changing anything, removes the job from both processing structures, and either requeues it or moves it to `jobs:failed`.

**Outcome:** Expired jobs can be recovered without relying on the original worker restarting. The in-script lease recheck also prevents acting on a stale expiration result if the lease was renewed after the initial lookup.

### Challenge 5: Retried jobs needed a finite stopping point

**Problem:** A job that repeatedly crashes or never completes must not circulate through the waiting queue forever.

**Solution:** Increment `attempts` on every claim and enforce `MAX_ATTEMPTS = 3`. When the third attempt's lease expires, recovery moves the job to the failed queue and records status `failed` in `jobs:data`.

**Outcome:** The system has a bounded retry policy and a failed-job destination.

### Challenge 6: Long-running jobs could be recovered while still active

**Problem:** A job may run longer than its 30-second visibility timeout. Without renewal, the recovery loop could mistake a healthy worker for a crashed one.

**Solution:** Added a heartbeat that renews the lease every 5 seconds. Renewal checks that the lease still exists and has not already expired before extending its deadline.

**Outcome:** Active workers can retain ownership of jobs that take longer than the initial visibility timeout.

### Challenge 7: Graceful shutdown and crash recovery needed different behavior

**Problem:** Stopping a worker with Ctrl+C should not be treated as a crash. Exiting immediately could interrupt an active job, while stopping its heartbeat too early could allow another worker to recover the same job.

**Solution:** The graceful-shutdown flow stops new claims, allows the active job to finish, keeps its heartbeat running, acknowledges the job, stops the recovery loop, and then closes Redis connections. Crash recovery was tested separately by forcefully terminating the worker process.

**Outcome:** The graceful-shutdown test confirmed that an active job could finish before exit; the separate forced-termination test confirmed lease-based recovery by another worker.

### Challenge 8: Job information disappeared from active queues after completion

**Problem:** Completed jobs are removed from the waiting and processing structures. Those structures alone could not support lookup of a job after it finished.

**Solution:** Introduced `jobs:data`, a Redis Hash keyed by job ID. Creation, claiming, acknowledgement, and recovery update the registry as part of their respective Lua scripts.

**Outcome:** The API can retrieve a job and its latest status independently of its current queue location.

### Challenge 9: HSCAN did not provide reliable API pagination

**Problem:** The first listing implementation used `HSCAN` with a cursor and `COUNT`. During testing, page sizes varied and a job from a previous page appeared again. `COUNT` is a scan hint, and an HSCAN cursor is an iteration cursor rather than a stable page boundary.

**Solution:** Added a creation-order index:

- `jobs:created` is a Sorted Set whose members are job IDs.
- `jobs:sequence` is an incrementing counter used as each job's unique score.
- `jobs:data` remains the canonical store for job JSON.

New job creation updates the waiting List, registry Hash, creation Sorted Set, and sequence counter in one Lua script. A one-time migration sorted existing records by `createdAt`, populated the index with scores, and initialized the counter. The migration checks whether either index key already exists so it does not overwrite an existing index.

**Outcome:** Listing now uses score-based pagination. The next request starts strictly after the previous cursor score, avoiding the duplicate-page behavior seen with HSCAN.

### Challenge 10: Status filtering could leave a page short

**Problem:** A page may encounter job IDs whose current status does not match the requested filter. Fetching only `limit` IDs could therefore return fewer than `limit` matching jobs even when more matching jobs exist later in the creation index.

**Solution:** Continue scanning the creation index until the page contains the requested number of matching jobs or the index is exhausted. The cursor tracks the last scanned creation score, not merely the last returned matching job.

**Outcome:** The tested dataset returned all 8 completed jobs across four pages of 2, without duplicate IDs. A request beyond the final completed-job cursor returned an empty page and `nextCursor: "0"`.

### Practical limitations identified during testing

- Pagination is not a snapshot across multiple HTTP requests. A job whose status changes between requests may appear or disappear from later filtered pages.
- Status filtering scans the shared creation index. For very large datasets, separate status-specific indexes may reduce the amount of scanning.
- The current implementation fetches job records individually from `jobs:data`; batched reads such as `HMGET` can reduce Redis round trips later.
- If a page exactly fills its limit at the end of the index, the client may make one additional request to receive the terminal empty page with `nextCursor: "0"`.

---

## 14. Important Redis Commands Learned

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
| `ZRANGEBYSCORE` | Retrieve members within a score range; used for cursor pagination and expired-job lookup |
| `ZCARD` | Get the number of members in a Sorted Set |
| `INCR` | Atomically increment the job creation sequence counter |
| `ZCARD` | Get Sorted Set cardinality |
| `HSET` | Set a field in a Hash |
| `HGET` | Retrieve a field from a Hash |
| `HGETALL` | Retrieve all fields and values from a Hash |
| `MULTI` / `EXEC` | Execute a Redis transaction |
| `EVAL` | Execute a Lua script in Redis |

---

## 15. Key Engineering Concepts Learned

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

## 16. Future Improvements

- Add status-specific Sorted Set indexes if status-filtered listing needs to scale beyond scanning the creation index.
- Use `HMGET` or batched reads to reduce Redis round trips when fetching page records.
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
- Cursor-based job pagination using a Sorted Set creation index.
- Optional status filtering during pagination.
- REST endpoints to create, list, filter, and retrieve jobs.

**Verified results:** 28/28 automated tests passed; the two-worker integration test completed all 10 jobs; Artillery received 600/600 and 1,500/1,500 successful enqueue responses at 20 and 50 requests/sec; and four workers sustained 1.99 simulated jobs/sec in the measured local benchmark. The final post-shutdown check found no jobs left in either processing structure.
