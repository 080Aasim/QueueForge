# QueueForge: Distributed Job System with Atomic Processing, Crash Recovery, Cursor Pagination & Load Testing

## 1. Objective

Implement a reliable distributed job queue where multiple workers can process jobs concurrently without:

- Two workers claiming the same job.
- Losing jobs when a worker crashes.
- Processing jobs indefinitely without retry limits.
- Leaving completed jobs in processing queues.
- Losing track of jobs whose processing lease expires.
- Losing access to job information after a job leaves the active queue structures.
- Allowing stale workers to acknowledge or renew jobs reclaimed by another worker.

**Technology:** Node.js, Express 5, Redis 8, Redis Node.js client (`redis` ^6.2.1), JavaScript (ES modules), Docker, PostgreSQL 18, Artillery, and Node.js `node:test`.

The queue coordination and job lifecycle are implemented with Redis Lists, Sorted Sets, Hashes, and Lua scripts. PostgreSQL is also configured in the API application.

---

## 2. Redis Queue Architecture

The system uses Redis Lists, Sorted Sets, and Hashes.

| Redis Key | Data Structure | Purpose |
|---|---|---|
| `jobs:waiting` | List | Stores jobs waiting to be processed |
| `jobs:processing:list` | List | Stores jobs currently claimed by workers |
| `jobs:processing` | Sorted Set | Stores jobs with their lease expiration timestamps |
| `jobs:failed` | List | Stores jobs that exceeded maximum retries |
| `jobs:data` | Hash | Stores the latest job record, indexed by job ID |
| `jobs:created` | Sorted Set | Stores job IDs ordered by a unique creation sequence score |
| `jobs:sequence` | String (integer counter) | Generates monotonically increasing scores for newly created jobs |

### Why use both a List and a Sorted Set for processing?

**Processing List:**
- Keeps the actual claimed job data.
- Allows us to locate jobs by their ID.
- Maintains a record of jobs currently being processed.

**Processing Sorted Set:**
- Stores the job as its member.
- Stores its lease expiration timestamp as its score.
- Allows efficient identification of expired jobs using `ZRANGEBYSCORE`.

The two processing structures are maintained together.

### Why use a separate `jobs:data` Hash?

The active queue structures change as a job moves through its lifecycle. The `jobs:data` Hash preserves its latest record so it can still be retrieved by ID after completion or failure.

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

### Solution: Lua Script + LMOVE

We replaced the separate operations with a single Lua script.

Each successful claim also receives a unique `claimToken`, generated using Node.js `randomUUID()`. This token identifies the specific worker attempt that currently owns the job.

```lua
local jobData = redis.call('LMOVE', KEYS[1], KEYS[2], 'LEFT', 'RIGHT')

if not jobData then
  return nil
end

local job = cjson.decode(jobData)
job.attempts = (job.attempts or 0) + 1
job.status = "processing"
job.claimToken = ARGV[2]

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
4. The job status changes to `processing`.
5. The unique claim token is stored in the job.
6. A visibility deadline is calculated.
7. `LSET` updates the job inside the processing list.
8. `ZADD` registers the job and its lease deadline.
9. `HSET` updates the job record in the central registry.

All operations happen inside one Lua script.

### Redis Lua atomicity

Redis executes a Lua script atomically. Other Redis commands cannot interleave with the script's execution.

This closes the crash window between moving a job and registering its lease.

### JavaScript `claimJob()`

The JavaScript function generates a new token for every claim attempt:

```js
import { randomUUID } from "node:crypto";

export async function claimJob() {
  const visibilityDeadline = Date.now() + VISIBILITY_TIMEOUT;
  const claimToken = randomUUID();

  const result = await redis.eval(CLAIM_JOB_SCRIPT, {
    keys: [
      WAITING_QUEUE,
      PROCESSING_LIST,
      PROCESSING_QUEUE,
      JOBS_DATA,
    ],
    arguments: [
      visibilityDeadline.toString(),
      claimToken,
    ],
  });

  if (!result) return null;

  const job = JSON.parse(result);

  console.log("Claimed job:", job.id);
  console.log("Lease deadline:", visibilityDeadline);

  return job;
}
```

We use non-blocking `LMOVE`, so `claimJob()` returns `null` when the queue is empty. The worker waits one second and tries again.

### Claim token and job ownership

A job ID identifies the job itself, but it does not identify which worker attempt currently owns it.

The `claimToken` solves this problem:

- Every successful claim receives a unique token.
- A reclaimed job receives a different token.
- Lease renewal requires the current token.
- Acknowledgement requires the current token.
- Recovery clears the old token before requeueing or failing the job.

This prevents a worker with an outdated job object from modifying a newer processing attempt.

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

The renewal Lua script verifies:

1. The job exists in the processing structure.
2. The job ID matches.
3. The claim token matches the current worker attempt.
4. The existing lease has not expired.

Only if these checks pass is the lease deadline extended.

The new deadline is:

```js
const newDeadline = Date.now() + VISIBILITY_TIMEOUT;
```

The renewal operation is atomic.

### Why check both the deadline and claim token?

A worker should not be allowed to renew a lease that has already expired.

It should also not be allowed to renew a job that another worker has reclaimed.

The claim token prevents an outdated worker from renewing the lease belonging to a newer worker attempt.

---

## 5. Job Acknowledgement

### What is acknowledgement?

Acknowledgement means that the worker has successfully completed the job.

After successful processing, the job must be removed from both processing structures. Its status must also be updated in the central registry.

### Implementation

Acknowledgement matches both the job ID and its claim token.

```js
export async function acknowledgeJob(job) {
  const script = `
    local processingJobs = redis.call('LRANGE', KEYS[1], 0, -1)

    for _, jobData in ipairs(processingJobs) do
      local currentJob = cjson.decode(jobData)

      if currentJob.id == ARGV[1]
        and currentJob.claimToken == ARGV[2] then

        currentJob.status = "completed"
        currentJob.claimToken = nil

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
    arguments: [
      String(job.id),
      String(job.claimToken),
    ],
  });

  if (result === 1) {
    console.log(`Job ${job.id} acknowledged`);
  } else {
    console.warn(`Job ${job.id} was not found for acknowledgement`);
  }
}
```

### Why use job ID and claim token?

The job ID identifies the job, while the claim token identifies its current processing attempt.

Matching both prevents a stale worker from acknowledging a job that has already been reclaimed.

Redis Lua's `cjson.encode()` and JavaScript's `JSON.stringify()` are not guaranteed to produce identical serialized JSON strings. Matching by job identity avoids depending on exact JSON serialization.

### Clearing the claim token

After successful acknowledgement:

```lua
currentJob.claimToken = nil
```

The completed job no longer needs an active ownership token.

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
4. Clear its old claim token.
5. Check the attempt counter.
6. Either requeue it or move it to the failed queue.
7. Update its status and central registry record.

These operations are executed inside a Lua script.

### Recovery rules

```text
If attempts < MAX_ATTEMPTS:
    Clear claimToken
    Move job to jobs:waiting
    Set status to waiting

If attempts >= MAX_ATTEMPTS:
    Clear claimToken
    Move job to jobs:failed
    Set status to failed
```

Our configuration:

```js
const MAX_ATTEMPTS = 3;
```

The recovery operation checks the lease again inside Lua to avoid acting on stale results from the initial expired-job lookup.

### Why clear the claim token during recovery?

The old worker's ownership has expired.

If the job is requeued, the next worker receives a fresh claim token when it claims the job.

This prevents the previous worker from renewing or acknowledging the new processing attempt.

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

The job registry, status lifecycle, API endpoints, cursor pagination, and Redis data-access functions remain as previously documented.

The central `jobs:data` Hash preserves the latest job record after it leaves the active queue structures.

The creation-order index uses `jobs:created` and `jobs:sequence` to provide cursor-based pagination.

**Important:** Pagination is not a snapshot across multiple HTTP requests. If a job's status changes between page requests, later pages can reflect the updated status.

### Available API endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/api/jobs` | Create a job |
| `GET` | `/api/jobs` | Retrieve a page of registered jobs |
| `GET` | `/api/jobs?status=waiting` | Retrieve a page filtered by status |
| `GET` | `/api/jobs?limit=10&cursor=2` | Retrieve the next page after a creation-sequence cursor |
| `GET` | `/api/jobs/:id` | Retrieve one job by ID |

---

## 9. Worker Implementation

The worker lifecycle, configuration, heartbeat behavior, and processing flow remain as previously documented.

The worker:

- Connects to Redis.
- Starts the recovery loop.
- Claims available jobs.
- Processes jobs while renewing their leases.
- Acknowledges successfully completed jobs.
- Stops gracefully when requested.

The worker's lease renewal and acknowledgement operations now validate the job's current claim token.

This protects against stale worker operations after a job has been reclaimed.

---

## 10. Graceful Worker Shutdown

Graceful shutdown behavior remains unchanged.

The worker:

1. Stops starting new claim cycles.
2. Allows the currently claimed job to finish.
3. Keeps renewing its lease while processing continues.
4. Acknowledges the job after successful completion.
5. Stops the recovery loop.
6. Closes Redis connections gracefully.

The heartbeat remains active while the current job is processing, even after a shutdown signal arrives.

---

## 11. Testing and Results

The project uses Node.js' built-in test runner. Tests run sequentially because they share Redis keys and fixtures.

Run the suite using:

```powershell
npm test
```

### Automated test suite

Latest full-suite result: **29 tests passed, 0 failed**.

| Test file | Tests | Coverage |
|---|---:|---|
| `test-acknowledgement.js` | 2 | Successful acknowledgement, processing cleanup, and unknown job handling |
| `test-job-route.js` | 7 | Job creation, lookup, validation, status handling, and 404 behavior |
| `test-lease.js` | 4 | Lease renewal, expired lease rejection, requeue, and retry exhaustion |
| `test-pagination.js` | 5 | Cursor validation, page boundaries, no duplicates, status filtering, and terminal page |
| `test-producer.js` | 2 | Job creation and atomic duplicate-ID rejection |
| `test-worker-reliability.js` | 4 | Heartbeat renewal, crash recovery, graceful shutdown, and stale worker ownership protection |
| `test-worker.js` | 4 | Claiming, empty queue, attempt increments, and concurrent claim safety |
| `test-multi-worker.js` | 1 | Two-worker integration test processing 10 jobs |
| **Total** | **29** | **All passed** |

The multi-worker integration test uses an isolated Redis database (`DB 1`) so it does not consume jobs from the normal development queue.

Both workers processed jobs, all 10 jobs reached `completed`, and each job had one claim in that test run.

This is a test observation, not an exactly-once delivery guarantee. The system uses at-least-once processing semantics.

### Stale worker ownership regression test

A regression test verifies the following scenario:

1. Worker A claims a job.
2. Worker A's lease expires.
3. Recovery requeues the job.
4. Worker B claims the same job with a new claim token.
5. Worker A attempts to renew its old lease.
6. Worker A attempts to acknowledge the job.
7. Both stale operations are rejected.
8. Worker B retains ownership of the processing job.

This verifies that an outdated worker cannot acknowledge or renew a job reclaimed by another worker.

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

---

## 12. Load Testing and Performance Results

Load testing was performed with Artillery against the job-creation API.

### API enqueue results

| Arrival rate | Duration | Requests | HTTP 202 accepted | Failed requests | Mean latency | p95 | p99 | Max |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 20 req/sec | 30 sec | 600 | 600 | 0 | 3.1 ms | 6 ms | 10.9 ms | 31 ms |
| 50 req/sec | 30 sec | 1,500 | 1,500 | 0 | 5.5 ms | 13.1 ms | 50.9 ms | 196 ms |

All measured requests in these two runs received `202 Accepted`.

These numbers describe API job-enqueue performance, not worker execution throughput.

### Four-worker processing throughput

A separate live benchmark measured job consumption with four worker processes. Each worker used a simulated processing duration of 2 seconds.

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

The measured rate was approximately 99.5% of the simple theoretical rate.

This is a local benchmark using simulated work. Real throughput depends on job type, CPU, network, Redis latency, and external services.

### Post-benchmark Redis consistency check

After all four workers were stopped gracefully, the final Redis snapshot was:

| Redis key | Value | Interpretation |
|---|---:|---|
| `jobs:waiting` | 684 | Remaining backlog |
| `jobs:processing` (`ZCARD`) | 0 | No active lease entries |
| `jobs:processing:list` (`LLEN`) | 0 | No jobs left in the processing list |
| `jobs:failed` (`LLEN`) | 0 | No failed entries in the final test state |

The non-empty waiting queue is expected because the benchmark was stopped with work still queued.

---

## 13. Challenges Encountered and Solutions Implemented

This section records the main engineering problems encountered while building and testing the queue.

### Challenge 1: A job could be lost between claiming and registering it

**Problem:** Separate Redis commands could leave a job untracked if a worker stopped between removing it from the waiting queue and registering it in processing.

**Solution:** Replaced the multi-command claim flow with one Lua script using `LMOVE`.

**Outcome:** The claim transition and its Redis bookkeeping execute atomically.

### Challenge 2: The processing List and lease Sorted Set needed matching job records

**Problem:** Claiming a job changes its JSON, so the processing structures needed to receive the updated record.

**Solution:** Used `LSET`, `ZADD`, and `HSET` inside the claim Lua script.

**Outcome:** The processing structures and registry receive the same logical job state during a claim.

### Challenge 3: JSON string equality was unreliable during acknowledgement and recovery

**Problem:** JavaScript and Redis Lua can serialize the same object with different property ordering.

**Solution:** Locate jobs by unique ID rather than comparing serialized JSON strings.

**Outcome:** Queue operations rely on stable job identity.

### Challenge 4: A worker crash could leave a job stuck in processing

**Problem:** A crashed worker stops renewing its lease.

**Solution:** Added an expiry recovery loop and Lua-based recovery operations.

**Outcome:** Expired jobs can be recovered without requiring the original worker to restart.

### Challenge 5: Retried jobs needed a finite stopping point

**Problem:** Jobs must not circulate through the waiting queue indefinitely.

**Solution:** Enforced `MAX_ATTEMPTS = 3`.

**Outcome:** The system has a bounded retry policy and a failed-job destination.

### Challenge 6: Long-running jobs could be recovered while still active

**Problem:** A job may run longer than its initial visibility timeout.

**Solution:** Added a heartbeat that renews the lease every 5 seconds.

**Outcome:** Active workers can retain ownership of long-running jobs.

### Challenge 7: Graceful shutdown and crash recovery needed different behavior

**Problem:** Graceful shutdown should allow an active job to finish, whereas a crashed worker must eventually lose ownership.

**Solution:** Implemented graceful shutdown separately from lease-based crash recovery.

**Outcome:** Both scenarios are tested independently.

### Challenge 8: Job information disappeared from active queues after completion

**Problem:** Completed jobs are removed from processing structures.

**Solution:** Introduced `jobs:data`, a Redis Hash keyed by job ID.

**Outcome:** The API can retrieve jobs independently of their queue location.

### Challenge 9: HSCAN did not provide reliable API pagination

**Problem:** `HSCAN` does not provide stable page boundaries.

**Solution:** Added `jobs:created`, a Sorted Set indexed by a monotonically increasing creation sequence.

**Outcome:** Listing now uses score-based pagination.

### Challenge 10: Status filtering could leave a page short

**Problem:** A page may encounter job IDs that do not match the requested status.

**Solution:** Continue scanning the creation index until the page contains the requested number of matching jobs or the index is exhausted.

**Outcome:** The tested dataset returned all 8 completed jobs across four pages of 2 without duplicate IDs.

### Challenge 11: A stale worker could acknowledge a job reclaimed by another worker

**Problem:** A worker could become unresponsive long enough for its lease to expire. Recovery would requeue the job, allowing another worker to claim it. However, if the original worker resumed, it could still attempt to renew or acknowledge the job using its outdated job object.

Matching only by job ID could allow the stale worker to interfere with the newer processing attempt.

**Solution:** Introduced a unique `claimToken` using Node.js `randomUUID()` for every successful claim.

The token is stored in the job record and checked by the Lua scripts responsible for lease renewal and acknowledgement.

Recovery clears the old token before requeueing or failing a job, and successful acknowledgement also clears it.

**Outcome:** Only the worker holding the current claim token can renew the lease or acknowledge the job. Stale worker operations are rejected, protecting the ownership of a reclaimed job.

### Practical limitations identified during testing

- Pagination is not a snapshot across multiple HTTP requests.
- Status filtering scans the shared creation index.
- The current implementation fetches job records individually from `jobs:data`.
- A page that exactly fills its limit at the end of the index may require one additional request to receive the terminal empty page.
- Claim tokens protect queue ownership, but external job handlers still need idempotency to handle repeated execution safely.

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
| `ZRANGEBYSCORE` | Retrieve members within a score range |
| `ZCARD` | Get the number of members in a Sorted Set |
| `INCR` | Atomically increment the job creation sequence counter |
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

A Redis Hash makes job records retrievable independently of their queue location.

**10. Graceful Shutdown**

A worker finishes and acknowledges its active job before closing its connections.

**11. Worker Ownership and Claim Tokens**

A unique claim token identifies the current processing attempt. It prevents stale workers from renewing or acknowledging jobs after another worker has reclaimed them.

The token acts as an ownership check within the queue. It does not guarantee exactly-once execution of external side effects.

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

Implemented and tested a distributed job processing system using Node.js and Redis Lists, Sorted Sets, Hashes, and Lua scripts.

The system supports:

- Concurrent job processing.
- Atomic job creation and claiming.
- Unique claim tokens for worker ownership.
- Lease-based job ownership.
- Heartbeat-based lease renewal.
- Atomic acknowledgement.
- Stale worker protection.
- Automatic recovery of expired jobs.
- Retry limits and failed-job handling.
- Graceful worker shutdown.
- Centralized job status tracking.
- Cursor-based job pagination using a Sorted Set creation index.
- Optional status filtering during pagination.
- REST endpoints to create, list, filter, and retrieve jobs.

**Verified results:** 29/29 automated tests passed; the two-worker integration test completed all 10 jobs; Artillery received 600/600 and 1,500/1,500 successful enqueue responses at 20 and 50 requests/sec; and four workers sustained 1.99 simulated jobs/sec in the measured local benchmark.

The stale-worker regression test also verified that an outdated worker cannot renew or acknowledge a job after another worker has reclaimed it.

The final post-shutdown check found no jobs left in either processing structure.