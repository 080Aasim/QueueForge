# Distributed Job System: Atomic Job Claiming, Lease Renewal & Crash Recovery

## 1. Objective

Implement a reliable distributed job queue where multiple workers can process jobs concurrently without:

- Two workers claiming the same job.

- Losing jobs when a worker crashes.

- Processing jobs indefinitely without retry limits.

- Leaving completed jobs in processing queues.

- Losing track of jobs whose processing lease expires.

**Technology:** Node.js, Redis 6.2.1, JavaScript, Docker.

---

## 2. Redis Queue Architecture

We use four Redis data structures.
| Redis Key | Data Structure | Purpose |
|---|---|---|
| `jobs:waiting` | List | Stores jobs waiting to be processed |
| `jobs:processing:list` | List | Stores jobs currently claimed by workers |
| `jobs:processing` | Sorted Set | Stores jobs with their lease expiration timestamps |
| `jobs:failed` | List | Stores jobs that exceeded maximum retries |

### Why use both a List and a Sorted Set for processing?

****Processing List:****

- Keeps the actual claimed job data.

- Allows us to locate jobs by their ID.

- Helps maintain a record of jobs currently being processed.

****Processing Sorted Set:****

- Stores the job as its member.

- Stores its lease expiration timestamp as its score.

- Allows efficient identification of expired jobs using `ZRANGEBYSCORE`.

The two structures are maintained together.

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

local jobData = redis.call(

  'LMOVE',

  KEYS[1],

  KEYS[2],

  'LEFT',

  'RIGHT'

)

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

```

### How it works

1. `LMOVE` moves a job from the waiting list to the processing list.

2. Redis decodes the job JSON.

3. The attempt counter is incremented.

4. A visibility deadline is calculated.

5. `LSET` updates the job inside the processing list.

6. `ZADD` registers the job and its lease deadline.

All operations happen inside one Lua script.

****Important concept: Redis Lua atomicity****

Redis executes a Lua script atomically. Other Redis commands cannot interleave with the script's execution.

This closes the crash window between moving a job and registering its lease.

### JavaScript claimJob()

```js

export async function claimJob() {

  const visibilityDeadline = Date.now() + VISIBILITY_TIMEOUT;

  const result = await redis.eval(CLAIM_JOB_SCRIPT, {

    keys: [

      WAITING_QUEUE,

      PROCESSING_LIST,

      PROCESSING_QUEUE,

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

We use non-blocking `LMOVE`, so `claimJob()` returns `null` when the queue is empty.

The worker waits one second and tries again.

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

After successful processing, the job must be removed from both processing structures.

### Implementation

We use the job ID instead of relying on matching serialized JSON.

```js

export async function acknowledgeJob(job) {

  const script = `

    local processingJobs = redis.call(

      'LRANGE',

      KEYS[1],

      0,

      -1

    )

    for \_, jobData in ipairs(processingJobs) do

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

```

### Why use job ID?

Redis Lua's `cjson.encode()` and JavaScript's `JSON.stringify()` are not guaranteed to produce identical serialized JSON strings.

Searching by a unique job ID avoids depending on exact JSON serialization.

Both removals happen atomically inside Lua.

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

These operations are executed inside a Lua script.

### Recovery rules

```text

If attempts < MAX_ATTEMPTS:

    Move job to jobs:waiting

If attempts >= MAX_ATTEMPTS:

    Move job to jobs:failed

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

## 8. Worker Implementation

### Main worker lifecycle

```text

Connect to Redis
      |

Start recovery loop
      |

Claim job
      |

Is a job available?

  /       \\

 No        Yes
 |          |

Wait 1 sec  Process job
 |          |

Retry       Heartbeat renews lease
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

**---

## 9. Graceful Worker Shutdown

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

Expected final log sequence:

```text
Shutdown signal received (SIGINT)
Recovery loop stopped
Lease renewed for job <job-id>
Job <job-id> completed
Job <job-id> acknowledged
Redis connections closed
Worker shutdown complete
```

**Result: PASSED.** The worker completed and acknowledged its active job, stopped the recovery loop, and closed its Redis connections before exiting.

---

## 10. Testing and Results**

We performed several tests using Postman, Redis CLI, Docker and multiple worker processes.

### Test 1: Multiple Workers

Submitted three jobs through the API and started two workers.

Results:

- Both workers claimed jobs.

- Workers processed different jobs concurrently.

- The third job was picked up when a worker became available.

- All three jobs completed and were acknowledged.

### Test 2: Heartbeat Renewal

Configured:

- Visibility timeout: 30 seconds.

- Heartbeat interval: 5 seconds.

- Processing duration: 30 seconds.

Observed repeated successful lease renewals.

Result: PASSED.

### Test 3: Job Acknowledgement

After successful processing, the worker acknowledged the job.

Both processing structures were cleaned up.

Result: PASSED.

### Test 4: Worker Crash Recovery

Procedure:

1. Added a job.

2. Started Worker 1.

3. Let it claim the job.

4. Terminated Worker 1 using Ctrl + C.

5. Verified that the job remained in both processing structures.

6. Waited for its visibility timeout to expire.

7. Started Worker 2.

Observed:

```text

Job \<id> recovered and re-queued. Attempt: 1

Claimed job: \<same-id>

Processing job \<same-id>

```

The job was reclaimed with:

```js

attempts: 2

```

The second worker renewed its lease, completed the job and acknowledged it.

Result: PASSED.

### Test 5: Maximum Retry Handling

Configured:

```js

const MAX_ATTEMPTS = 3;

```

Repeatedly crashed workers while they were processing a fresh job.

After the third attempt expired, the recovery loop logged:

```text

Job \<id> exceeded max attempts and moved to failed queue

```

Result: PASSED.

### Final Redis verification

Executed:

```redis

LLEN jobs:waiting

LLEN jobs:processing:list

ZCARD jobs:processing

LLEN jobs:failed

```

Observed:
| Redis structure | Count |
|---|---:|
| Waiting | 0 |
| Processing List | 0 |
| Processing Sorted Set | 0 |
| Failed | 1 |

This confirmed that the exhausted job was moved to the failed queue and removed from processing.

---

## 11. Important Redis Commands Learned
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
| `EVAL` | Execute a Lua script in Redis |

---

## 12. Key Engineering Concepts Learned

****1. Atomicity****

Multiple Redis operations can be executed as one atomic Lua script, preventing other commands from interleaving.

****2. Visibility Timeout****

A lease gives a worker temporary ownership of a job.

****3. Heartbeat****

A periodic signal that tells the system the worker is still alive and processing.

****4. At-least-once Processing****

A job may be processed again if a worker crashes before acknowledgement. Therefore, job handlers should ideally be idempotent.

****5. Crash Recovery****

Expired leases allow the system to detect abandoned jobs and make them available again.

****6. Retry Limit****

A maximum attempt count prevents jobs from retrying forever.

****7. Dead-Letter Queue****

The failed queue stores jobs that have exhausted their allowed retries.

****8. Stable Job Identity****

Using a unique job ID is more reliable than depending on exact JSON serialization.

---

## 13. Future Improvements

\
- Better empty-queue polling or blocking notification.

- Monitoring and metrics for workers and queues.

- Job priorities.

- Job scheduling and delayed execution.

- Better consistency checks between processing structures.

- Idempotent job execution.

- Redis Streams and Consumer Groups as a possible future architecture.

---

## Final Outcome

Implemented and tested a distributed job processing system using Node.js and Redis Lists + Sorted Sets.

The system supports:

- Concurrent job processing.

- Atomic job claiming.

- Lease-based job ownership.

- Heartbeat-based lease renewal.

- Atomic acknowledgement.

- Automatic recovery of expired jobs.

- Retry limits.

- Failed-job handling.

****Core job lifecycle and crash-recovery tests passed.****
