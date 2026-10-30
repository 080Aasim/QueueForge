import { v4 as uuid } from "uuid";
import { addJob } from "./queue.js";

export async function createJob(type, payload) {
  const job = {
    id: uuid(),
    type,
    payload,
    status: "queued",
    attempts: 0,
    createdAt: new Date().toISOString(),
  };

  await addJob(job);
  return job;
}
