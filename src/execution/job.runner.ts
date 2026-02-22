import type { ScheduledJob } from "../types/job.types.js";
import {
  createAndStart,
  getContainerLogs,
  killContainer,
  removeContainer,
  waitContainer,
} from "./container.runtime.js";
import { logger } from "../utils/logger.js";

export interface JobRunResult {
  exitCode: number;
  timedOut: boolean;
}

/**
 * Full lifecycle of one job: create → start → wait → cleanup.
 * Enforces timeout via docker kill.
 * Always removes container after completion or failure.
 */
export async function runJob(job: ScheduledJob, timeoutMs: number): Promise<JobRunResult> {
  const containerId = `computebay-${job.jobId}`;
  logger.info({ jobId: job.jobId, containerId, image: job.image }, "Starting job");

  try {
    await createAndStart({
      containerId,
      image: job.image,
      cpu: job.cpu,
      memoryMb: job.memoryMB,
      command: job.command,
      env: job.env,
    });
  } catch (err) {
    logger.error({ err, jobId: job.jobId }, "Failed to create container");
    await removeContainer(containerId).catch(() => {});
    throw err;
  }

  const jobTimeoutMs = job.timeoutMs ?? timeoutMs;
  let timedOut = false;
  logger.info({ jobId: job.jobId, timeoutMs: jobTimeoutMs }, "Waiting for container");

  const waitPromise = waitContainer(containerId);
  const timeoutPromise = new Promise<never>((_, reject) => {
    setTimeout(() => {
      timedOut = true;
      killContainer(containerId)
        .then(() => reject(new Error("TIMEOUT")))
        .catch(reject);
    }, jobTimeoutMs);
  });

  let exitCode: number;
  try {
    const result = await Promise.race([waitPromise, timeoutPromise]);
    exitCode = result.exitCode;
  } catch (err) {
    if (timedOut || (err instanceof Error && err.message === "TIMEOUT")) {
      timedOut = true;
      exitCode = 137;
      logger.warn({ jobId: job.jobId, containerId }, "Job timed out, container killed");
    } else {
      logger.error({ err, jobId: job.jobId }, "Container wait/kill error");
      throw err;
    }
  }

  try {
    const output = await getContainerLogs(containerId);
    logger.info({ jobId: job.jobId, exitCode, timedOut, output }, "Job output");
  } catch (logErr) {
    logger.warn({ jobId: job.jobId, err: logErr }, "Failed to fetch container logs");
  }

  await removeContainer(containerId).catch((err) => {
    logger.warn({ err, containerId }, "Failed to remove container");
  });

  return { exitCode, timedOut };
}
