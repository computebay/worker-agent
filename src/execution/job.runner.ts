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
  output: string;
}

/**
 * Full lifecycle of one job: create → start → wait → cleanup.
 * Enforces timeout via docker kill.
 * Always removes container after completion or failure.
 */
/** Node -e snippet that runs code from CODE_B64 env (avoids shell escaping). */
const NODE_EVAL_CODE =
  "eval(require('buffer').Buffer.from(process.env.CODE_B64,'base64').toString('utf8'))";

function buildContainerOptions(job: ScheduledJob): {
  image: string;
  command: string[];
  env: Record<string, string>;
} {
  if (job.inlineCode) {
    const codeB64 = Buffer.from(job.inlineCode, "utf8").toString("base64");
    return {
      image: job.image,
      command: ["node", "-e", NODE_EVAL_CODE],
      env: { ...job.env, CODE_B64: codeB64 },
    };
  }
  return {
    image: job.image,
    command: job.command,
    env: job.env ?? {},
  };
}

export async function runJob(job: ScheduledJob, timeoutMs: number): Promise<JobRunResult> {
  const containerId = `computebay-${job.jobId}`;
  const opts = buildContainerOptions(job);
  logger.info(
    { jobId: job.jobId, containerId, image: opts.image, hasInlineCode: Boolean(job.inlineCode) },
    "Starting job"
  );

  try {
    await createAndStart({
      containerId,
      image: opts.image,
      cpu: job.cpu,
      memoryMb: job.memoryMB,
      command: opts.command,
      env: opts.env,
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

  let output = "";
  try {
    output = await getContainerLogs(containerId);
    logger.info({ jobId: job.jobId, exitCode, timedOut, output }, "Job output");
  } catch (logErr) {
    logger.warn({ jobId: job.jobId, err: logErr }, "Failed to fetch container logs");
  }

  await removeContainer(containerId).catch((err) => {
    logger.warn({ err, containerId }, "Failed to remove container");
  });

  return { exitCode, timedOut, output };
}
