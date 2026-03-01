import { mkdtemp, rm, appendFile, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { ScheduledJob } from "../types/job.types.js";
import {
  createAndStart,
  getContainerLogs,
  killContainer,
  removeContainer,
  waitContainer,
  streamContainerLogs,
  type ContainerMount,
} from "./container.runtime.js";
import { gitClone } from "../libs/git.clone.js";
import { uploadLogFile } from "../libs/log.upload.js";
import type { S3Config } from "../libs/s3.js";
import { logger } from "../utils/logger.js";

export interface JobRunResult {
  exitCode: number;
  timedOut: boolean;
  output: string;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
}

/** Required for log upload to MinIO. */
export interface LogUploadConfig {
  s3Config: S3Config;
}

const CONTAINER_WORKSPACE = "/workspace";

/** Chunk size for streaming logs to RabbitMQ (bytes). */
const LOG_CHUNK_SIZE = 4096;

export interface RunJobOptions {
  timeoutMs: number;
  logUploadConfig?: LogUploadConfig;
  /** Called with log chunks for streaming to RabbitMQ. */
  onLogChunk?: (chunk: string) => void;
  /** When aborted, kills the container. */
  abortSignal?: AbortSignal;
}

export async function runJob(
  job: ScheduledJob,
  options: RunJobOptions
): Promise<JobRunResult> {
  const {
    timeoutMs,
    logUploadConfig,
    onLogChunk,
    abortSignal,
  } = options;

  const containerId = `computebay-${job.jobId}`;

  // Resolve current process UID/GID so the container runs as the same user
  // that owns the git-cloned files on the host. This prevents permission errors
  // when the container tries to read bind-mounted workspace files.
  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;

  // Create temp working directory
  const workDirPath = await mkdtemp(path.join(os.tmpdir(), `computebay-${job.jobId}-`));
  const logFilePath = path.join(workDirPath, "full.log");

  await writeFile(logFilePath, "");

  try {
    // Git clone
    logger.info(
      { jobId: job.jobId, repoUrl: job.repoUrl, branch: job.branch },
      "Cloning repository"
    );
    const repoPath = await gitClone(job.repoUrl, job.branch, workDirPath);

    const mounts: ContainerMount[] = [
      { hostPath: repoPath, containerPath: CONTAINER_WORKSPACE },
    ];
    const command = ["sh", "-c", job.startCommand];

    logger.info(
      {
        jobId: job.jobId,
        containerId,
        image: job.image,
        startCommand: job.startCommand,
        runAsUser,
      },
      "Starting job container"
    );

    const env = {
      HOME: CONTAINER_WORKSPACE,
      // Redirect temp dirs so pip/npm find a writable location (the /tmp tmpfs mount)
      TMPDIR: "/tmp",
      TEMP: "/tmp",
      TMP: "/tmp",
      // Disable pip's cache entirely — avoids cache-dir permission warnings
      // and speeds up cold starts slightly
      PIP_NO_CACHE_DIR: "1",
      // Redirect npm cache into the /tmp tmpfs mount
      npm_config_cache: "/tmp/.npm",
      ...(job.env ?? {}),
    };

    try {
      await createAndStart({
        containerId,
        image: job.image,
        cpu: job.cpu,
        memoryMb: job.memoryMB,
        command,
        env,
        workDir: CONTAINER_WORKSPACE,
        mounts,
        networkEnabled: job.networkEnabled,
        runAsUser,
      });
    } catch (err) {
      logger.error({ err, jobId: job.jobId }, "Failed to create container");
      await removeContainer(containerId).catch(() => {});
      throw err;
    }

    // Stream logs: append to file and emit chunks
    let logBuffer = "";
    const flushChunk = (): void => {
      if (logBuffer.length >= LOG_CHUNK_SIZE && onLogChunk) {
        onLogChunk(logBuffer);
        logBuffer = "";
      }
    };

    const stopLogStream = streamContainerLogs(containerId, {
      onChunk: async (chunk) => {
        await appendFile(logFilePath, chunk);
        logBuffer += chunk;
        flushChunk();
      },
      onEnd: () => flushChunk(),
      onError: (err) => logger.warn({ err, jobId: job.jobId }, "Log stream error"),
    });

    const jobTimeoutMs = job.timeoutMs ?? timeoutMs;
    let timedOut = false;

    const waitPromise = waitContainer(containerId);
    const timeoutPromise = new Promise<never>((_, reject) => {
      setTimeout(() => {
        timedOut = true;
        killContainer(containerId)
          .then(() => reject(new Error("TIMEOUT")))
          .catch(reject);
      }, jobTimeoutMs);
    });

    const abortPromise = abortSignal
      ? new Promise<never>((_, reject) => {
          const onAbort = (): void => {
            killContainer(containerId)
              .then(() => reject(new Error("CANCELLED")))
              .catch(reject);
          };
          if (abortSignal.aborted) {
            onAbort();
          } else {
            abortSignal.addEventListener("abort", onAbort, { once: true });
          }
        })
      : new Promise<never>(() => {}); // Never resolves

    let exitCode: number;
    try {
      const result = await Promise.race([
        waitPromise,
        timeoutPromise,
        abortPromise,
      ]);
      exitCode = result.exitCode;
    } catch (err) {
      if (timedOut || (err instanceof Error && err.message === "TIMEOUT")) {
        timedOut = true;
        exitCode = 137;
        logger.warn({ jobId: job.jobId, containerId }, "Job timed out, container killed");
      } else if (err instanceof Error && err.message === "CANCELLED") {
        exitCode = 137;
        logger.info({ jobId: job.jobId, containerId }, "Job cancelled, container killed");
      } else {
        logger.error({ err, jobId: job.jobId }, "Container wait/kill error");
        throw err;
      }
    } finally {
      stopLogStream();
    }

    // Flush remaining log buffer
    if (logBuffer && onLogChunk) {
      onLogChunk(logBuffer);
    }

    // Fetch full logs from Docker (authoritative; stream may miss output if container exits quickly)
    let output = "";
    try {
      output = await getContainerLogs(containerId);
      await writeFile(logFilePath, output || "(no output)");
    } catch (logErr) {
      logger.warn({ jobId: job.jobId, err: logErr }, "Failed to fetch container logs");
    }

    await removeContainer(containerId).catch((err) => {
      logger.warn({ err, containerId }, "Failed to remove container");
    });

    // Upload full log to MinIO
    let logObjectKey: string | undefined;
    if (logUploadConfig) {
      try {
        logObjectKey = await uploadLogFile(
          logUploadConfig.s3Config,
          job.jobId,
          logFilePath
        );
        logger.info({ jobId: job.jobId, logObjectKey }, "Uploaded log to MinIO");
      } catch (uploadErr) {
        logger.warn(
          { err: uploadErr, jobId: job.jobId },
          "Failed to upload log to MinIO"
        );
      }
    }

    return { exitCode, timedOut, output, logObjectKey };
  } finally {
    try {
      await rm(workDirPath, { recursive: true, force: true });
      logger.info({ jobId: job.jobId, workDirPath }, "Cleaned up job working directory");
    } catch (cleanupErr) {
      logger.warn({ err: cleanupErr, jobId: job.jobId, workDirPath }, "Failed to cleanup work dir");
    }
  }
}