import { mkdtemp, rm, appendFile, writeFile, mkdir } from "node:fs/promises";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import path from "node:path";
import os from "node:os";
const execFileAsync = promisify(execFile);
import type { ScheduledJob, ScheduledBatchJob, BatchImageJob } from "../types/job.types.js";
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
import { collectAndUploadArtifacts } from "../libs/artifact.upload.js";
import { downloadInput } from "../libs/artifact.download.js";
import type { S3Config } from "../libs/s3.js";
import { logger } from "../utils/logger.js";

export interface JobRunResult {
  exitCode: number;
  timedOut: boolean;
  output: string;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
  /** Output artifacts collected from /workspace/outputs */
  artifacts?: import("../types/job.types.js").JobArtifact[];
}

/** Required for log upload to MinIO. */
export interface LogUploadConfig {
  s3Config: S3Config;
}

const CONTAINER_WORKSPACE = "/workspace";

export interface RunJobOptions {
  timeoutMs: number;
  logUploadConfig?: LogUploadConfig;
  /** Called with log chunks for streaming to RabbitMQ. */
  onLogChunk?: (chunk: string) => void;
  /** When aborted, kills the container. */
  abortSignal?: AbortSignal;
  /** Root directory for per-job scratch dirs (default: os.tmpdir()). */
  jobWorkDir?: string;
}

export async function runJob(
  job: ScheduledJob,
  options: RunJobOptions
): Promise<JobRunResult> {
  if (job.type === "batch-image") {
    return runBatchImageJob(job, options);
  }
  if (job.type !== "batch") {
    throw new Error(`runJob called with non-batch job type: ${job.type}`);
  }
  return runGitBatchJob(job, options);
}

/** Git-based batch job — existing path, completely untouched. */
async function runGitBatchJob(
  job: ScheduledBatchJob,
  options: RunJobOptions
): Promise<JobRunResult> {
  const {
    timeoutMs,
    logUploadConfig,
    onLogChunk,
    abortSignal,
  } = options;
  const batchJob = job;

  // Resolve current process UID/GID so the container runs as the same user
  // that owns the git-cloned files on the host. This prevents permission errors
  // when the container tries to read bind-mounted workspace files.
  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;

  if (!batchJob.repoUrl) throw new Error(`[${batchJob.jobId}] repoUrl is required for batch jobs`);
  if (!batchJob.startCommand) throw new Error(`[${batchJob.jobId}] startCommand is required for batch jobs`);

  const repoUrl = batchJob.repoUrl;
  const startCommand = batchJob.startCommand;

  const containerId = `computebay-${batchJob.jobId}`;

  // Create temp working directory
  const baseWorkDir = options.jobWorkDir ?? os.tmpdir();
  const workDirPath = await mkdtemp(path.join(baseWorkDir, `computebay-${batchJob.jobId}-`));
  const logFilePath = path.join(workDirPath, "full.log");

  await writeFile(logFilePath, "");

  try {
    // Git clone
    logger.info(
      { jobId: batchJob.jobId, repoUrl, branch: batchJob.branch },
      "Cloning repository"
    );
    const repoPath = await gitClone(repoUrl, batchJob.branch ?? "main", workDirPath);

    // Pre-create outputs directory when job declares artifacts
    if (batchJob.hasArtifacts) {
      await mkdir(path.join(repoPath, "outputs"), { recursive: true });
    }

    const mounts: ContainerMount[] = [
      { hostPath: repoPath, containerPath: CONTAINER_WORKSPACE },
    ];
    const command = ["sh", "-c", startCommand];

    logger.info(
      {
        jobId: batchJob.jobId,
        containerId,
        image: batchJob.image,
        startCommand,
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
      ...(batchJob.env ?? {}),
    };

    try {
      await createAndStart({
        containerId,
        image: batchJob.image,
        cpu: batchJob.cpu,
        memoryMb: batchJob.memoryMB,
        command,
        env,
        workDir: CONTAINER_WORKSPACE,
        mounts,
        networkEnabled: batchJob.networkEnabled,
        runAsUser,
      });
    } catch (err) {
      logger.error({ err, jobId: batchJob.jobId }, "Failed to create container");
      await removeContainer(containerId).catch(() => {});
      throw err;
    }

    // Stream logs: append to file and emit chunks immediately
    const stopLogStream = streamContainerLogs(containerId, {
      onChunk: async (chunk) => {
        await appendFile(logFilePath, chunk);
        if (onLogChunk) {
          onLogChunk(chunk);
        }
      },
      onEnd: () => {},
      onError: (err) => logger.warn({ err, jobId: batchJob.jobId }, "Log stream error"),
    });

    const jobTimeoutMs = batchJob.timeoutMs ?? timeoutMs;
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
        logger.warn({ jobId: batchJob.jobId, containerId }, "Job timed out, container killed");
      } else if (err instanceof Error && err.message === "CANCELLED") {
        exitCode = 137;
        logger.info({ jobId: batchJob.jobId, containerId }, "Job cancelled, container killed");
      } else {
        logger.error({ err, jobId: batchJob.jobId }, "Container wait/kill error");
        throw err;
      }
    } finally {
      stopLogStream();
    }

    // Fetch full logs from Docker (authoritative; stream may miss output if container exits quickly)
    let output = "";
    try {
      output = await getContainerLogs(containerId);
      await writeFile(logFilePath, output || "(no output)");
    } catch (logErr) {
      logger.warn({ jobId: batchJob.jobId, err: logErr }, "Failed to fetch container logs");
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
          batchJob.jobId,
          logFilePath
        );
        logger.info({ jobId: batchJob.jobId, logObjectKey }, "Uploaded log to MinIO");
      } catch (uploadErr) {
        logger.warn(
          { err: uploadErr, jobId: batchJob.jobId },
          "Failed to upload log to MinIO"
        );
      }
    }

    // Collect and upload output artifacts from /workspace/outputs
    let artifacts: import("../types/job.types.js").JobArtifact[] | undefined;
    if (logUploadConfig && batchJob.hasArtifacts) {
      const outputsDir = path.join(repoPath, "outputs");
      try {
        artifacts = await collectAndUploadArtifacts(
          logUploadConfig.s3Config,
          batchJob.jobId,
          outputsDir
        );
        if (artifacts.length > 0) {
          logger.info(
            { jobId: batchJob.jobId, artifactCount: artifacts.length },
            "Uploaded output artifacts to MinIO"
          );
        }
      } catch (artifactErr) {
        logger.warn(
          { err: artifactErr, jobId: batchJob.jobId },
          "Failed to collect/upload artifacts"
        );
      }
    }

    return { exitCode, timedOut, output, logObjectKey, artifacts };
  } finally {
    try {
      await rm(workDirPath, { recursive: true, force: true });
      logger.info({ jobId: batchJob.jobId, workDirPath }, "Cleaned up job working directory");
    } catch (cleanupErr) {
      logger.warn({ err: cleanupErr, jobId: batchJob.jobId, workDirPath }, "Failed to cleanup work dir");
    }
  }
}

/** Batch-image job — parallel code path, no git clone. */
async function runBatchImageJob(
  job: BatchImageJob,
  options: RunJobOptions
): Promise<JobRunResult> {
  const { timeoutMs, logUploadConfig, onLogChunk, abortSignal } = options;

  if (!logUploadConfig) {
    throw new Error("S3/MinIO not configured — cannot run batch-image jobs");
  }

  const containerId = `computebay-img-${job.jobId}`;
  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;

  const baseWorkDir = options.jobWorkDir ?? os.tmpdir();
  const workDirPath = await mkdtemp(
    path.join(baseWorkDir, `computebay-img-${job.jobId}-`)
  );
  const logFilePath = path.join(workDirPath, "full.log");
  await writeFile(logFilePath, "");

  try {
    // 1. Input: download S3 object into local dir, mount read-only
    const inputDir = path.join(workDirPath, "input");
    await mkdir(inputDir, { recursive: true });
    const inputFileName = job.input.name ?? path.basename(job.input.mountPath);
    const inputLocalPath = path.join(inputDir, inputFileName);

    await downloadInput(logUploadConfig.s3Config, job.input.url!, inputLocalPath);

    // 2. Output: empty dir for container to write into, mounted read-write
    const outputDir = path.join(workDirPath, "output");
    await mkdir(outputDir, { recursive: true });

    // 3. Ensure directories are writable by the container user (uid:gid)
    await execFileAsync("chmod", ["777", inputDir, outputDir]);

    // 3. Mounts: input read-only, output read-write
    const mounts: ContainerMount[] = [
      { hostPath: inputDir, containerPath: job.input.mountPath },
      { hostPath: outputDir, containerPath: job.output.uploadFrom },
    ];

    logger.info(
      {
        jobId: job.jobId,
        containerId,
        image: job.image,
        command: job.command,
        inputMount: job.input.mountPath,
        outputMount: job.output.uploadFrom,
      },
      "Starting batch-image container"
    );

    // 4. Create and start container — command passed as exec array, no sh -c
    try {
      await createAndStart({
        containerId,
        image: job.image,
        cpu: job.cpu,
        memoryMb: job.memoryMB,
        command: job.command,
        mounts,
        networkEnabled: job.networkEnabled,
        runAsUser,
      });
    } catch (err) {
      logger.error({ err, jobId: job.jobId }, "Failed to create batch-image container");
      await removeContainer(containerId).catch(() => {});
      throw err;
    }

    // Stream logs
    const stopLogStream = streamContainerLogs(containerId, {
      onChunk: async (chunk) => {
        await appendFile(logFilePath, chunk);
        onLogChunk?.(chunk);
      },
      onEnd: () => {},
      onError: (err) => logger.warn({ err, jobId: job.jobId }, "Log stream error"),
    });

    // Wait with timeout + abort
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
          if (abortSignal.aborted) onAbort();
          else abortSignal.addEventListener("abort", onAbort, { once: true });
        })
      : new Promise<never>(() => {});

    let exitCode: number;
    try {
      const result = await Promise.race([waitPromise, timeoutPromise, abortPromise]);
      exitCode = result.exitCode;
    } catch (err) {
      if (timedOut || (err instanceof Error && err.message === "TIMEOUT")) {
        timedOut = true;
        exitCode = 137;
        logger.warn({ jobId: job.jobId, containerId }, "Batch-image job timed out");
      } else if (err instanceof Error && err.message === "CANCELLED") {
        exitCode = 137;
        logger.info({ jobId: job.jobId, containerId }, "Batch-image job cancelled");
      } else {
        logger.error({ err, jobId: job.jobId }, "Container wait/kill error");
        throw err;
      }
    } finally {
      stopLogStream();
    }

    // Fetch full logs
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

    // Upload log
    let logObjectKey: string | undefined;
    try {
      logObjectKey = await uploadLogFile(
        logUploadConfig.s3Config, job.jobId, logFilePath
      );
      logger.info({ jobId: job.jobId, logObjectKey }, "Uploaded log to MinIO");
    } catch (uploadErr) {
      logger.warn({ err: uploadErr, jobId: job.jobId }, "Failed to upload log to MinIO");
    }

    // 5. Upload output artifacts via existing collectAndUploadArtifacts
    let artifacts: import("../types/job.types.js").JobArtifact[] | undefined;
    try {
      artifacts = await collectAndUploadArtifacts(
        logUploadConfig.s3Config, job.jobId, outputDir
      );
      if (artifacts.length > 0) {
        logger.info(
          { jobId: job.jobId, artifactCount: artifacts.length },
          "Uploaded batch-image output artifacts"
        );
      }
    } catch (artifactErr) {
      logger.warn(
        { err: artifactErr, jobId: job.jobId },
        "Failed to upload batch-image output artifacts"
      );
    }

    return { exitCode, timedOut, output, logObjectKey, artifacts };
  } finally {
    // 6. Cleanup scratch dir
    try {
      await rm(workDirPath, { recursive: true, force: true });
      logger.info({ jobId: job.jobId, workDirPath }, "Cleaned up batch-image working directory");
    } catch (cleanupErr) {
      logger.warn({ err: cleanupErr, jobId: job.jobId, workDirPath }, "Failed to cleanup work dir");
    }
  }
}