import { rm } from "node:fs/promises";
import path from "node:path";
import type { ScheduledJob } from "../types/job.types.js";
import {
  createAndStart,
  getContainerLogs,
  killContainer,
  removeContainer,
  waitContainer,
  type ContainerMount,
} from "./container.runtime.js";
import {
  downloadArtifact,
  extractZipToDir,
  uploadOutputArtifact,
} from "../libs/artifact.download.js";
import type { S3Config } from "../libs/s3.js";
import { logger } from "../utils/logger.js";

export interface JobRunResult {
  exitCode: number;
  timedOut: boolean;
  output: string;
  /** Set when job produced /workspace/output.zip and it was uploaded to MinIO (outputs/<jobId>/result.zip). */
  outputObjectKey?: string;
}

/** Required when job has inputArtifactObjectKey: MinIO config and root dir for per-job work dirs. */
export interface ArtifactConfig {
  s3Config: S3Config;
  jobWorkDir: string;
}

/** Node -e snippet that runs code from CODE_B64 env (avoids shell escaping). */
const NODE_EVAL_CODE =
  "eval(require('buffer').Buffer.from(process.env.CODE_B64,'base64').toString('utf8'))";

const CONTAINER_WORKSPACE = "/workspace";

function buildContainerOptions(
  job: ScheduledJob,
  workDirPath?: string
): {
  image: string;
  command: string[];
  env: Record<string, string>;
  workDir?: string;
  mounts?: ContainerMount[];
} {
  if (job.inlineCode) {
    const codeB64 = Buffer.from(job.inlineCode, "utf8").toString("base64");
    return {
      image: job.image,
      command: ["node", "-e", NODE_EVAL_CODE],
      env: { ...job.env, CODE_B64: codeB64 },
    };
  }
  const base = {
    image: job.image,
    command: job.command,
    env: job.env ?? {},
  };
  if (workDirPath) {
    return {
      ...base,
      workDir: CONTAINER_WORKSPACE,
      mounts: [{ hostPath: workDirPath, containerPath: CONTAINER_WORKSPACE }],
    };
  }
  return base;
}

export async function runJob(
  job: ScheduledJob,
  timeoutMs: number,
  artifactConfig?: ArtifactConfig
): Promise<JobRunResult> {
  const containerId = `computebay-${job.jobId}`;
  let workDirPath: string | undefined;

  if (job.inputArtifactObjectKey) {
    if (!artifactConfig) {
      throw new Error(
        "Job has inputArtifactObjectKey but MinIO/artifact config is not set. Set MINIO_* and JOB_WORK_DIR."
      );
    }
    workDirPath = path.join(artifactConfig.jobWorkDir, job.jobId);
    try {
      logger.info(
        { jobId: job.jobId, objectKey: job.inputArtifactObjectKey },
        "Downloading artifact from MinIO"
      );
      const zipBuffer = await downloadArtifact(
        artifactConfig.s3Config,
        job.inputArtifactObjectKey
      );
      await extractZipToDir(zipBuffer, workDirPath);
      logger.info({ jobId: job.jobId, workDirPath }, "Artifact extracted");
    } catch (err) {
      logger.error({ err, jobId: job.jobId }, "Artifact download or extract failed");
      throw err;
    }
  }

  const opts = buildContainerOptions(job, workDirPath);
  logger.info(
    {
      jobId: job.jobId,
      containerId,
      image: opts.image,
      hasInlineCode: Boolean(job.inlineCode),
      hasWorkspaceMount: Boolean(workDirPath),
    },
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
      workDir: opts.workDir,
      mounts: opts.mounts,
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

  let outputObjectKey: string | undefined;
  if (workDirPath && artifactConfig) {
    try {
      outputObjectKey = await uploadOutputArtifact(
        artifactConfig.s3Config,
        job.jobId,
        workDirPath
      );
      if (outputObjectKey) {
        logger.info(
          { jobId: job.jobId, outputObjectKey },
          "Uploaded output artifact to MinIO"
        );
      }
    } catch (uploadErr) {
      logger.warn(
        { err: uploadErr, jobId: job.jobId },
        "Failed to upload output artifact"
      );
    }
    try {
      await rm(workDirPath, { recursive: true, force: true });
      logger.info({ jobId: job.jobId, workDirPath }, "Cleaned up job working directory");
    } catch (cleanupErr) {
      logger.warn({ err: cleanupErr, jobId: job.jobId, workDirPath }, "Failed to cleanup work dir");
    }
  } else if (workDirPath) {
    try {
      await rm(workDirPath, { recursive: true, force: true });
      logger.info({ jobId: job.jobId, workDirPath }, "Cleaned up job working directory");
    } catch (cleanupErr) {
      logger.warn({ err: cleanupErr, jobId: job.jobId, workDirPath }, "Failed to cleanup work dir");
    }
  }

  return { exitCode, timedOut, output, outputObjectKey };
}
