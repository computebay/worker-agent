import { rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { ScheduledJob } from "../types/job.types.js";
import type { S3Config } from "../libs/s3.js";
import { putObjectBuffer } from "../libs/s3.js";
import {
  tarDirectory,
  uploadArtifact,
  artifactKey,
  buildLogKey,
} from "../libs/artifact.js";
import {
  createAndStart,
  waitContainer,
  killContainer,
  removeContainer,
  streamContainerLogs,
  getContainerLogs,
} from "./container.runtime.js";
import type { FrameworkPreset } from "./framework.detector.js";
import { logger } from "../utils/logger.js";

export interface BuildResult {
  /** MinIO key for the uploaded artifact tarball. */
  artifactKey: string;
  /** SHA-256 checksum of the tarball (hex). */
  checksum: string;
  buildDurationMs: number;
  /** Runtime command to pass to the service container. */
  runtimeCommand: string;
}

export interface BuildServiceOptions {
  s3Config: S3Config;
  /** Hard wall-clock limit applied to each container phase (install + build). */
  buildTimeoutMs: number;
  onLogChunk?: (chunk: string) => void;
}

const CONTAINER_WORKDIR = "/app";

/**
 * Execute the build pipeline for a service job:
 *  1. (Optional) Install dependencies in a container  — writes node_modules to repoPath
 *  2. (Optional) Build the application in a container — writes dist/.next/etc to repoPath
 *  3. Tar the entire repoPath (minus .git) — includes node_modules + build artefacts
 *  4. Upload tarball to MinIO at deployments/<serviceId>/app.tar.gz
 *  5. Upload accumulated build log to MinIO at deployments/<serviceId>/build.log
 *
 * Both install and build containers bind-mount repoPath → /app so all writes
 * (node_modules, dist, .next …) land on the host and end up in the final tarball.
 *
 * @throws if any install/build container exits non-zero or if the upload fails.
 */
export async function buildServiceArtifact(
  job: ScheduledJob,
  repoPath: string,
  preset: FrameworkPreset,
  options: BuildServiceOptions
): Promise<BuildResult> {
  const { s3Config, buildTimeoutMs, onLogChunk } = options;
  const t0 = Date.now();
  const buildLogs: string[] = [];

  const log = (chunk: string): void => {
    buildLogs.push(chunk);
    onLogChunk?.(chunk);
  };

  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;
  const mount = { hostPath: repoPath, containerPath: CONTAINER_WORKDIR };
  const baseEnv: Record<string, string> = {
    HOME: CONTAINER_WORKDIR,
    TMPDIR: "/tmp",
    TEMP: "/tmp",
    TMP: "/tmp",
    PIP_NO_CACHE_DIR: "1",
    npm_config_cache: "/tmp/.npm",
    ...(job.env ?? {}),
  };

  // ── Phase 1: Install ───────────────────────────────────────────────────────
  if (preset.installCommand) {
    log(`[install] ${preset.installCommand}\n`);
    const id = `computebay-svc-install-${job.jobId}`;
    try {
      await createAndStart({
        containerId: id,
        image: job.image,
        cpu: job.cpu,
        // Build tools often need more RAM than the runtime app itself.
        memoryMb: Math.max(job.memoryMB, 1024),
        command: ["sh", "-c", preset.installCommand],
        env: baseEnv,
        workDir: CONTAINER_WORKDIR,
        mounts: [mount],
        runAsUser,
        // Install writes node_modules into the bind-mounted /app on the host
        // and needs /tmp for package tarballs — both require a writable fs.
        readOnly: false,
        tmpSizeMb: 512,
      });

      const stopStream = streamContainerLogs(id, {
        onChunk: log,
        onEnd: () => {},
        onError: (err) => logger.warn({ err, containerId: id }, "Install log stream error"),
      });
      const { exitCode } = await waitWithTimeout(id, buildTimeoutMs);
      stopStream();

      if (exitCode !== 0) {
        const tail = await getContainerLogs(id).catch(() => "(no logs)");
        throw new Error(`Dependency install failed (exit ${exitCode}).\n${tail}`);
      }
      log(`[install] Done (exit 0)\n`);
    } finally {
      await removeContainer(id).catch((err) =>
        logger.warn({ err, containerId: id }, "Failed to remove install container")
      );
    }
  }

  // ── Phase 2: Build ─────────────────────────────────────────────────────────
  if (preset.buildCommand) {
    log(`[build] ${preset.buildCommand}\n`);
    const id = `computebay-svc-build-${job.jobId}`;
    try {
      await createAndStart({
        containerId: id,
        image: job.image,
        cpu: job.cpu,
        // Build tools often need more RAM than the runtime app itself.
        memoryMb: Math.max(job.memoryMB, 1024),
        command: ["sh", "-c", preset.buildCommand],
        env: { ...baseEnv, NODE_ENV: "production" },
        workDir: CONTAINER_WORKDIR,
        mounts: [mount],
        runAsUser,
        // Build writes compiled output (dist/, .next/, etc.) into bind-mounted /app.
        readOnly: false,
        tmpSizeMb: 512,
      });

      const stopStream = streamContainerLogs(id, {
        onChunk: log,
        onEnd: () => {},
        onError: (err) => logger.warn({ err, containerId: id }, "Build log stream error"),
      });
      const { exitCode } = await waitWithTimeout(id, buildTimeoutMs);
      stopStream();

      if (exitCode !== 0) {
        const tail = await getContainerLogs(id).catch(() => "(no logs)");
        throw new Error(`Build step failed (exit ${exitCode}).\n${tail}`);
      }
      log(`[build] Done (exit 0)\n`);
    } finally {
      await removeContainer(id).catch((err) =>
        logger.warn({ err, containerId: id }, "Failed to remove build container")
      );
    }
  }

  // ── Phase 3: Package ───────────────────────────────────────────────────────
  const tarPath = path.join(os.tmpdir(), `computebay-artifact-${job.jobId}.tar.gz`);
  log(`[package] Archiving repo (excluding .git)...\n`);
  logger.info({ serviceId: job.jobId, repoPath, tarPath }, "Packaging artifact");

  const checksum = await tarDirectory(repoPath, tarPath);
  log(`[package] Archive ready (sha256: ${checksum})\n`);

  // ── Phase 4: Upload artifact ───────────────────────────────────────────────
  const key = artifactKey(job.jobId);
  log(`[upload] Uploading artifact → ${key}\n`);
  await uploadArtifact(s3Config, key, tarPath);
  await rm(tarPath, { force: true }).catch(() => {});
  log(`[upload] Done.\n`);

  // ── Phase 5: Upload build log ──────────────────────────────────────────────
  await putObjectBuffer(
    s3Config,
    buildLogKey(job.jobId),
    Buffer.from(buildLogs.join(""), "utf-8")
  ).catch((err) => logger.warn({ err }, "Failed to upload build log"));

  const buildDurationMs = Date.now() - t0;
  logger.info({ serviceId: job.jobId, key, checksum, buildDurationMs }, "Build phase complete");

  return { artifactKey: key, checksum, buildDurationMs, runtimeCommand: preset.runtimeCommand };
}

/**
 * Wait for a container to exit, killing it if timeoutMs elapses first.
 */
async function waitWithTimeout(
  containerId: string,
  timeoutMs: number
): Promise<{ exitCode: number }> {
  return new Promise((resolve, reject) => {
    let done = false;

    const timer = setTimeout(() => {
      if (done) return;
      done = true;
      killContainer(containerId)
        .then(() =>
          reject(new Error(`Container ${containerId} exceeded build timeout (${timeoutMs}ms)`))
        )
        .catch(reject);
    }, timeoutMs);

    waitContainer(containerId)
      .then((r) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        resolve(r);
      })
      .catch((err) => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        reject(err);
      });
  });
}
