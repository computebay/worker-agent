import { mkdtemp, rm } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type { ScheduledJob } from "../types/job.types.js";
import {
  createAndStart,
  killContainer,
  removeContainer,
  getContainerLogs,
  inspectContainerRunning,
  waitContainer,
  streamContainerLogs,
} from "./container.runtime.js";
import { portManager, isPortFree, waitForPort } from "../network/port.manager.js";
import { gitClone } from "../libs/git.clone.js";
import { detectFramework, applyJobOverrides } from "./framework.detector.js";
import type { FrameworkPreset } from "./framework.detector.js";
import { buildServiceArtifact } from "./service.builder.js";
import type { BuildResult } from "./service.builder.js";
import { downloadArtifact, untarToDirectory } from "../libs/artifact.js";
import type { S3Config } from "../libs/s3.js";
import { logger } from "../utils/logger.js";

export interface ServiceStartResult {
  containerId: string;
  hostPort: number;
  serviceId: string;
  stopLogStream?: () => void;
  /** Path on host to clean up when the service is stopped. */
  workDirPath?: string;
  /** MinIO artifact key (build-step services only). */
  artifactKey?: string;
  /** Framework detected during build (for event publishing). */
  framework?: string;
}

export interface RunServiceOptions {
  onLogChunk?: (chunk: string) => void;
  /** Called immediately before the build pipeline starts. */
  onServiceBuilding?: (frameworkName: string | undefined) => void;
  /** Called once the artifact has been uploaded to MinIO. */
  onServiceBuilt?: (result: BuildResult) => void;
  /** S3 config — required for build-step services. */
  s3Config?: S3Config;
  /** Per-phase build timeout in ms (default: 600_000). */
  buildTimeoutMs?: number;
}

const CONTAINER_WORKDIR = "/app";

/**
 * Start a long-running service container.
 *
 * ## Build-step services (Next.js, Vite, node-build presets)
 *   clone → detect framework → install → build → tar repo → MinIO upload
 *   → download artifact → extract to runtimeDir
 *   → launch runtime container from runtimeDir
 *
 * ## Non-build services (plain Node/Express — no compilation needed)
 *   clone → install deps (separate container) → launch runtime container
 *   directly from cloneDir bind-mount  (existing approach, unchanged)
 *
 * @throws if Docker, git, build steps, or port probe fails.
 */
export async function runService(
  job: ScheduledJob,
  options: RunServiceOptions = {}
): Promise<ServiceStartResult> {
  if (job.type !== "service") {
    throw new Error(`runService called with non-service job: ${job.type}`);
  }
  if (!job.servicePort) {
    throw new Error(`servicePort is required for service jobs (jobId=${job.jobId})`);
  }
  if (!job.repoUrl) {
    throw new Error(`repoUrl is required for service jobs (jobId=${job.jobId})`);
  }

  const {
    onLogChunk,
    onServiceBuilding,
    onServiceBuilt,
    s3Config,
    buildTimeoutMs = 600_000,
  } = options;

  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;

  // ── Step 1: Clone repository ───────────────────────────────────────────────
  const cloneDir = await mkdtemp(path.join(os.tmpdir(), `computebay-svc-${job.jobId}-`));
  let runtimeDir: string | undefined; // populated only for build-step path

  logger.info(
    { serviceId: job.jobId, repoUrl: job.repoUrl, branch: job.branch },
    "Cloning repository for service"
  );
  const repoPath = await gitClone(job.repoUrl, job.branch ?? "main", cloneDir);

  // ── Step 2: Framework detection ────────────────────────────────────────────
  let rawPreset = detectFramework(repoPath);
  if (!rawPreset) {
    // Undetected repo: treat as build-step if user supplied a buildCommand, else pure-node
    rawPreset = {
      name: job.buildCommand ? "custom-build" : "node",
      hasBuildStep: Boolean(job.buildCommand),
      runtimeCommand: job.runtimeCommand ?? job.startCommand ?? "node index.js",
    };
  }
  const preset: FrameworkPreset = applyJobOverrides(rawPreset, {
    buildCommand: job.buildCommand,
    runtimeCommand: job.runtimeCommand ?? job.startCommand,
  });

  logger.info(
    { serviceId: job.jobId, framework: preset.name, hasBuildStep: preset.hasBuildStep },
    "Framework detected"
  );

  // ── Step 3: Build pipeline (build-step services only) ─────────────────────
  let buildResult: BuildResult | undefined;
  let workDirPath: string; // what gets bind-mounted and cleaned up on stop

  if (preset.hasBuildStep) {
    if (!s3Config) {
      throw new Error(
        `MinIO must be configured (MINIO_* env vars) to deploy build-step services ` +
          `(framework: ${preset.name}, jobId: ${job.jobId})`
      );
    }

    onServiceBuilding?.(preset.name);
    logger.info({ serviceId: job.jobId, framework: preset.name }, "Starting build phase");

    buildResult = await buildServiceArtifact(job, repoPath, preset, {
      s3Config,
      buildTimeoutMs,
      onLogChunk,
    });

    onServiceBuilt?.(buildResult);
    logger.info(
      { serviceId: job.jobId, artifactKey: buildResult.artifactKey },
      "Build complete — downloading artifact for runtime"
    );

    // Clone dir no longer needed — artifact is in MinIO
    await rm(cloneDir, { recursive: true, force: true }).catch((err) =>
      logger.warn({ err, cloneDir }, "Failed to clean up clone dir after build")
    );

    // Download and extract artifact into a fresh dir that the runtime container will mount
    runtimeDir = await mkdtemp(path.join(os.tmpdir(), `computebay-runtime-${job.jobId}-`));
    const tarPath = path.join(os.tmpdir(), `computebay-dl-${job.jobId}.tar.gz`);

    await downloadArtifact(s3Config, buildResult.artifactKey, tarPath);
    await untarToDirectory(tarPath, runtimeDir);
    await rm(tarPath, { force: true }).catch(() => {});

    workDirPath = runtimeDir;
  } else {
    // ── Non-build: install deps inside a container, bind-mount repo directly ──
    workDirPath = repoPath; // keep cloneDir alive — it's what the runtime mounts

    if (preset.installCommand) {
      const installId = `computebay-svc-install-${job.jobId}`;
      logger.info({ serviceId: job.jobId, installCommand: preset.installCommand }, "Running install");
      try {
        await createAndStart({
          containerId: installId,
          image: job.image,
          cpu: job.cpu,
          // Install tools often need more RAM than the runtime app itself
          memoryMb: Math.max(job.memoryMB, 1024),
          command: ["sh", "-c", preset.installCommand],
          env: {
            HOME: CONTAINER_WORKDIR,
            TMPDIR: "/tmp",
            TEMP: "/tmp",
            TMP: "/tmp",
            npm_config_cache: "/tmp/.npm",
            ...(job.env ?? {}),
          },
          workDir: CONTAINER_WORKDIR,
          mounts: [{ hostPath: repoPath, containerPath: CONTAINER_WORKDIR }],
          runAsUser,
          // Needs writable access to root (to write into the bind-mounted /app)
          readOnly: false,
        });

        let stopStream: (() => void) | undefined;
        if (onLogChunk) {
          stopStream = streamContainerLogs(installId, {
            onChunk: onLogChunk,
            onEnd: () => {},
            onError: (err) => logger.warn({ err, containerId: installId }, "Install log stream error"),
          });
        }

        const { exitCode } = await waitContainer(installId);
        stopStream?.();

        if (exitCode !== 0) {
          const logs = await getContainerLogs(installId).catch(() => "(no logs)");
          throw new Error(`Dependency install failed (exit ${exitCode}).\n${logs}`);
        }
      } finally {
        await removeContainer(installId).catch((err) =>
          logger.warn({ err, containerId: installId }, "Failed to remove install container")
        );
      }
    }
  }

  // ── Step 4: Allocate host port & pre-flight check ─────────────────────────
  const hostPort = portManager.allocatePort();
  if (!(await isPortFree(hostPort))) {
    portManager.releasePort(hostPort);
    throw new Error(`Allocated host port ${hostPort} is not free`);
  }

  const containerId = `computebay-svc-${job.jobId}`;
  const runtimeCommand = buildResult?.runtimeCommand ?? preset.runtimeCommand;
  const env: Record<string, string> = {
    HOME: CONTAINER_WORKDIR,
    TMPDIR: "/tmp",
    TEMP: "/tmp",
    TMP: "/tmp",
    npm_config_cache: "/tmp/.npm",
    NODE_ENV: "production",
    PORT: String(job.servicePort),
    ...(job.env ?? {}),
  };

  const cleanup = async (): Promise<void> => {
    if (stopLogStream) stopLogStream();
    await killContainer(containerId).catch(() => {});
    await removeContainer(containerId).catch(() => {});
    portManager.releasePort(hostPort);
    if (workDirPath) {
      await rm(workDirPath, { recursive: true, force: true }).catch((err) =>
        logger.warn({ err, workDirPath }, "Failed to clean up service work dir")
      );
    }
  };

  logger.info(
    { serviceId: job.jobId, containerId, hostPort, runtimeCommand },
    "Starting runtime container"
  );

  // ── Step 5: Launch runtime container ──────────────────────────────────────
  let stopLogStream: (() => void) | undefined;

  try {
    await createAndStart({
      containerId,
      image: job.image,
      cpu: job.cpu,
      memoryMb: job.memoryMB,
      command: job.entrypoint ?? ["sh", "-c", runtimeCommand],
      env,
      workDir: CONTAINER_WORKDIR,
      mounts: [{ hostPath: workDirPath, containerPath: CONTAINER_WORKDIR }],
      portBindings: [{ hostPort, containerPort: job.servicePort }],
      runAsUser,
      // Runtime service container must be writable: the app writes logs, sockets,
      // pid files, etc. inside its working directory at runtime.
      readOnly: false,
    });

    if (onLogChunk) {
      stopLogStream = streamContainerLogs(containerId, {
        onChunk: onLogChunk,
        onEnd: () => {},
        onError: (err) => logger.warn({ err, containerId }, "Service log stream error"),
      });
    }

    // ── Step 6: Wait for readiness ───────────────────────────────────────────
    logger.info({ serviceId: job.jobId, hostPort }, "Waiting for port readiness");
    await waitForPort(hostPort, 30_000);

    // docker-proxy binds the host port immediately — verify the app inside is actually running
    const running = await inspectContainerRunning(containerId);
    if (!running) {
      const logs = await getContainerLogs(containerId).catch(() => "(no logs)");
      throw new Error(
        `Service container exited during startup (docker-proxy false-positive).\n${logs}`
      );
    }
  } catch (err) {
    logger.error(
      { err, serviceId: job.jobId, containerId },
      "Service container failed to start or become ready"
    );
    await cleanup();
    throw err;
  }

  logger.info(
    { serviceId: job.jobId, containerId, hostPort, framework: preset.name },
    "Service container started and ready"
  );

  return {
    containerId,
    hostPort,
    serviceId: job.jobId,
    stopLogStream,
    workDirPath,
    artifactKey: buildResult?.artifactKey,
    framework: preset.name,
  };
}
