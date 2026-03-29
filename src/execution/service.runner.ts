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
} from "./container.runtime.js";
import { portManager, isPortFree, waitForPort } from "../network/port.manager.js";
import { gitClone } from "../libs/git.clone.js";
import { logger } from "../utils/logger.js";

export interface ServiceStartResult {
  containerId: string;
  hostPort: number;
  serviceId: string;
}

/**
 * Starts a long-running service container with a host port binding.
 *
 * If the job includes a repoUrl the repo is cloned onto a host tmpdir and
 * bind-mounted at /app inside the container so that startCommand can run
 * `npm install && npm run start` (or equivalent) against real code.
 *
 * After the container spawns and the port probe completes we verify the
 * container is still actually running — Docker's userland proxy (docker-proxy)
 * binds the host port immediately on `docker run -d`, so a raw TCP probe
 * returns "ready" even if the app inside has already exited.
 *
 * @throws if Docker fails to start, the container exits during startup,
 *         or the port never becomes ready — releases port before rethrowing.
 */
export async function runService(job: ScheduledJob): Promise<ServiceStartResult> {
  if (job.type !== "service") {
    throw new Error(`runService called with non-service job type: ${job.type}`);
  }
  if (!job.servicePort) {
    throw new Error(`servicePort is required for service jobs (jobId=${job.jobId})`);
  }

  const hostPort = portManager.allocatePort();
  const containerId = `computebay-svc-${job.jobId}`;
  const CONTAINER_WORKDIR = "/app";

  // Pre-flight check: ensure the allocated port is genuinely free.
  if (!(await isPortFree(hostPort))) {
    portManager.releasePort(hostPort);
    throw new Error(`Allocated host port ${hostPort} is not free`);
  }

  logger.info(
    { serviceId: job.jobId, containerId, image: job.image, hostPort, containerPort: job.servicePort },
    "Starting service container"
  );

  // If the scheduler supplied a repoUrl, clone the code onto the host and
  // mount it into /app so the startCommand has something to work with.
  let workDirPath: string | undefined;
  const mounts: Array<{ hostPath: string; containerPath: string }> = [];

  if (job.repoUrl) {
    workDirPath = await mkdtemp(path.join(os.tmpdir(), `computebay-svc-${job.jobId}-`));
    logger.info({ serviceId: job.jobId, repoUrl: job.repoUrl, branch: job.branch }, "Cloning repository for service");
    const repoPath = await gitClone(job.repoUrl, job.branch ?? "main", workDirPath);
    mounts.push({ hostPath: repoPath, containerPath: CONTAINER_WORKDIR });
    logger.info({ serviceId: job.jobId, repoPath }, "Repository cloned");
  }

  // Guard: service jobs must have code to run. Without a bind-mount at /app
  // the container starts against an empty image root and crashes immediately.
  if (mounts.length === 0) {
    portManager.releasePort(hostPort);
    throw new Error(
      `Service job ${job.jobId} has no repoUrl — cannot start without code at ${CONTAINER_WORKDIR}. ` +
      `Either supply a repoUrl or use an image with the application already baked in ` +
      `and pass an explicit entrypoint.`
    );
  }

  // Env vars: redirect npm/pip caches into the /tmp tmpfs so they don't
  // write to the container root (or fail on a read-only mount).
  const env: Record<string, string> = {
    HOME: CONTAINER_WORKDIR,
    TMPDIR: "/tmp",
    TEMP: "/tmp",
    TMP: "/tmp",
    PIP_NO_CACHE_DIR: "1",
    npm_config_cache: "/tmp/.npm",
    ...(job.env ?? {}),
  };

  // Build the container command.
  // Prefer explicit entrypoint array; fall back to startCommand shell string.
  const command = job.entrypoint ?? (job.startCommand ? ["sh", "-c", job.startCommand] : []);

  const cleanup = async (): Promise<void> => {
    try { await killContainer(containerId); } catch { /* may not exist */ }
    try { await removeContainer(containerId); } catch { /* may not exist */ }
    portManager.releasePort(hostPort);
    if (workDirPath) {
      await rm(workDirPath, { recursive: true, force: true }).catch((e) =>
        logger.warn({ err: e, workDirPath }, "Failed to clean up service work dir")
      );
    }
  };

  try {
    await createAndStart({
      containerId,
      image: job.image,
      cpu: job.cpu,
      memoryMb: job.memoryMB,
      command,
      env,
      workDir: mounts.length > 0 ? CONTAINER_WORKDIR : undefined,
      mounts,
      portBindings: [{ hostPort, containerPort: job.servicePort }],
    });

    // Wait for readiness — NOTE: waitForPort probes the host port, which is
    // served by Docker's userland proxy (docker-proxy) and is available almost
    // immediately.  A successful TCP probe does NOT mean the app inside is up.
    logger.info({ serviceId: job.jobId, hostPort }, "Waiting for port readiness");
    await waitForPort(hostPort, 30_000);

    // Verify the container is still running.  The port probe above connects to
    // docker-proxy, which accepts TCP connections even if the app has crashed.
    const running = await inspectContainerRunning(containerId);
    if (!running) {
      const logs = await getContainerLogs(containerId).catch(() => "(no logs)");
      throw new Error(
        `Service container exited during startup (docker-proxy gave false-positive).\nContainer logs:\n${logs}`
      );
    }

  } catch (err) {
    logger.error({ err, serviceId: job.jobId, containerId }, "Service container failed to start or become ready");
    await cleanup();
    throw err;
  }

  logger.info(
    { serviceId: job.jobId, containerId, hostPort },
    "Service container started and ready"
  );

  return { containerId, hostPort, serviceId: job.jobId };
}
