import { mkdtemp, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { existsSync } from "node:fs";
import { spawn } from "node:child_process";
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
import { logger } from "../utils/logger.js";

export interface ServiceStartResult {
  containerId: string;
  hostPort: number;
  serviceId: string;
  publicUrl: string;
  stopLogStream?: () => void;
  workDirPath?: string;
}

export interface RunServiceOptions {
  onLogChunk?: (chunk: string) => void;
}

export async function runService(
  job: ScheduledJob,
  options?: RunServiceOptions
): Promise<ServiceStartResult> {
  if (job.type !== "service") {
    throw new Error(`runService called with non-service job type: ${job.type}`);
  }
  if (!job.servicePort) {
    throw new Error(`servicePort is required (jobId=${job.jobId})`);
  }

  const hostPort = portManager.allocatePort();
  const containerId = `computebay-svc-${job.jobId}`;
  const CONTAINER_WORKDIR = "/app";

  if (!(await isPortFree(hostPort))) {
    portManager.releasePort(hostPort);
    throw new Error(`Port ${hostPort} not free`);
  }

  const currentUid = process.getuid?.() ?? 1000;
  const currentGid = process.getgid?.() ?? 1000;
  const runAsUser = `${currentUid}:${currentGid}`;

  let workDirPath: string | undefined;
  const mounts: Array<{ hostPath: string; containerPath: string }> = [];

  // 🔽 Clone repo
  if (job.repoUrl) {
    workDirPath = await mkdtemp(
      path.join(os.tmpdir(), `computebay-svc-${job.jobId}-`)
    );
    const repoPath = await gitClone(
      job.repoUrl,
      job.branch ?? "main",
      workDirPath
    );
    mounts.push({ hostPath: repoPath, containerPath: CONTAINER_WORKDIR });

    let installCmd: string | undefined;
    if (existsSync(path.join(repoPath, "package.json"))) {
      installCmd = "npm install";
    } else if (existsSync(path.join(repoPath, "requirements.txt"))) {
      installCmd = "pip install -r requirements.txt";
    }

    if (installCmd) {
      const setupId = `computebay-svc-setup-${job.jobId}`;

      await createAndStart({
        containerId: setupId,
        image: job.image,
        cpu: job.cpu,
        memoryMb: job.memoryMB,
        command: ["sh", "-c", installCmd],
        env: { ...(job.env ?? {}) },
        workDir: CONTAINER_WORKDIR,
        mounts,
        runAsUser,
      });

      await waitContainer(setupId);
      await removeContainer(setupId).catch(() => {});
    }
  }

  if (mounts.length === 0) {
    portManager.releasePort(hostPort);
    throw new Error(`No repoUrl provided`);
  }

  const command =
    job.entrypoint ??
    (job.startCommand ? ["sh", "-c", job.startCommand] : []);

  let stopLogStream: (() => void) | undefined;
  let frpcProcess: any;
  let frpcConfigPath: string;

  const cleanup = async () => {
    if (stopLogStream) stopLogStream();

    try {
      await killContainer(containerId);
    } catch {}

    try {
      await removeContainer(containerId);
    } catch {}

    if (frpcProcess?.pid) {
      try {
        process.kill(-frpcProcess.pid);
      } catch {}
    }

    if (frpcConfigPath) {
      await rm(frpcConfigPath, { force: true }).catch(() => {});
    }

    portManager.releasePort(hostPort);

    if (workDirPath) {
      await rm(workDirPath, { recursive: true, force: true }).catch(() => {});
    }
  };

  try {
    // 🔽 Start container
    await createAndStart({
      containerId,
      image: job.image,
      cpu: job.cpu,
      memoryMb: job.memoryMB,
      command,
      workDir: CONTAINER_WORKDIR,
      mounts,
      portBindings: [{ hostPort, containerPort: job.servicePort }],
      runAsUser,
    });

    if (options?.onLogChunk) {
      stopLogStream = streamContainerLogs(containerId, {
        onChunk: options.onLogChunk,
        onEnd: () => {},
        onError: () => {},
      });
    }

    await waitForPort(hostPort, 30000);

    const running = await inspectContainerRunning(containerId);
    if (!running) {
      const logs = await getContainerLogs(containerId);
      throw new Error(`Container crashed:\n${logs}`);
    }

    // 🔥 FRP INTEGRATION STARTS HERE

    frpcConfigPath = path.join(
      os.tmpdir(),
      `frpc-${job.jobId}.ini`
    );

    const frpcConfig = `
[common]
server_addr = ${process.env.FRP_SERVER_ADDR}
server_port = ${process.env.FRP_SERVER_PORT}

[service-${job.jobId}]
type = http
local_port = ${hostPort}
subdomain = ${job.jobId}
`;

    await writeFile(frpcConfigPath, frpcConfig);

    const frpcBinPath = path.resolve(process.cwd(), "frp_0.56.0_linux_amd64", "frpc");
    frpcProcess = spawn(frpcBinPath, ["-c", frpcConfigPath], {
      detached: true,
      stdio: "ignore",
    });

    frpcProcess.unref();

    logger.info(
      { serviceId: job.jobId },
      "FRP tunnel started"
    );

  } catch (err) {
    await cleanup();
    throw err;
  }

  const publicUrl = `http://${job.jobId}.computebay.online`;

  logger.info(
    { serviceId: job.jobId, publicUrl },
    "Service ready with public URL"
  );

  return {
    containerId,
    hostPort,
    serviceId: job.jobId,
    publicUrl,
    stopLogStream,
    workDirPath,
  };
}