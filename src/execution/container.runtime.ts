import { spawn } from "node:child_process";
import { logger } from "../utils/logger.js";

export interface ContainerMount {
  hostPath: string;
  containerPath: string;
}

export interface ContainerCreateOptions {
  containerId: string;
  image: string;
  cpu: number;
  memoryMb: number;
  command?: string[];
  env?: Record<string, string>;
  /** Working directory inside the container (e.g. /workspace). */
  workDir?: string;
  /** Bind mounts: host path → container path (e.g. extracted artifact dir → /workspace). */
  mounts?: ContainerMount[];
}

export interface ContainerResult {
  exitCode: number;
  killed: boolean;
}

/**
 * Pure Docker CLI wrapper. No business logic.
 * Uses: --cpus, --memory, --network none, --pids-limit 64, --read-only, --cap-drop ALL, --security-opt no-new-privileges
 */
export function createAndStart(options: ContainerCreateOptions): Promise<void> {
  const {
    containerId,
    image,
    cpu,
    memoryMb,
    command = [],
    env = {},
    workDir,
    mounts = [],
  } = options;

  const args = [
    "run",
    "-d",
    "--name",
    containerId,
    "--cpus",
    String(cpu),
    "--memory",
    `${memoryMb}m`,
    "--network",
    "none",
    "--pids-limit",
    "64",
    "--read-only",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    ...Object.entries(env).flatMap(([k, v]) => ["-e", `${k}=${v}`]),
    ...mounts.flatMap((m) => [
      "--mount",
      `type=bind,source=${m.hostPath},target=${m.containerPath}`,
    ]),
    ...(workDir ? ["-w", workDir] : []),
    image,
    ...command,
  ];
  const cmd = `docker ${args.join(" ")}`;
  logger.info({ containerId, image, cmd }, "Spawning Docker container");

  return new Promise((resolve, reject) => {
    const proc = spawn("docker", args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stderr = "";
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code, signal) => {
      if (code === 0) {
        logger.info({ containerId }, "Docker container spawned successfully");
        resolve();
      } else {
        logger.error({ containerId, code, signal, stderr }, "Docker run failed");
        reject(new Error(`docker run failed: exit ${code}, signal ${signal}, stderr: ${stderr}`));
      }
    });

    proc.on("error", (err) => {
      logger.error({ containerId, err }, "Docker spawn error");
      reject(err);
    });
  });
}

export function killContainer(containerId: string): Promise<void> {
  logger.info({ containerId }, "Docker kill (timeout)");
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["kill", containerId], {
      stdio: ["ignore", "pipe", "pipe"],
    });

    proc.on("close", (code) => {
      if (code === 0) {
        logger.info({ containerId }, "Docker kill succeeded");
        resolve();
      } else {
        logger.error({ containerId, code }, "Docker kill failed");
        reject(new Error(`docker kill failed: exit ${code}`));
      }
    });

    proc.on("error", reject);
  });
}

export function removeContainer(containerId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["rm", "-f", containerId], {
      stdio: ["ignore", "pipe", "pipe"],
    });

    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`docker rm failed: exit ${code}`));
    });

    proc.on("error", reject);
  });
}

export function getContainerLogs(containerId: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["logs", "--tail", "10000", containerId], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    proc.stdout?.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    proc.on("close", (code) => {
      if (code === 0 || code === 1) {
        const out = stdout + (stderr ? `\n[stderr]\n${stderr}` : "");
        resolve(out.trim() || "(no output)");
      } else {
        reject(new Error(`docker logs failed: exit ${code}`));
      }
    });
    proc.on("error", reject);
  });
}

export function waitContainer(containerId: string): Promise<ContainerResult> {
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["wait", containerId], {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    proc.stdout?.on("data", (chunk) => {
      stdout += chunk.toString();
    });

    proc.on("close", (code, signal) => {
      if (code === 0) {
        const exitCode = parseInt(stdout.trim(), 10);
        resolve({ exitCode: Number.isNaN(exitCode) ? 1 : exitCode, killed: false });
      } else {
        reject(new Error(`docker wait failed: exit ${code}, signal ${signal}`));
      }
    });

    proc.on("error", reject);
  });
}
