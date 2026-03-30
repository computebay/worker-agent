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
  /** Bind mounts: host path → container path. */
  mounts?: ContainerMount[];
  /** Enable network (default: true, uses bridge). */
  networkEnabled?: boolean;
  /** Run container as this UID:GID (e.g. "1000:1000") to match host file ownership. */
  runAsUser?: string;
  /** Port bindings for service containers: { hostPort, containerPort }[]. */
  portBindings?: Array<{ hostPort: number; containerPort: number }>;
  /**
   * Mount the container root filesystem as read-only.
   * Default: true for batch/build containers, false for runtime service containers.
   * Set explicitly — do NOT rely on port binding presence to infer this.
   */
  readOnly?: boolean;
  /**
   * Size of the /tmp tmpfs in MB.
   * Default: 128. Build/install containers should pass a larger value (e.g. 512)
   * because npm/pip write package tarballs and cache files to /tmp during install.
   */
  tmpSizeMb?: number;
}

export interface ContainerResult {
  exitCode: number;
  killed: boolean;
}

/**
 * Pure Docker CLI wrapper. No business logic.
 * Uses: --cpus, --memory, --network none (or bridge if networkEnabled), --pids-limit 64, --read-only,
 *       --tmpfs /tmp, --tmpfs /var/tmp, --cap-drop ALL, --security-opt no-new-privileges
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
    runAsUser,
    portBindings = [],
    // readOnly defaults to true (safe for batch + build containers).
    // Runtime service containers pass readOnly: false explicitly.
    readOnly = true,
    // 128 MB is enough for batch jobs; build/install containers pass 512.
    tmpSizeMb = 128,
  } = options;

  if (portBindings.length > 0) {
    for (const pb of portBindings) {
      if (pb.containerPort < 1 || pb.containerPort > 65535) {
        throw new Error(`Invalid container port: ${pb.containerPort}`);
      }
      if (pb.hostPort < 1 || pb.hostPort > 65535) {
        throw new Error(`Invalid host port: ${pb.hostPort}`);
      }
    }
  }

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
    "bridge",
    "--dns",
    "8.8.8.8",
    "--dns",
    "8.8.4.4",
    "--pids-limit",
    "64",
    ...(readOnly ? [
      "--read-only",
      "--tmpfs",
      `/tmp:rw,noexec,nosuid,size=${tmpSizeMb}m`,
      "--tmpfs",
      "/var/tmp:rw,noexec,nosuid,size=64m",
    ] : []),
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    ...(runAsUser ? ["--user", runAsUser] : []),
    ...portBindings.flatMap(({ hostPort, containerPort }) => ["-p", `${hostPort}:${containerPort}`]),
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

/**
 * Returns true if the container exists and is currently running.
 * Used to detect docker-proxy false-positives during service startup:
 * docker-proxy binds the host port immediately, so a TCP probe succeeds
 * even if the app inside has already exited.
 */
export function inspectContainerRunning(containerId: string): Promise<boolean> {
  return new Promise((resolve) => {
    const proc = spawn(
      "docker",
      ["inspect", "--format", "{{.State.Running}}", containerId],
      { stdio: ["ignore", "pipe", "pipe"] }
    );

    let stdout = "";
    proc.stdout?.on("data", (chunk) => { stdout += chunk.toString(); });

    proc.on("close", (code) => {
      if (code !== 0) {
        // Non-zero exit means container doesn't exist (already removed).
        resolve(false);
      } else {
        resolve(stdout.trim() === "true");
      }
    });

    proc.on("error", () => resolve(false));
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

export interface LogStreamCallbacks {
  onChunk: (chunk: string) => void;
  onEnd: () => void;
  onError: (err: Error) => void;
}

/**
 * Stream container logs via docker logs -f.
 * Calls onChunk for each chunk, onEnd when process exits, onError on failure.
 * Returns a function to kill the log stream process.
 */
export function streamContainerLogs(
  containerId: string,
  callbacks: LogStreamCallbacks
): () => void {
  const proc = spawn("docker", ["logs", "-f", "--tail", "0", containerId], {
    stdio: ["ignore", "pipe", "pipe"],
  });

  proc.stdout?.on("data", (chunk) => {
    callbacks.onChunk(chunk.toString());
  });
  proc.stderr?.on("data", (chunk) => {
    callbacks.onChunk(chunk.toString());
  });
  proc.on("close", (code) => {
    if (code === 0 || code === 1) {
      callbacks.onEnd();
    } else {
      callbacks.onError(new Error(`docker logs -f exited with code ${code}`));
    }
  });
  proc.on("error", (err) => {
    callbacks.onError(err);
  });

  return () => {
    proc.kill("SIGTERM");
  };
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