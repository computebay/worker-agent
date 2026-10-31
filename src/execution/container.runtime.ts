import { spawn } from "node:child_process";
import { logger } from "../utils/logger.js";
import {
  GpuExecutionError,
  type GpuExecutionErrorCode,
} from "../monitoring/gpu.requirement.js";

export interface ContainerMount {
  hostPath: string;
  containerPath: string;
}

/**
 * Vendor-neutral GPU request handed to the Docker wrapper. Only the NVIDIA
 * implementation exists in this phase; unsupported vendors fail closed.
 */
export interface ContainerGpuConfig {
  vendor: string;
  backend: string;
  /** Current local host indices. Never an enumeration index from another host. */
  deviceIndices: number[];
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
  /** Enable network (default: false, uses --network none). Set true for scraping jobs. */
  networkEnabled?: boolean;
  /** Run container as this UID:GID (e.g. "1000:1000") to match host file ownership. */
  runAsUser?: string;
  /** Capabilities to add back after --cap-drop ALL (e.g. ["SETUID", "SETGID"]). */
  capAdd?: string[];
  /** Port bindings for service containers: { hostPort, containerPort }[]. */
  portBindings?: Array<{ hostPort: number; containerPort: number }>;
  /** Optional GPU exposure. Omitted for CPU-only jobs. */
  gpu?: ContainerGpuConfig;
}

/**
 * Build the vendor-specific Docker device-selection arguments.
 *
 * NVIDIA uses Docker's supported `--gpus device=<indices>` syntax. The value is
 * emitted as one argv element; it is never assembled through a shell and never
 * uses `all`.
 */
export function buildContainerGpuArgs(
  gpu: ContainerGpuConfig | undefined
): string[] {
  if (!gpu) return [];

  if (gpu.vendor.trim().toLowerCase() !== "nvidia") {
    throw new GpuExecutionError(
      "DOCKER_GPU_UNSUPPORTED",
      `GPU vendor '${gpu.vendor}' is not supported for Docker execution`
    );
  }
  const backend = gpu.backend.trim().toLowerCase();
  if (backend !== "cuda" && backend !== "nvenc") {
    throw new GpuExecutionError(
      "DOCKER_GPU_UNSUPPORTED",
      `GPU backend '${gpu.backend}' is not supported for Docker execution`
    );
  }

  const indices = gpu.deviceIndices;
  if (
    !Array.isArray(indices) ||
    indices.length === 0 ||
    indices.some(
      (index) => !Number.isInteger(index) || index < 0
    ) ||
    new Set(indices).size !== indices.length
  ) {
    throw new GpuExecutionError(
      "GPU_ASSIGNMENT_INVALID",
      "GPU device assignment is empty, duplicated, or contains invalid indices"
    );
  }

  return ["--gpus", `device=${indices.join(",")}`];
}

export interface DockerErrorClassification {
  code: GpuExecutionErrorCode;
  message: string;
}

/** Map Docker/NVIDIA stderr to a sanitized, actionable GPU failure. */
export function classifyDockerGpuError(stderr: string): DockerErrorClassification {
  const text = stderr.toLowerCase();

  if (
    text.includes("unknown flag: --gpus") ||
    text.includes("unknown shorthand flag") ||
    text.includes("flag provided but not defined") ||
    text.includes("--gpus: unknown")
  ) {
    return {
      code: "DOCKER_GPU_UNSUPPORTED",
      message: "Docker on this host does not support the --gpus option",
    };
  }

  if (
    text.includes("could not select device driver") ||
    text.includes("nvidia-container-cli: initialization error") ||
    text.includes("nvidia-container-runtime") ||
    text.includes("failed to initialize nvml") ||
    text.includes("no nvidia devices") ||
    text.includes("nvidia-container-toolkit")
  ) {
    return {
      code: "DOCKER_GPU_RUNTIME_UNAVAILABLE",
      message:
        "NVIDIA Container Toolkit/runtime is missing or misconfigured on this host",
    };
  }

  if (
    text.includes("permission denied") ||
    text.includes("operation not permitted") ||
    text.includes("failed to open /dev/nvidia") ||
    text.includes("cannot open /dev/nvidia")
  ) {
    return {
      code: "DOCKER_GPU_DEVICE_ACCESS",
      message: "Docker cannot access the assigned NVIDIA device",
    };
  }

  if (
    text.includes("no such device") ||
    text.includes("device not found") ||
    text.includes("invalid device") ||
    text.includes("unknown device")
  ) {
    return {
      code: "GPU_DEVICE_NOT_FOUND",
      message: "The assigned GPU device is no longer available to Docker",
    };
  }

  return {
    code: "DOCKER_GPU_START_FAILED",
    message: "GPU container failed to start",
  };
}

function secretValues(env: Record<string, string>): string[] {
  return Object.values(env).filter(
    (value) => typeof value === "string" && value.length > 0
  );
}

function sanitizeText(text: string, secrets: string[]): string {
  let safe = text;
  for (const secret of secrets) {
    if (secret.length >= 3) {
      safe = safe.split(secret).join("***");
    }
  }
  return safe.slice(0, 4000);
}

/** Redact `-e KEY=value` pairs and any env values that leak into log text. */
export function redactDockerArgs(
  args: string[],
  env: Record<string, string> = {}
): string[] {
  const secrets = secretValues(env);
  const redacted: string[] = [];

  for (let i = 0; i < args.length; i++) {
    const arg = args[i]!;
    if (arg === "-e" && i + 1 < args.length) {
      redacted.push(arg);
      const assignment = args[++i]!;
      const separator = assignment.indexOf("=");
      redacted.push(
        separator >= 0
          ? `${assignment.slice(0, separator)}=***`
          : "***"
      );
      continue;
    }
    redacted.push(sanitizeText(arg, secrets));
  }

  return redacted;
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
    networkEnabled = false,
    runAsUser,
    capAdd = [],
    portBindings = [],
    gpu,
  } = options;

  // Validate the vendor/backend before spawning. A GPU job must never fall
  // back to CPU execution if its device configuration is unusable.
  const gpuArgs = buildContainerGpuArgs(gpu);

  // Service containers (those with port bindings) run user code that typically
  // needs to write to its working directory (e.g. npm install -> node_modules/).
  // --read-only would cause those writes to fail and the container to exit immediately.
  // For services we skip --read-only and instead add a writable tmpfs at /app so
  // runtime deps can be installed without touching the host filesystem.
  // const isService = portBindings.length > 0;

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
    ...gpuArgs,
    "--cpus",
    String(cpu),
    "--memory",
    `${memoryMb}m`,
    "--network",
    networkEnabled ? "bridge" : "none",
    ...(networkEnabled ? ["--dns", "8.8.8.8", "--dns", "8.8.4.4"] : []),
    "--pids-limit",
    "64",
    "--tmpfs",
    "/tmp:rw,exec,nosuid,size=512m",
    "--tmpfs",
    "/var/tmp:rw,nosuid,size=64m",
    "--cap-drop",
    "ALL",
    ...capAdd.flatMap((cap) => ["--cap-add", cap]),
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
  const secrets = secretValues(env);
  const loggableArgs = redactDockerArgs(args, env);
  logger.info(
    { containerId, image, args: loggableArgs, gpu: gpu ? { vendor: gpu.vendor, backend: gpu.backend, deviceIndices: gpu.deviceIndices } : undefined },
    "Spawning Docker container"
  );

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
        const safeStderr = sanitizeText(stderr, secrets);
        logger.error(
          { containerId, code, signal, stderr: safeStderr, gpu: Boolean(gpu) },
          "Docker run failed"
        );
        if (gpu) {
          const classified = classifyDockerGpuError(stderr);
          const detail = safeStderr.trim();
          reject(
            new GpuExecutionError(
              classified.code,
              detail
                ? `${classified.message}: ${detail}`
                : classified.message
            )
          );
        } else {
          reject(
            new Error(
              `docker run failed: exit ${code}, signal ${signal}, stderr: ${safeStderr}`
            )
          );
        }
      }
    });

    proc.on("error", (err) => {
      logger.error(
        { containerId, err: { code: (err as NodeJS.ErrnoException).code, message: err.message } },
        "Docker spawn error"
      );
      if (gpu) {
        reject(
          new GpuExecutionError(
            "DOCKER_GPU_RUNTIME_UNAVAILABLE",
            "Docker is not available on this worker"
          )
        );
      } else {
        reject(err);
      }
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