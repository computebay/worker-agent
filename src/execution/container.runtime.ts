import { spawn } from "node:child_process";

export interface ContainerCreateOptions {
  containerId: string;
  image: string;
  cpu: number;
  memoryMb: number;
  command?: string[];
  env?: Record<string, string>;
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
  const { containerId, image, cpu, memoryMb, command = [], env = {} } = options;

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
    image,
    ...command,
  ];

  return new Promise((resolve, reject) => {
    const proc = spawn("docker", args, {
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stderr = "";
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code, signal) => {
      if (code === 0) resolve();
      else reject(new Error(`docker run failed: exit ${code}, signal ${signal}, stderr: ${stderr}`));
    });

    proc.on("error", reject);
  });
}

export function killContainer(containerId: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = spawn("docker", ["kill", containerId], {
      stdio: ["ignore", "pipe", "pipe"],
    });

    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`docker kill failed: exit ${code}`));
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
