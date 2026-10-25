import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { logger } from "../utils/logger.js";

/**
 * A single physical GPU detected on the worker host.
 * `memoryMb` is the GPU's dedicated VRAM in MiB.
 */
export interface GpuSpec {
  index: number;
  vendor: string;
  name: string;
  memoryMb: number;
  driverVersion?: string;
  computeCapability?: string;
}

/** Device nodes that indicate a GPU is present on the host. */
const GPU_DEVICE_PATHS = [
  "/dev/nvidia0",
  "/dev/nvidiactl",
  "/dev/dri/renderD128",
];

const NVIDIA_SMI_ARGS = [
  "--query-gpu=index,name,memory.total,driver_version,compute_cap",
  "--format=csv,noheader,nounits",
];

// Older nvidia-smi builds do not understand compute_cap. Retry without it.
const NVIDIA_SMI_FALLBACK_ARGS = [
  "--query-gpu=index,name,memory.total,driver_version",
  "--format=csv,noheader,nounits",
];

function runNvidiaSmi(args: string[]): string {
  return execFileSync("nvidia-smi", args, {
    encoding: "utf8",
    timeout: 5_000,
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
}

/**
 * Parse `nvidia-smi --query-gpu=... --format=csv,noheader,nounits` output.
 * Columns: index, name, memory.total (MiB), driver_version[, compute_cap].
 * Unparseable lines are skipped so one bad row does not hide the other GPUs.
 */
export function parseNvidiaSmiOutput(output: string): GpuSpec[] {
  const gpus: GpuSpec[] = [];

  for (const rawLine of output.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;

    const [indexRaw, nameRaw, memoryRaw, driverRaw, computeRaw] = line
      .split(",")
      .map((part) => part.trim());

    const index = Number.parseInt(indexRaw ?? "", 10);
    const memoryMb = Number.parseInt(memoryRaw ?? "", 10);

    if (!Number.isInteger(index) || !nameRaw || !Number.isFinite(memoryMb)) {
      logger.debug({ line }, "Skipping unparseable nvidia-smi GPU row");
      continue;
    }

    gpus.push({
      index,
      vendor: "NVIDIA",
      name: nameRaw,
      memoryMb,
      ...(driverRaw && driverRaw !== "N/A"
        ? { driverVersion: driverRaw }
        : {}),
      ...(computeRaw && computeRaw !== "N/A"
        ? { computeCapability: computeRaw }
        : {}),
    });
  }

  return gpus;
}

/**
 * Scan the host for usable GPUs.
 *
 * NVIDIA is detected through `nvidia-smi`, which gives us the name, VRAM and
 * driver/CUDA metadata needed for advertising the node. A machine without
 * `nvidia-smi` (or without a GPU) is treated as CPU-only — we deliberately do
 * not advertise a GPU we cannot run containers on.
 */
export function detectGpuSpecs(): GpuSpec[] {
  try {
    return parseNvidiaSmiOutput(runNvidiaSmi(NVIDIA_SMI_ARGS));
  } catch (err) {
    logger.debug(
      { err },
      "nvidia-smi query with compute capability failed; retrying without it"
    );
  }

  try {
    return parseNvidiaSmiOutput(runNvidiaSmi(NVIDIA_SMI_FALLBACK_ARGS));
  } catch (err) {
    logger.debug({ err }, "nvidia-smi GPU detection unavailable");
    return [];
  }
}

/**
 * True when GPU device nodes exist even if no supported detection tool is
 * installed. Used only for diagnostics so a misconfigured host is visible in
 * the logs instead of silently registering as CPU-only.
 */
export function hasGpuDeviceNodes(): boolean {
  return GPU_DEVICE_PATHS.some((devicePath) => existsSync(devicePath));
}
