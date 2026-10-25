import { execFileSync } from "node:child_process";
import { logger } from "../utils/logger.js";
import {
  normalizePciAddress,
  type GpuSpec,
} from "./gpu.types.js";

export interface NvidiaSmiParseOptions {
  /** Column is present in the query. */
  computeCapability?: boolean;
  uuid?: boolean;
  pciBusId?: boolean;
}

interface NvidiaDiscoveryResult {
  gpus: GpuSpec[];
  succeeded: boolean;
  error?: string;
}

interface NvidiaQuery {
  args: string[];
  options: NvidiaSmiParseOptions;
}

const BASE_COLUMNS = "index,name,memory.total,driver_version";

// Most capable query first. Each fallback drops columns that older drivers do
// not understand, ending at the original query for maximum compatibility.
const NVIDIA_QUERIES: NvidiaQuery[] = [
  {
    args: [
      `--query-gpu=${BASE_COLUMNS},compute_cap,uuid,pci.bus_id`,
      "--format=csv,noheader,nounits",
    ],
    options: { computeCapability: true, uuid: true, pciBusId: true },
  },
  {
    args: [
      `--query-gpu=${BASE_COLUMNS},uuid,pci.bus_id`,
      "--format=csv,noheader,nounits",
    ],
    options: { uuid: true, pciBusId: true },
  },
  {
    args: [
      `--query-gpu=${BASE_COLUMNS}`,
      "--format=csv,noheader,nounits",
    ],
    options: {},
  },
];

function runNvidiaSmi(args: string[]): string {
  return execFileSync("nvidia-smi", args, {
    encoding: "utf8",
    timeout: 5_000,
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
}

function parseMemoryMb(raw: string | undefined): number | undefined {
  if (!raw || raw === "N/A" || raw === "[N/A]") return undefined;
  const value = Number.parseInt(raw, 10);
  if (!Number.isFinite(value) || value < 0) return undefined;
  return value;
}

/**
 * Parse `nvidia-smi --query-gpu=... --format=csv,noheader,nounits` output.
 *
 * The default column layout is the historical
 * `index,name,memory.total,driver_version[,compute_cap]`, so the parser stays
 * compatible with existing callers/tests. Unparseable rows are skipped so one
 * bad row does not hide the other GPUs.
 */
export function parseNvidiaSmiOutput(
  output: string,
  options: NvidiaSmiParseOptions = { computeCapability: true },
): GpuSpec[] {
  const gpus: GpuSpec[] = [];
  const hasComputeCapability = options.computeCapability ?? false;
  const hasUuid = options.uuid ?? false;
  const hasPciBusId = options.pciBusId ?? false;

  for (const rawLine of output.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;

    const columns = line.split(",").map((part) => part.trim());
    const [indexRaw, nameRaw, memoryRaw, driverRaw] = columns;
    const index = Number.parseInt(indexRaw ?? "", 10);

    if (!Number.isInteger(index) || index < 0) {
      logger.debug({ line }, "Skipping unparseable nvidia-smi GPU row");
      continue;
    }

    let cursor = 4;
    const computeRaw = hasComputeCapability ? columns[cursor++] : undefined;
    const uuidRaw = hasUuid ? columns[cursor++] : undefined;
    const pciRaw = hasPciBusId ? columns[cursor++] : undefined;

    const memoryMb = parseMemoryMb(memoryRaw);
    const driverVersion =
      driverRaw && driverRaw !== "N/A" && driverRaw !== "[N/A]"
        ? driverRaw
        : undefined;
    const computeCapability =
      computeRaw && computeRaw !== "N/A" && computeRaw !== "[N/A]"
        ? computeRaw
        : undefined;
    const uuid =
      uuidRaw && uuidRaw !== "N/A" && uuidRaw !== "[N/A]"
        ? uuidRaw
        : undefined;
    const pciAddress = normalizePciAddress(pciRaw);

    gpus.push({
      ...(uuid || pciAddress ? { id: uuid ?? `pci:${pciAddress}` } : {}),
      index,
      vendor: "NVIDIA",
      ...(nameRaw && nameRaw !== "N/A" && nameRaw !== "[N/A]"
        ? { name: nameRaw }
        : {}),
      ...(memoryMb !== undefined ? { memoryMb } : {}),
      ...(driverVersion ? { driverVersion } : {}),
      ...(computeCapability ? { computeCapability } : {}),
      pciVendorId: "10de",
      ...(pciAddress ? { pciAddress } : {}),
      discoverySource: "nvidia-smi",
    });
  }

  return gpus;
}

function describeExecError(err: unknown): string {
  if (err && typeof err === "object") {
    const error = err as NodeJS.ErrnoException & { signal?: string };
    if (error.code === "ENOENT") return "nvidia-smi is not installed";
    if (error.code === "ETIMEDOUT" || error.signal === "SIGTERM") {
      return "nvidia-smi timed out";
    }
    if (error.code) return `nvidia-smi failed (${error.code})`;
    if (error.message) return error.message.slice(0, 200);
  }
  return "nvidia-smi query failed";
}

function isPermanentFailure(err: unknown): boolean {
  if (!err || typeof err !== "object") return false;
  const error = err as NodeJS.ErrnoException;
  return error.code === "ENOENT" || error.code === "ETIMEDOUT";
}

/**
 * Query NVIDIA GPUs through nvidia-smi. Failure is non-fatal: callers fall
 * back to generic Linux discovery.
 */
export function discoverNvidiaGpus(): NvidiaDiscoveryResult {
  let lastError: unknown;

  for (const query of NVIDIA_QUERIES) {
    try {
      const gpus = parseNvidiaSmiOutput(runNvidiaSmi(query.args), query.options);
      return { gpus, succeeded: true };
    } catch (err) {
      lastError = err;
      logger.debug({ err }, "nvidia-smi GPU query failed; trying next query shape");
      if (isPermanentFailure(err)) break;
    }
  }

  return {
    gpus: [],
    succeeded: false,
    error: describeExecError(lastError),
  };
}
