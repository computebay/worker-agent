import type { GpuSpec } from "./gpu.types.js";

export const GPU_VENDORS = ["nvidia", "amd", "intel"] as const;
export const GPU_BACKENDS = ["cuda", "nvenc"] as const;

export type GpuVendor = (typeof GPU_VENDORS)[number];
export type GpuExecutionBackend = (typeof GPU_BACKENDS)[number];

/** Canonical GPU requirement used by scheduling and execution. */
export interface GpuRequirement {
  required: true;
  count: number;
  vendor?: GpuVendor;
  minMemoryMb?: number;
  backend?: GpuExecutionBackend;
}

export interface GpuAssignmentDevice {
  /** Stable device identity when the scheduler had one (NVIDIA UUID or PCI id). */
  id?: string;
  /** Index reported by the worker at scheduling time. Not trusted when `id`/`pciAddress` exists. */
  index: number;
  vendor: string;
  pciAddress?: string;
}

export interface GpuAssignment {
  vendor: GpuVendor;
  backend: GpuExecutionBackend;
  count: number;
  devices: GpuAssignmentDevice[];
}

export type GpuExecutionErrorCode =
  | "GPU_ASSIGNMENT_MISSING"
  | "GPU_ASSIGNMENT_INVALID"
  | "GPU_DEVICE_NOT_FOUND"
  | "GPU_BACKEND_UNSUPPORTED"
  | "GPU_VRAM_INSUFFICIENT"
  | "GPU_VRAM_UNKNOWN"
  | "GPU_DEVICE_BUSY"
  | "DOCKER_GPU_UNSUPPORTED"
  | "DOCKER_GPU_DEVICE_ACCESS"
  | "DOCKER_GPU_RUNTIME_UNAVAILABLE"
  | "DOCKER_GPU_START_FAILED";

/** A GPU failure that is safe to surface to the job owner. */
export class GpuExecutionError extends Error {
  constructor(
    public readonly code: GpuExecutionErrorCode,
    message: string
  ) {
    super(message);
    this.name = "GpuExecutionError";
  }
}

export interface ResolvedGpuDevice {
  spec: GpuSpec;
  key: string;
  stable: boolean;
}

export interface GpuExecutionPlan {
  vendor: "nvidia";
  backend: GpuExecutionBackend;
  /** Current local host indices. Docker receives these, never stale ones. */
  deviceIndices: number[];
  /** Stable keys used for the local exclusivity guard. */
  deviceKeys: string[];
  devices: GpuSpec[];
}

const BACKEND_VENDOR: Record<GpuExecutionBackend, GpuVendor> = {
  cuda: "nvidia",
  nvenc: "nvidia",
};

export function normalizeGpuVendor(
  vendor: string | undefined
): GpuVendor | undefined {
  if (!vendor) return undefined;
  const value = vendor.trim().toLowerCase();
  if (value === "nvidia") return "nvidia";
  if (value === "amd" || value === "advanced micro devices") return "amd";
  if (value === "intel") return "intel";
  return undefined;
}

export function normalizeGpuBackend(
  backend: string | undefined
): GpuExecutionBackend | undefined {
  if (!backend) return undefined;
  const value = backend.trim().toLowerCase();
  if (value === "cuda") return "cuda";
  if (value === "nvenc") return "nvenc";
  return undefined;
}

export function gpuVendorOf(spec: GpuSpec): GpuVendor | undefined {
  const vendor = normalizeGpuVendor(spec.vendor);
  if (vendor) return vendor;
  switch (spec.pciVendorId?.trim().toLowerCase()) {
    case "10de":
      return "nvidia";
    case "1002":
      return "amd";
    case "8086":
      return "intel";
    default:
      return undefined;
  }
}

/**
 * Stable key when the device has one; conservative index fallback otherwise.
 * Index keys are never restored across scheduler restarts (see allocation
 * hydration), so they cannot accidentally reserve a different physical GPU.
 */
export function gpuDeviceKey(spec: GpuSpec): {
  key: string;
  stable: boolean;
} {
  if (spec.id && spec.id.trim().length > 0) {
    return { key: `id:${spec.id.trim()}`, stable: true };
  }
  if (spec.pciAddress && spec.pciAddress.trim().length > 0) {
    return {
      key: `pci:${spec.pciAddress.trim().toLowerCase()}`,
      stable: true,
    };
  }
  return { key: `index:${spec.index}`, stable: false };
}

export interface GpuRequirementParseResult {
  requirement: GpuRequirement | null;
  error?: string;
}

/**
 * Accept the legacy `"any" | "none"` values and the explicit object form.
 * Returns a canonical requirement (or null for CPU-only jobs).
 */
export function parseGpuRequirement(
  raw: unknown
): GpuRequirementParseResult {
  if (raw === undefined || raw === null || raw === "none") {
    return { requirement: null };
  }
  if (raw === "any") {
    return { requirement: { required: true, count: 1 } };
  }
  if (typeof raw !== "object" || Array.isArray(raw)) {
    return {
      requirement: null,
      error: "resources.gpu must be 'any', 'none', or an object",
    };
  }

  const value = raw as Record<string, unknown>;
  if (typeof value.required !== "boolean") {
    return {
      requirement: null,
      error: "resources.gpu.required must be a boolean",
    };
  }
  if (!value.required) {
    const extra = ["count", "vendor", "minMemoryMb", "backend"].filter(
      (key) => value[key] !== undefined
    );
    if (extra.length > 0) {
      return {
        requirement: null,
        error: `resources.gpu.${extra[0]} is only valid when required is true`,
      };
    }
    return { requirement: null };
  }

  const count = value.count === undefined ? 1 : value.count;
  if (
    typeof count !== "number" ||
    !Number.isInteger(count) ||
    count < 1 ||
    count > 64
  ) {
    return {
      requirement: null,
      error: "resources.gpu.count must be an integer between 1 and 64",
    };
  }

  let vendor: GpuVendor | undefined;
  if (value.vendor !== undefined) {
    if (typeof value.vendor !== "string") {
      return {
        requirement: null,
        error: "resources.gpu.vendor must be a string",
      };
    }
    vendor = normalizeGpuVendor(value.vendor);
    if (!vendor) {
      return {
        requirement: null,
        error: "resources.gpu.vendor must be one of: nvidia, amd, intel",
      };
    }
  }

  let backend: GpuExecutionBackend | undefined;
  if (value.backend !== undefined) {
    if (typeof value.backend !== "string") {
      return {
        requirement: null,
        error: "resources.gpu.backend must be a string",
      };
    }
    backend = normalizeGpuBackend(value.backend);
    if (!backend) {
      return {
        requirement: null,
        error: "resources.gpu.backend must be one of: cuda, nvenc",
      };
    }
  }

  if (backend && vendor && BACKEND_VENDOR[backend] !== vendor) {
    return {
      requirement: null,
      error: `resources.gpu.backend '${backend}' is not supported by vendor '${vendor}'`,
    };
  }
  const resolvedVendor = vendor ?? (backend ? BACKEND_VENDOR[backend] : undefined);

  let minMemoryMb: number | undefined;
  if (value.minMemoryMb !== undefined) {
    if (
      typeof value.minMemoryMb !== "number" ||
      !Number.isInteger(value.minMemoryMb) ||
      value.minMemoryMb <= 0
    ) {
      return {
        requirement: null,
        error: "resources.gpu.minMemoryMb must be a positive integer",
      };
    }
    minMemoryMb = value.minMemoryMb;
  }

  return {
    requirement: {
      required: true,
      count,
      ...(resolvedVendor ? { vendor: resolvedVendor } : {}),
      ...(minMemoryMb !== undefined ? { minMemoryMb } : {}),
      ...(backend ? { backend } : {}),
    },
  };
}

export interface GpuAssignmentParseResult {
  assignment?: GpuAssignment;
  error?: string;
}

/** Shape-check an assignment received in `job.scheduled`. */
export function parseGpuAssignment(raw: unknown): GpuAssignmentParseResult {
  if (raw === undefined || raw === null) return {};

  if (typeof raw !== "object" || Array.isArray(raw)) {
    return { error: "gpuAssignment must be an object" };
  }

  const value = raw as Record<string, unknown>;
  if (typeof value.vendor !== "string") {
    return { error: "gpuAssignment.vendor must be a string" };
  }
  const vendor = normalizeGpuVendor(value.vendor);
  if (!vendor) {
    return { error: "gpuAssignment.vendor is not a supported vendor" };
  }

  if (typeof value.backend !== "string") {
    return { error: "gpuAssignment.backend must be a string" };
  }
  const backend = normalizeGpuBackend(value.backend);
  if (!backend) {
    return { error: "gpuAssignment.backend is not a supported backend" };
  }

  if (
    typeof value.count !== "number" ||
    !Number.isInteger(value.count) ||
    value.count < 1 ||
    value.count > 64
  ) {
    return {
      error: "gpuAssignment.count must be an integer between 1 and 64",
    };
  }

  if (!Array.isArray(value.devices) || value.devices.length === 0) {
    return { error: "gpuAssignment.devices must be a non-empty array" };
  }

  const devices: GpuAssignmentDevice[] = [];
  for (const rawDevice of value.devices) {
    if (typeof rawDevice !== "object" || rawDevice === null) {
      return { error: "gpuAssignment.devices entries must be objects" };
    }
    const device = rawDevice as Record<string, unknown>;
    if (
      typeof device.index !== "number" ||
      !Number.isInteger(device.index) ||
      device.index < 0
    ) {
      return {
        error: "gpuAssignment.devices[].index must be a non-negative integer",
      };
    }
    if (device.id !== undefined && typeof device.id !== "string") {
      return { error: "gpuAssignment.devices[].id must be a string" };
    }
    if (
      device.pciAddress !== undefined &&
      typeof device.pciAddress !== "string"
    ) {
      return { error: "gpuAssignment.devices[].pciAddress must be a string" };
    }
    if (device.vendor !== undefined && typeof device.vendor !== "string") {
      return { error: "gpuAssignment.devices[].vendor must be a string" };
    }

    const deviceVendor: GpuVendor | undefined =
      device.vendor === undefined
        ? vendor
        : typeof device.vendor === "string"
          ? normalizeGpuVendor(device.vendor)
          : undefined;
    if (!deviceVendor) {
      return {
        error: "gpuAssignment.devices[].vendor is not a supported vendor",
      };
    }
    if (deviceVendor !== vendor) {
      return {
        error: "gpuAssignment mixes GPU vendors in one allocation",
      };
    }

    devices.push({
      ...(device.id ? { id: device.id as string } : {}),
      index: device.index,
      vendor: deviceVendor,
      ...(device.pciAddress
        ? { pciAddress: (device.pciAddress as string).toLowerCase() }
        : {}),
    });
  }

  if (devices.length !== value.count) {
    return {
      error: "gpuAssignment.count does not match the number of assigned devices",
    };
  }

  return {
    assignment: {
      vendor,
      backend,
      count: value.count,
      devices,
    },
  };
}

function findAssignedDevice(
  assigned: GpuAssignmentDevice,
  inventory: GpuSpec[]
): GpuSpec | undefined {
  if (assigned.id) {
    return inventory.find((gpu) => gpu.id === assigned.id);
  }
  if (assigned.pciAddress) {
    const address = assigned.pciAddress.toLowerCase();
    return inventory.find(
      (gpu) => gpu.pciAddress?.toLowerCase() === address
    );
  }
  // No stable identity: index is the only conservative match available.
  return inventory.find((gpu) => gpu.index === assigned.index);
}

/**
 * Resolve the scheduler's assignment against the worker's current inventory.
 * The returned indices are the local indices used for Docker `--gpus`.
 */
export function resolveGpuAssignment(
  requirement: GpuRequirement | null,
  assignment: GpuAssignment | undefined,
  inventory: GpuSpec[]
): GpuExecutionPlan | null {
  if (!requirement) return null;

  if (!assignment) {
    throw new GpuExecutionError(
      "GPU_ASSIGNMENT_MISSING",
      "GPU job has no device assignment; refusing to run it as a CPU-only job"
    );
  }

  if (assignment.vendor !== "nvidia") {
    throw new GpuExecutionError(
      "GPU_BACKEND_UNSUPPORTED",
      `GPU vendor '${assignment.vendor}' is not supported for Docker execution`
    );
  }
  if (requirement.vendor && requirement.vendor !== assignment.vendor) {
    throw new GpuExecutionError(
      "GPU_ASSIGNMENT_INVALID",
      `Assigned GPU vendor '${assignment.vendor}' does not satisfy the requested vendor '${requirement.vendor}'`
    );
  }
  if (requirement.backend && requirement.backend !== assignment.backend) {
    throw new GpuExecutionError(
      "GPU_ASSIGNMENT_INVALID",
      `Assigned backend '${assignment.backend}' does not satisfy the requested backend '${requirement.backend}'`
    );
  }
  if (
    assignment.count !== requirement.count ||
    assignment.devices.length !== requirement.count
  ) {
    throw new GpuExecutionError(
      "GPU_ASSIGNMENT_INVALID",
      `Assigned ${assignment.devices.length} GPU device(s) but the job requires ${requirement.count}`
    );
  }

  const resolved: ResolvedGpuDevice[] = [];
  const seenKeys = new Set<string>();

  for (const assigned of assignment.devices) {
    const spec = findAssignedDevice(assigned, inventory);
    if (!spec) {
      throw new GpuExecutionError(
        "GPU_DEVICE_NOT_FOUND",
        `Assigned GPU (${assigned.id ?? assigned.pciAddress ?? `index ${assigned.index}`}) is not present on this worker`
      );
    }

    const localVendor = gpuVendorOf(spec);
    if (localVendor !== assignment.vendor) {
      throw new GpuExecutionError(
        "GPU_ASSIGNMENT_INVALID",
        "Assigned GPU does not match the local device vendor"
      );
    }
    if (requirement.vendor && localVendor !== requirement.vendor) {
      throw new GpuExecutionError(
        "GPU_ASSIGNMENT_INVALID",
        `Local GPU vendor '${localVendor}' does not satisfy '${requirement.vendor}'`
      );
    }

    if (!spec.executionBackends?.includes(assignment.backend)) {
      throw new GpuExecutionError(
        "GPU_BACKEND_UNSUPPORTED",
        `GPU execution backend '${assignment.backend}' is not available on the assigned device`
      );
    }

    if (requirement.minMemoryMb !== undefined) {
      if (spec.memoryMb === undefined) {
        throw new GpuExecutionError(
          "GPU_VRAM_UNKNOWN",
          `Assigned GPU VRAM is unknown; cannot verify the ${requirement.minMemoryMb} MiB requirement`
        );
      }
      if (spec.memoryMb < requirement.minMemoryMb) {
        throw new GpuExecutionError(
          "GPU_VRAM_INSUFFICIENT",
          `Assigned GPU has ${spec.memoryMb} MiB VRAM but the job requires at least ${requirement.minMemoryMb} MiB`
        );
      }
    }

    const { key, stable } = gpuDeviceKey(spec);
    if (seenKeys.has(key)) {
      throw new GpuExecutionError(
        "GPU_ASSIGNMENT_INVALID",
        "GPU assignment contains the same physical device more than once"
      );
    }
    seenKeys.add(key);
    resolved.push({ spec, key, stable });
  }

  return {
    vendor: "nvidia",
    backend: assignment.backend,
    deviceIndices: resolved.map((device) => device.spec.index),
    deviceKeys: resolved.map((device) => device.key),
    devices: resolved.map((device) => device.spec),
  };
}
