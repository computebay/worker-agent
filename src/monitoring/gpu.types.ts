/**
 * Normalized GPU metadata shared by every discovery adapter.
 *
 * Only properties that can be established reliably are populated. In
 * particular, `memoryMb` and `name` are optional: a GPU discovered through
 * sysfs may have no trustworthy model/VRAM source, and representing that as
 * zero or a fabricated name would mislead the scheduler.
 */
export type GpuDiscoverySource = "nvidia-smi" | "linux-sysfs";

export interface GpuSpec {
  /** Stable per-device identifier when available (NVIDIA UUID or PCI address). */
  id?: string;
  /** Index within the normalized discovery result. Not a device-node number. */
  index: number;
  vendor?: string;
  name?: string;
  /** Dedicated VRAM in MiB. Omitted when the value is unknown. */
  memoryMb?: number;
  driverVersion?: string;
  computeCapability?: string;
  /** Normalized PCI vendor ID, e.g. `10de`. */
  pciVendorId?: string;
  /** Normalized PCI device ID, e.g. `2684`. */
  pciDeviceId?: string;
  /**
   * Normalized PCI address, e.g. `0000:01:00.0`.
   * This is the stable identity used to deduplicate device nodes.
   */
  pciAddress?: string;
  discoverySource: GpuDiscoverySource;
  /**
   * Docker execution backends verified for this device, e.g. `["cuda",
   * "nvenc"]`. Absent or empty when discovery found hardware but the worker
   * could not verify that Docker can expose it. Discovery alone never sets it.
   */
  executionBackends?: string[];
}

/** A display device that could not be tied to a supported GPU vendor. */
export interface UnclassifiedGpuDevice {
  devicePath: string;
  pciAddress?: string;
  pciVendorId?: string;
  pciDeviceId?: string;
  reason: string;
}

export interface GpuDiscoveryDiagnostics {
  nvidiaSmiSucceeded: boolean;
  nvidiaSmiError?: string;
  linuxSysfsSucceeded: boolean;
  linuxSysfsError?: string;
  /** True when at least one `/dev` GPU/DRM node is visible. */
  deviceNodesPresent: boolean;
  /** Sources that actually contributed at least one GPU to the result. */
  sources: GpuDiscoverySource[];
  unclassifiedDevices: UnclassifiedGpuDevice[];
}

export interface GpuDiscoveryResult {
  gpus: GpuSpec[];
  diagnostics: GpuDiscoveryDiagnostics;
}

/** USB/PCI-style vendor IDs are always compared in lowercase without `0x`. */
export function normalizePciId(raw: string | null | undefined): string | undefined {
  if (!raw) return undefined;
  const value = raw.trim().toLowerCase().replace(/^0x/, "");
  if (!/^[0-9a-f]+$/.test(value)) return undefined;
  return value;
}

/**
 * Normalize a PCI address to the sysfs 4-digit-domain form.
 *
 * `nvidia-smi` reports an 8-digit domain on some drivers
 * (`00000000:01:00.0`), while sysfs uses `0000:01:00.0`. Both normalize to
 * the same value so cross-source deduplication works.
 */
export function normalizePciAddress(raw: string | null | undefined): string | undefined {
  if (!raw) return undefined;
  const value = raw.trim().toLowerCase();
  const match = /^(?:([0-9a-f]+):)?([0-9a-f]{2}):([0-9a-f]{2})\.([0-7])$/.exec(value);
  if (!match) return undefined;

  const [, domain = "0", bus, device, fn] = match;
  return `${domain.padStart(4, "0").slice(-4)}:${bus}:${device}.${fn}`;
}

/** Properties required before metadata is considered complete. */
export function missingGpuMetadata(gpu: GpuSpec): string[] {
  const missing: string[] = [];
  if (!gpu.name) missing.push("model");
  if (gpu.memoryMb === undefined) missing.push("vram");
  if (!gpu.driverVersion) missing.push("driverVersion");
  return missing;
}

export function isGpuMetadataComplete(gpu: GpuSpec): boolean {
  return missingGpuMetadata(gpu).length === 0;
}

export function allGpuMetadataComplete(gpus: GpuSpec[]): boolean {
  return gpus.length > 0 && gpus.every(isGpuMetadataComplete);
}

/**
 * Aggregate VRAM is only meaningful when every GPU reports a known value.
 * Returning `undefined` (instead of 0 or a partial sum) prevents the scheduler
 * from misrepresenting partial metadata as total capacity.
 */
export function totalKnownGpuMemoryMb(gpus: GpuSpec[]): number | undefined {
  if (gpus.length === 0) return undefined;
  if (!gpus.every((gpu) => typeof gpu.memoryMb === "number" && gpu.memoryMb >= 0)) {
    return undefined;
  }
  return gpus.reduce((total, gpu) => total + (gpu.memoryMb as number), 0);
}
