import { existsSync } from "node:fs";
import { discoverLinuxGpus } from "./gpu.linux.js";
import { discoverNvidiaGpus, parseNvidiaSmiOutput } from "./gpu.nvidia.js";
import {
  normalizePciAddress,
  type GpuDiscoveryResult,
  type GpuDiscoverySource,
  type GpuSpec,
} from "./gpu.types.js";

export type {
  GpuDiscoveryDiagnostics,
  GpuDiscoveryResult,
  GpuDiscoverySource,
  GpuSpec,
  UnclassifiedGpuDevice,
} from "./gpu.types.js";
export {
  allGpuMetadataComplete,
  isGpuMetadataComplete,
  missingGpuMetadata,
  normalizePciAddress,
  normalizePciId,
  totalKnownGpuMemoryMb,
} from "./gpu.types.js";
export { parseNvidiaSmiOutput };

/** Device nodes that indicate a GPU is present on the host (diagnostics only). */
const GPU_DEVICE_PATHS = [
  "/dev/nvidia0",
  "/dev/nvidiactl",
  "/dev/dri",
];

export interface GpuMergeResult {
  gpus: GpuSpec[];
  /** Number of sysfs entries that enriched an existing nvidia-smi entry. */
  mergedCount: number;
}

function fillMissing(target: GpuSpec, candidate: GpuSpec): void {
  if (!target.name && candidate.name) target.name = candidate.name;
  if (target.memoryMb === undefined && candidate.memoryMb !== undefined) {
    target.memoryMb = candidate.memoryMb;
  }
  if (!target.driverVersion && candidate.driverVersion) {
    target.driverVersion = candidate.driverVersion;
  }
  if (!target.vendor && candidate.vendor) target.vendor = candidate.vendor;
  if (!target.pciVendorId && candidate.pciVendorId) {
    target.pciVendorId = candidate.pciVendorId;
  }
  if (!target.pciDeviceId && candidate.pciDeviceId) {
    target.pciDeviceId = candidate.pciDeviceId;
  }
  if (!target.pciAddress && candidate.pciAddress) {
    target.pciAddress = candidate.pciAddress;
  }
  if (!target.id) {
    target.id =
      candidate.id ??
      (target.pciAddress ? `pci:${target.pciAddress}` : undefined);
  }
}

/**
 * Merge NVIDIA-tooling results with generic Linux discovery.
 *
 * nvidia-smi is authoritative for the properties it reports, while sysfs can
 * fill in a PCI identity or a missing value. Entries are deduplicated by PCI
 * address. For very old nvidia-smi versions that cannot report a PCI address,
 * a best-effort vendor+VRAM match prevents the sysfs pass from duplicating the
 * same hardware.
 */
export function mergeGpuLists(
  nvidiaGpus: GpuSpec[],
  linuxGpus: GpuSpec[],
): GpuMergeResult {
  const merged = nvidiaGpus.map((gpu) => ({ ...gpu }));
  const byAddress = new Map<string, GpuSpec>();

  for (const gpu of merged) {
    const address = normalizePciAddress(gpu.pciAddress);
    if (!address) continue;
    gpu.pciAddress = address;
    byAddress.set(address, gpu);
  }

  const appended: GpuSpec[] = [];
  let mergedCount = 0;

  for (const candidate of linuxGpus) {
    const address = normalizePciAddress(candidate.pciAddress);
    let target = address ? byAddress.get(address) : undefined;

    if (!target && candidate.vendor === "NVIDIA") {
      target = merged.find(
        (gpu) =>
          !gpu.pciAddress &&
          gpu.vendor === candidate.vendor &&
          (gpu.memoryMb === undefined ||
            candidate.memoryMb === undefined ||
            gpu.memoryMb === candidate.memoryMb),
      );
    }

    if (target) {
      fillMissing(target, candidate);
      if (address) {
        target.pciAddress = address;
        byAddress.set(address, target);
      }
      mergedCount += 1;
      continue;
    }

    appended.push({
      ...candidate,
      ...(address ? { pciAddress: address } : {}),
    });
  }

  let nextIndex =
    merged.length > 0 ? Math.max(...merged.map((gpu) => gpu.index)) + 1 : 0;
  for (const gpu of appended) {
    gpu.index = nextIndex++;
  }

  return {
    gpus: [...merged, ...appended].sort((a, b) => a.index - b.index),
    mergedCount,
  };
}

/**
 * Run every discovery adapter and normalize the result.
 *
 * Failure of one adapter never prevents the other from reporting hardware.
 * Execution readiness is intentionally not probed here; a discovered GPU is
 * not necessarily a GPU that a workload can execute on.
 */
export function discoverGpus(): GpuDiscoveryResult {
  const nvidia = discoverNvidiaGpus();
  const linux = discoverLinuxGpus();
  const merged = mergeGpuLists(nvidia.gpus, linux.gpus);

  const sources = new Set<GpuDiscoverySource>(
    merged.gpus.map((gpu) => gpu.discoverySource),
  );
  if (merged.mergedCount > 0) sources.add("linux-sysfs");

  return {
    gpus: merged.gpus,
    diagnostics: {
      nvidiaSmiSucceeded: nvidia.succeeded,
      ...(nvidia.error ? { nvidiaSmiError: nvidia.error } : {}),
      linuxSysfsSucceeded: linux.succeeded,
      ...(linux.error ? { linuxSysfsError: linux.error } : {}),
      deviceNodesPresent: hasGpuDeviceNodes(),
      sources: [...sources],
      unclassifiedDevices: linux.unclassifiedDevices,
    },
  };
}

/**
 * Backward-compatible wrapper for callers that only need the GPU list.
 * Prefer `discoverGpus()` when diagnostics are useful.
 */
export function detectGpuSpecs(): GpuSpec[] {
  return discoverGpus().gpus;
}

/** True when GPU/DRM device nodes are visible (diagnostics only). */
export function hasGpuDeviceNodes(): boolean {
  return GPU_DEVICE_PATHS.some((devicePath) => existsSync(devicePath));
}
