import { readFileSync, readdirSync, realpathSync } from "node:fs";
import { basename, join } from "node:path";
import {
  normalizePciAddress,
  normalizePciId,
  type GpuSpec,
  type UnclassifiedGpuDevice,
} from "./gpu.types.js";

/** PCI vendor IDs we can identify without vendor management tooling. */
const SUPPORTED_VENDORS: Record<string, string> = {
  "10de": "NVIDIA",
  "1002": "AMD",
  "8086": "Intel",
};

const DRM_CARD_RE = /^card\d+$/;
const PCI_ADDRESS_RE = /^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]$/i;

export interface LinuxDiscoveryOptions {
  /** Defaults to `/sys/class/drm`. */
  drmRoot?: string;
  /** Defaults to `/sys/bus/pci/devices`. */
  pciRoot?: string;
  /** Defaults to `/sys/module`. */
  moduleRoot?: string;
  /** Defaults to `/proc`. */
  procRoot?: string;
}

export interface LinuxDiscoveryResult {
  gpus: GpuSpec[];
  unclassifiedDevices: UnclassifiedGpuDevice[];
  succeeded: boolean;
  error?: string;
}

interface ReadResult {
  entries: string[];
  error?: string;
}

function readText(path: string): string | undefined {
  try {
    const value = readFileSync(path, "utf8").trim();
    return value.length > 0 ? value : undefined;
  } catch {
    return undefined;
  }
}

function readPciId(path: string): string | undefined {
  return normalizePciId(readText(path));
}

function safeRealpath(path: string): string | undefined {
  try {
    return realpathSync(path);
  } catch {
    return undefined;
  }
}

function readDirectory(path: string): ReadResult {
  try {
    return { entries: readdirSync(path) };
  } catch (err) {
    const error = err as NodeJS.ErrnoException;
    return { entries: [], error: error.code ?? error.message };
  }
}

function naturalSort(a: string, b: string): number {
  return a.localeCompare(b, undefined, { numeric: true, sensitivity: "base" });
}

function extractPciAddress(resolvedDevicePath: string): string | undefined {
  // Only a device whose resolved path ends in a PCI address has a PCI
  // identity. Falling back to an address elsewhere in the path could mistake a
  // parent bridge for the GPU.
  return normalizePciAddress(basename(resolvedDevicePath));
}

function isDisplayClass(classCode: string | undefined): boolean {
  return classCode?.startsWith("03") ?? false;
}

function readDriverName(devicePath: string): string | undefined {
  const resolvedDriver = safeRealpath(join(devicePath, "driver"));
  if (resolvedDriver) return basename(resolvedDriver);

  const uevent = readText(join(devicePath, "uevent"));
  const match = uevent ? /^DRIVER=(.+)$/m.exec(uevent) : null;
  return match?.[1]?.trim() || undefined;
}

function readDriverVersion(
  moduleRoot: string,
  driverName: string | undefined,
  vendorId: string | undefined,
): string | undefined {
  const candidates = new Set<string>();
  if (driverName) candidates.add(driverName);
  // The NVIDIA kernel module is named `nvidia` even if the device's `driver`
  // link is not readable.
  if (vendorId === "10de") candidates.add("nvidia");

  for (const driver of candidates) {
    const version = readText(join(moduleRoot, driver, "version"));
    if (version && version !== "N/A") return version;
  }
  return undefined;
}

function readModelName(
  devicePath: string,
  pciAddress: string | undefined,
  procRoot: string,
  vendorId: string | undefined,
): string | undefined {
  const productName =
    readText(join(devicePath, "product_name")) ??
    readText(join(devicePath, "device", "product_name"));
  if (productName && productName !== "N/A") return productName;

  // The NVIDIA kernel driver publishes a stable model string even when
  // nvidia-smi is unavailable. This is a Linux interface, not vendor tooling.
  if (vendorId === "10de" && pciAddress) {
    const information = readText(
      join(procRoot, "driver", "nvidia", "gpus", pciAddress, "information"),
    );
    const model = information ? /^Model:\s*(.+)$/m.exec(information) : null;
    if (model?.[1]?.trim() && model[1].trim() !== "N/A") {
      return model[1].trim();
    }
  }

  return undefined;
}

/**
 * Read dedicated VRAM from a trustworthy sysfs source.
 *
 * `mem_info_vram_total` (amdgpu) and `lmem_total_bytes` (Intel discrete) are
 * byte counters. A zero value usually means shared/system memory rather than
 * an actual dedicated allocation, so it is reported as unknown instead of 0.
 */
function readMemoryMb(devicePath: string): number | undefined {
  const candidates = ["mem_info_vram_total", "lmem_total_bytes"];

  for (const fileName of candidates) {
    const raw = readText(join(devicePath, fileName));
    if (!raw) continue;

    const bytes = Number.parseInt(raw, 10);
    if (!Number.isFinite(bytes) || bytes <= 0) continue;

    return Math.max(1, Math.round(bytes / (1024 * 1024)));
  }

  return undefined;
}

function buildGpu(input: {
  index: number;
  devicePath: string;
  vendorId: string;
  deviceId?: string;
  pciAddress?: string;
  moduleRoot: string;
  procRoot: string;
}): GpuSpec {
  const { vendorId, deviceId, pciAddress } = input;
  const driverName = readDriverName(input.devicePath);
  const driverVersion = readDriverVersion(
    input.moduleRoot,
    driverName,
    vendorId,
  );
  const name = readModelName(
    input.devicePath,
    pciAddress,
    input.procRoot,
    vendorId,
  );
  const memoryMb = readMemoryMb(input.devicePath);

  return {
    ...(pciAddress ? { id: `pci:${pciAddress}` } : {}),
    index: input.index,
    vendor: SUPPORTED_VENDORS[vendorId],
    ...(name ? { name } : {}),
    ...(memoryMb !== undefined ? { memoryMb } : {}),
    ...(driverVersion ? { driverVersion } : {}),
    pciVendorId: vendorId,
    ...(deviceId ? { pciDeviceId: deviceId } : {}),
    ...(pciAddress ? { pciAddress } : {}),
    discoverySource: "linux-sysfs",
  };
}

function unclassifiedReason(vendorId: string | undefined): string {
  if (!vendorId) return "PCI vendor ID unavailable";
  return `unsupported PCI vendor 0x${vendorId}`;
}

/**
 * Discover GPUs through Linux sysfs and PCI identifiers.
 *
 * Only `/sys/class/drm/cardN` entries are considered as DRM devices, so
 * render nodes and connectors are never counted as additional GPUs. Multiple
 * nodes pointing at the same hardware device are deduplicated by resolved
 * device path and PCI address. Devices without a supported PCI vendor are
 * reported as diagnostics rather than fabricated GPUs.
 */
export function discoverLinuxGpus(
  options: LinuxDiscoveryOptions = {},
): LinuxDiscoveryResult {
  const drmRoot = options.drmRoot ?? "/sys/class/drm";
  const pciRoot = options.pciRoot ?? "/sys/bus/pci/devices";
  const moduleRoot = options.moduleRoot ?? "/sys/module";
  const procRoot = options.procRoot ?? "/proc";

  const gpus: GpuSpec[] = [];
  const unclassifiedDevices: UnclassifiedGpuDevice[] = [];
  const seenDevicePaths = new Set<string>();
  const seenAddresses = new Set<string>();
  const reportedUnclassified = new Set<string>();

  const addUnclassified = (device: UnclassifiedGpuDevice) => {
    const key = device.pciAddress ?? device.devicePath;
    if (reportedUnclassified.has(key)) return;
    reportedUnclassified.add(key);
    unclassifiedDevices.push(device);
  };

  const drm = readDirectory(drmRoot);
  const drmCards = drm.entries
    .filter((entry) => DRM_CARD_RE.test(entry))
    .sort(naturalSort);

  for (const card of drmCards) {
    const cardPath = join(drmRoot, card);
    const devicePath = safeRealpath(join(cardPath, "device"));

    if (!devicePath) {
      addUnclassified({
        devicePath: cardPath,
        reason: "DRM card has no resolvable hardware device",
      });
      continue;
    }

    if (seenDevicePaths.has(devicePath)) continue;
    seenDevicePaths.add(devicePath);

    const vendorId = readPciId(join(devicePath, "vendor"));
    const deviceId = readPciId(join(devicePath, "device"));
    const classCode = readPciId(join(devicePath, "class"));
    const pciAddress = extractPciAddress(devicePath);

    if (pciAddress && seenAddresses.has(pciAddress)) continue;

    if (!vendorId || !SUPPORTED_VENDORS[vendorId]) {
      addUnclassified({
        devicePath: cardPath,
        ...(pciAddress ? { pciAddress } : {}),
        ...(vendorId ? { pciVendorId: vendorId } : {}),
        ...(deviceId ? { pciDeviceId: deviceId } : {}),
        reason: unclassifiedReason(vendorId),
      });
      continue;
    }

    if (classCode && !isDisplayClass(classCode)) {
      addUnclassified({
        devicePath: cardPath,
        ...(pciAddress ? { pciAddress } : {}),
        pciVendorId: vendorId,
        ...(deviceId ? { pciDeviceId: deviceId } : {}),
        reason: `not a display controller (PCI class 0x${classCode})`,
      });
      continue;
    }

    if (pciAddress) seenAddresses.add(pciAddress);

    gpus.push(
      buildGpu({
        index: gpus.length,
        devicePath,
        vendorId,
        deviceId,
        pciAddress,
        moduleRoot,
        procRoot,
      }),
    );
  }

  // A GPU can be present without a DRM node (for example a headless server
  // with a minimal kernel config). Scan PCI display controllers to fill those
  // gaps, while ignoring audio/usb functions on the same card.
  const pci = readDirectory(pciRoot);
  const pciDevices = pci.entries
    .filter((entry) => PCI_ADDRESS_RE.test(entry))
    .sort(naturalSort);

  for (const pciEntry of pciDevices) {
    const pciAddress = normalizePciAddress(pciEntry);
    if (!pciAddress || seenAddresses.has(pciAddress)) continue;

    const devicePath = join(pciRoot, pciEntry);
    const vendorId = readPciId(join(devicePath, "vendor"));
    const deviceId = readPciId(join(devicePath, "device"));
    const classCode = readPciId(join(devicePath, "class"));

    // An unreadable class cannot prove this is a display controller.
    if (!classCode || !isDisplayClass(classCode)) continue;

    if (!vendorId || !SUPPORTED_VENDORS[vendorId]) {
      addUnclassified({
        devicePath,
        pciAddress,
        ...(vendorId ? { pciVendorId: vendorId } : {}),
        ...(deviceId ? { pciDeviceId: deviceId } : {}),
        reason: unclassifiedReason(vendorId),
      });
      continue;
    }

    seenAddresses.add(pciAddress);

    gpus.push(
      buildGpu({
        index: gpus.length,
        devicePath,
        vendorId,
        deviceId,
        pciAddress,
        moduleRoot,
        procRoot,
      }),
    );
  }

  // Succeed when at least one sysfs interface was readable, even if it
  // contained no supported GPU.
  const readSucceeded = drm.error === undefined || pci.error === undefined;
  const errors = [
    drm.error ? `DRM: ${drm.error}` : undefined,
    pci.error ? `PCI: ${pci.error}` : undefined,
  ].filter((value): value is string => Boolean(value));

  return {
    gpus,
    unclassifiedDevices,
    succeeded: readSucceeded,
    ...(readSucceeded ? {} : { error: errors.join("; ") || "sysfs unavailable" }),
  };
}
