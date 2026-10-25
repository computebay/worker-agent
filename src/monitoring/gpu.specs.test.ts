import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  allGpuMetadataComplete,
  mergeGpuLists,
  normalizePciAddress,
  parseNvidiaSmiOutput,
  totalKnownGpuMemoryMb,
  type GpuSpec,
} from "./gpu.specs.js";

describe("parseNvidiaSmiOutput", () => {
  test("parses one row per GPU with name, VRAM and driver metadata", () => {
    const gpus = parseNvidiaSmiOutput(
      "0, NVIDIA GeForce RTX 4090, 24564, 550.54.14, 8.9\n" +
        "1, NVIDIA A100-SXM4-40GB, 40960, 550.54.14, 8.0\n"
    );

    assert.equal(gpus.length, 2);
    assert.deepEqual(gpus[0], {
      index: 0,
      vendor: "NVIDIA",
      name: "NVIDIA GeForce RTX 4090",
      memoryMb: 24564,
      driverVersion: "550.54.14",
      computeCapability: "8.9",
      pciVendorId: "10de",
      discoverySource: "nvidia-smi",
    });
    assert.equal(gpus[1]?.name, "NVIDIA A100-SXM4-40GB");
    assert.equal(gpus[1]?.memoryMb, 40960);
  });

  test("supports the fallback query without a compute capability column", () => {
    const gpus = parseNvidiaSmiOutput(
      "0, Tesla T4, 15360, 535.104.05\n"
    );

    assert.equal(gpus.length, 1);
    assert.equal(gpus[0]?.computeCapability, undefined);
    assert.equal(gpus[0]?.driverVersion, "535.104.05");
  });

  test("treats N/A metadata as absent", () => {
    const gpus = parseNvidiaSmiOutput("0, Test GPU, N/A, N/A, N/A\n");

    assert.equal(gpus.length, 1);
    assert.equal(gpus[0]?.memoryMb, undefined);
    assert.equal(gpus[0]?.driverVersion, undefined);
    assert.equal(gpus[0]?.computeCapability, undefined);
  });

  test("skips blank and unparseable rows without dropping valid GPUs", () => {
    const gpus = parseNvidiaSmiOutput(
      "\nnot-a-gpu-row\n0, Valid GPU, 8192, 550.54.14, 8.6\n"
    );

    assert.equal(gpus.length, 1);
    assert.equal(gpus[0]?.name, "Valid GPU");
  });

  test("reads UUID and PCI bus ID when the query provides them", () => {
    const gpus = parseNvidiaSmiOutput(
      "0, NVIDIA L40S, 46068, 550.54.14, 8.9, GPU-1234, 00000000:01:00.0\n",
      { computeCapability: true, uuid: true, pciBusId: true }
    );

    assert.equal(gpus.length, 1);
    assert.equal(gpus[0]?.id, "GPU-1234");
    assert.equal(gpus[0]?.pciAddress, "0000:01:00.0");
  });

  test("reads UUID and PCI bus ID without compute capability", () => {
    const gpus = parseNvidiaSmiOutput(
      "2, NVIDIA T4, 15360, 535.104.05, GPU-abcd, 0000:02:00.0\n",
      { uuid: true, pciBusId: true }
    );

    assert.equal(gpus.length, 1);
    assert.equal(gpus[0]?.index, 2);
    assert.equal(gpus[0]?.id, "GPU-abcd");
    assert.equal(gpus[0]?.pciAddress, "0000:02:00.0");
  });
});

describe("normalizePciAddress", () => {
  test("normalizes 8-digit and 4-digit domains to sysfs form", () => {
    assert.equal(normalizePciAddress("00000000:01:00.0"), "0000:01:00.0");
    assert.equal(normalizePciAddress("0000:02:1A.0"), "0000:02:1a.0");
    assert.equal(normalizePciAddress("01:00.0"), "0000:01:00.0");
  });

  test("returns undefined for non-PCI values", () => {
    assert.equal(normalizePciAddress("N/A"), undefined);
    assert.equal(normalizePciAddress("platform/soc:gpu"), undefined);
  });
});

describe("mergeGpuLists", () => {
  const nvidia: GpuSpec = {
    index: 0,
    vendor: "NVIDIA",
    name: "NVIDIA L40S",
    memoryMb: 46068,
    driverVersion: "550.54.14",
    pciVendorId: "10de",
    pciAddress: "0000:01:00.0",
    discoverySource: "nvidia-smi",
  };

  test("deduplicates sysfs entries by PCI address and fills missing fields", () => {
    const sysfs: GpuSpec = {
      index: 0,
      vendor: "NVIDIA",
      pciVendorId: "10de",
      pciDeviceId: "26b9",
      pciAddress: "0000:01:00.0",
      discoverySource: "linux-sysfs",
    };

    const result = mergeGpuLists([nvidia], [sysfs]);

    assert.equal(result.gpus.length, 1);
    assert.equal(result.mergedCount, 1);
    assert.equal(result.gpus[0]?.name, "NVIDIA L40S");
    assert.equal(result.gpus[0]?.pciDeviceId, "26b9");
    assert.equal(result.gpus[0]?.id, "pci:0000:01:00.0");
  });

  test("matches an old nvidia-smi row without PCI identity by vendor and VRAM", () => {
    const legacyNvidia: GpuSpec = {
      index: 0,
      vendor: "NVIDIA",
      name: "NVIDIA T4",
      memoryMb: 15360,
      discoverySource: "nvidia-smi",
    };
    const sysfs: GpuSpec = {
      index: 0,
      vendor: "NVIDIA",
      pciVendorId: "10de",
      pciAddress: "0000:02:00.0",
      discoverySource: "linux-sysfs",
    };

    const result = mergeGpuLists([legacyNvidia], [sysfs]);

    assert.equal(result.gpus.length, 1);
    assert.equal(result.gpus[0]?.pciAddress, "0000:02:00.0");
    assert.equal(result.gpus[0]?.id, "pci:0000:02:00.0");
  });

  test("appends sysfs-only GPUs after nvidia indices", () => {
    const amd: GpuSpec = {
      index: 0,
      vendor: "AMD",
      pciVendorId: "1002",
      pciAddress: "0000:03:00.0",
      discoverySource: "linux-sysfs",
    };

    const result = mergeGpuLists([nvidia], [amd]);

    assert.equal(result.gpus.length, 2);
    assert.equal(result.gpus[0]?.index, 0);
    assert.equal(result.gpus[1]?.vendor, "AMD");
    assert.equal(result.gpus[1]?.index, 1);
  });

  test("reindexes sysfs-only discovery from zero", () => {
    const amd: GpuSpec = {
      index: 5,
      vendor: "AMD",
      pciVendorId: "1002",
      pciAddress: "0000:03:00.0",
      discoverySource: "linux-sysfs",
    };
    const intel: GpuSpec = {
      index: 9,
      vendor: "Intel",
      pciVendorId: "8086",
      pciAddress: "0000:04:00.0",
      discoverySource: "linux-sysfs",
    };

    const result = mergeGpuLists([], [amd, intel]);

    assert.deepEqual(result.gpus.map((gpu) => gpu.index), [0, 1]);
  });
});

describe("GPU metadata helpers", () => {
  test("does not aggregate memory when any GPU has unknown VRAM", () => {
    const complete: GpuSpec = {
      index: 0,
      vendor: "AMD",
      memoryMb: 8192,
      discoverySource: "linux-sysfs",
    };
    const unknown: GpuSpec = {
      index: 1,
      vendor: "Intel",
      discoverySource: "linux-sysfs",
    };

    assert.equal(totalKnownGpuMemoryMb([complete]), 8192);
    assert.equal(totalKnownGpuMemoryMb([complete, unknown]), undefined);
    assert.equal(totalKnownGpuMemoryMb([]), undefined);
  });

  test("requires model, VRAM and driver before metadata is complete", () => {
    const complete: GpuSpec = {
      index: 0,
      vendor: "NVIDIA",
      name: "NVIDIA L40S",
      memoryMb: 46068,
      driverVersion: "550.54.14",
      discoverySource: "nvidia-smi",
    };

    assert.equal(allGpuMetadataComplete([complete]), true);
    assert.equal(
      allGpuMetadataComplete([{ ...complete, driverVersion: undefined }]),
      false
    );
  });
});
