import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  GpuExecutionError,
  gpuDeviceKey,
  parseGpuAssignment,
  parseGpuRequirement,
  resolveGpuAssignment,
} from "./gpu.requirement.js";
import type { GpuSpec } from "./gpu.types.js";

function spec(index: number, overrides: Partial<GpuSpec> = {}): GpuSpec {
  return {
    id: `GPU-${index}`,
    index,
    vendor: "NVIDIA",
    name: "NVIDIA A100",
    memoryMb: 40960,
    driverVersion: "550.54.14",
    pciVendorId: "10de",
    pciAddress: `0000:01:00.${index}`,
    discoverySource: "nvidia-smi",
    executionBackends: ["cuda", "nvenc"],
    ...overrides,
  };
}

describe("parseGpuRequirement (worker)", () => {
  test("accepts legacy and canonical forms", () => {
    assert.equal(parseGpuRequirement("none").requirement, null);
    assert.deepEqual(parseGpuRequirement("any").requirement, {
      required: true,
      count: 1,
    });
    assert.deepEqual(
      parseGpuRequirement({ required: true, count: 2, backend: "cuda" })
        .requirement,
      { required: true, count: 2, vendor: "nvidia", backend: "cuda" },
    );
  });

  test("rejects invalid values", () => {
    assert.ok(parseGpuRequirement("sometimes").error);
    assert.ok(parseGpuRequirement({ required: true, count: 0 }).error);
    assert.ok(parseGpuRequirement({ required: true, minMemoryMb: -1 }).error);
    assert.ok(
      parseGpuRequirement({ required: true, vendor: "amd", backend: "nvenc" })
        .error,
    );
    assert.ok(
      parseGpuRequirement({ required: false, count: 1 }).error,
    );
  });
});

describe("parseGpuAssignment", () => {
  test("accepts a valid assignment", () => {
    const { assignment, error } = parseGpuAssignment({
      vendor: "nvidia",
      backend: "cuda",
      count: 1,
      devices: [{ id: "GPU-0", index: 0, vendor: "nvidia" }],
    });
    assert.equal(error, undefined);
    assert.equal(assignment?.devices[0]?.id, "GPU-0");
  });

  test("rejects malformed assignments", () => {
    assert.ok(parseGpuAssignment("cuda").error);
    assert.ok(parseGpuAssignment({ vendor: 1 }).error);
    assert.ok(
      parseGpuAssignment({
        vendor: "nvidia",
        backend: "cuda",
        count: 1,
        devices: [],
      }).error,
    );
    assert.ok(
      parseGpuAssignment({
        vendor: "nvidia",
        backend: "cuda",
        count: 2,
        devices: [{ index: 0 }],
      }).error,
    );
    assert.ok(
      parseGpuAssignment({
        vendor: "nvidia",
        backend: "cuda",
        count: 1,
        devices: [{ index: 0, vendor: "amd" }],
      }).error,
    );
  });

  test("returns no assignment when the field is absent", () => {
    const { assignment, error } = parseGpuAssignment(undefined);
    assert.equal(error, undefined);
    assert.equal(assignment, undefined);
  });
});

describe("resolveGpuAssignment", () => {
  test("fails safely when a GPU job has no assignment", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1 },
          undefined,
          [spec(0)],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError && err.code === "GPU_ASSIGNMENT_MISSING",
    );
  });

  test("resolves assigned devices to local indices", () => {
    const plan = resolveGpuAssignment(
      { required: true, count: 2 },
      {
        vendor: "nvidia",
        backend: "cuda",
        count: 2,
        devices: [
          { id: "GPU-1", index: 5, vendor: "nvidia" },
          { id: "GPU-0", index: 0, vendor: "nvidia" },
        ],
      },
      [spec(0), spec(1)],
    );
    // Assignment order is preserved: each assigned identity maps to its
    // current local index (GPU-1 → 1, GPU-0 → 0).
    assert.deepEqual(plan?.deviceIndices, [1, 0]);
    assert.deepEqual(plan?.deviceKeys, ["id:GPU-1", "id:GPU-0"]);
  });

  test("rejects devices missing from local inventory", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1 },
          {
            vendor: "nvidia",
            backend: "cuda",
            count: 1,
            devices: [{ id: "GPU-9", index: 9, vendor: "nvidia" }],
          },
          [spec(0)],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError && err.code === "GPU_DEVICE_NOT_FOUND",
    );
  });

  test("does not trust stale indices when a stable id exists", () => {
    // Scheduler recorded index 7 (stale) but identity GPU-0 is local index 0.
    const plan = resolveGpuAssignment(
      { required: true, count: 1 },
      {
        vendor: "nvidia",
        backend: "cuda",
        count: 1,
        devices: [{ id: "GPU-0", index: 7, vendor: "nvidia" }],
      },
      [spec(0)],
    );
    assert.deepEqual(plan?.deviceIndices, [0]);
  });

  test("falls back to index matching only without stable identity", () => {
    const local = spec(0, { id: undefined, pciAddress: undefined });
    const plan = resolveGpuAssignment(
      { required: true, count: 1 },
      {
        vendor: "nvidia",
        backend: "cuda",
        count: 1,
        devices: [{ index: 0, vendor: "nvidia" }],
      },
      [local],
    );
    assert.deepEqual(plan?.deviceIndices, [0]);
  });

  test("rejects vendor/backend mismatches", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1, vendor: "nvidia", backend: "nvenc" },
          {
            vendor: "nvidia",
            backend: "cuda",
            count: 1,
            devices: [{ id: "GPU-0", index: 0, vendor: "nvidia" }],
          },
          [spec(0)],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError &&
        err.code === "GPU_ASSIGNMENT_INVALID",
    );
  });

  test("rejects devices without the verified backend", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1 },
          {
            vendor: "nvidia",
            backend: "nvenc",
            count: 1,
            devices: [{ id: "GPU-0", index: 0, vendor: "nvidia" }],
          },
          [spec(0, { executionBackends: ["cuda"] })],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError &&
        err.code === "GPU_BACKEND_UNSUPPORTED",
    );
  });

  test("unknown VRAM cannot satisfy a VRAM requirement", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1, minMemoryMb: 8192 },
          {
            vendor: "nvidia",
            backend: "cuda",
            count: 1,
            devices: [{ id: "GPU-0", index: 0, vendor: "nvidia" }],
          },
          [spec(0, { memoryMb: undefined })],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError && err.code === "GPU_VRAM_UNKNOWN",
    );
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 1, minMemoryMb: 8192 },
          {
            vendor: "nvidia",
            backend: "cuda",
            count: 1,
            devices: [{ id: "GPU-0", index: 0, vendor: "nvidia" }],
          },
          [spec(0, { memoryMb: 4096 })],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError &&
        err.code === "GPU_VRAM_INSUFFICIENT",
    );
  });

  test("rejects duplicate physical devices in one assignment", () => {
    assert.throws(
      () =>
        resolveGpuAssignment(
          { required: true, count: 2 },
          {
            vendor: "nvidia",
            backend: "cuda",
            count: 2,
            devices: [
              { id: "GPU-0", index: 0, vendor: "nvidia" },
              { id: "GPU-0", index: 0, vendor: "nvidia" },
            ],
          },
          [spec(0)],
        ),
      (err: unknown) =>
        err instanceof GpuExecutionError &&
        err.code === "GPU_ASSIGNMENT_INVALID",
    );
  });
});

describe("gpuDeviceKey (worker)", () => {
  test("stable identity beats index fallback", () => {
    assert.deepEqual(gpuDeviceKey(spec(2)), {
      key: "id:GPU-2",
      stable: true,
    });
    assert.deepEqual(
      gpuDeviceKey(spec(2, { id: undefined, pciAddress: "0000:03:00.0" })),
      { key: "pci:0000:03:00.0", stable: true },
    );
    assert.deepEqual(
      gpuDeviceKey(spec(2, { id: undefined, pciAddress: undefined })),
      { key: "index:2", stable: false },
    );
  });
});
