import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { WorkerPool } from "./worker.pool.js";
import { ResourceMonitor } from "../monitoring/resource.monitor.js";
import type { GpuSpec } from "../monitoring/gpu.types.js";
import type { ScheduledJob } from "../types/job.types.js";

function nvidiaSpec(index: number): GpuSpec {
  return {
    id: `GPU-${index}`,
    index,
    vendor: "NVIDIA",
    name: "NVIDIA A100",
    memoryMb: 40960,
    driverVersion: "550.54.14",
    pciVendorId: "10de",
    discoverySource: "nvidia-smi",
    executionBackends: ["cuda", "nvenc"],
  };
}

function job(
  jobId: string,
  gpu: ScheduledJob["gpu"],
  devices: Array<{ id: string; index: number }> = [],
): ScheduledJob {
  return {
    jobId,
    type: "batch",
    image: "node:20",
    startCommand: "true",
    repoUrl: "https://github.com/owner/repo",
    branch: "main",
    cpu: 1,
    memoryMB: 512,
    networkEnabled: false,
    hasArtifacts: false,
    gpu,
    ...(devices.length > 0
      ? {
          gpuAssignment: {
            vendor: "nvidia",
            backend: "cuda",
            count: devices.length,
            devices: devices.map((device) => ({
              ...device,
              vendor: "nvidia",
            })),
          },
        }
      : {}),
  } as ScheduledJob;
}

function makePool(gpus: GpuSpec[]): WorkerPool {
  return new WorkerPool({
    nodeId: "test-node",
    resourceMonitor: new ResourceMonitor({
      allocatableCpu: 8,
      allocatableMemoryMb: 16384,
    }),
    maxConcurrency: 4,
    jobTimeoutMs: 1000,
    onJobResult: () => {},
    gpus,
  });
}

describe("WorkerPool GPU acceptance", () => {
  test("accepts CPU-only jobs without a GPU inventory", () => {
    const pool = makePool([]);
    assert.equal(pool.canAccept(job("j1", null)), true);
  });

  test("accepts a valid GPU assignment", () => {
    const pool = makePool([nvidiaSpec(0), nvidiaSpec(1)]);
    assert.equal(
      pool.canAccept(
        job("j1", { required: true, count: 1 }, [
          { id: "GPU-0", index: 0 },
        ]),
      ),
      true,
    );
  });

  test("accepts GPU jobs with permanent GPU problems so run() can fail them", () => {
    // A missing/invalid assignment must not be requeued forever: it is
    // accepted here and reported as a normal job failure with an actionable
    // error when the reservation is attempted.
    const pool = makePool([nvidiaSpec(0)]);
    assert.equal(
      pool.canAccept(job("j1", { required: true, count: 1 })),
      true,
    );
    assert.equal(
      pool.canAccept(
        job("j2", { required: true, count: 1 }, [
          { id: "GPU-9", index: 9 },
        ]),
      ),
      true,
    );
  });
});
