import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  buildContainerGpuArgs,
  classifyDockerGpuError,
  redactDockerArgs,
} from "./container.runtime.js";
import { GpuExecutionError } from "../monitoring/gpu.requirement.js";

describe("buildContainerGpuArgs", () => {
  test("emits one argv element per flag with only assigned devices", () => {
    const args = buildContainerGpuArgs({
      vendor: "nvidia",
      backend: "cuda",
      deviceIndices: [0, 1],
    });
    assert.deepEqual(args, ["--gpus", "device=0,1"]);
  });

  test("rejects unsupported vendors/backends", () => {
    assert.throws(
      () =>
        buildContainerGpuArgs({
          vendor: "amd",
          backend: "cuda",
          deviceIndices: [0],
        }),
      (err: unknown) =>
        err instanceof GpuExecutionError && err.code === "DOCKER_GPU_UNSUPPORTED",
    );
    assert.throws(
      () =>
        buildContainerGpuArgs({
          vendor: "nvidia",
          backend: "vulkan",
          deviceIndices: [0],
        }),
      (err: unknown) =>
        err instanceof GpuExecutionError && err.code === "DOCKER_GPU_UNSUPPORTED",
    );
  });

  test("rejects empty, duplicate or negative device lists", () => {
    for (const deviceIndices of [[], [0, 0], [-1]]) {
      assert.throws(
        () =>
          buildContainerGpuArgs({
            vendor: "nvidia",
            backend: "cuda",
            deviceIndices,
          }),
        (err: unknown) =>
          err instanceof GpuExecutionError &&
          err.code === "GPU_ASSIGNMENT_INVALID",
      );
    }
  });
});

describe("classifyDockerGpuError", () => {
  test("maps docker/toolkit stderr to actionable codes", () => {
    assert.equal(
      classifyDockerGpuError("unknown flag: --gpus").code,
      "DOCKER_GPU_UNSUPPORTED",
    );
    assert.equal(
      classifyDockerGpuError(
        "docker: Error response from daemon: could not select device driver \"\" with capabilities: [[gpu]]"
      ).code,
      "DOCKER_GPU_RUNTIME_UNAVAILABLE",
    );
    assert.equal(
      classifyDockerGpuError("docker: Error response from daemon: permission denied").code,
      "DOCKER_GPU_DEVICE_ACCESS",
    );
    assert.equal(
      classifyDockerGpuError("no such device: /dev/nvidia1").code,
      "GPU_DEVICE_NOT_FOUND",
    );
    assert.equal(
      classifyDockerGpuError("something entirely different").code,
      "DOCKER_GPU_START_FAILED",
    );
  });
});

describe("redactDockerArgs", () => {
  test("redacts -e values and leaked secret strings", () => {
    const args = redactDockerArgs(
      [
        "run",
        "-e",
        "S3_SECRET=hunter2",
        "-e",
        "MINIO_ACCESS_KEY=AKIAEXAMPLE",
        "--name",
        "computebay-job-1",
        "image",
        "sh",
        "-c",
        "echo hunter2 && echo AKIAEXAMPLE",
      ],
      { S3_SECRET: "hunter2", MINIO_ACCESS_KEY: "AKIAEXAMPLE" },
    );
    assert.equal(args[2], "S3_SECRET=***");
    assert.equal(args[4], "MINIO_ACCESS_KEY=***");
    assert.ok(!args.join(" ").includes("hunter2"));
    assert.ok(!args.join(" ").includes("AKIAEXAMPLE"));
  });

  test("leaves device selection visible for diagnostics", () => {
    const args = redactDockerArgs(
      ["--gpus", "device=0,1", "--cpus", "2"],
      {},
    );
    assert.deepEqual(args, ["--gpus", "device=0,1", "--cpus", "2"]);
  });
});
