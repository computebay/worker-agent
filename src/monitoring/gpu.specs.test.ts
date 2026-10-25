import { describe, test } from "node:test";
import assert from "node:assert/strict";
import { parseNvidiaSmiOutput } from "./gpu.specs.js";

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
    const gpus = parseNvidiaSmiOutput("0, Test GPU, 8192, N/A, N/A\n");

    assert.equal(gpus.length, 1);
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
});
