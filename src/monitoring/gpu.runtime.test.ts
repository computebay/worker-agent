import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  applyGpuExecutionReadiness,
  describeGpuReadiness,
  probeGpuExecution,
  type GpuProbeCommandResult,
} from "./gpu.runtime.js";
import type { GpuSpec } from "./gpu.types.js";

function nvidiaSpec(): GpuSpec {
  return {
    id: "GPU-0",
    index: 0,
    vendor: "NVIDIA",
    name: "NVIDIA A100",
    memoryMb: 40960,
    driverVersion: "550.54.14",
    pciVendorId: "10de",
    discoverySource: "nvidia-smi",
  };
}

function ok(stdout = "ok"): Promise<GpuProbeCommandResult> {
  return Promise.resolve({ ok: true, stdout, stderr: "" });
}

function fail(stderr = "boom"): Promise<GpuProbeCommandResult> {
  return Promise.resolve({ ok: false, stdout: "", stderr, errorCode: "ENOENT" });
}

const HEALTHY_COMMANDS = async (command: string, args: string[]) => {
  if (command === "docker" && args[0] === "version") return ok("29.3.0");
  if (command === "docker" && args[0] === "run") return ok("--gpus ...");
  if (command === "docker" && args[0] === "info") {
    return ok(JSON.stringify({ nvidia: {} }));
  }
  return ok();
};

describe("probeGpuExecution", () => {
  test("reports not-ready on a CPU-only host without failing", async () => {
    const readiness = await probeGpuExecution([], { runCommand: HEALTHY_COMMANDS });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.backends.length, 0);
    assert.equal(readiness.diagnostics.hardwareDetected, false);
    assert.equal(readiness.diagnostics.reason, "No NVIDIA GPU hardware detected");
  });

  test("detects a fully working NVIDIA Docker setup", async () => {
    const readiness = await probeGpuExecution([nvidiaSpec()], {
      runCommand: HEALTHY_COMMANDS,
      fileExists: () => true,
    });
    assert.equal(readiness.ready, true);
    assert.deepEqual(readiness.backends, ["cuda", "nvenc"]);
    assert.equal(readiness.diagnostics.dockerAvailable, true);
    assert.equal(readiness.diagnostics.nvidiaToolkitAvailable, true);
    assert.equal(readiness.diagnostics.deviceNodesVisible, true);
  });

  test("distinguishes a missing driver from missing tooling", async () => {
    const spec = nvidiaSpec();
    delete spec.driverVersion;
    const readiness = await probeGpuExecution([spec], {
      runCommand: HEALTHY_COMMANDS,
      fileExists: () => true,
    });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.diagnostics.driverReady, false);
    assert.ok(readiness.diagnostics.reason?.includes("driver"));
  });

  test("detects an unreachable Docker daemon", async () => {
    const readiness = await probeGpuExecution([nvidiaSpec()], {
      runCommand: async (command) =>
        command === "docker" ? fail() : ok(),
      fileExists: () => true,
    });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.diagnostics.dockerAvailable, false);
  });

  test("detects Docker without --gpus support", async () => {
    const readiness = await probeGpuExecution([nvidiaSpec()], {
      runCommand: async (command, args) => {
        if (command === "docker" && args[0] === "run") {
          return ok("--memory --cpus (no gpu flag)");
        }
        return HEALTHY_COMMANDS(command, args);
      },
      fileExists: () => true,
    });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.diagnostics.dockerGpuFlagSupported, false);
  });

  test("detects a missing NVIDIA Container Toolkit", async () => {
    const readiness = await probeGpuExecution([nvidiaSpec()], {
      runCommand: async (command, args) => {
        if (command === "docker" && args[0] === "info") {
          return ok(JSON.stringify({ runc: {} }));
        }
        if (command.startsWith("nvidia-container")) return fail();
        return HEALTHY_COMMANDS(command, args);
      },
      fileExists: () => true,
    });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.diagnostics.nvidiaToolkitAvailable, false);
  });

  test("detects invisible device nodes", async () => {
    const readiness = await probeGpuExecution([nvidiaSpec()], {
      runCommand: HEALTHY_COMMANDS,
      fileExists: () => false,
    });
    assert.equal(readiness.ready, false);
    assert.equal(readiness.diagnostics.deviceNodesVisible, false);
  });

  test("never marks non-NVIDIA devices as execution ready", () => {
    const amd: GpuSpec = {
      index: 0,
      vendor: "AMD",
      pciVendorId: "1002",
      discoverySource: "linux-sysfs",
    };
    const enriched = applyGpuExecutionReadiness([amd], {
      ready: true,
      backends: ["cuda", "nvenc"],
      diagnostics: {
        hardwareDetected: false,
        driverReady: true,
        dockerAvailable: true,
        dockerGpuFlagSupported: true,
        nvidiaToolkitAvailable: true,
        deviceNodesVisible: true,
      },
    });
    assert.deepEqual(enriched[0]?.executionBackends, []);
  });

  test("describes readiness in human-readable terms", () => {
    assert.equal(
      describeGpuReadiness({
        ready: true,
        backends: ["cuda"],
        diagnostics: {
          hardwareDetected: true,
          driverReady: true,
          dockerAvailable: true,
          dockerGpuFlagSupported: true,
          nvidiaToolkitAvailable: true,
          deviceNodesVisible: true,
        },
      }),
      "NVIDIA Docker execution ready (cuda)",
    );
    assert.equal(
      describeGpuReadiness({
        ready: false,
        backends: [],
        diagnostics: {
          hardwareDetected: true,
          driverReady: true,
          dockerAvailable: false,
          dockerGpuFlagSupported: false,
          nvidiaToolkitAvailable: false,
          deviceNodesVisible: false,
          reason: "Docker daemon is unavailable",
        },
      }),
      "Docker daemon is unavailable",
    );
  });
});
