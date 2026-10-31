import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { promisify } from "node:util";
import type { GpuSpec } from "./gpu.types.js";

const execFileAsync = promisify(execFile);

export type GpuExecutionBackend = "cuda" | "nvenc";

export interface GpuProbeCommandResult {
  ok: boolean;
  stdout: string;
  stderr: string;
  errorCode?: string;
}

export type GpuProbeCommand = (
  command: string,
  args: string[]
) => Promise<GpuProbeCommandResult>;

export interface GpuExecutionDiagnostics {
  /** NVIDIA hardware was discovered (not an execution-readiness claim). */
  hardwareDetected: boolean;
  /** A host driver version is known for every NVIDIA device. */
  driverReady: boolean;
  /** The Docker daemon is reachable. */
  dockerAvailable: boolean;
  /** `docker run` advertises the `--gpus` option. */
  dockerGpuFlagSupported: boolean;
  /** NVIDIA Container Toolkit (or a registered NVIDIA Docker runtime) exists. */
  nvidiaToolkitAvailable: boolean;
  /** `/dev/nvidia*` device nodes are visible on the host. */
  deviceNodesVisible: boolean;
  /** Short, user-safe description of the first blocking check. */
  reason?: string;
}

export interface GpuExecutionReadiness {
  ready: boolean;
  backends: GpuExecutionBackend[];
  diagnostics: GpuExecutionDiagnostics;
}

export interface GpuExecutionProbeOptions {
  /** Injected for tests. */
  runCommand?: GpuProbeCommand;
  /** Injected for tests. */
  fileExists?: (path: string) => boolean;
}

function defaultRunCommand(
  command: string,
  args: string[]
): Promise<GpuProbeCommandResult> {
  return execFileAsync(command, args, {
    encoding: "utf8",
    timeout: 5_000,
    maxBuffer: 1024 * 1024,
  }).then(
    ({ stdout, stderr }) => ({
      ok: true,
      stdout: String(stdout ?? ""),
      stderr: String(stderr ?? ""),
    }),
    (err: NodeJS.ErrnoException & { stdout?: string; stderr?: string }) => ({
      ok: false,
      stdout: String(err.stdout ?? ""),
      stderr: String(err.stderr ?? ""),
      errorCode: err.code ?? err.name,
    })
  );
}

export function normalizeGpuVendor(
  vendor: string | undefined
): "nvidia" | "amd" | "intel" | undefined {
  if (!vendor) return undefined;
  const value = vendor.trim().toLowerCase();
  if (value === "nvidia") return "nvidia";
  if (value === "amd" || value === "advanced micro devices") return "amd";
  if (value === "intel") return "intel";
  return undefined;
}

export function isNvidiaGpu(gpu: GpuSpec): boolean {
  return (
    normalizeGpuVendor(gpu.vendor) === "nvidia" ||
    gpu.pciVendorId?.trim().toLowerCase() === "10de"
  );
}

/** Run only the lightweight checks needed to advertise GPU execution. */
export async function probeGpuExecution(
  gpus: GpuSpec[],
  options: GpuExecutionProbeOptions = {}
): Promise<GpuExecutionReadiness> {
  const runCommand = options.runCommand ?? defaultRunCommand;
  const fileExists = options.fileExists ?? existsSync;

  const nvidiaGpus = gpus.filter(isNvidiaGpu);
  const diagnostics: GpuExecutionDiagnostics = {
    hardwareDetected: nvidiaGpus.length > 0,
    driverReady: false,
    dockerAvailable: false,
    dockerGpuFlagSupported: false,
    nvidiaToolkitAvailable: false,
    deviceNodesVisible: false,
  };

  if (nvidiaGpus.length === 0) {
    diagnostics.reason = "No NVIDIA GPU hardware detected";
    return { ready: false, backends: [], diagnostics };
  }

  diagnostics.driverReady = nvidiaGpus.every(
    (gpu) => typeof gpu.driverVersion === "string" && gpu.driverVersion.length > 0
  );
  if (!diagnostics.driverReady) {
    diagnostics.reason = "NVIDIA driver version is unknown for at least one device";
    return { ready: false, backends: [], diagnostics };
  }

  const docker = await runCommand("docker", [
    "version",
    "--format",
    "{{.Server.Version}}",
  ]);
  diagnostics.dockerAvailable = docker.ok && docker.stdout.trim().length > 0;
  if (!diagnostics.dockerAvailable) {
    diagnostics.reason = "Docker daemon is unavailable";
    return { ready: false, backends: [], diagnostics };
  }

  const dockerRunHelp = await runCommand("docker", ["run", "--help"]);
  diagnostics.dockerGpuFlagSupported =
    dockerRunHelp.ok &&
    `${dockerRunHelp.stdout}\n${dockerRunHelp.stderr}`.includes("--gpus");
  if (!diagnostics.dockerGpuFlagSupported) {
    diagnostics.reason = "Docker does not support the --gpus option";
    return { ready: false, backends: [], diagnostics };
  }

  // Modern NVIDIA Container Toolkit installations register an `nvidia`
  // runtime; some CDI-based setups only expose the CLI. Accept either, but do
  // not infer toolkit support from the GPU hardware alone.
  const runtimeInfo = await runCommand("docker", [
    "info",
    "--format",
    "{{json .Runtimes}}",
  ]);
  let hasNvidiaRuntime = false;
  if (runtimeInfo.ok) {
    try {
      const runtimes = JSON.parse(runtimeInfo.stdout) as Record<string, unknown>;
      hasNvidiaRuntime = Object.keys(runtimes).some(
        (name) => name.toLowerCase() === "nvidia"
      );
    } catch {
      hasNvidiaRuntime = false;
    }
  }

  const containerCli = await runCommand("nvidia-container-cli", ["--version"]);
  const containerRuntime = await runCommand("nvidia-container-runtime", [
    "--version",
  ]);
  diagnostics.nvidiaToolkitAvailable =
    hasNvidiaRuntime || containerCli.ok || containerRuntime.ok;
  if (!diagnostics.nvidiaToolkitAvailable) {
    diagnostics.reason =
      "NVIDIA Container Toolkit is missing or not registered with Docker";
    return { ready: false, backends: [], diagnostics };
  }

  diagnostics.deviceNodesVisible =
    fileExists("/dev/nvidiactl") && fileExists("/dev/nvidia0");
  if (!diagnostics.deviceNodesVisible) {
    diagnostics.reason =
      "NVIDIA device nodes are not visible on the host (/dev/nvidiactl, /dev/nvidia0)";
    return { ready: false, backends: [], diagnostics };
  }

  // CUDA and NVENC use the same device exposure. The worker does not probe
  // application-level codec support here (that would require a workload).
  return {
    ready: true,
    backends: ["cuda", "nvenc"],
    diagnostics,
  };
}

/**
 * Copy discovery results and attach verified backends only to NVIDIA devices.
 * Non-NVIDIA devices explicitly advertise an empty list so the scheduler never
 * mistakes discovery for working Docker execution support.
 */
export function applyGpuExecutionReadiness(
  gpus: GpuSpec[],
  readiness: GpuExecutionReadiness
): GpuSpec[] {
  return gpus.map((gpu) =>
    isNvidiaGpu(gpu)
      ? {
          ...gpu,
          executionBackends: readiness.ready ? [...readiness.backends] : [],
        }
      : { ...gpu, executionBackends: [] }
  );
}

export function describeGpuReadiness(
  readiness: GpuExecutionReadiness
): string {
  return readiness.ready
    ? `NVIDIA Docker execution ready (${readiness.backends.join(", ")})`
    : readiness.diagnostics.reason ?? "NVIDIA Docker execution is unavailable";
}
