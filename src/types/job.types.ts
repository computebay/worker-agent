/**
 * Thrown when the message payload is invalid.
 * Consumer should nack without requeue to avoid an infinite loop.
 */
export class InvalidJobPayloadError extends Error {
  constructor(message: string, public readonly payload: unknown) {
    super(message);
    this.name = "InvalidJobPayloadError";
  }
}

export type JobType = "batch" | "service" | "batch-image";

export interface InputSpec {
  type: "s3";
  url: string;
  mountPath: string;
}

export interface OutputSpec {
  type: "s3";
  uploadFrom: string;
}

export interface BatchImageResources {
  cpu: number;
  memoryMb: number;
  gpu: "any" | "none";
}

export interface BatchImageJob extends ScheduledJobBase {
  type: "batch-image";
  image: string;
  command: string[];
  input: InputSpec;
  output: OutputSpec;
  cpu: number;
  memoryMB: number;
  gpu: "any" | "none";
}

/**
 * Job payload shape from Job Service (same as CreateJobInput).
 * Sent to worker via job.scheduled.{nodeId} with jobId added by Scheduler.
 */
export interface CreateJobInput {
  jobType: string;
  /** Discriminates batch vs. service vs. batch-image execution. Default: "batch". */
  type?: JobType;
  runtime: string;
  /** Git repo URL to clone. Required for batch jobs. */
  repoUrl?: string;
  /** Git branch to checkout (default: main). */
  branch?: string;
  /** Shell command to run inside /workspace (e.g. "npm install && npm run build"). */
  startCommand?: string;
  /** Command passed directly to the container entrypoint (service jobs). */
  entrypoint?: string[];
  /** Command array for batch-image jobs. */
  command?: string[];
  resources: Record<string, unknown>;
  /** Optional: enable network for scraping jobs. Default false. */
  networkEnabled?: boolean;
  /** Container-internal port to expose (required for service jobs). */
  servicePort?: number;
  /** When true, an outputs/ dir is pre-created and files in it are uploaded as artifacts after the job. */
  hasArtifacts?: boolean;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
  /** Input specification for batch-image jobs. */
  input?: InputSpec;
  /** Output specification for batch-image jobs. */
  output?: OutputSpec;
}

/**
 * Message received on job.scheduled.{nodeId}: jobId (from Scheduler) + CreateJobInput.
 */
export interface ScheduledJobMessage {
  jobId: string;
  jobType: string;
  /** Discriminates batch vs. service vs. batch-image execution. Default: "batch". */
  type?: JobType;
  runtime: string;
  /** Docker image for batch-image jobs. */
  image?: string;
  /** Required for batch jobs; optional for service jobs. */
  repoUrl?: string;
  branch?: string;
  /** Required for batch jobs; optional for service jobs. */
  startCommand?: string;
  /** Command passed directly to the container entrypoint (service jobs). */
  entrypoint?: string[];
  /** Command array for batch-image jobs. */
  command?: string[];
  resources: Record<string, unknown>;
  networkEnabled?: boolean;
  /** Container-internal port to expose (required for service jobs). */
  servicePort?: number;
  /** When true, an outputs/ dir is pre-created and files in it are uploaded as artifacts after the job. */
  hasArtifacts?: boolean;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
  /** Input specification for batch-image jobs. */
  input?: InputSpec;
  /** Output specification for batch-image jobs. */
  output?: OutputSpec;
}

/**
 * Internal job shape used for container execution (derived from ScheduledJobMessage).
 * Discriminated union: batch | service | batch-image.
 */

interface ScheduledJobBase {
  jobId: string;
  cpu: number;
  memoryMB: number;
  timeoutMs?: number;
  env?: Record<string, string>;
  networkEnabled: boolean;
  hasArtifacts: boolean;
}

export interface ScheduledBatchJob extends ScheduledJobBase {
  type: "batch";
  image: string;
  startCommand: string;
  repoUrl: string;
  branch: string;
}

export interface ScheduledServiceJob extends ScheduledJobBase {
  type: "service";
  image: string;
  entrypoint?: string[];
  startCommand?: string;
  servicePort: number;
  repoUrl?: string;
  branch?: string;
}

export type ScheduledJob =
  | ScheduledBatchJob
  | ScheduledServiceJob
  | BatchImageJob;

/**
 * Maps Job Service payload to internal ScheduledJob.
 */
export function toScheduledJob(msg: ScheduledJobMessage): ScheduledJob {
  // Normalize: check `type` first, fall back to `jobType`, ignore case.
  const rawType = (msg.type ?? msg.jobType ?? "").toLowerCase();

  const cpu = msg.resources?.cpu;
  const memoryMB = msg.resources?.memoryMB ?? msg.resources?.memoryMb;
  if (typeof cpu !== "number" || cpu <= 0) {
    throw new InvalidJobPayloadError(
      "resources.cpu must be a positive number",
      msg
    );
  }
  if (typeof memoryMB !== "number" || memoryMB <= 0) {
    throw new InvalidJobPayloadError(
      "resources.memoryMB (or memoryMb) must be a positive number",
      msg
    );
  }

  // --- batch-image ---
  if (rawType === "batch-image") {
    if (!msg.image || typeof msg.image !== "string") {
      throw new InvalidJobPayloadError(
        "image is required for batch-image jobs",
        msg
      );
    }
    if (!Array.isArray(msg.command) || msg.command.length === 0) {
      throw new InvalidJobPayloadError(
        "command must be a non-empty array for batch-image jobs",
        msg
      );
    }
    if (
      !msg.input ||
      typeof msg.input !== "object" ||
      msg.input.type !== "s3" ||
      typeof msg.input.url !== "string" ||
      typeof msg.input.mountPath !== "string"
    ) {
      throw new InvalidJobPayloadError(
        "input must be { type: 's3', url: string, mountPath: string } for batch-image jobs",
        msg
      );
    }
    if (
      !msg.output ||
      typeof msg.output !== "object" ||
      msg.output.type !== "s3" ||
      typeof msg.output.uploadFrom !== "string"
    ) {
      throw new InvalidJobPayloadError(
        "output must be { type: 's3', uploadFrom: string } for batch-image jobs",
        msg
      );
    }
    const gpu = msg.resources?.gpu;
    if (gpu !== "any" && gpu !== "none") {
      throw new InvalidJobPayloadError(
        "resources.gpu must be 'any' or 'none' for batch-image jobs",
        msg
      );
    }

    return {
      jobId: msg.jobId,
      type: "batch-image",
      image: msg.image,
      command: msg.command as string[],
      input: msg.input as InputSpec,
      output: msg.output as OutputSpec,
      cpu,
      memoryMB,
      gpu,
      networkEnabled: false,
      hasArtifacts: false,
    };
  }

  if (!msg.runtime || typeof msg.runtime !== "string") {
    throw new InvalidJobPayloadError("runtime (Docker image) is required", msg);
  }

  // --- service ---
  if (rawType === "service") {
    if (!msg.servicePort || typeof msg.servicePort !== "number" || msg.servicePort <= 0) {
      throw new InvalidJobPayloadError(
        "servicePort must be a positive number for service jobs",
        msg
      );
    }
    return {
      jobId: msg.jobId,
      type: "service",
      image: msg.runtime,
      entrypoint: msg.entrypoint,
      startCommand: msg.startCommand,
      cpu,
      memoryMB,
      networkEnabled: true,
      hasArtifacts: msg.hasArtifacts === true,
      servicePort: msg.servicePort,
      repoUrl: msg.repoUrl,
      branch: msg.branch,
    };
  }

  // --- batch (existing) ---
  if (!msg.repoUrl || typeof msg.repoUrl !== "string") {
    throw new InvalidJobPayloadError("repoUrl is required for batch jobs", msg);
  }
  if (!msg.startCommand || typeof msg.startCommand !== "string") {
    throw new InvalidJobPayloadError("startCommand is required for batch jobs", msg);
  }

  return {
    jobId: msg.jobId,
    type: "batch",
    image: msg.runtime,
    startCommand: msg.startCommand,
    cpu,
    memoryMB,
    repoUrl: msg.repoUrl,
    branch: msg.branch ?? "main",
    networkEnabled: msg.networkEnabled !== false,
    hasArtifacts: msg.hasArtifacts === true,
  };
}

/**
 * Node registration payload for node.registered
 */
export interface NodeRegisteredEvent {
  nodeId: string;
  totalCpu: number;
  totalMemoryMb: number;
  allocatableCpu: number;
  allocatableMemoryMb: number;
  timestamp: string;
}

/**
 * Heartbeat payload for node.heartbeat
 */
export interface NodeHeartbeatEvent {
  nodeId: string;
  totalCpu: number;
  totalMemoryMb: number;
  usedCpu: number;
  usedMemoryMb: number;
  availableCpu: number;
  availableMemoryMb: number;
  timestamp: string;
  draining?: boolean;
}

/**
 * Job lifecycle event base
 */
export interface JobLifecycleEvent {
  jobId: string;
  nodeId: string;
  timestamp: string;
}

export interface JobStartedEvent extends JobLifecycleEvent {
  event: "job.started";
}

export interface JobCompletedEvent extends JobLifecycleEvent {
  event: "job.completed";
  exitCode: number;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
  /** Output artifacts collected from /workspace/outputs */
  artifacts?: JobArtifact[];
}

export interface JobFailedEvent extends JobLifecycleEvent {
  event: "job.failed";
  error: string;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
  /** Output artifacts collected from /workspace/outputs (if any were produced before failure) */
  artifacts?: JobArtifact[];
}

export interface JobTimeoutEvent extends JobLifecycleEvent {
  event: "job.timeout";
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
}

/**
 * Artifact metadata for files uploaded to MinIO
 */
export interface JobArtifact {
  name: string;
  key: string;
  sizeBytes: number;
  contentType?: string;
  kind: "output" | "log" | "manifest";
  createdAt: string;
}

/**
 * Service lifecycle events
 */
export interface ServiceLifecycleEvent {
  serviceId: string;
  nodeId: string;
  timestamp: string;
}

export interface ServiceStartedEvent extends ServiceLifecycleEvent {
  event: "service.started";
  port: number;
  url: string;
}

export interface ServiceFailedEvent extends ServiceLifecycleEvent {
  event: "service.failed";
  error: string;
}

export interface ServiceStoppedEvent extends ServiceLifecycleEvent {
  event: "service.stopped";
  port: number;
}
