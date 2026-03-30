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

export type JobType = "batch" | "service";

/**
 * Job payload shape from Job Service (same as CreateJobInput).
 * Sent to worker via job.scheduled.{nodeId} with jobId added by Scheduler.
 */
export interface CreateJobInput {
  jobType: string;
  /** Discriminates batch vs. service execution. Default: "batch". */
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
  resources: Record<string, unknown>;
  /** Optional: enable network for scraping jobs. Default false. */
  networkEnabled?: boolean;
  /** Container-internal port to expose (required for service jobs). */
  servicePort?: number;
  /**
   * Override the auto-detected build command (e.g. "npm run build").
   * When provided, the worker always treats this as a build-step service.
   */
  buildCommand?: string;
  /** Override the auto-detected runtime start command (e.g. "node dist/index.js"). */
  runtimeCommand?: string;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
}

/**
 * Message received on job.scheduled.{nodeId}: jobId (from Scheduler) + CreateJobInput.
 */
export interface ScheduledJobMessage {
  jobId: string;
  jobType: string;
  /** Discriminates batch vs. service execution. Default: "batch". */
  type?: JobType;
  runtime: string;
  /** Required for batch jobs; optional for service jobs. */
  repoUrl?: string;
  branch?: string;
  /** Required for batch jobs; optional for service jobs. */
  startCommand?: string;
  /** Command passed directly to the container entrypoint (service jobs). */
  entrypoint?: string[];
  resources: Record<string, unknown>;
  networkEnabled?: boolean;
  /** Container-internal port to expose (required for service jobs). */
  servicePort?: number;
  /** Override auto-detected build command. */
  buildCommand?: string;
  /** Override auto-detected runtime start command. */
  runtimeCommand?: string;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
}

/**
 * Internal job shape used for container execution (derived from ScheduledJobMessage).
 * Batch: clone repo, mount as /workspace, run startCommand via sh -c.
 * Service: run image directly with optional entrypoint, expose servicePort.
 */
export interface ScheduledJob {
  jobId: string;
  /** Discriminates batch vs. service execution. */
  type: JobType;
  image: string;
  /** Shell command to run (batch jobs). */
  startCommand?: string;
  /** Direct container entrypoint (service jobs). */
  entrypoint?: string[];
  cpu: number;
  memoryMB: number;
  timeoutMs?: number;
  env?: Record<string, string>;
  /** Git repo URL. Required for batch jobs. */
  repoUrl?: string;
  /** Git branch (default: main). */
  branch?: string;
  /** Enable network in container (e.g. for scraping). Default false. */
  networkEnabled: boolean;
  /** Container-internal port (required for service jobs). */
  servicePort?: number;
  /** User-supplied override for the build command (beats framework auto-detection). */
  buildCommand?: string;
  /** User-supplied override for the runtime start command (beats auto-detection). */
  runtimeCommand?: string;
}

/**
 * Maps Job Service payload to internal ScheduledJob.
 */
export function toScheduledJob(msg: ScheduledJobMessage): ScheduledJob {
  // Normalize: check `type` first, fall back to `jobType`, ignore case.
  const rawType = (msg.type ?? msg.jobType ?? "").toLowerCase();
  const jobType: JobType = rawType === "service" ? "service" : "batch";

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
  if (!msg.runtime || typeof msg.runtime !== "string") {
    throw new InvalidJobPayloadError("runtime (Docker image) is required", msg);
  }

  if (jobType === "service") {
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
      networkEnabled: true, // services always need network
      servicePort: msg.servicePort,
      repoUrl: msg.repoUrl,
      branch: msg.branch,
      buildCommand: msg.buildCommand,
      runtimeCommand: msg.runtimeCommand,
    };
  }

  // Batch job — existing validation
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
    networkEnabled: Boolean(msg.networkEnabled),
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
}

export interface JobFailedEvent extends JobLifecycleEvent {
  event: "job.failed";
  error: string;
}

export interface JobTimeoutEvent extends JobLifecycleEvent {
  event: "job.timeout";
}

/**
 * Service lifecycle events
 */
export interface ServiceLifecycleEvent {
  serviceId: string;
  nodeId: string;
  timestamp: string;
}

export interface ServiceBuildingEvent extends ServiceLifecycleEvent {
  event: "service.building";
  repoUrl?: string;
  /** Auto-detected or user-supplied framework name, e.g. "nextjs" | "vite" | "node-build". */
  framework?: string;
}

export interface ServiceBuiltEvent extends ServiceLifecycleEvent {
  event: "service.built";
  artifactKey: string;
  checksum: string;
  buildDurationMs: number;
}

export interface ServiceStartingEvent extends ServiceLifecycleEvent {
  event: "service.starting";
  artifactKey: string;
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
