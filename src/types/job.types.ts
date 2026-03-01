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

/**
 * Job payload shape from Job Service (same as CreateJobInput).
 * Sent to worker via job.scheduled.{nodeId} with jobId added by Scheduler.
 */
export interface CreateJobInput {
  jobType: string;
  runtime: string;
  /** Git repo URL to clone. */
  repoUrl: string;
  /** Git branch to checkout (default: main). */
  branch?: string;
  /** Shell command to run inside /workspace (e.g. "npm install && npm run build"). */
  startCommand: string;
  resources: Record<string, unknown>;
  /** Optional: enable network for scraping jobs. Default false. */
  networkEnabled?: boolean;
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
  runtime: string;
  repoUrl: string;
  branch?: string;
  startCommand: string;
  resources: Record<string, unknown>;
  networkEnabled?: boolean;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
}

/**
 * Internal job shape used for container execution (derived from ScheduledJobMessage).
 * Git-based: clone repo, mount as /workspace, run startCommand via sh -c.
 */
export interface ScheduledJob {
  jobId: string;
  image: string;
  /** Shell command to run (e.g. "npm install && npm run build"). */
  startCommand: string;
  cpu: number;
  memoryMB: number;
  timeoutMs?: number;
  env?: Record<string, string>;
  /** Git repo URL. */
  repoUrl: string;
  /** Git branch (default: main). */
  branch: string;
  /** Enable network in container (e.g. for scraping). Default false. */
  networkEnabled: boolean;
}

/**
 * Maps Job Service payload to internal ScheduledJob.
 */
export function toScheduledJob(msg: ScheduledJobMessage): ScheduledJob {
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
  if (!msg.repoUrl || typeof msg.repoUrl !== "string") {
    throw new InvalidJobPayloadError("repoUrl is required", msg);
  }
  if (!msg.startCommand || typeof msg.startCommand !== "string") {
    throw new InvalidJobPayloadError("startCommand is required", msg);
  }

  return {
    jobId: msg.jobId,
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
