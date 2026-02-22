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
  entrypoint: string[];
  resources: Record<string, unknown>;
  inputArtifacts: Record<string, unknown>;
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
  entrypoint: string[];
  resources: Record<string, unknown>;
  inputArtifacts: Record<string, unknown>;
  retryPolicy?: Record<string, unknown> | null;
  priority?: number;
  orgId: string;
}

/**
 * Internal job shape used for container execution (derived from ScheduledJobMessage).
 * When inlineCode is set, the worker runs this JS in a Node container (runtime should be a Node image).
 */
export interface ScheduledJob {
  jobId: string;
  image: string;
  command: string[];
  cpu: number;
  memoryMB: number;
  timeoutMs?: number;
  env?: Record<string, string>;
  /** When set, executed in container via node -e (base64 env). Requires Node image. */
  inlineCode?: string;
}

/**
 * Maps Job Service payload to internal ScheduledJob.
 * If inputArtifacts.code is a string, it is executed as JS in the container (Node runtime).
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
  const entrypoint = Array.isArray(msg.entrypoint) ? msg.entrypoint : [];
  if (!entrypoint.every((e): e is string => typeof e === "string")) {
    throw new InvalidJobPayloadError("entrypoint must be an array of strings", msg);
  }
  const inlineCode =
    typeof msg.inputArtifacts?.code === "string" ? msg.inputArtifacts.code : undefined;
  return {
    jobId: msg.jobId,
    image: msg.runtime,
    command: inlineCode ? [] : entrypoint,
    cpu,
    memoryMB,
    inlineCode,
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
}

export interface JobFailedEvent extends JobLifecycleEvent {
  event: "job.failed";
  error: string;
}

export interface JobTimeoutEvent extends JobLifecycleEvent {
  event: "job.timeout";
}
