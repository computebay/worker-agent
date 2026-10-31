import type { ScheduledJob } from "../types/job.types.js";
import type { ResourceMonitor } from "../monitoring/resource.monitor.js";
import type { GpuSpec } from "../monitoring/gpu.types.js";
import {
  GpuExecutionError,
  resolveGpuAssignment,
  type GpuExecutionPlan,
} from "../monitoring/gpu.requirement.js";
import type { ContainerGpuConfig } from "./container.runtime.js";
import { runJob, type LogUploadConfig } from "./job.runner.js";
import { runService } from "./service.runner.js";
import { serviceRegistry } from "./service.registry.js";
import { logger } from "../utils/logger.js";

export type JobResultCallback = (result: {
  jobId: string;
  exitCode: number;
  timedOut: boolean;
  output: string;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
  /** Output artifacts collected from /workspace/outputs */
  artifacts?: import("../types/job.types.js").JobArtifact[];
  error?: Error;
}) => void;

export type LogChunkCallback = (jobId: string, chunk: string) => void;

export type ServiceResultCallback = (result: {
  serviceId: string;
  hostPort: number;
  url: string;
  error?: Error;
}) => void;

export interface WorkerPoolOptions {
  nodeId: string;
  resourceMonitor: ResourceMonitor;
  maxConcurrency: number;
  jobTimeoutMs: number;
  onJobResult: JobResultCallback;
  /** Called with log chunks for streaming to RabbitMQ. */
  onLogChunk?: LogChunkCallback;
  /** Required for uploading logs to MinIO. */
  logUploadConfig?: LogUploadConfig;
  /** Called when a service container starts or fails to start. */
  onServiceResult?: ServiceResultCallback;
  /** Root directory for per-job scratch dirs (default: os.tmpdir()). */
  jobWorkDir?: string;
  /**
   * Local GPU inventory with verified execution backends. Used to translate the
   * scheduler's stable device assignment into current local Docker indices.
   */
  gpus?: GpuSpec[];
}

/**
 * Resource-aware scheduling. Enforces:
 * usedCpu + job.cpu <= allocatableCpu
 * usedMemoryMb + job.memoryMB <= allocatableMemoryMb
 * Concurrency and drain logic.
 */
export class WorkerPool {
  private readonly _nodeId: string;
  private readonly resourceMonitor: ResourceMonitor;
  private readonly maxConcurrency: number;
  private readonly jobTimeoutMs: number;
  private readonly onJobResult: JobResultCallback;
  private readonly onLogChunk: LogChunkCallback | undefined;
  private readonly logUploadConfig: LogUploadConfig | undefined;
  private readonly onServiceResult: ServiceResultCallback | undefined;
  private readonly jobWorkDir: string | undefined;
  private readonly gpus: GpuSpec[];
  private activeCount = 0;
  private draining = false;

  /** Map of jobId -> AbortController for cancellation. */
  private readonly cancelControllers = new Map<string, AbortController>();

  /**
   * Whole-device exclusivity guard keyed by stable device identity. The
   * scheduler allocates devices, but the worker is the final authority on what
   * is actually running locally.
   */
  private readonly reservedGpuKeys = new Set<string>();
  /** Map of jobId/serviceId -> GPU keys, so every exit path releases them. */
  private readonly jobGpuKeys = new Map<string, string[]>();

  constructor(options: WorkerPoolOptions) {
    this._nodeId = options.nodeId;
    this.resourceMonitor = options.resourceMonitor;
    this.maxConcurrency = options.maxConcurrency;
    this.jobTimeoutMs = options.jobTimeoutMs;
    this.onJobResult = options.onJobResult;
    this.onLogChunk = options.onLogChunk;
    this.logUploadConfig = options.logUploadConfig;
    this.onServiceResult = options.onServiceResult;
    this.jobWorkDir = options.jobWorkDir;
    this.gpus = options.gpus ?? [];

    // Wire resource-release callback into the service registry so that
    // stopService() correctly decrements the ResourceMonitor and releases the
    // service's GPU reservation.
    serviceRegistry.setReleaseCallback((serviceId, cpu, memoryMB) => {
      this.resourceMonitor.release(cpu, memoryMB);
      this.releaseGpu(serviceId);
      this.activeCount--;
    });
  }

  canAccept(job: ScheduledJob): boolean {
    if (this.draining) return false;
    if (this.activeCount >= this.maxConcurrency) return false;
    if (!this.resourceMonitor.canAllocate(job.cpu, job.memoryMB)) return false;
    return this.hasGpuCapacity(job);
  }

  /**
   * Non-throwing capacity check used before accepting a job. Permanent GPU
   * problems (invalid/missing assignment, VRAM mismatch, unsupported backend)
   * return true so `run()` can report a normal `job.failed` instead of the
   * message being requeued forever. Only local contention is treated as
   * "not now".
   */
  private hasGpuCapacity(job: ScheduledJob): boolean {
    if (!job.gpu) return true;
    try {
      const plan = resolveGpuAssignment(
        job.gpu,
        job.gpuAssignment,
        this.gpus
      );
      if (!plan) return true;
      return plan.deviceKeys.every((key) => !this.reservedGpuKeys.has(key));
    } catch {
      // Permanent GPU problems are surfaced by `reserveGpu()` as a normal
      // job failure. Rejecting here would requeue the message indefinitely.
      return true;
    }
  }

  /**
   * Resolve the scheduler assignment and reserve whole devices for this job.
   * Synchronous by design so the check and reservation cannot interleave with
   * another job on the event loop.
   */
  private reserveGpu(job: ScheduledJob): GpuExecutionPlan | null {
    if (!job.gpu) return null;

    const plan = resolveGpuAssignment(job.gpu, job.gpuAssignment, this.gpus);
    if (!plan) return null;

    const busy = plan.deviceKeys.filter((key) =>
      this.reservedGpuKeys.has(key)
    );
    if (busy.length > 0) {
      throw new GpuExecutionError(
        "GPU_DEVICE_BUSY",
        `Assigned GPU device(s) are already in use on this worker (${busy.join(", ")})`
      );
    }

    for (const key of plan.deviceKeys) {
      this.reservedGpuKeys.add(key);
    }
    this.jobGpuKeys.set(job.jobId, plan.deviceKeys);
    logger.info(
      {
        jobId: job.jobId,
        vendor: plan.vendor,
        backend: plan.backend,
        deviceIndices: plan.deviceIndices,
      },
      "Reserved GPU devices for job"
    );
    return plan;
  }

  /** Idempotently release every GPU held by a job/service. */
  private releaseGpu(jobId: string): void {
    const keys = this.jobGpuKeys.get(jobId);
    if (!keys) return;

    for (const key of keys) {
      this.reservedGpuKeys.delete(key);
    }
    this.jobGpuKeys.delete(jobId);
    logger.info({ jobId, devices: keys }, "Released GPU devices");
  }

  private static toContainerGpu(
    plan: GpuExecutionPlan | null
  ): ContainerGpuConfig | undefined {
    if (!plan) return undefined;
    return {
      vendor: plan.vendor,
      backend: plan.backend,
      deviceIndices: plan.deviceIndices,
    };
  }

  /** Cancel a running job by jobId. Kills the container. */
  cancelJob(jobId: string): void {
    const controller = this.cancelControllers.get(jobId);
    if (controller) {
      controller.abort();
      this.cancelControllers.delete(jobId);
    }
    if (serviceRegistry.getService(jobId)) {
      serviceRegistry.stopService(jobId).catch((err) =>
        logger.warn({ err, jobId }, "Failed to cancel service job")
      );
    }
  }

  async run(job: ScheduledJob): Promise<void> {
    if (job.type === "service") {
      return this.runService(job);
    }
    if (job.type === "batch-image") {
      return this.runBatchImageJob(job);
    }
    return this.runBatchJob(job);
  }

  private async runBatchJob(job: ScheduledJob): Promise<void> {
    if (!this.canAccept(job)) {
      throw new Error(
        `[${this._nodeId}] Cannot accept job ${job.jobId}: draining=${this.draining} ` +
          `active=${this.activeCount}/${this.maxConcurrency} or insufficient resources`
      );
    }

    this.activeCount++;
    this.resourceMonitor.allocate(job.cpu, job.memoryMB);

    const abortController = new AbortController();
    this.cancelControllers.set(job.jobId, abortController);

    try {
      const gpuPlan = this.reserveGpu(job);
      const result = await runJob(job, {
        timeoutMs: this.jobTimeoutMs,
        logUploadConfig: this.logUploadConfig,
        onLogChunk: this.onLogChunk
          ? (chunk) => this.onLogChunk!(job.jobId, chunk)
          : undefined,
        abortSignal: abortController.signal,
        jobWorkDir: this.jobWorkDir,
        gpu: WorkerPool.toContainerGpu(gpuPlan),
      });
      this.cancelControllers.delete(job.jobId);
      logger.info(
        { jobId: job.jobId, exitCode: result.exitCode, timedOut: result.timedOut },
        "Job completed"
      );
      this.onJobResult({
        jobId: job.jobId,
        exitCode: result.exitCode,
        timedOut: result.timedOut,
        output: result.output,
        logObjectKey: result.logObjectKey,
        artifacts: result.artifacts,
      });
    } catch (err) {
      this.cancelControllers.delete(job.jobId);
      logger.error({ err, jobId: job.jobId }, "Job execution failed");
      this.onJobResult({
        jobId: job.jobId,
        exitCode: -1,
        timedOut: false,
        output: "",
        error: err instanceof Error ? err : new Error(String(err)),
      });
    } finally {
      this.cancelControllers.delete(job.jobId);
      this.releaseGpu(job.jobId);
      this.activeCount--;
      this.resourceMonitor.release(job.cpu, job.memoryMB);
    }
  }

  private async runBatchImageJob(job: ScheduledJob): Promise<void> {
    if (!this.canAccept(job)) {
      throw new Error(
        `[${this._nodeId}] Cannot accept batch-image ${job.jobId}: draining=${this.draining} ` +
          `active=${this.activeCount}/${this.maxConcurrency} or insufficient resources`
      );
    }

    this.activeCount++;
    this.resourceMonitor.allocate(job.cpu, job.memoryMB);

    const abortController = new AbortController();
    this.cancelControllers.set(job.jobId, abortController);

    try {
      const gpuPlan = this.reserveGpu(job);
      const result = await runJob(job, {
        timeoutMs: this.jobTimeoutMs,
        logUploadConfig: this.logUploadConfig,
        onLogChunk: this.onLogChunk
          ? (chunk) => this.onLogChunk!(job.jobId, chunk)
          : undefined,
        abortSignal: abortController.signal,
        jobWorkDir: this.jobWorkDir,
        gpu: WorkerPool.toContainerGpu(gpuPlan),
      });
      this.cancelControllers.delete(job.jobId);
      logger.info(
        { jobId: job.jobId, exitCode: result.exitCode, timedOut: result.timedOut },
        "Batch-image job completed"
      );
      this.onJobResult({
        jobId: job.jobId,
        exitCode: result.exitCode,
        timedOut: result.timedOut,
        output: result.output,
        logObjectKey: result.logObjectKey,
        artifacts: result.artifacts,
      });
    } catch (err) {
      this.cancelControllers.delete(job.jobId);
      logger.error({ err, jobId: job.jobId }, "Batch-image job execution failed");
      this.onJobResult({
        jobId: job.jobId,
        exitCode: -1,
        timedOut: false,
        output: "",
        error: err instanceof Error ? err : new Error(String(err)),
      });
    } finally {
      this.cancelControllers.delete(job.jobId);
      this.releaseGpu(job.jobId);
      this.activeCount--;
      this.resourceMonitor.release(job.cpu, job.memoryMB);
    }
  }

  /**
   * Runs a long-running service container.
   * Resources are held until stopService() is called via the registry.
   * activeCount is decremented by the releaseCallback wired into serviceRegistry.
   */
  private async runService(job: ScheduledJob): Promise<void> {
    if (!this.canAccept(job)) {
      throw new Error(
        `[${this._nodeId}] Cannot accept service ${job.jobId}: draining=${this.draining} ` +
          `active=${this.activeCount}/${this.maxConcurrency} or insufficient resources`
      );
    }

    this.activeCount++;
    this.resourceMonitor.allocate(job.cpu, job.memoryMB);

    try {
      const gpuPlan = this.reserveGpu(job);
      const result = await runService(job, {
        onLogChunk: this.onLogChunk
          ? (chunk) => this.onLogChunk!(job.jobId, chunk)
          : undefined,
        gpu: WorkerPool.toContainerGpu(gpuPlan),
      });
      serviceRegistry.registerService(
        {
          serviceId: result.serviceId, 
          containerId: result.containerId, 
          port: result.hostPort,
          stopLogStream: result.stopLogStream,
          workDirPath: result.workDirPath
        },
        { cpu: job.cpu, memoryMB: job.memoryMB }
      );
      const url = `http://${result.serviceId}.computebay.online`;
      logger.info({ serviceId: result.serviceId, hostPort: result.hostPort, url }, "Service started");
      this.onServiceResult?.({
        serviceId: result.serviceId,
        hostPort: result.hostPort,
        url,
      });
    } catch (err) {
      // Resources released on failure; service never registered.
      this.releaseGpu(job.jobId);
      this.activeCount--;
      this.resourceMonitor.release(job.cpu, job.memoryMB);
      logger.error({ err, serviceId: job.jobId }, "Service failed to start");
      this.onServiceResult?.({
        serviceId: job.jobId,
        hostPort: 0,
        url: "",
        error: err instanceof Error ? err : new Error(String(err)),
      });
    }
  }

  startDrain(): void {
    this.draining = true;
  }

  isDraining(): boolean {
    return this.draining;
  }

  getActiveCount(): number {
    return this.activeCount;
  }

  hasActiveJobs(): boolean {
    return this.activeCount > 0;
  }
}
