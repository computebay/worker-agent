import type { ScheduledJob } from "../types/job.types.js";
import type { ResourceMonitor } from "../monitoring/resource.monitor.js";
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
  private activeCount = 0;
  private draining = false;

  /** Map of jobId -> AbortController for cancellation. */
  private readonly cancelControllers = new Map<string, AbortController>();

  constructor(options: WorkerPoolOptions) {
    this._nodeId = options.nodeId;
    this.resourceMonitor = options.resourceMonitor;
    this.maxConcurrency = options.maxConcurrency;
    this.jobTimeoutMs = options.jobTimeoutMs;
    this.onJobResult = options.onJobResult;
    this.onLogChunk = options.onLogChunk;
    this.logUploadConfig = options.logUploadConfig;
    this.onServiceResult = options.onServiceResult;

    // Wire resource-release callback into the service registry so that
    // stopService() correctly decrements the ResourceMonitor.
    serviceRegistry.setReleaseCallback((cpu, memoryMB) => {
      this.resourceMonitor.release(cpu, memoryMB);
      this.activeCount--;
    });
  }

  canAccept(job: ScheduledJob): boolean {
    if (this.draining) return false;
    if (this.activeCount >= this.maxConcurrency) return false;
    return this.resourceMonitor.canAllocate(job.cpu, job.memoryMB);
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
      const result = await runJob(job, {
        timeoutMs: this.jobTimeoutMs,
        logUploadConfig: this.logUploadConfig,
        onLogChunk: this.onLogChunk
          ? (chunk) => this.onLogChunk!(job.jobId, chunk)
          : undefined,
        abortSignal: abortController.signal,
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
      this.activeCount--;
      this.resourceMonitor.release(job.cpu, job.memoryMB);
    }
  }

  /**
   * Runs a batch-image job: pull image, mount S3 input, execute command, upload S3 output.
   * TODO: implement actual container lifecycle (image pull, volume mounts, S3 sync).
   */
  private async runBatchImageJob(
    job: Extract<ScheduledJob, { type: "batch-image" }>
  ): Promise<void> {
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
      logger.info(
        { jobId: job.jobId, image: job.image, command: job.command },
        "Batch-image job started"
      );

      // TODO: pull image, mount S3 input to job.input.mountPath,
      //       run job.command, upload job.output.uploadFrom to S3

      this.cancelControllers.delete(job.jobId);
      logger.info({ jobId: job.jobId }, "Batch-image job completed");
      this.onJobResult({
        jobId: job.jobId,
        exitCode: 0,
        timedOut: false,
        output: "",
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
      const result = await runService(job, {
        onLogChunk: this.onLogChunk
          ? (chunk) => this.onLogChunk!(job.jobId, chunk)
          : undefined,
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
