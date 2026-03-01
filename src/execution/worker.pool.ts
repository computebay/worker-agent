import type { ScheduledJob } from "../types/job.types.js";
import type { ResourceMonitor } from "../monitoring/resource.monitor.js";
import { runJob, type LogUploadConfig } from "./job.runner.js";
import { logger } from "../utils/logger.js";

export type JobResultCallback = (result: {
  jobId: string;
  exitCode: number;
  timedOut: boolean;
  output: string;
  /** MinIO object key for full log: logs/<jobId>/full.log */
  logObjectKey?: string;
  error?: Error;
}) => void;

export type LogChunkCallback = (jobId: string, chunk: string) => void;

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
  }

  async run(job: ScheduledJob): Promise<void> {
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
