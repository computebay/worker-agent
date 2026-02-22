import type { ScheduledJob } from "../types/job.types.js";
import type { ResourceMonitor } from "../monitoring/resource.monitor.js";
import { runJob } from "./job.runner.js";
import { logger } from "../utils/logger.js";

export type JobResultCallback = (result: {
  jobId: string;
  exitCode: number;
  timedOut: boolean;
  error?: Error;
}) => void;

export interface WorkerPoolOptions {
  nodeId: string;
  resourceMonitor: ResourceMonitor;
  maxConcurrency: number;
  jobTimeoutMs: number;
  onJobResult: JobResultCallback;
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
  private activeCount = 0;
  private draining = false;

  constructor(options: WorkerPoolOptions) {
    this._nodeId = options.nodeId;
    this.resourceMonitor = options.resourceMonitor;
    this.maxConcurrency = options.maxConcurrency;
    this.jobTimeoutMs = options.jobTimeoutMs;
    this.onJobResult = options.onJobResult;
  }

  canAccept(job: ScheduledJob): boolean {
    if (this.draining) return false;
    if (this.activeCount >= this.maxConcurrency) return false;
    return this.resourceMonitor.canAllocate(job.cpu, job.memoryMB);
  }

  async run(job: ScheduledJob): Promise<void> {
    if (!this.canAccept(job)) {
      throw new Error(
        `Cannot accept job ${job.jobId}: draining=${this.draining} ` +
          `active=${this.activeCount}/${this.maxConcurrency} or insufficient resources`
      );
    }

    this.activeCount++;
    this.resourceMonitor.allocate(job.cpu, job.memoryMB);

    try {
      const result = await runJob(job, this.jobTimeoutMs);
      this.onJobResult({
        jobId: job.jobId,
        exitCode: result.exitCode,
        timedOut: result.timedOut,
      });
    } catch (err) {
      logger.error({ err, jobId: job.jobId }, "Job execution failed");
      this.onJobResult({
        jobId: job.jobId,
        exitCode: -1,
        timedOut: false,
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
