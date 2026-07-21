import type { ScheduledJob } from "../types/job.types.js";
import type { ResourceMonitor } from "../monitoring/resource.monitor.js";
import { runJob, type LogUploadConfig } from "./job.runner.js";
import { runService } from "./service.runner.js";
import { serviceRegistry } from "./service.registry.js";
import {
  createAndStart,
  getContainerLogs,
  killContainer,
  removeContainer,
  waitContainer,
  streamContainerLogs,
  type ContainerMount,
} from "./container.runtime.js";
import { downloadInput, parseS3Url } from "../libs/artifact.download.js";
import { uploadLogFile } from "../libs/log.upload.js";
import { logger } from "../utils/logger.js";
import { mkdtemp, rm, mkdir, writeFile, readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import os from "node:os";

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
   * Runs a batch-image job:
   *  1. Create temp working directory
   *  2. Download S3 input → local dir, bind-mount at job.input.mountPath
   *  3. Pull image, create container, wait for exit
   *  4. Upload local output dir → S3 at job.output.uploadFrom
   *  5. Cleanup temp dir and container
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

    const containerId = `computebay-${job.jobId}`;
    let workDirPath: string | undefined;

    try {
      logger.info(
        { jobId: job.jobId, image: job.image, command: job.command },
        "Batch-image job started"
      );

      // 1. Create temp working directory
      workDirPath = await mkdtemp(
        path.join(os.tmpdir(), `computebay-img-${job.jobId}-`)
      );
      const logFilePath = path.join(workDirPath, "full.log");
      await writeFile(logFilePath, "");

      // 2. Download S3 input
      const inputDir = path.join(workDirPath, "input");
      await mkdir(inputDir, { recursive: true });
      const inputLocalPath = path.join(inputDir, path.basename(job.input.mountPath));
      await downloadInput(this.logUploadConfig!.s3Config, job.input.url, inputLocalPath);

      // 3. Prepare container mounts
      const mounts: ContainerMount[] = [
        { hostPath: inputLocalPath, containerPath: job.input.mountPath },
      ];

      // Pre-create output directory inside workDir if uploadFrom is specified
      if (job.output?.uploadFrom) {
        const outputDir = path.dirname(path.join(workDirPath, job.output.uploadFrom));
        await mkdir(outputDir, { recursive: true });
      }

      const currentUid = process.getuid?.() ?? 1000;
      const currentGid = process.getgid?.() ?? 1000;
      const runAsUser = `${currentUid}:${currentGid}`;

      const logUploadConfig = this.logUploadConfig;

      // 4. Create and start container
      logger.info(
        { jobId: job.jobId, containerId, image: job.image, command: job.command },
        "Starting batch-image container"
      );

      await createAndStart({
        containerId,
        image: job.image,
        cpu: job.cpu,
        memoryMb: job.memoryMB,
        command: job.command,
        workDir: "/",
        mounts,
        networkEnabled: job.networkEnabled,
        runAsUser,
      });

      // Stream logs
      const stopLogStream = streamContainerLogs(containerId, {
        onChunk: async (chunk) => {
          await writeFile(logFilePath, chunk, { flag: "a" });
          this.onLogChunk?.(job.jobId, chunk);
        },
        onEnd: () => {},
        onError: (err) =>
          logger.warn({ err, jobId: job.jobId }, "Log stream error"),
      });

      // Wait with timeout + cancellation
      const jobTimeoutMs = job.timeoutMs ?? this.jobTimeoutMs;
      let timedOut = false;

      const waitPromise = waitContainer(containerId);
      const timeoutPromise = new Promise<never>((_, reject) => {
        setTimeout(() => {
          timedOut = true;
          killContainer(containerId)
            .then(() => reject(new Error("TIMEOUT")))
            .catch(reject);
        }, jobTimeoutMs);
      });

      const cancelPromise = abortController.signal.aborted
        ? Promise.reject(new Error("CANCELLED"))
        : new Promise<never>((_, reject) => {
            abortController.signal.addEventListener(
              "abort",
              () =>
                killContainer(containerId)
                  .then(() => reject(new Error("CANCELLED")))
                  .catch(reject),
              { once: true }
            );
          });

      let exitCode: number;
      try {
        const result = await Promise.race([
          waitPromise,
          timeoutPromise,
          cancelPromise,
        ]);
        exitCode = result.exitCode;
      } catch (err) {
        if (timedOut || (err instanceof Error && err.message === "TIMEOUT")) {
          timedOut = true;
          exitCode = 137;
          logger.warn(
            { jobId: job.jobId, containerId },
            "Batch-image job timed out"
          );
        } else if (err instanceof Error && err.message === "CANCELLED") {
          exitCode = 137;
          logger.info(
            { jobId: job.jobId, containerId },
            "Batch-image job cancelled"
          );
        } else {
          throw err;
        }
      } finally {
        stopLogStream();
      }

      // Fetch full logs
      let output = "";
      try {
        output = await getContainerLogs(containerId);
        await writeFile(logFilePath, output || "(no output)");
      } catch (logErr) {
        logger.warn(
          { jobId: job.jobId, err: logErr },
          "Failed to fetch container logs"
        );
      }

      await removeContainer(containerId).catch((err) => {
        logger.warn({ err, containerId }, "Failed to remove container");
      });

      // 5. Upload log to MinIO
      let logObjectKey: string | undefined;
      if (logUploadConfig) {
        try {
          logObjectKey = await uploadLogFile(
            logUploadConfig.s3Config,
            job.jobId,
            logFilePath
          );
        } catch (uploadErr) {
          logger.warn(
            { err: uploadErr, jobId: job.jobId },
            "Failed to upload log to MinIO"
          );
        }
      }

      // 6. Upload output files to S3
      if (job.output?.uploadFrom && logUploadConfig) {
        const localOutputDir = path.join(workDirPath, job.output.uploadFrom);
        try {
          await stat(localOutputDir);
          const { bucket: outputBucket, key: outputPrefix } = parseS3Url(
            job.output.uploadFrom
          );

          const entries = await readdir(localOutputDir, {
            withFileTypes: true,
          });
          for (const entry of entries) {
            if (!entry.isFile()) continue;
            const filePath = path.join(localOutputDir, entry.name);
            const body = await readFile(filePath);
            const objectKey = `${outputPrefix}/${entry.name}`;

            const s3 = (
              await import("../libs/s3.js")
            ).getS3Client({
              ...logUploadConfig.s3Config,
              bucket: outputBucket,
            });
            await s3.send(
              new (
                await import("@aws-sdk/client-s3")
              ).PutObjectCommand({
                Bucket: outputBucket,
                Key: objectKey,
                Body: body,
              })
            );
            logger.info(
              { jobId: job.jobId, key: objectKey, size: body.length },
              "Uploaded output to S3"
            );
          }
        } catch (outputErr) {
          if (
            outputErr instanceof Error &&
            outputErr.message.includes("ENOENT")
          ) {
            logger.info(
              { jobId: job.jobId, uploadFrom: job.output.uploadFrom },
              "No output directory found, skipping upload"
            );
          } else {
            logger.error(
              { err: outputErr, jobId: job.jobId },
              "Failed to upload output to S3"
            );
          }
        }
      }

      this.cancelControllers.delete(job.jobId);
      logger.info(
        { jobId: job.jobId, exitCode, timedOut },
        "Batch-image job completed"
      );
      this.onJobResult({
        jobId: job.jobId,
        exitCode,
        timedOut,
        output,
        logObjectKey,
      });
    } catch (err) {
      this.cancelControllers.delete(job.jobId);
      await removeContainer(containerId).catch(() => {});
      logger.error({ err, jobId: job.jobId }, "Batch-image job execution failed");
      this.onJobResult({
        jobId: job.jobId,
        exitCode: -1,
        timedOut: false,
        output: "",
        error: err instanceof Error ? err : new Error(String(err)),
      });
    } finally {
      if (workDirPath) {
        await rm(workDirPath, { recursive: true, force: true }).catch(() => {});
      }
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
