import { loadConfig } from "./config/config.js";
import { getMachineId } from "./security/identity.js";
import { detectSystemSpecs } from "./monitoring/system.specs.js";
import { computeAllocatable } from "./monitoring/capacity.js";
import { ResourceMonitor } from "./monitoring/resource.monitor.js";
import { createConnection, closeConnection } from "./messaging/rabbit.connection.js";
import { consume, cancelConsumer } from "./messaging/rabbit.consumer.js";
import { publish } from "./messaging/rabbit.publisher.js";
import { WorkerPool } from "./execution/worker.pool.js";
import { serviceRegistry } from "./execution/service.registry.js";
import { gracefulShutdown, installSigTermHandler } from "./lifecycle/shutdown.js";
import type { ScheduledJobMessage } from "./types/job.types.js";
import { InvalidJobPayloadError, toScheduledJob } from "./types/job.types.js";
import { logger } from "./utils/logger.js";
import { registerNodeWithRetry, sendHeartbeat } from "./bootstrap/scheduler.client.js";
import { loadBootstrapState, saveBootstrapState } from "./bootstrap/bootstrap.state.js";
import { getAgentVersion } from "./utils/version.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const machineId = getMachineId();
  logger.info({ machineId }, "Worker starting");

  const specs = detectSystemSpecs();
  const { allocatableCpu, allocatableMemoryMb } = computeAllocatable(
    specs,
    config.CPU_LIMIT,
    config.MEMORY_LIMIT_MB
  );

  const maxConcurrency =
    config.MAX_CONCURRENCY ?? Math.max(1, Math.floor(allocatableCpu));

  const resourceMonitor = new ResourceMonitor({
    allocatableCpu,
    allocatableMemoryMb,
  });

  const agentVersion = config.AGENT_VERSION ?? getAgentVersion();

  const logUploadConfig =
    config.MINIO_ENDPOINT &&
    config.MINIO_REGION &&
    config.MINIO_ACCESS_KEY &&
    config.MINIO_SECRET_KEY &&
    config.MINIO_BUCKET
      ? {
          s3Config: {
            endpoint: config.MINIO_ENDPOINT,
            region: config.MINIO_REGION,
            accessKey: config.MINIO_ACCESS_KEY,
            secretKey: config.MINIO_SECRET_KEY,
            bucket: config.MINIO_BUCKET,
          },
        }
      : undefined;
  if (!logUploadConfig) {
    logger.info("MinIO not configured; logs will not be uploaded");
  }

  const existingBootstrap = loadBootstrapState();

  let nodeId: string;
  let rabbitmqUrl: string;
  let queue: string;
  let publishExchange: string;

  if (existingBootstrap) {
    nodeId = existingBootstrap.nodeId;
    rabbitmqUrl = existingBootstrap.rabbitmqUrl;
    queue = existingBootstrap.queue;
    publishExchange = existingBootstrap.publishExchange;
    logger.info(
      { nodeId, rabbitmqUrl, queue, publishExchange },
      "Loaded existing bootstrap state; skipping registration"
    );
  } else {
    logger.info({ schedulerBaseUrl: config.SCHEDULER_BASE_URL }, "Registering node via Scheduler");
    const reg = await registerNodeWithRetry(config.SCHEDULER_BASE_URL, {
      machineId,
      cpuCores: specs.totalCpu,
      memoryMB: specs.totalMemoryMb,
      agentVersion,
    });

    nodeId = reg.nodeId;
    rabbitmqUrl = reg.rabbitmq.url;
    const queueTemplate = reg.rabbitmq.queue;
    publishExchange = reg.rabbitmq.publishExchange;
    queue = queueTemplate
      .split("<nodeId>")
      .join(nodeId)
      .split("{nodeId}")
      .join(nodeId)
      .split("${nodeId}")
      .join(nodeId);

    saveBootstrapState({
      nodeId,
      rabbitmqUrl,
      queue,
      publishExchange,
      registeredAt: new Date().toISOString(),
    });
  }

  if (config.RABBITMQ_URL) {
    rabbitmqUrl = config.RABBITMQ_URL;
  }

  logger.info({ nodeId, rabbitmqUrl, queue, publishExchange }, "Connecting to RabbitMQ");
  const ctx = await createConnection(rabbitmqUrl, publishExchange);
  logger.info("RabbitMQ connected");

  const publishNodeEvent = (event: string, payload: Record<string, unknown>): void => {
    const routingKey = `node.${nodeId}.events`;
    publish(ctx.channel, ctx.publishExchange, routingKey, {
      event,
      nodeId,
      timestamp: new Date().toISOString(),
      ...payload,
    });
  };

  const workerPool = new WorkerPool({
    nodeId,
    resourceMonitor,
    maxConcurrency,
    jobTimeoutMs: config.JOB_TIMEOUT_MS,
    logUploadConfig,
    jobWorkDir: config.JOB_WORK_DIR,
    onLogChunk: (jobId, chunk) => {
      publishNodeEvent("job.log.chunk", { jobId, chunk });
    },
    onJobResult: ({ jobId, exitCode, timedOut, output, logObjectKey, artifacts, error }) => {
      if (timedOut) {
        publishNodeEvent("job.timeout", { jobId, output: output ?? "", ...(logObjectKey != null ? { logObjectKey } : {}) });
      } else if (error) {
        publishNodeEvent("job.failed", { jobId, error: error.message, output: output ?? "", ...(logObjectKey != null ? { logObjectKey } : {}), ...(artifacts?.length ? { artifacts } : {}) });
      } else if (exitCode === 0) {
        publishNodeEvent("job.completed", {
          jobId,
          exitCode,
          output: output ?? "",
          ...(logObjectKey != null ? { logObjectKey } : {}),
          ...(artifacts?.length ? { artifacts } : {}),
        });
      } else {
        publishNodeEvent("job.failed", {
          jobId,
          error: `Exit code ${exitCode}`,
          output: output ?? "",
          ...(logObjectKey != null ? { logObjectKey } : {}),
          ...(artifacts?.length ? { artifacts } : {}),
        });
      }
    },
    onServiceResult: ({ serviceId, hostPort, url, error }) => {
      if (error) {
        publishNodeEvent("service.failed", { jobId: serviceId, serviceId, error: error.message });
      } else {
        publishNodeEvent("service.started", { jobId: serviceId, serviceId, port: hostPort, url });
      }
    },
  });

  logger.info({ nodeId, maxConcurrency }, "Starting job consumer");

  const consumerTag = await consume(
    ctx.channel,
    queue,
    maxConcurrency,
    async (payload: unknown) => {
      const msg = payload as ScheduledJobMessage;
      const typeStr = (msg.type ?? msg.jobType ?? "").toLowerCase();
      
      if (typeStr === "cancel" || typeStr === "cancelled") {
        logger.info({ jobId: msg.jobId }, "Received job cancellation dispatch from queue");
        if (msg.jobId) workerPool.cancelJob(msg.jobId);
        return;
      }

      const isService = typeStr === "service";
      const isBatchImage = typeStr === "batch-image";
      logger.info({ jobId: msg.jobId, runtime: msg.runtime, jobType: msg.jobType }, "Job received");

      // Common required fields for all job types
      if (
        !msg.jobId ||
        !msg.jobType ||
        !msg.resources ||
        typeof msg.resources !== "object" ||
        !msg.orgId
      ) {
        logger.warn(
          { payload },
          "Invalid job payload: missing jobId, jobType, resources, or orgId. Discarding."
        );
        throw new InvalidJobPayloadError("Payload missing required fields", payload);
      }

      // batch-image: require image, command, input, output
      if (isBatchImage) {
        if (!msg.image || typeof msg.image !== "string") {
          logger.warn({ payload }, "batch-image job missing image. Discarding.");
          throw new InvalidJobPayloadError("image is required for batch-image jobs", payload);
        }
        if (!Array.isArray(msg.command) || msg.command.length === 0) {
          logger.warn({ payload }, "batch-image job missing command. Discarding.");
          throw new InvalidJobPayloadError("command must be a non-empty array for batch-image jobs", payload);
        }
        if (!msg.input || typeof msg.input !== "object") {
          logger.warn({ payload }, "batch-image job missing input. Discarding.");
          throw new InvalidJobPayloadError("input is required for batch-image jobs", payload);
        }
        if (!msg.output || typeof msg.output !== "object") {
          logger.warn({ payload }, "batch-image job missing output. Discarding.");
          throw new InvalidJobPayloadError("output is required for batch-image jobs", payload);
        }
      }

      // runtime is required for batch and service (not batch-image which uses image directly)
      if (!isBatchImage && (!msg.runtime || typeof msg.runtime !== "string")) {
        logger.warn({ payload }, "Job missing runtime. Discarding.");
        throw new InvalidJobPayloadError("runtime (Docker image) is required", payload);
      }

      // Batch-only required fields
      if (!isService && !isBatchImage && (!msg.repoUrl || typeof msg.repoUrl !== "string")) {
        logger.warn({ payload }, "Batch job missing repoUrl. Discarding.");
        throw new InvalidJobPayloadError("repoUrl is required for batch jobs", payload);
      }
      if (!isService && !isBatchImage && (!msg.startCommand || typeof msg.startCommand !== "string")) {
        logger.warn({ payload }, "Batch job missing startCommand. Discarding.");
        throw new InvalidJobPayloadError("startCommand is required for batch jobs", payload);
      }

      const job = toScheduledJob(msg);

      if (!workerPool.canAccept(job)) {
        logger.warn({ jobId: job.jobId }, "Rejecting job: insufficient resources or draining");
        throw new Error(`Cannot accept job ${job.jobId}: resources unavailable or draining`);
      }

      // Publish job.started for ALL jobs so the backend transitions status from QUEUED to RUNNING
      publishNodeEvent("job.started", { jobId: job.jobId });

      if (isService) {
        logger.info({ jobId: job.jobId }, "Service job accepted, starting container");
      } else {
        logger.info({ jobId: job.jobId }, "Job started, running container");
      }

      await workerPool.run(job);
      logger.info({ jobId: job.jobId, type: job.type }, "Job/service dispatched");
    }
  );

  const heartbeatInterval = setInterval(() => {
    const { availableCpu, availableMemoryMb } = resourceMonitor.getAvailable();
    sendHeartbeat(config.SCHEDULER_BASE_URL, {
      nodeId,
      availableCpu,
      availableMemoryMB: availableMemoryMb,
    }).catch((err) => {
      logger.warn({ err, nodeId }, "Heartbeat failed");
    });
  }, config.HEARTBEAT_INTERVAL_MS);

  let shutdownStarted = false;
  const stopServicesOnShutdown = process.env.STOP_SERVICES_ON_SHUTDOWN === "true";

  installSigTermHandler(() => {
    if (shutdownStarted) return;
    shutdownStarted = true;

    gracefulShutdown({
      onSignal: () => {
        workerPool.startDrain();
        cancelConsumer(ctx.channel, consumerTag);
        clearInterval(heartbeatInterval);
      },
      drainWaiter: () => workerPool.hasActiveJobs(),
      steps: [
        async () => {
          if (stopServicesOnShutdown) {
            const running = serviceRegistry.listServices().filter((s) => s.status === "running");
            logger.info({ count: running.length }, "Stopping running services on shutdown");
            await Promise.allSettled(
              running.map((s) =>
                serviceRegistry.stopService(s.serviceId).then(() => {
                  publishNodeEvent("service.stopped", {
                    serviceId: s.serviceId,
                    port: s.port,
                    url: `http://${s.serviceId}.computebay.online`,
                  });
                }).catch((err) => {
                  logger.warn({ err, serviceId: s.serviceId }, "Failed to stop service on shutdown");
                })
              )
            );
          }
        },
        async () => {
          const { availableCpu, availableMemoryMb } = resourceMonitor.getAvailable();
          await sendHeartbeat(config.SCHEDULER_BASE_URL, {
            nodeId,
            availableCpu,
            availableMemoryMB: availableMemoryMb,
          }).catch((err) => {
            logger.warn({ err, nodeId }, "Final heartbeat failed");
          });
        },
        () => closeConnection(ctx),
      ],
    }).then(
      () => process.exit(0),
      (err) => {
        logger.fatal({ err }, "Shutdown failed");
        process.exit(1);
      }
    );
  });
}

main().catch((err) => {
  logger.fatal({ err }, "Bootstrap failed");
  process.exit(1);
});
