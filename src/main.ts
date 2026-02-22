import { loadConfig } from "./config/config.js";
import { getNodeId, getPublicKey } from "./security/identity.js";
import { detectSystemSpecs } from "./monitoring/system.specs.js";
import { computeAllocatable } from "./monitoring/capacity.js";
import { ResourceMonitor } from "./monitoring/resource.monitor.js";
import { createConnection, closeConnection } from "./messaging/rabbit.connection.js";
import { consume, cancelConsumer } from "./messaging/rabbit.consumer.js";
import { publish, publishHeartbeat } from "./messaging/rabbit.publisher.js";
import { WorkerPool } from "./execution/worker.pool.js";
import { gracefulShutdown, installSigTermHandler } from "./lifecycle/shutdown.js";
import type { ScheduledJob } from "./types/job.types.js";
import { InvalidJobPayloadError } from "./types/job.types.js";
import { logger } from "./utils/logger.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const nodeId = getNodeId();
  logger.info({ nodeId }, "Worker starting");


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

  logger.info({ url: config.RABBITMQ_URL }, "Connecting to RabbitMQ");
  const ctx = await createConnection(config.RABBITMQ_URL);
  logger.info("RabbitMQ connected");

  publish(ctx.channel, "node.registered", {
    nodeId,
    publicKey: getPublicKey(),
    totalCpu: specs.totalCpu,
    totalMemoryMb: specs.totalMemoryMb,
    allocatableCpu,
    allocatableMemoryMb,
    timestamp: new Date().toISOString(),
  });
  logger.info({ nodeId, allocatableCpu, allocatableMemoryMb }, "Node registered, publishing to Scheduler");

  const publishJobEvent = (routingKey: string, payload: Record<string, unknown>): void => {
    publish(ctx.channel, routingKey, payload);
  };

  const workerPool = new WorkerPool({
    nodeId,
    resourceMonitor,
    maxConcurrency,
    jobTimeoutMs: config.JOB_TIMEOUT_MS,
    onJobResult: ({ jobId, exitCode, timedOut, error }) => {
      if (timedOut) {
        publishJobEvent("job.timeout", { jobId });
      } else if (error) {
        publishJobEvent("job.failed", { jobId, error: error.message });
      } else if (exitCode === 0) {
        publishJobEvent("job.completed", { jobId, exitCode });
      } else {
        publishJobEvent("job.failed", { jobId, error: `Exit code ${exitCode}` });
      }
    },
  });

  logger.info({ nodeId, maxConcurrency }, "Starting job consumer");
  const consumerTag = await consume(
    ctx.channel,
    nodeId,
    maxConcurrency,
    async (payload: unknown) => {
      const job = payload as ScheduledJob;
      logger.info({ jobId: job.jobId, image: job.image }, "Job received");
      if (
        !job.jobId ||
        !job.image ||
        typeof job.cpu !== "number" ||
        typeof job.memoryMB !== "number"
      ) {
        logger.warn(
          { payload, missing: "image, cpu, and/or memoryMB" },
          "Invalid job payload: Scheduler must send jobId, image, cpu, memoryMB. Discarding (no requeue)."
        );
        throw new InvalidJobPayloadError(
          "Payload missing image, cpu, or memoryMB",
          payload
        );
      }

      if (!workerPool.canAccept(job)) {
        logger.warn({ jobId: job.jobId }, "Rejecting job: insufficient resources or draining");
        throw new Error(
          `Cannot accept job ${job.jobId}: resources unavailable or draining`
        );
      }

      publishJobEvent("job.started", { jobId: job.jobId });
      logger.info({ jobId: job.jobId }, "Job started, running container");

      await workerPool.run(job);
      logger.info({ jobId: job.jobId }, "Job finished");
    }
  );

  const heartbeatInterval = setInterval(() => {
    const usage = resourceMonitor.getUsage();
    const { availableCpu, availableMemoryMb } = resourceMonitor.getAvailable();
    publishHeartbeat(ctx.channel, {
      nodeId,
      totalCpu: specs.totalCpu,
      totalMemoryMb: specs.totalMemoryMb,
      usedCpu: usage.usedCpu,
      usedMemoryMb: usage.usedMemoryMb,
      availableCpu,
      availableMemoryMb,
      timestamp: new Date().toISOString(),
      draining: workerPool.isDraining(),
    });
  }, config.HEARTBEAT_INTERVAL_MS);

  let shutdownStarted = false;

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
          const usage = resourceMonitor.getUsage();
          const { availableCpu, availableMemoryMb } = resourceMonitor.getAvailable();
          publishHeartbeat(ctx.channel, {
            nodeId,
            totalCpu: specs.totalCpu,
            totalMemoryMb: specs.totalMemoryMb,
            usedCpu: usage.usedCpu,
            usedMemoryMb: usage.usedMemoryMb,
            availableCpu,
            availableMemoryMb,
            timestamp: new Date().toISOString(),
            draining: true,
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
