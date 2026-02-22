import { loadConfig } from "./config/config.js";
import { getNodeId, getPublicKey } from "./security/identity.js";
import { detectSystemSpecs } from "./monitoring/system.specs.js";
import { computeAllocatable } from "./monitoring/capacity.js";
import { ResourceMonitor } from "./monitoring/resource.monitor.js";
import { createConnection, closeConnection } from "./messaging/rabbit.connection.js";
import { consume, cancelConsumer, waitForApproval } from "./messaging/rabbit.consumer.js";
import { publishSigned } from "./messaging/rabbit.publisher.js";
import { WorkerPool } from "./execution/worker.pool.js";
import { gracefulShutdown, installSigTermHandler } from "./lifecycle/shutdown.js";
import type { ScheduledJob } from "./types/job.types.js";
import { logger } from "./utils/logger.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const nodeId = getNodeId();

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

  const ctx = await createConnection(config.RABBITMQ_URL);

  publishSigned(ctx.channel, "node.registration.requested", {
    nodeId,
    publicKey: getPublicKey(),
    totalCpu: specs.totalCpu,
    totalMemoryMb: specs.totalMemoryMb,
    allocatableCpu,
    allocatableMemoryMb,
    timestamp: new Date().toISOString(),
  });
  logger.info({ nodeId, allocatableCpu, allocatableMemoryMb }, "Registration requested");

  await waitForApproval(ctx.channel, nodeId);
  logger.info({ nodeId }, "Node approved, starting heartbeat and job consumer");

  const publishJobEvent = (routingKey: string, payload: Record<string, unknown>): void => {
    publishSigned(ctx.channel, routingKey, payload);
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

  const consumerTag = await consume(
    ctx.channel,
    nodeId,
    maxConcurrency,
    async (payload: unknown) => {
      const job = payload as ScheduledJob;
      if (
        !job.jobId ||
        !job.image ||
        typeof job.cpu !== "number" ||
        typeof job.memoryMB !== "number"
      ) {
        throw new Error(`Invalid job payload: ${JSON.stringify(payload)}`);
      }

      if (!workerPool.canAccept(job)) {
        throw new Error(
          `Cannot accept job ${job.jobId}: resources unavailable or draining`
        );
      }

      publishJobEvent("job.started", { jobId: job.jobId });

      await workerPool.run(job);
    }
  );

  const heartbeatInterval = setInterval(() => {
    const usage = resourceMonitor.getUsage();
    const { availableCpu, availableMemoryMb } = resourceMonitor.getAvailable();
    publishSigned(ctx.channel, "node.heartbeat", {
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
          publishSigned(ctx.channel, "node.heartbeat", {
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
