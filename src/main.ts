import { loadConfig } from "./config/config.js";
import { getMachineId } from "./security/identity.js";
import { detectSystemSpecs } from "./monitoring/system.specs.js";
import { computeAllocatable } from "./monitoring/capacity.js";
import { ResourceMonitor } from "./monitoring/resource.monitor.js";
import { createConnection, closeConnection } from "./messaging/rabbit.connection.js";
import { consume, cancelConsumer } from "./messaging/rabbit.consumer.js";
import { publish } from "./messaging/rabbit.publisher.js";
import { WorkerPool } from "./execution/worker.pool.js";
import { gracefulShutdown, installSigTermHandler } from "./lifecycle/shutdown.js";
import type { ScheduledJobMessage } from "./types/job.types.js";
import { InvalidJobPayloadError, toScheduledJob } from "./types/job.types.js";
import { logger } from "./utils/logger.js";
import { registerNodeWithRetry, sendHeartbeat } from "./bootstrap/scheduler.client.js";
import { saveBootstrapState } from "./bootstrap/bootstrap.state.js";
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
  logger.info({ schedulerBaseUrl: config.SCHEDULER_BASE_URL }, "Registering node via Scheduler");
  const reg = await registerNodeWithRetry(config.SCHEDULER_BASE_URL, {
    machineId,
    cpuCores: specs.totalCpu,
    memoryMB: specs.totalMemoryMb,
    agentVersion,
  });

  const nodeId = reg.nodeId;
  const rabbitmqUrl = reg.rabbitmq.url;
  const queueTemplate = reg.rabbitmq.queue;
  const publishExchange = reg.rabbitmq.publishExchange;
  const queue = queueTemplate
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

  logger.info({ nodeId, rabbitmqUrl, queue, publishExchange }, "Registration complete; connecting to RabbitMQ");
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
    onJobResult: ({ jobId, exitCode, timedOut, output, error }) => {
      if (timedOut) {
        publishNodeEvent("job.timeout", { jobId, output: output ?? "" });
      } else if (error) {
        publishNodeEvent("job.failed", { jobId, error: error.message, output: output ?? "" });
      } else if (exitCode === 0) {
        publishNodeEvent("job.completed", { jobId, exitCode, output: output ?? "" });
      } else {
        publishNodeEvent("job.failed", {
          jobId,
          error: `Exit code ${exitCode}`,
          output: output ?? "",
        });
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
      logger.info({ jobId: msg.jobId, runtime: msg.runtime }, "Job received");
      if (
        !msg.jobId ||
        !msg.jobType ||
        !msg.runtime ||
        !Array.isArray(msg.entrypoint) ||
        !msg.resources ||
        typeof msg.resources !== "object" ||
        !msg.inputArtifacts ||
        typeof msg.inputArtifacts !== "object" ||
        !msg.orgId
      ) {
        logger.warn(
          { payload },
          "Invalid job payload: must match CreateJobInput (jobId, jobType, runtime, entrypoint, resources, inputArtifacts, orgId). Discarding (no requeue)."
        );
        throw new InvalidJobPayloadError("Payload missing required CreateJobInput fields", payload);
      }

      const job = toScheduledJob(msg);

      if (!workerPool.canAccept(job)) {
        logger.warn({ jobId: job.jobId }, "Rejecting job: insufficient resources or draining");
        throw new Error(
          `Cannot accept job ${job.jobId}: resources unavailable or draining`
        );
      }

      publishNodeEvent("job.started", { jobId: job.jobId });
      logger.info({ jobId: job.jobId }, "Job started, running container");

      await workerPool.run(job);
      logger.info({ jobId: job.jobId }, "Job finished");
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
