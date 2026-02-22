import { logger } from "../utils/logger.js";

export type DrainWaiter = () => boolean;
export type ShutdownStep = () => Promise<void>;

export interface ShutdownOptions {
  onSignal: () => void;
  drainWaiter: DrainWaiter;
  steps: ShutdownStep[];
  drainPollMs?: number;
}

/**
 * Graceful shutdown: wait for drain, then run steps in order.
 */
export async function gracefulShutdown(options: ShutdownOptions): Promise<void> {
  const { onSignal, drainWaiter, steps, drainPollMs = 500 } = options;

  onSignal();
  logger.info("Shutdown initiated, waiting for active jobs");

  while (drainWaiter()) {
    await new Promise((r) => setTimeout(r, drainPollMs));
  }

  logger.info("All jobs drained, running shutdown steps");

  for (const step of steps) {
    try {
      await step();
    } catch (err) {
      logger.error({ err }, "Shutdown step failed");
    }
  }

  logger.info("Shutdown complete");
}

export function installSigTermHandler(
  handler: () => void
): void {
  process.on("SIGTERM", () => {
    logger.info("Received SIGTERM");
    handler();
  });
}
