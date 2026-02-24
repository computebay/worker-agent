import { z } from "zod";
import dotenv from "dotenv";

dotenv.config();

const configSchema = z.object({
  SCHEDULER_BASE_URL: z.string().url().min(1),
  CPU_LIMIT: z.coerce.number().positive().optional(),
  MEMORY_LIMIT_MB: z.coerce.number().positive().optional(),
  HEARTBEAT_INTERVAL_MS: z.coerce.number().positive().default(10000),
  MAX_CONCURRENCY: z.coerce.number().int().positive().optional(),
  JOB_TIMEOUT_MS: z.coerce.number().positive().default(3600000),
  AGENT_VERSION: z.string().min(1).optional(),
});

export type Config = z.infer<typeof configSchema>;

export function loadConfig(): Config {
  const parsed = configSchema.safeParse(process.env);

  if (!parsed.success) {
    const msg = parsed.error.errors
      .map((e) => `${e.path.join(".")}: ${e.message}`)
      .join("; ");
    throw new Error(`Invalid configuration: ${msg}`);
  }

  return parsed.data;
}
