import { readFile } from "node:fs/promises";
import type { S3Config } from "./s3.js";
import { putObjectBuffer } from "./s3.js";

/** Object key prefix for job logs in MinIO. */
export const LOG_ARTIFACT_KEY_PREFIX = "logs";

/**
 * Upload full log file to MinIO at logs/<jobId>/full.log.
 * Returns the object key.
 */
export async function uploadLogFile(
  config: S3Config,
  jobId: string,
  logFilePath: string
): Promise<string> {
  const body = await readFile(logFilePath);
  const objectKey = `${LOG_ARTIFACT_KEY_PREFIX}/${jobId}/full.log`;
  await putObjectBuffer(config, objectKey, body);
  return objectKey;
}
