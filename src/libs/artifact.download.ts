import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import type { S3Config } from "./s3.js";
import { getObjectBuffer } from "./s3.js";
import { logger } from "../utils/logger.js";

/**
 * Parse an s3:// URL into bucket and key.
 * Accepts: s3://bucket/key/prefix  →  bucket = "bucket", key = "key/prefix"
 */
export function parseS3Url(url: string): { bucket: string; key: string } {
  if (!url.startsWith("s3://")) {
    throw new Error(`Invalid S3 URL: must start with s3://, got: ${url}`);
  }
  const withoutScheme = url.slice("s3://".length);
  const slashIdx = withoutScheme.indexOf("/");
  if (slashIdx <= 0) {
    throw new Error(
      `Invalid S3 URL: must contain s3://bucket/key, got: ${url}`
    );
  }
  const bucket = withoutScheme.slice(0, slashIdx);
  const key = withoutScheme.slice(slashIdx + 1);
  if (!key) {
    throw new Error(`Invalid S3 URL: key part is empty in ${url}`);
  }
  return { bucket, key };
}

/**
 * Download a single S3/MinIO object to a local file path.
 *
 * @param config  S3/MinIO client configuration (endpoint, credentials).
 * @param sourceUrl  Full s3:// URL (e.g. s3://mybucket/data/input.zip).
 * @param destPath   Absolute local path to write the downloaded file.
 *                   Parent directories are created automatically.
 * @throws If the URL is malformed, the object does not exist, or the
 *         download / file write fails.
 */
export async function downloadInput(
  config: S3Config,
  sourceUrl: string,
  destPath: string
): Promise<void> {
  let key: string;
  if (sourceUrl.startsWith("staging://")) {
    // staging:// prefix — raw S3 key, use configured bucket
    key = sourceUrl.slice("staging://".length);
  } else {
    const parsed = parseS3Url(sourceUrl);
    key = parsed.key;
  }

  logger.info({ key, destPath }, "Downloading S3 input");

  let body: Buffer;
  try {
    body = await getObjectBuffer(config, key);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    if (msg.includes("NoSuchKey") || msg.includes("404")) {
      throw new Error(
        `S3 object not found: ${sourceUrl} (key=${key})`
      );
    }
    throw new Error(
      `Failed to download S3 object ${sourceUrl}: ${msg}`
    );
  }

  const dir = path.dirname(destPath);
  await mkdir(dir, { recursive: true });

  await writeFile(destPath, body);
  logger.info(
    { key, destPath, sizeBytes: body.length },
    "S3 input downloaded"
  );
}
