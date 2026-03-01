import JSZip from "jszip";
import { mkdir, writeFile } from "node:fs/promises";
import path from "node:path";
import type { S3Config } from "./s3.js";
import { getObjectBuffer } from "./s3.js";

/**
 * Download the job input artifact (zip) from MinIO by object key.
 * Key is typically inputs/<jobId>/bundle.zip.
 */
export async function downloadArtifact(
  config: S3Config,
  objectKey: string
): Promise<Buffer> {
  return getObjectBuffer(config, objectKey);
}

/**
 * Extract a zip buffer into a directory. Creates the directory if needed.
 * Normalizes entry names to prevent zip slip (paths outside targetDir are rejected).
 */
export async function extractZipToDir(
  zipBuffer: Buffer,
  targetDir: string
): Promise<void> {
  const zip = await JSZip.loadAsync(zipBuffer);
  const resolvedDir = path.resolve(targetDir);
  await mkdir(resolvedDir, { recursive: true });

  const entries = Object.entries(zip.files).filter(
    ([_, entry]) => !entry.dir
  ) as [string, JSZip.JSZipObject][];

  for (const [entryPath, entry] of entries) {
    const normalized = path.normalize(entryPath);
    const destResolved = path.resolve(resolvedDir, normalized);
    const relative = path.relative(resolvedDir, destResolved);
    if (relative.startsWith("..") || path.isAbsolute(relative)) {
      throw new Error(`Zip slip rejected: ${entryPath}`);
    }
    await mkdir(path.dirname(destResolved), { recursive: true });
    const content = await entry.async("nodebuffer");
    await writeFile(destResolved, content);
  }
}
