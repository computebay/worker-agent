import JSZip from "jszip";
import { mkdir, writeFile, readFile } from "node:fs/promises";
import path from "node:path";
import type { S3Config } from "./s3.js";
import { getObjectBuffer, putObjectBuffer } from "./s3.js";

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

/** Filename the container may write in /workspace for output artifact. */
export const OUTPUT_ARTIFACT_FILENAME = "output.zip";

/** Object key prefix for job output artifacts in MinIO. */
export const OUTPUT_ARTIFACT_KEY_PREFIX = "outputs";

/**
 * If output.zip exists in the job workspace, upload it to MinIO at outputs/<jobId>/result.zip.
 * Returns the object key if uploaded, otherwise undefined.
 */
export async function uploadOutputArtifact(
  config: S3Config,
  jobId: string,
  workDirPath: string
): Promise<string | undefined> {
  const outputPath = path.join(workDirPath, OUTPUT_ARTIFACT_FILENAME);
  try {
    const body = await readFile(outputPath);
    const objectKey = `${OUTPUT_ARTIFACT_KEY_PREFIX}/${jobId}/result.zip`;
    await putObjectBuffer(config, objectKey, body);
    return objectKey;
  } catch (err: unknown) {
    if (err && typeof err === "object" && "code" in err && err.code === "ENOENT") {
      return undefined;
    }
    throw err;
  }
}

/**
 * Build a zip containing stdout/stderr as output.txt, then upload to MinIO at outputs/<jobId>/result.zip.
 * Used when the container did not produce output.zip.
 */
export async function uploadOutputFromStdout(
  config: S3Config,
  jobId: string,
  output: string
): Promise<string> {
  const zip = new JSZip();
  zip.file("output.txt", output);
  const body = await zip.generateAsync({ type: "nodebuffer" });
  const objectKey = `${OUTPUT_ARTIFACT_KEY_PREFIX}/${jobId}/result.zip`;
  await putObjectBuffer(config, objectKey, body);
  return objectKey;
}
