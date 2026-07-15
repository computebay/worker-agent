import { readdir, stat, readFile } from "node:fs/promises";
import path from "node:path";
import type { S3Config } from "./s3.js";
import { putObjectBuffer } from "./s3.js";
import type { JobArtifact } from "../types/job.types.js";
import { logger } from "../utils/logger.js";

/** Object key prefix for job artifacts in MinIO. */
export const ARTIFACT_KEY_PREFIX = "jobs";

/** Maximum artifact count per job */
export const MAX_ARTIFACT_COUNT = 100;

/** Maximum single artifact size (50MB) */
export const MAX_SINGLE_ARTIFACT_SIZE = 50 * 1024 * 1024;

/** Maximum total artifact size (500MB) */
export const MAX_TOTAL_ARTIFACT_SIZE = 500 * 1024 * 1024;

/**
 * Get content type based on file extension
 */
function getContentType(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  const contentTypes: Record<string, string> = {
    ".js": "application/javascript",
    ".ts": "application/typescript",
    ".json": "application/json",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".html": "text/html",
    ".css": "text/css",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
    ".svg": "image/svg+xml",
    ".mp4": "video/mp4",
    ".webm": "video/webm",
    ".zip": "application/zip",
    ".tar": "application/x-tar",
    ".gz": "application/gzip",
    ".pdf": "application/pdf",
    ".csv": "text/csv",
    ".xml": "application/xml",
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
  };
  return contentTypes[ext] || "application/octet-stream";
}

/**
 * Recursively enumerate files in a directory, skipping symlinks
 */
async function enumerateFiles(
  dirPath: string,
  basePath: string = dirPath
): Promise<{ relativePath: string; absolutePath: string }[]> {
  const files: { relativePath: string; absolutePath: string }[] = [];
  
  try {
    const entries = await readdir(dirPath, { withFileTypes: true });
    
    for (const entry of entries) {
      const absolutePath = path.join(dirPath, entry.name);
      
      // Skip symlinks for security
      if (entry.isSymbolicLink()) {
        logger.warn({ path: absolutePath }, "Skipping symlink in artifact collection");
        continue;
      }
      
      if (entry.isDirectory()) {
        const subFiles = await enumerateFiles(absolutePath, basePath);
        files.push(...subFiles);
      } else if (entry.isFile()) {
        const relativePath = path.relative(basePath, absolutePath);
        files.push({ relativePath, absolutePath });
      }
    }
  } catch (err) {
    logger.error({ err, dirPath }, "Failed to enumerate files");
  }
  
  return files;
}

/**
 * Collect and upload artifacts from the outputs directory
 * @param s3Config MinIO configuration
 * @param jobId Job identifier
 * @param outputsDir Path to the outputs directory (e.g., /workspace/outputs)
 * @returns Array of uploaded artifact metadata
 */
export async function collectAndUploadArtifacts(
  s3Config: S3Config,
  jobId: string,
  outputsDir: string
): Promise<JobArtifact[]> {
  const artifacts: JobArtifact[] = [];
  let totalSize = 0;
  
  try {
    // Check if outputs directory exists
    await stat(outputsDir);
  } catch {
    // Directory doesn't exist, no artifacts to collect
    logger.info({ jobId, outputsDir }, "No outputs directory found, skipping artifact collection");
    return artifacts;
  }
  
  const files = await enumerateFiles(outputsDir);
  
  if (files.length === 0) {
    logger.info({ jobId, outputsDir }, "No files found in outputs directory");
    return artifacts;
  }
  
  if (files.length > MAX_ARTIFACT_COUNT) {
    logger.warn(
      { jobId, count: files.length, max: MAX_ARTIFACT_COUNT },
      "Too many artifacts, only uploading first batch"
    );
  }
  
  const filesToUpload = files.slice(0, MAX_ARTIFACT_COUNT);
  
  for (const file of filesToUpload) {
    try {
      const fileStat = await stat(file.absolutePath);
      const fileSize = fileStat.size;
      
      // Check single file size limit
      if (fileSize > MAX_SINGLE_ARTIFACT_SIZE) {
        logger.warn(
          { jobId, file: file.relativePath, size: fileSize, max: MAX_SINGLE_ARTIFACT_SIZE },
          "Skipping artifact exceeding size limit"
        );
        continue;
      }
      
      // Check total size limit
      if (totalSize + fileSize > MAX_TOTAL_ARTIFACT_SIZE) {
        logger.warn(
          { jobId, file: file.relativePath, totalSize, max: MAX_TOTAL_ARTIFACT_SIZE },
          "Skipping artifact exceeding total size limit"
        );
        continue;
      }
      
      const body = await readFile(file.absolutePath);
      const objectKey = `${ARTIFACT_KEY_PREFIX}/${jobId}/outputs/${file.relativePath}`;
      const contentType = getContentType(file.absolutePath);
      
      await putObjectBuffer(s3Config, objectKey, body);
      
      const artifact: JobArtifact = {
        name: path.basename(file.relativePath),
        key: objectKey,
        sizeBytes: fileSize,
        contentType,
        kind: "output",
        createdAt: new Date().toISOString(),
      };
      
      artifacts.push(artifact);
      totalSize += fileSize;
      
      logger.debug(
        { jobId, file: file.relativePath, key: objectKey, size: fileSize },
        "Uploaded artifact"
      );
    } catch (err) {
      logger.error(
        { err, jobId, file: file.relativePath },
        "Failed to upload artifact"
      );
    }
  }
  
  logger.info(
    { jobId, artifactCount: artifacts.length, totalSize },
    "Artifact collection complete"
  );
  
  return artifacts;
}
