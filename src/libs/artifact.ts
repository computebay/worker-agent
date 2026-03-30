import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createReadStream, createWriteStream, statSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { pipeline } from "node:stream/promises";
import type { Readable } from "node:stream";
import { GetObjectCommand } from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";
import type { S3Config } from "./s3.js";
import { getS3Client } from "./s3.js";
import { logger } from "../utils/logger.js";

export const DEPLOYMENTS_PREFIX = "deployments";

/** MinIO key for a service artifact: deployments/<serviceId>/app.tar.gz */
export function artifactKey(serviceId: string): string {
  return `${DEPLOYMENTS_PREFIX}/${serviceId}/app.tar.gz`;
}

/** MinIO key for a service build log: deployments/<serviceId>/build.log */
export function buildLogKey(serviceId: string): string {
  return `${DEPLOYMENTS_PREFIX}/${serviceId}/build.log`;
}

/**
 * Archive srcDir into destTarPath using system `tar czf`.
 * Excludes `.git`. Returns the sha256 hex checksum of the produced archive.
 */
export async function tarDirectory(srcDir: string, destTarPath: string): Promise<string> {
  await new Promise<void>((resolve, reject) => {
    const proc = spawn(
      "tar",
      ["czf", destTarPath, "--exclude=./.git", "-C", srcDir, "."],
      { stdio: ["ignore", "pipe", "pipe"] }
    );
    let stderr = "";
    proc.stderr?.on("data", (c: Buffer) => { stderr += c.toString(); });
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`tar czf failed (exit ${code}): ${stderr}`));
    });
    proc.on("error", reject);
  });
  return computeFileChecksum(destTarPath);
}

/**
 * Extract a .tar.gz archive into destDir using system `tar xzf`.
 * Creates destDir if it does not exist.
 */
export async function untarToDirectory(tarPath: string, destDir: string): Promise<void> {
  await mkdir(destDir, { recursive: true });
  await new Promise<void>((resolve, reject) => {
    const proc = spawn(
      "tar",
      ["xzf", tarPath, "-C", destDir],
      { stdio: ["ignore", "pipe", "pipe"] }
    );
    let stderr = "";
    proc.stderr?.on("data", (c: Buffer) => { stderr += c.toString(); });
    proc.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`tar xzf failed (exit ${code}): ${stderr}`));
    });
    proc.on("error", reject);
  });
}

/** Compute the sha256 hex checksum of a local file. */
export async function computeFileChecksum(filePath: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const stream = createReadStream(filePath);
    stream.on("data", (chunk) => hash.update(chunk as Buffer));
    stream.on("end", () => resolve(hash.digest("hex")));
    stream.on("error", reject);
  });
}

/** Upload a local file to MinIO at the given key using multipart upload. */
export async function uploadArtifact(
  s3Config: S3Config,
  key: string,
  tarPath: string
): Promise<void> {
  const s3 = getS3Client(s3Config);
  const fileSize = statSync(tarPath).size;

  const upload = new Upload({
    client: s3,
    params: {
      Bucket: s3Config.bucket,
      Key: key,
      Body: createReadStream(tarPath),
      ContentLength: fileSize,
    },
    queueSize: 4,
    partSize: 10 * 1024 * 1024, // 10MB per part
  });

  upload.on("httpUploadProgress", (progress) => {
    logger.info({ key, ...progress }, "Artifact upload progress");
  });

  await upload.done();
  logger.info({ key, fileSize }, "Artifact uploaded to MinIO");
}

/** Download an artifact from MinIO and write it to destPath. */
export async function downloadArtifact(
  s3Config: S3Config,
  key: string,
  destPath: string
): Promise<void> {
  const s3 = getS3Client(s3Config);
  const response = await s3.send(
    new GetObjectCommand({ Bucket: s3Config.bucket, Key: key })
  );
  if (!response.Body) {
    throw new Error(`No body in S3 response for artifact key: ${key}`);
  }
  await pipeline(response.Body as Readable, createWriteStream(destPath));
  logger.info({ key, destPath }, "Artifact downloaded from MinIO");
}