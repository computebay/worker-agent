import {
  S3Client,
  GetObjectCommand,
  type GetObjectCommandOutput,
} from "@aws-sdk/client-s3";
import { Readable } from "node:stream";

export interface S3Config {
  endpoint: string;
  region: string;
  accessKey: string;
  secretKey: string;
  bucket: string;
}

let client: S3Client | null = null;

/**
 * Create or return a cached S3 client configured for MinIO.
 * Uses MINIO_ENDPOINT, MINIO_REGION, MINIO_ACCESS_KEY, MINIO_SECRET_KEY from config.
 */
export function getS3Client(config: S3Config): S3Client {
  if (!client) {
    client = new S3Client({
      region: config.region,
      endpoint: config.endpoint,
      credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
      },
      forcePathStyle: true,
    });
  }
  return client;
}

/**
 * Download an object from S3/MinIO and return its body as a Buffer.
 */
export async function getObjectBuffer(
  config: S3Config,
  key: string
): Promise<Buffer> {
  const s3 = getS3Client(config);
  const response: GetObjectCommandOutput = await s3.send(
    new GetObjectCommand({ Bucket: config.bucket, Key: key })
  );
  const body = response.Body;
  if (!body || !(body instanceof Readable)) {
    throw new Error(`Empty or invalid response body for key ${key}`);
  }
  const chunks: Buffer[] = [];
  for await (const chunk of body) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
