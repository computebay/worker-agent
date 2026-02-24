import { createPrivateKey, generateKeyPairSync, randomUUID, sign } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { logger } from "../utils/logger.js";

const IDENTITY_DIR = join(homedir(), ".computebay");
const IDENTITY_FILE = join(IDENTITY_DIR, "identity.json");

interface IdentityData {
  nodeId: string;
  privateKey: string;
  publicKey: string;
}

let identity: IdentityData | null = null;

function loadIdentity(): IdentityData {
  if (identity) return identity;

  if (!existsSync(IDENTITY_FILE)) {
    const nodeId = randomUUID();
    const { publicKey, privateKey } = generateKeyPairSync("ed25519", {
      publicKeyEncoding: { type: "spki", format: "pem" },
      privateKeyEncoding: { type: "pkcs8", format: "pem" },
    });
    identity = { nodeId, privateKey, publicKey };
    mkdirSync(IDENTITY_DIR, { recursive: true });
    try {
      writeFileSync(IDENTITY_FILE, JSON.stringify(identity, null, 0), {
        mode: 0o600,
        flag: "wx",
      });
      logger.info({ nodeId, identityFile: IDENTITY_FILE }, "Created new node identity");
    } catch (err: unknown) {
      if ((err as NodeJS.ErrnoException)?.code === "EEXIST") {
        const raw = readFileSync(IDENTITY_FILE, "utf-8");
        identity = JSON.parse(raw) as IdentityData;
        logger.info({ nodeId: identity.nodeId }, "Loaded existing identity (race)");
        return identity;
      }
      throw err;
    }
    return identity;
  }

  const raw = readFileSync(IDENTITY_FILE, "utf-8");
  identity = JSON.parse(raw) as IdentityData;
  if (
    typeof identity.nodeId !== "string" ||
    typeof identity.privateKey !== "string" ||
    typeof identity.publicKey !== "string"
  ) {
    throw new Error("Invalid identity file format");
  }
  logger.info({ nodeId: identity.nodeId, identityFile: IDENTITY_FILE }, "Loaded existing node identity");
  return identity;
}

/**
 * Get the stable machine fingerprint (UUID) from the persisted identity file.
 *
 * Note: Historically this was called nodeId; in the hybrid architecture the Scheduler
 * assigns a per-session/per-registration nodeId, while this value remains stable per machine.
 */
export function getNodeId(): string {
  return loadIdentity().nodeId;
}

export function getMachineId(): string {
  return loadIdentity().nodeId;
}

/**
 * Get the public key in PEM format.
 */
export function getPublicKey(): string {
  return loadIdentity().publicKey;
}

/**
 * Sign a payload (without signature field) and return Base64-encoded signature.
 * Payload must include nodeId and timestamp for all outgoing messages.
 */
export function signPayload(payload: object): string {
  const { signature: _, ...payloadWithoutSignature } = payload as Record<
    string,
    unknown
  > & { signature?: string };
  const data = Buffer.from(JSON.stringify(payloadWithoutSignature), "utf-8");
  const key = createPrivateKey(loadIdentity().privateKey);
  const sig = sign(null, data, key);
  return sig.toString("base64");
}
