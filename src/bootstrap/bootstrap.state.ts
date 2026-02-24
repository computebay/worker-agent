import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { logger } from "../utils/logger.js";

const BOOTSTRAP_DIR = join(homedir(), ".computebay");
const BOOTSTRAP_FILE = join(BOOTSTRAP_DIR, "bootstrap.json");

export interface BootstrapState {
  nodeId: string;
  rabbitmqUrl: string;
  queue: string;
  publishExchange: string;
  registeredAt: string;
}

export function loadBootstrapState(): BootstrapState | null {
  try {
    if (!existsSync(BOOTSTRAP_FILE)) return null;
    const raw = readFileSync(BOOTSTRAP_FILE, "utf-8");
    const parsed = JSON.parse(raw) as Partial<BootstrapState>;
    if (
      typeof parsed.nodeId !== "string" ||
      typeof parsed.rabbitmqUrl !== "string" ||
      typeof parsed.queue !== "string" ||
      typeof parsed.publishExchange !== "string" ||
      typeof parsed.registeredAt !== "string"
    ) {
      return null;
    }
    return parsed as BootstrapState;
  } catch (err) {
    logger.warn({ err }, "Failed to load bootstrap state; ignoring");
    return null;
  }
}

export function saveBootstrapState(state: BootstrapState): void {
  mkdirSync(BOOTSTRAP_DIR, { recursive: true });
  writeFileSync(BOOTSTRAP_FILE, JSON.stringify(state, null, 0), {
    mode: 0o600,
  });
}

