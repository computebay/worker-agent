import { logger } from "../utils/logger.js";

export type NodeRegisterRequest = {
  machineId: string;
  cpuCores: number;
  memoryMB: number;
  agentVersion: string;
}

export type NodeRegisterResponse = {
  nodeId: string;
  rabbitmq: {
    url: string;
    queue: string;
    publishExchange: string;
  };
}

export type NodeHeartbeatRequest = {
  nodeId: string;
  availableCpu: number;
  availableMemoryMB: number;
}

function joinUrl(base: string, path: string): string {
  const b = base.endsWith("/") ? base.slice(0, -1) : base;
  const p = path.startsWith("/") ? path : `/${path}`;
  return `${b}${p}`;
}

async function postJson<TResponse>(
  url: string,
  body: unknown,
  timeoutMs: number
): Promise<TResponse> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  const authToken = process.env.AUTH_TOKEN;
  if (!authToken) {
    throw new Error("AUTH_TOKEN environment variable is not set");
  }
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json", Authorization: `Bearer ${authToken}` },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    if (!res.ok) {
      const text = await res.text().catch(() => "");
      throw new Error(`HTTP ${res.status} ${res.statusText}${text ? `: ${text}` : ""}`);
    }

    return (await res.json()) as TResponse;
  } finally {
    clearTimeout(t);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

function backoffMs(attempt: number): number {
  const base = Math.min(60_000, 1000 * 2 ** Math.min(10, attempt));
  const jitter = Math.floor(Math.random() * 250);
  return base + jitter;
}

export async function registerNodeWithRetry(
  schedulerBaseUrl: string,
  req: NodeRegisterRequest
): Promise<NodeRegisterResponse> {
  const url = joinUrl(schedulerBaseUrl, "/nodes/register");
  let attempt = 0;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    attempt += 1;
    try {
      const resp = await postJson<NodeRegisterResponse>(url, req, 10_000);
      if (
        !resp ||
        typeof resp.nodeId !== "string" ||
        !resp.rabbitmq ||
        typeof resp.rabbitmq.url !== "string" ||
        typeof resp.rabbitmq.queue !== "string" ||
        typeof resp.rabbitmq.publishExchange !== "string"
      ) {
        throw new Error("Invalid /nodes/register response shape");
      }
      return resp;
    } catch (err) {
      const wait = backoffMs(attempt);
      logger.warn(
        { err, attempt, waitMs: wait, schedulerBaseUrl },
        "Node registration failed; retrying"
      );
      await sleep(wait);
    }
  }
}

export async function sendHeartbeat(
  schedulerBaseUrl: string,
  req: NodeHeartbeatRequest
): Promise<void> {
  const url = joinUrl(schedulerBaseUrl, "/nodes/heartbeat");
  await postJson<void>(url, req, 5_000);
}

