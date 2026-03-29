import net from "node:net";

/**
 * In-memory port allocator for service containers.
 * Recycles released ports before incrementing the counter.
 */
class PortManager {
  private counter = 30000;
  private readonly released = new Set<number>();

  allocatePort(): number {
    if (this.released.size > 0) {
      const port = this.released.values().next().value as number;
      this.released.delete(port);
      return port;
    }
    return this.counter++;
  }

  releasePort(port: number): void {
    this.released.add(port);
  }
}

export const portManager = new PortManager();

export async function isPortFree(port: number): Promise<boolean> {
  return new Promise((resolve) => {
    const server = net.createServer();
    server.once("error", () => resolve(false));
    server.once("listening", () => {
      server.close();
      resolve(true);
    });
    server.listen(port);
  });
}

export async function waitForPort(port: number, timeoutMs = 10000): Promise<void> {
  // Brief initial delay so:
  //  1. The isPortFree probe socket has fully released the OS binding before we poll.
  //  2. The container process has a moment to start the server before the first attempt.
  await new Promise((r) => setTimeout(r, 500));

  const startTime = Date.now();
  const pollMs = 500;

  while (Date.now() - startTime < timeoutMs) {
    const isReady = await new Promise<boolean>((resolve) => {
      const socket = new net.Socket();
      socket.setTimeout(300);
      socket.once("connect", () => {
        socket.destroy();
        resolve(true);
      });
      socket.once("error", () => resolve(false));
      socket.once("timeout", () => {
        socket.destroy();
        resolve(false);
      });
      socket.connect(port, "127.0.0.1");
    });

    if (isReady) return;
    await new Promise((r) => setTimeout(r, pollMs));
  }
  throw new Error(`Timeout waiting for port ${port} to become ready`);
}
