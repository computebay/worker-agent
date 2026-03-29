import { logger } from "../utils/logger.js";
import { killContainer, removeContainer } from "./container.runtime.js";
import { portManager } from "../network/port.manager.js";

export type ServiceStatus = "running" | "stopped" | "failed";

export interface ServiceEntry {
  serviceId: string;
  containerId: string;
  port: number;
  status: ServiceStatus;
  stopLogStream?: () => void;
}

type ReleaseCallback = (cpu: number, memoryMB: number) => void;
type ResourceEntry = { cpu: number; memoryMB: number };

class ServiceRegistry {
  private readonly services = new Map<string, ServiceEntry>();
  private readonly resources = new Map<string, ResourceEntry>();
  private releaseCallback: ReleaseCallback | undefined;

  setReleaseCallback(cb: ReleaseCallback): void {
    this.releaseCallback = cb;
  }

  registerService(
    entry: Omit<ServiceEntry, "status">,
    resource: ResourceEntry
  ): void {
    this.services.set(entry.serviceId, { ...entry, status: "running" });
    this.resources.set(entry.serviceId, resource);
    logger.info(
      { serviceId: entry.serviceId, containerId: entry.containerId, port: entry.port },
      "Service registered"
    );
  }

  async stopService(serviceId: string): Promise<void> {
    const entry = this.services.get(serviceId);
    if (!entry) {
      throw new Error(`Service ${serviceId} not found in registry`);
    }
    if (entry.status === "stopped") {
      logger.warn({ serviceId }, "Service already stopped");
      return;
    }

    logger.info({ serviceId, containerId: entry.containerId }, "Stopping service");

    if (entry.stopLogStream) {
      try {
        entry.stopLogStream();
      } catch (err) {
        logger.warn({ err, serviceId }, "Error stopping log stream");
      }
    }

    try {
      await killContainer(entry.containerId);
    } catch (err) {
      logger.warn({ err, serviceId }, "Kill failed (container may already be gone)");
    }

    try {
      await removeContainer(entry.containerId);
    } catch (err) {
      logger.warn({ err, serviceId }, "Remove failed (container may already be gone)");
    }

    portManager.releasePort(entry.port);
    entry.status = "stopped";

    const resource = this.resources.get(serviceId);
    if (resource && this.releaseCallback) {
      this.releaseCallback(resource.cpu, resource.memoryMB);
    }
    this.resources.delete(serviceId);

    logger.info({ serviceId }, "Service stopped and resources released");
  }

  listServices(): ServiceEntry[] {
    return Array.from(this.services.values());
  }

  getService(serviceId: string): ServiceEntry | undefined {
    return this.services.get(serviceId);
  }
}

export const serviceRegistry = new ServiceRegistry();
