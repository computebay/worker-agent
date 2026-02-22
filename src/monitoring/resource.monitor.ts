export interface ResourceUsage {
  usedCpu: number;
  usedMemoryMb: number;
}

export interface ResourceMonitorSpecs {
  allocatableCpu: number;
  allocatableMemoryMb: number;
}

/**
 * Tracks runtime CPU and memory usage across active jobs.
 * Must enforce: usedCpu + job.cpu <= allocatableCpu and usedMemory + job.memoryMB <= allocatableMemoryMb
 */
export class ResourceMonitor {
  private usedCpu = 0;
  private usedMemoryMb = 0;
  private readonly allocatableCpu: number;
  private readonly allocatableMemoryMb: number;

  constructor(specs: ResourceMonitorSpecs) {
    this.allocatableCpu = specs.allocatableCpu;
    this.allocatableMemoryMb = specs.allocatableMemoryMb;
  }

  getUsage(): ResourceUsage {
    return {
      usedCpu: this.usedCpu,
      usedMemoryMb: this.usedMemoryMb,
    };
  }

  getAvailable(): { availableCpu: number; availableMemoryMb: number } {
    return {
      availableCpu: Math.max(0, this.allocatableCpu - this.usedCpu),
      availableMemoryMb: Math.max(0, this.allocatableMemoryMb - this.usedMemoryMb),
    };
  }

  canAllocate(cpu: number, memoryMb: number): boolean {
    return (
      this.usedCpu + cpu <= this.allocatableCpu &&
      this.usedMemoryMb + memoryMb <= this.allocatableMemoryMb
    );
  }

  allocate(cpu: number, memoryMb: number): void {
    if (!this.canAllocate(cpu, memoryMb)) {
      throw new Error(
        `Resource over-allocation: cannot allocate cpu=${cpu} mem=${memoryMb}Mb ` +
          `(used: ${this.usedCpu} cpu, ${this.usedMemoryMb} Mb)`
      );
    }
    this.usedCpu += cpu;
    this.usedMemoryMb += memoryMb;
  }

  release(cpu: number, memoryMb: number): void {
    this.usedCpu = Math.max(0, this.usedCpu - cpu);
    this.usedMemoryMb = Math.max(0, this.usedMemoryMb - memoryMb);
  }

  getSpecs(): ResourceMonitorSpecs {
    return {
      allocatableCpu: this.allocatableCpu,
      allocatableMemoryMb: this.allocatableMemoryMb,
    };
  }
}
