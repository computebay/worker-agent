import type { SystemSpecs } from "./system.specs.js";

export interface AllocatableCapacity {
  allocatableCpu: number;
  allocatableMemoryMb: number;
}

/**
 * Compute allocatable capacity from physical specs and user-imposed limits.
 */
export function computeAllocatable(
  specs: SystemSpecs,
  cpuLimit?: number,
  memoryLimitMb?: number
): AllocatableCapacity {
  let allocatableCpu = specs.totalCpu;
  let allocatableMemoryMb = specs.totalMemoryMb;

  if (cpuLimit !== undefined && cpuLimit > 0) {
    allocatableCpu = Math.min(allocatableCpu, cpuLimit);
  }

  if (memoryLimitMb !== undefined && memoryLimitMb > 0) {
    allocatableMemoryMb = Math.min(allocatableMemoryMb, memoryLimitMb);
  }

  return {
    allocatableCpu,
    allocatableMemoryMb,
  };
}
