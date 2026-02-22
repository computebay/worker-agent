import os from "node:os";

export interface SystemSpecs {
  totalCpu: number;
  totalMemoryMb: number;
}

/**
 * Detect physical CPU cores and total RAM.
 */
export function detectSystemSpecs(): SystemSpecs {
  const totalCpu = os.cpus().length;
  const totalMemoryMb = Math.floor(os.totalmem() / 1024 / 1024);

  return {
    totalCpu,
    totalMemoryMb,
  };
}
