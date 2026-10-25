import { describe, test } from "node:test";
import assert from "node:assert/strict";
import {
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { discoverLinuxGpus } from "./gpu.linux.js";

interface FakePciDeviceOptions {
  vendor: string;
  device: string;
  classCode: string;
}

interface FakeSysfs {
  root: string;
  drmRoot: string;
  pciRoot: string;
  moduleRoot: string;
  procRoot: string;
  addPciDevice: (address: string, options: FakePciDeviceOptions) => string;
  addDrmCard: (card: string, devicePath: string) => void;
  cleanup: () => void;
}

function write(path: string, value: string): void {
  writeFileSync(path, `${value}\n`);
}

function createFakeSysfs(): FakeSysfs {
  const root = mkdtempSync(join(tmpdir(), "computebay-gpu-"));
  const drmRoot = join(root, "drm");
  const pciRoot = join(root, "pci");
  const devicesRoot = join(root, "devices");
  const moduleRoot = join(root, "module");
  const procRoot = join(root, "proc");

  for (const dir of [drmRoot, pciRoot, devicesRoot, moduleRoot, procRoot]) {
    mkdirSync(dir, { recursive: true });
  }

  const addPciDevice = (
    address: string,
    options: FakePciDeviceOptions,
  ): string => {
    const devicePath = join(devicesRoot, address);
    mkdirSync(devicePath, { recursive: true });
    write(join(devicePath, "vendor"), options.vendor);
    write(join(devicePath, "device"), options.device);
    write(join(devicePath, "class"), options.classCode);
    symlinkSync(devicePath, join(pciRoot, address));
    return devicePath;
  };

  const addDrmCard = (card: string, devicePath: string): void => {
    const cardPath = join(drmRoot, card);
    mkdirSync(cardPath, { recursive: true });
    symlinkSync(devicePath, join(cardPath, "device"));
  };

  return {
    root,
    drmRoot,
    pciRoot,
    moduleRoot,
    procRoot,
    addPciDevice,
    addDrmCard,
    cleanup: () => rmSync(root, { recursive: true, force: true }),
  };
}

describe("discoverLinuxGpus", () => {
  test("discovers NVIDIA, AMD and Intel cards without vendor tooling", () => {
    const sysfs = createFakeSysfs();
    try {
      const amd = sysfs.addPciDevice("0000:01:00.0", {
        vendor: "0x1002",
        device: "0x73bf",
        classCode: "0x030000",
      });
      write(join(amd, "mem_info_vram_total"), "17163091968");
      write(join(amd, "product_name"), "AMD Radeon RX 6900 XT");

      const nvidia = sysfs.addPciDevice("0000:02:00.0", {
        vendor: "0x10de",
        device: "0x26b9",
        classCode: "0x030000",
      });
      mkdirSync(join(sysfs.root, "drivers", "nvidia"), { recursive: true });
      symlinkSync(join(sysfs.root, "drivers", "nvidia"), join(nvidia, "driver"));
      mkdirSync(join(sysfs.moduleRoot, "nvidia"), { recursive: true });
      write(join(sysfs.moduleRoot, "nvidia", "version"), "550.54.14");
      const procGpu = join(
        sysfs.procRoot,
        "driver",
        "nvidia",
        "gpus",
        "0000:02:00.0",
      );
      mkdirSync(procGpu, { recursive: true });
      write(join(procGpu, "information"), "Model: NVIDIA L40S\nIRQ: 1");

      const intel = sysfs.addPciDevice("0000:03:00.0", {
        vendor: "0x8086",
        device: "0x56a0",
        classCode: "0x030000",
      });

      // Natural card ordering, and duplicate nodes for the same GPU.
      sysfs.addDrmCard("card2", nvidia);
      sysfs.addDrmCard("card0", amd);
      sysfs.addDrmCard("card1", amd);
      sysfs.addDrmCard("card10", intel);
      // Render/connector nodes must not be treated as additional GPUs.
      mkdirSync(join(sysfs.drmRoot, "renderD128"), { recursive: true });
      mkdirSync(join(sysfs.drmRoot, "card0-DP-1"), { recursive: true });

      const result = discoverLinuxGpus({
        drmRoot: sysfs.drmRoot,
        pciRoot: sysfs.pciRoot,
        moduleRoot: sysfs.moduleRoot,
        procRoot: sysfs.procRoot,
      });

      assert.equal(result.succeeded, true);
      assert.equal(result.unclassifiedDevices.length, 0);
      assert.equal(result.gpus.length, 3);
      assert.deepEqual(
        result.gpus.map((gpu) => gpu.vendor),
        ["AMD", "NVIDIA", "Intel"],
      );
      assert.deepEqual(
        result.gpus.map((gpu) => gpu.index),
        [0, 1, 2],
      );

      assert.equal(result.gpus[0]?.name, "AMD Radeon RX 6900 XT");
      assert.equal(result.gpus[0]?.memoryMb, 16368);
      assert.equal(result.gpus[0]?.pciAddress, "0000:01:00.0");
      assert.equal(result.gpus[0]?.pciVendorId, "1002");
      assert.equal(result.gpus[0]?.pciDeviceId, "73bf");

      assert.equal(result.gpus[1]?.name, "NVIDIA L40S");
      assert.equal(result.gpus[1]?.driverVersion, "550.54.14");
      assert.equal(result.gpus[1]?.memoryMb, undefined);

      assert.equal(result.gpus[2]?.vendor, "Intel");
      assert.equal(result.gpus[2]?.memoryMb, undefined);
      assert.equal(result.gpus[2]?.discoverySource, "linux-sysfs");
    } finally {
      sysfs.cleanup();
    }
  });

  test("finds display-class PCI GPUs without DRM nodes", () => {
    const sysfs = createFakeSysfs();
    try {
      const amd = sysfs.addPciDevice("0000:04:00.0", {
        vendor: "0x1002",
        device: "0x744c",
        classCode: "0x030000",
      });
      write(join(amd, "mem_info_vram_total"), "4294967296");

      // Audio function on the same physical card.
      sysfs.addPciDevice("0000:04:00.1", {
        vendor: "0x1002",
        device: "0xab30",
        classCode: "0x040300",
      });

      // BMC VGA: display class but not a supported GPU vendor.
      sysfs.addPciDevice("0000:05:00.0", {
        vendor: "0x1a03",
        device: "0x2000",
        classCode: "0x030000",
      });

      sysfs.addPciDevice("0000:06:00.0", {
        vendor: "0x8086",
        device: "0x56a0",
        classCode: "0x030000",
      });

      const result = discoverLinuxGpus({
        drmRoot: sysfs.drmRoot,
        pciRoot: sysfs.pciRoot,
        moduleRoot: sysfs.moduleRoot,
        procRoot: sysfs.procRoot,
      });

      assert.equal(result.gpus.length, 2);
      assert.deepEqual(
        result.gpus.map((gpu) => gpu.vendor),
        ["AMD", "Intel"],
      );
      assert.equal(result.gpus[0]?.memoryMb, 4096);
      assert.equal(result.unclassifiedDevices.length, 1);
      assert.equal(result.unclassifiedDevices[0]?.pciVendorId, "1a03");
      assert.equal(result.unclassifiedDevices[0]?.pciAddress, "0000:05:00.0");
    } finally {
      sysfs.cleanup();
    }
  });

  test("reports DRM devices without a supported PCI identity", () => {
    const sysfs = createFakeSysfs();
    try {
      const platformDevice = join(sysfs.root, "devices", "platform-gpu");
      mkdirSync(platformDevice, { recursive: true });
      sysfs.addDrmCard("card0", platformDevice);

      const result = discoverLinuxGpus({
        drmRoot: sysfs.drmRoot,
        pciRoot: sysfs.pciRoot,
        moduleRoot: sysfs.moduleRoot,
        procRoot: sysfs.procRoot,
      });

      assert.equal(result.gpus.length, 0);
      assert.equal(result.unclassifiedDevices.length, 1);
      assert.match(
        result.unclassifiedDevices[0]?.reason ?? "",
        /vendor ID unavailable/i,
      );
    } finally {
      sysfs.cleanup();
    }
  });

  test("treats a zero VRAM counter as unknown", () => {
    const sysfs = createFakeSysfs();
    try {
      const amd = sysfs.addPciDevice("0000:07:00.0", {
        vendor: "0x1002",
        device: "0x15d8",
        classCode: "0x030000",
      });
      write(join(amd, "mem_info_vram_total"), "0");
      sysfs.addDrmCard("card0", amd);

      const result = discoverLinuxGpus({
        drmRoot: sysfs.drmRoot,
        pciRoot: sysfs.pciRoot,
        moduleRoot: sysfs.moduleRoot,
        procRoot: sysfs.procRoot,
      });

      assert.equal(result.gpus.length, 1);
      assert.equal(result.gpus[0]?.memoryMb, undefined);
    } finally {
      sysfs.cleanup();
    }
  });

  test("handles missing sysfs roots without throwing", () => {
    const result = discoverLinuxGpus({
      drmRoot: "/nonexistent/computebay/drm",
      pciRoot: "/nonexistent/computebay/pci",
      moduleRoot: "/nonexistent/computebay/module",
      procRoot: "/nonexistent/computebay/proc",
    });

    assert.equal(result.succeeded, false);
    assert.equal(result.gpus.length, 0);
    assert.match(result.error ?? "", /DRM|PCI/);
  });
});
