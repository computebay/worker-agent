import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";

function findPackageJson(startDir: string): string | null {
  let cur = startDir;
  for (let i = 0; i < 6; i += 1) {
    const candidate = join(cur, "package.json");
    if (existsSync(candidate)) return candidate;
    const parent = dirname(cur);
    if (parent === cur) break;
    cur = parent;
  }
  return null;
}

export function getAgentVersion(fallback = "unknown"): string {
  if (process.env.AGENT_VERSION && process.env.AGENT_VERSION.trim()) {
    return process.env.AGENT_VERSION.trim();
  }
  if (process.env.npm_package_version && process.env.npm_package_version.trim()) {
    return process.env.npm_package_version.trim();
  }

  const pkgPath =
    findPackageJson(process.cwd()) ?? findPackageJson(dirname(process.argv[1] ?? process.cwd()));
  if (!pkgPath) return fallback;

  try {
    const raw = readFileSync(pkgPath, "utf-8");
    const pkg = JSON.parse(raw) as { version?: unknown };
    return typeof pkg.version === "string" && pkg.version.trim() ? pkg.version.trim() : fallback;
  } catch {
    return fallback;
  }
}

