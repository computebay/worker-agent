import { existsSync, readFileSync } from "node:fs";
import path from "node:path";

export interface FrameworkPreset {
  /** Human-readable name used in lifecycle events (e.g. "nextjs", "vite", "node-build", "node"). */
  name: string;
  /**
   * True if the framework requires a dedicated build step before the runtime container starts.
   * When true the worker runs the full artifact pipeline (install → build → tar → MinIO → extract → run).
   * When false the worker uses the existing install + direct bind-mount approach.
   */
  hasBuildStep: boolean;
  /** Shell command to install dependencies inside a container (e.g. "npm install"). */
  installCommand?: string;
  /** Shell command that compiles/bundles the app (only when hasBuildStep = true). */
  buildCommand?: string;
  /** Shell command that starts the HTTP server in the runtime container. */
  runtimeCommand: string;
}

type PackageJson = { scripts?: Record<string, string>; main?: string };

function readPackageJson(repoPath: string): PackageJson | null {
  const p = path.join(repoPath, "package.json");
  if (!existsSync(p)) return null;
  try {
    return JSON.parse(readFileSync(p, "utf-8")) as PackageJson;
  } catch {
    return null;
  }
}

/**
 * Inspect a cloned repo directory and return the best-matching FrameworkPreset.
 *
 * Detection priority (most-specific first):
 *   1. Next.js   — next.config.{js,ts,mjs,cjs}
 *   2. Vite      — vite.config.{js,ts,mjs}
 *   3. Node w/ build script — package.json scripts.build present
 *   4. Pure Node server    — package.json, no build script
 *
 * Returns null when the repo is unidentifiable (user must supply build config in payload).
 */
export function detectFramework(repoPath: string): FrameworkPreset | null {
  // 1. Next.js
  const nextConfigs = ["next.config.js", "next.config.ts", "next.config.mjs", "next.config.cjs"];
  if (nextConfigs.some((f) => existsSync(path.join(repoPath, f)))) {
    return {
      name: "nextjs",
      hasBuildStep: true,
      installCommand: "npm install",
      buildCommand: "npm run build",
      runtimeCommand: "npm run start",
    };
  }

  // 2. Vite
  const viteConfigs = ["vite.config.js", "vite.config.ts", "vite.config.mjs"];
  if (viteConfigs.some((f) => existsSync(path.join(repoPath, f)))) {
    return {
      name: "vite",
      hasBuildStep: true,
      installCommand: "npm install",
      buildCommand: "npm run build",
      // npm run preview is the standard Vite production preview server
      runtimeCommand: "npm run preview",
    };
  }

  // 3 & 4. Generic Node.js
  const pkg = readPackageJson(repoPath);
  if (pkg) {
    if (pkg.scripts?.["build"]) {
      // Has a build step (TypeScript, custom bundler, etc.)
      const startScript = pkg.scripts?.["start"];
      const mainEntry = pkg.main ?? "dist/index.js";
      return {
        name: "node-build",
        hasBuildStep: true,
        installCommand: "npm install",
        buildCommand: "npm run build",
        runtimeCommand: startScript ? "npm run start" : `node ${mainEntry}`,
      };
    }

    // Pure Node server — no build compilation required
    const startScript = pkg.scripts?.["start"];
    const mainEntry = pkg.main ?? "index.js";
    return {
      name: "node",
      hasBuildStep: false,
      installCommand: "npm install",
      runtimeCommand: startScript ? "npm run start" : `node ${mainEntry}`,
    };
  }

  return null;
}

/**
 * Apply job-payload overrides on top of a detected preset.
 * Job-level fields always win over auto-detection.
 */
export function applyJobOverrides(
  preset: FrameworkPreset,
  overrides: { buildCommand?: string; runtimeCommand?: string }
): FrameworkPreset {
  return {
    ...preset,
    buildCommand: overrides.buildCommand ?? preset.buildCommand,
    runtimeCommand: overrides.runtimeCommand ?? preset.runtimeCommand,
    // If the user explicitly provides a buildCommand, treat it as a build-step app
    hasBuildStep: overrides.buildCommand !== undefined ? true : preset.hasBuildStep,
  };
}
