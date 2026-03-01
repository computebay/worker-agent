import { spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { logger } from "../utils/logger.js";

/**
 * Clone a Git repository into the given working directory.
 * Uses: git clone --depth 1 --branch <branch> <repoUrl> <workingDir>
 *
 * @throws Error if clone fails
 */
export async function gitClone(
  repoUrl: string,
  branch: string,
  workingDir: string
): Promise<void> {
  await mkdir(path.dirname(workingDir), { recursive: true });

  return new Promise((resolve, reject) => {
    const proc = spawn(
      "git",
      ["clone", "--depth", "1", "--branch", branch, repoUrl, workingDir],
      { stdio: ["ignore", "pipe", "pipe"] }
    );

    let stderr = "";
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code, signal) => {
      if (code === 0) {
        logger.info({ repoUrl, branch, workingDir }, "Git clone succeeded");
        resolve();
      } else {
        const errMsg = `git clone failed: exit ${code}, signal ${signal}${stderr ? `, stderr: ${stderr}` : ""}`;
        logger.error({ repoUrl, branch, workingDir, code, stderr }, "Git clone failed");
        reject(new Error(errMsg));
      }
    });

    proc.on("error", (err) => {
      logger.error({ err, repoUrl, branch }, "Git spawn error");
      reject(err);
    });
  });
}
