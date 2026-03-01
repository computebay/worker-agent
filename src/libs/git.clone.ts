import { spawn } from "node:child_process";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { logger } from "../utils/logger.js";

/**
 * Clone a Git repository into a `repo` subdirectory inside the given working directory.
 * Using a subdirectory avoids the "already exists and is not an empty directory" error
 * that occurs when cloning into a pre-created temp dir (e.g. from mkdtemp).
 *
 * Uses: git clone --depth 1 --branch <branch> <repoUrl> <workingDir>/repo
 *
 * @returns Full path to the cloned repo directory
 * @throws Error if clone fails
 */
export async function gitClone(
  repoUrl: string,
  branch: string,
  workingDir: string
): Promise<string> {
  await mkdir(workingDir, { recursive: true });

  const repoPath = path.join(workingDir, "repo");

  return new Promise((resolve, reject) => {
    const proc = spawn(
      "git",
      ["clone", "--depth", "1", "--branch", branch, repoUrl, repoPath],
      { stdio: ["ignore", "pipe", "pipe"] }
    );

    let stderr = "";
    proc.stderr?.on("data", (chunk) => {
      stderr += chunk.toString();
    });

    proc.on("close", (code, signal) => {
      if (code === 0) {
        logger.info({ repoUrl, branch, repoPath }, "Git clone succeeded");
        resolve(repoPath);
      } else {
        const errMsg = `git clone failed: exit ${code}, signal ${signal}${stderr ? `, stderr: ${stderr}` : ""}`;
        logger.error({ repoUrl, branch, repoPath, code, stderr }, "Git clone failed");
        reject(new Error(errMsg));
      }
    });

    proc.on("error", (err) => {
      logger.error({ err, repoUrl, branch }, "Git spawn error");
      reject(err);
    });
  });
}