## Worker Agent Workflow (Git-Based Jobs)

This document walks through the worker agent end-to-end, with a **deep dive into the Docker runtime path**.

---

## 1. Configuration & Bootstrap

### 1.1 Config loading (`src/config/config.ts`)

- **Source**: `process.env`, parsed via `zod`.
- **Key fields**:
  - **`SCHEDULER_BASE_URL`**: Scheduler HTTP API base URL.
  - **`CPU_LIMIT` / `MEMORY_LIMIT_MB`**: Hard caps on how much of the host the worker should advertise and use.
  - **`HEARTBEAT_INTERVAL_MS`**: How often to send node heartbeats.
  - **`MAX_CONCURRENCY`**: Upper bound on number of concurrent jobs.
  - **`JOB_TIMEOUT_MS`**: Default per-job timeout if the job doesn’t specify one.
  - **MinIO** (`MINIO_ENDPOINT`, `MINIO_REGION`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_BUCKET`): enables log upload.

If the env does not pass validation, the worker fails fast with a clear error.

### 1.2 Machine identity (`src/security/identity.ts`)

- On first start, the worker generates an **Ed25519 key pair** and a **random UUID** (`nodeId`) and persists them in:
  - `~/.computebay/identity.json` (mode `600`).
- On subsequent starts, it loads the same identity.
- **`getMachineId()`** returns this stable UUID and is used when registering with the Scheduler.

### 1.3 System capacity & GPU discovery (`src/monitoring/`)

- `detectSystemSpecs()` reads:
  - `totalCpu` = `os.cpus().length`.
  - `totalMemoryMb` = total RAM in MB.
- `computeAllocatable()` clamps these by `CPU_LIMIT` and `MEMORY_LIMIT_MB` if set.
  - Result: **`allocatableCpu`**, **`allocatableMemoryMb`**.
- `ResourceMonitor` keeps track of **used vs allocatable** CPU & memory across jobs.
- `discoverGpus()` runs a layered discovery pipeline (`src/monitoring/gpu.*.ts`):
  1. **NVIDIA adapter** (`gpu.nvidia.ts`) queries `nvidia-smi` for index, name,
     VRAM, driver version and — when the driver supports it — compute
     capability, UUID and PCI bus ID. Missing executables, timeouts, nonzero
     exits and malformed rows are non-fatal and fall through to generic Linux
     discovery.
  2. **Linux sysfs adapter** (`gpu.linux.ts`) inspects
     `/sys/class/drm/cardN` and `/sys/bus/pci/devices`. It recognizes NVIDIA
     (`0x10de`), AMD (`0x1002`) and Intel (`0x8086`), resolves DRM nodes to
     their underlying PCI devices, and deduplicates multiple nodes for the same
     physical GPU by resolved device path and PCI address. It does **not**
     require `lspci`, ROCm, CUDA or NVML.
     - AMD VRAM is read from `mem_info_vram_total`; Intel discrete VRAM from
       `lmem_total_bytes`. These are reported only when present and non-zero.
     - NVIDIA model/driver details may come from the kernel `proc`/`sys`
       interfaces when `nvidia-smi` is unavailable.
     - Unknown model, VRAM or driver values stay **unknown**; they are never
       replaced with zero or fabricated values.
- Results from both adapters are merged: NVIDIA data wins for properties it
  reports, while sysfs fills gaps and adds non-NVIDIA devices.
  - `gpuAvailable` means **GPU hardware detected** — it is not proof that a
    workload can execute.
  - `gpuMetadataComplete` means model, VRAM and driver are known for every GPU.
  - `gpuExecutionReady` becomes `true` only after the **execution readiness
    probe** below verifies a usable NVIDIA Docker backend; discovery alone
    never advertises readiness.
  - Aggregate `gpuMemoryMb` is only reported when **every** GPU's VRAM is known.
- **Execution readiness probe** (`src/monitoring/gpu.runtime.ts`): after
  discovery, the worker runs lightweight, non-mutating checks and stores the
  verified backend list (`executionBackends`, e.g. `["cuda","nvenc"]`) on each
  NVIDIA device before registration/heartbeat:
  1. NVIDIA hardware + a known host driver version,
  2. Docker daemon reachable (`docker version`),
  3. `docker run --help` advertises `--gpus`,
  4. NVIDIA Container Toolkit present (`nvidia` Docker runtime in
     `docker info`, or `nvidia-container-cli` / `nvidia-container-runtime`),
  5. `/dev/nvidiactl` and `/dev/nvidia0` device nodes visible.
  - Any failed check leaves the device with an **empty** backend list and the
    worker still starts and serves CPU-only jobs. Diagnostics distinguish
    missing hardware, missing driver info, missing Docker, missing `--gpus`
    support, missing toolkit and invisible device nodes.
  - Non-NVIDIA devices always advertise an empty backend list; AMD/Intel
    execution backends are not implemented yet.
- Startup logs distinguish NVIDIA discovery, sysfs fallback, detected hardware
  with incomplete metadata, unclassified display devices, and CPU-only
  registration.

### 1.4 Scheduler registration (`src/bootstrap/scheduler.client.ts`, `src/bootstrap/bootstrap.state.ts`)

- On startup, `src/main.ts` either:
  - **Loads bootstrap state** from `~/.computebay/bootstrap.json` (nodeId, RabbitMQ URL, queue, publish exchange), or
  - **Calls** `POST /nodes/register` with:
    - `machineId` (stable UUID), physical CPU/RAM, `agentVersion`,
    - `gpus`: detected GPU list (empty for CPU-only nodes).
- Registration returns:
  - `nodeId` (session/node ID for this registration),
  - `rabbitmq.url`,
  - `rabbitmq.queue` (template containing `<nodeId>`),
  - `rabbitmq.publishExchange`.
- The worker expands the queue template with `nodeId` and persists this bootstrap state for next boot.
- Registration uses exponential backoff with jitter; it **retries indefinitely** until success.

### 1.5 RabbitMQ connection (`src/messaging/rabbit.connection.ts`)

- `createConnection(rabbitmqUrl, publishExchange)`:
  - Connects via `amqplib.connect(url)`.
  - Attaches **error** and **close** listeners to log issues.
  - Creates a **channel** and returns a `RabbitContext`:
    - `connection`, `channel`, `publishExchange`.

### 1.6 WorkerPool construction (`src/execution/worker.pool.ts`)

- `main.ts` constructs a **`WorkerPool`** with:
  - `nodeId` (for logging),
  - `resourceMonitor` (tracks used CPU/RAM across jobs),
  - `maxConcurrency` (min of config and allocatable CPU),
  - `jobTimeoutMs`,
  - `logUploadConfig` (MinIO client config),
  - `gpus`: the local GPU inventory **with verified `executionBackends`**,
  - `onLogChunk(jobId, chunk)` → publishes `job.log.chunk` events,
  - `onJobResult(...)` → publishes final `job.completed` / `job.failed` / `job.timeout` events.

### 1.7 GPU job execution (`src/monitoring/gpu.requirement.ts`, `src/execution/`)

- `toScheduledJob` normalizes `resources.gpu` (legacy `"any"`/`"none"` and the
  canonical object form) into a requirement and shape-checks the scheduler's
  `gpuAssignment` from `job.scheduled`.
- Before every GPU job, the worker:
  1. Resolves the assignment against its **current local inventory** — stable
     ids (`id:`) or PCI addresses (`pci:`) win; a scheduler-reported index is
     only used when no stable identity exists.
  2. Rejects (as a normal `job.failed`, never a CPU fallback) missing
     assignments, vendor/backend mismatches, devices missing locally, devices
     without the verified backend, unknown/insufficient VRAM, and duplicated
     physical devices.
  3. Reserves whole devices in a local exclusivity set (`WorkerPool`) and
     passes `{ vendor, backend, deviceIndices }` to the Docker wrapper.
- Docker receives **only** the assigned devices: `--gpus device=0,1` (one
  argv element, never `all`, never `--privileged`, never a `/dev` mount).
  Indices sent to Docker are the worker's **current local** indices, never the
  ones recorded at scheduling time.
- GPU reservations are released when the job reaches a terminal state; service
  jobs hold their GPUs until `stopService()` releases them through the
  service registry.
- Docker GPU failures are classified (`classifyDockerGpuError`) into actionable
  errors: missing `--gpus` support, missing/misconfigured NVIDIA toolkit,
  device access problems, disappeared devices, or generic start failures.
  `-e KEY=value` arguments and any leaked secret values are redacted from logs.

At this point the worker is registered, connected to RabbitMQ, and ready to consume jobs.

---

## 2. Job Ingress Path

### 2.1 Rabbit consumer (`src/messaging/rabbit.consumer.ts`)

- `consume(channel, queue, prefetch, handler)`:
  - Sets `channel.prefetch(prefetch)` = `maxConcurrency` to limit in-flight jobs.
  - Calls `channel.consume(queue, onMessage, { noAck: false })`.
  - For each message:
    - Parses JSON payload.
    - Calls the async `handler(payload)`.
    - **On success**: `channel.ack(msg)`.
    - **On error**:
      - If error is `InvalidJobPayloadError` → `nack` without requeue (drop bad messages).
      - Else → `nack` with requeue (transient failure).

### 2.2 Message validation & mapping (`src/main.ts`, `src/types/job.types.ts`)

- Incoming payload type: `ScheduledJobMessage` (extends Job Service `CreateJobInput` with `jobId`).
- `main.ts` first does a **cheap shape check**:
  - `jobId`, `jobType`, `runtime`, `repoUrl`, `startCommand`, `resources`, `orgId` existence & basic types.
- Then it calls **`toScheduledJob(msg)`**, which:
  - Extracts `cpu` and `memoryMB` from `resources`, enforcing **positive numbers**.
  - Requires `runtime` (Docker image string).
  - Requires `repoUrl` and `startCommand` strings.
  - Computes:

    ```ts
    const scheduled: ScheduledJob = {
      jobId: msg.jobId,
      image: msg.runtime,
      startCommand: msg.startCommand,
      cpu,
      memoryMB,
      repoUrl: msg.repoUrl,
      branch: msg.branch ?? "main",
      networkEnabled: Boolean(msg.networkEnabled),
    };
    ```

- Any validation failure throws `InvalidJobPayloadError`, which the consumer treats as **non-retriable** (nack without requeue).

### 2.3 Resource admission control (`WorkerPool.canAccept`)

Before actually running:

- `WorkerPool.canAccept(job)` enforces:
  - Not draining (`this.draining === false`).
  - `activeCount < maxConcurrency`.
  - `resourceMonitor.canAllocate(job.cpu, job.memoryMB)`.
- If this fails, the consumer throws an error and **nacks with requeue** so the job can be picked up later or by another node.

If accepted, `main.ts`:

- Publishes **`job.started`** event.
- Calls `await workerPool.run(job)`.

---

## 3. WorkerPool & Job Lifecycle (`src/execution/worker.pool.ts`)

### 3.1 State & responsibilities

`WorkerPool` is responsible for:

- Enforcing **concurrency limits** and **capacity constraints**.
- Running jobs via `runJob` (the Docker/Git pipeline).
- Tracking **in-progress jobs** to support future cancellation.
- Emitting job result metadata through `onJobResult`.

Key fields:

- `activeCount`: number of running jobs.
- `draining`: when `true`, `canAccept` always returns false.
- `cancelControllers: Map<jobId, AbortController>`: allows external cancellation per job.

### 3.2 `run(job)` flow

1. **Admission re-check**:
   - Re-validates `canAccept(job)` (defensive; should match earlier check).

2. **Resource accounting**:
   - `activeCount++`.
   - `resourceMonitor.allocate(job.cpu, job.memoryMB)`.

3. **Abort controller**:
   - Creates `AbortController` and stores in `cancelControllers[jobId]`.
   - Its signal is passed to `runJob` and used to kill Docker containers on cancel.

4. **Invoke `runJob`**:

   ```ts
   const result = await runJob(job, {
     timeoutMs: this.jobTimeoutMs,
     logUploadConfig: this.logUploadConfig,
     onLogChunk: this.onLogChunk
       ? (chunk) => this.onLogChunk!(job.jobId, chunk)
       : undefined,
     abortSignal: abortController.signal,
   });
   ```

5. **Result handling**:
   - On success, calls `onJobResult` with:
     - `jobId`, `exitCode`, `timedOut`, `output`, `logObjectKey`.
   - On error, calls `onJobResult` with `exitCode: -1` and an `Error` instance.

6. **Cleanup** (always):
   - Deletes the job’s `AbortController`.
   - `activeCount--`.
   - `resourceMonitor.release(job.cpu, job.memoryMB)`.

### 3.3 Future cancellation hook

- `cancelJob(jobId: string)`:
  - Looks up the job’s `AbortController` and calls `abort()`, which triggers the cancel path inside `runJob` (see below).
  - Currently there’s no active cancel consumer wired from RabbitMQ (the code exists but is not wired in `main.ts`), but the mechanism is ready.

---

## 4. Docker Execution Pipeline (`src/execution/job.runner.ts`)

This is where the **Git-based job** is turned into a real Docker container run.

High-level steps per job:

1. Create a **unique temp workspace** under `/tmp`.
2. **Git clone** the requested repo + branch into a `repo` subdirectory.
3. Launch a Docker container with:
   - Read-only root filesystem.
   - Tmpfs-backed `/tmp` & `/var/tmp`.
   - CPU, memory, DNS, user, and security constraints.
   - The cloned repo bind-mounted at `/workspace`.
4. Stream logs to both disk and RabbitMQ.
5. Enforce timeout / cancellation.
6. Gather full logs from Docker.
7. Upload the log file to MinIO.
8. Tear down the container and delete the workspace directory.

### 4.1 Workspace creation & identity

```ts
const containerId = `computebay-${job.jobId}`;

const currentUid = process.getuid?.() ?? 1000;
const currentGid = process.getgid?.() ?? 1000;
const runAsUser = `${currentUid}:${currentGid}`;

const workDirPath = await mkdtemp(path.join(os.tmpdir(), `computebay-${job.jobId}-`));
const logFilePath = path.join(workDirPath, "full.log");
await writeFile(logFilePath, "");
```

- Each job gets a **unique** directory like `/tmp/computebay-<jobId>-XXXXXX`.
- The container is named `computebay-<jobId>`.
- `runAsUser` is set to match the host user running the worker so that:
  - Git clones owned by the host user are readable/writable by the container when bind-mounted.

### 4.2 Git clone (`src/libs/git.clone.ts`)

```ts
const repoPath = await gitClone(job.repoUrl, job.branch, workDirPath);
```

- `gitClone(repoUrl, branch, workingDir)`:
  - Ensures `workingDir` exists.
  - Clones into `<workingDir>/repo` using:

    ```bash
git clone --depth 1 --branch <branch> <repoUrl> <workingDir>/repo
    ```

  - Captures `stderr` and rejects with a detailed error if `git clone` fails.
  - Returns `repoPath`, the absolute path to the cloned repo.

### 4.3 Mounts, command, and env

```ts
const mounts: ContainerMount[] = [
  { hostPath: repoPath, containerPath: CONTAINER_WORKSPACE },
];
const command = ["sh", "-c", job.startCommand];

const env = {
  HOME: CONTAINER_WORKSPACE,
  TMPDIR: "/tmp",
  TEMP: "/tmp",
  TMP: "/tmp",
  PIP_NO_CACHE_DIR: "1",
  npm_config_cache: "/tmp/.npm",
  ...(job.env ?? {}),
};
```

- The cloned repo is bind-mounted into the container at **`/workspace`**.
- The working directory inside the container is set to `/workspace` (see below).
- `startCommand` is run via `sh -c` inside `/workspace`, so job authors can specify:
  - `python scripts/foo.py` or `npm install && npm test`, etc.
- Environment tweaks:
  - `HOME=/workspace` → many tools write configs/caches under `$HOME`.
  - `TMPDIR`, `TEMP`, `TMP` → point to `/tmp`, which is a tmpfs.
  - `PIP_NO_CACHE_DIR=1` → disable pip caches.
  - `npm_config_cache=/tmp/.npm` → keep npm cache ephemeral, in tmpfs.

### 4.4 Docker run (`src/execution/container.runtime.ts`)

`createAndStart(options)` builds a **plain Docker CLI** invocation and spawns it with `child_process.spawn`.

Effective command:

```bash
docker run -d \
  --name <containerId> \
  --cpus <job.cpu> \
  --memory <job.memoryMB>m \
  --network bridge \
  --dns 8.8.8.8 --dns 8.8.4.4 \
  --pids-limit 64 \
  --read-only \
  --tmpfs /tmp:rw,noexec,nosuid,size=128m \
  --tmpfs /var/tmp:rw,noexec,nosuid,size=64m \
  --cap-drop ALL \
  --security-opt no-new-privileges \
  --user <uid:gid> \
  -e KEY=VALUE ... \
  --mount type=bind,source=<repoPath>,target=/workspace \
  -w /workspace \
  <job.image> \
  sh -c "<startCommand>"
```

#### 4.4.1 Resource flags

- **`--cpus <job.cpu>`**: fractional CPU limit (e.g. `0.5`, `2`).
- **`--memory <job.memoryMB>m`**: hard memory limit in MB.
- Combined with `ResourceMonitor`, this ensures the node never runs more work than advertised.

#### 4.4.2 Network & DNS

- Currently `--network bridge` is always used (DNS + outbound by default).
- If you later want hard network isolation, you can wire `job.networkEnabled` into the `networkEnabled` flag and switch between `bridge` and `none`.
- DNS is forced to Google’s resolvers for consistency: `8.8.8.8` and `8.8.4.4`.

#### 4.4.3 Security posture

- **`--read-only`**: container root filesystem is read-only.
  - Writes must go to bind mounts (`/workspace`) or tmpfs (`/tmp`, `/var/tmp`).
- **`--tmpfs /tmp` & `--tmpfs /var/tmp`**:
  - Ephemeral, in-memory file systems for temp data and caches.
  - `noexec,nosuid` reduce risk from scripts dropped there.
- **`--cap-drop ALL`**: remove all Linux capabilities from the container.
- **`--security-opt no-new-privileges`**: prevents processes gaining extra privileges (e.g. via setuid binaries).
- **`--user <uid:gid>`**: drop root; process runs as the **same UID/GID as the host user** running the worker.

Overall this is a **locked-down, non-root, read-only** container with constrained resources and explicit write targets.

### 4.5 Log streaming & aggregation

#### 4.5.1 Streaming during execution

```ts
const stopLogStream = streamContainerLogs(containerId, {
  onChunk: async (chunk) => {
    await appendFile(logFilePath, chunk);
    logBuffer += chunk;
    flushChunk();
  },
  onEnd: () => flushChunk(),
  onError: (err) => logger.warn({ err, jobId: job.jobId }, "Log stream error"),
});
```

- `streamContainerLogs` runs `docker logs -f --tail 0 <containerId>`.
- For each chunk:
  - Appends to `full.log` on disk.
  - Buffers in-memory and, once the buffer exceeds **4 KB**, flushes via `onLogChunk`:
    - This surfaces to `WorkerPool`, which calls `onLogChunk(jobId, chunk)`.
    - `main.ts` then publishes **`job.log.chunk`** events per job.
- On natural completion (`docker logs -f` exits) it flushes any remaining buffered text.
- On error it logs but does not fail the job.

#### 4.5.2 Timeout / cancellation / normal exit

The worker races three things:

- `waitContainer(containerId)` → `docker wait`, returns the container’s **exit code**.
- `timeoutPromise` → after `job.timeoutMs` or global `JOB_TIMEOUT_MS`, calls `docker kill` and rejects with `"TIMEOUT"`.
- `abortPromise` → if the `AbortController` is triggered, kills the container and rejects with `"CANCELLED"`.

Result handling:

- **Normal exit**: `waitContainer` wins, exit code is captured.
- **Timeout**: `timeoutPromise` wins:
  - Sets `timedOut = true`.
  - Sets `exitCode = 137` (conventional “killed by SIGKILL” style code).
  - Logs a warning.
- **Cancelled**: `abortPromise` wins:
  - Sets `exitCode = 137`.
  - Logs an info message.

In all cases, `finally { stopLogStream(); }` is called to tear down the streaming process.

#### 4.5.3 Final log fetch & file overwrite

After the container is done:

```ts
let output = "";
try {
  output = await getContainerLogs(containerId);
  await writeFile(logFilePath, output || "(no output)");
} catch (logErr) {
  logger.warn({ jobId: job.jobId, err: logErr }, "Failed to fetch container logs");
}
```

- `getContainerLogs` runs `docker logs --tail 10000` and captures **all** logs (stdout + stderr) as a single string.
- The worker **overwrites** `full.log` with this canonical output.
  - The earlier streaming writes were mostly for real-time log events.
  - The final file therefore reflects exactly what Docker reports, not a partial tail from streaming.

### 4.6 Container teardown & workspace cleanup

```ts
await removeContainer(containerId).catch((err) => {
  logger.warn({ err, containerId }, "Failed to remove container");
});
```

- `removeContainer` runs `docker rm -f <containerId>`.
- Any error is logged but not fatal to the job result.

Then, in the `finally` block of `runJob`:

```ts
await rm(workDirPath, { recursive: true, force: true });
```

- Deletes the entire workspace directory (the `mkdtemp` root), including:
  - The cloned Git repo.
  - Any `node_modules`, `venv`, compiled artifacts, etc.
  - The `full.log` file (after it’s uploaded, if MinIO is configured).

This ensures **no per-job disk usage** persists on the host beyond execution.

### 4.7 Log upload to MinIO (`src/libs/log.upload.ts`)

If `logUploadConfig` is defined:

```ts
logObjectKey = await uploadLogFile(
  logUploadConfig.s3Config,
  job.jobId,
  logFilePath
);
```

- Reads the `full.log` file as a buffer.
- Uploads to **`logs/<jobId>/full.log`** in the configured bucket.
- The resulting `logObjectKey` is included in the `job.completed` event so the job service can link to it.

If upload fails, the job still completes; only a warning is logged.

---

## 5. Events & Observability

### 5.1 Node-level events

Published via `publishNodeEvent(event, payload)` with routing key `node.<nodeId>.events`:

- **`job.started`** – when a job is accepted and `run(job)` begins.
- **`job.log.chunk`** – streaming log chunks during execution.
- **`job.completed`** – on successful exit, includes:
  - `exitCode`,
  - `output` (full logs from `docker logs`),
  - optional `logObjectKey` (MinIO path).
- **`job.failed`** – on internal errors or non-zero exit code.
- **`job.timeout`** – when the job exceeded its timeout and was killed.

### 5.2 Heartbeats (`sendHeartbeat`)

At `HEARTBEAT_INTERVAL_MS` the worker:

- Reads current `availableCpu` / `availableMemoryMb` from `ResourceMonitor`.
- Posts `NodeHeartbeatRequest` to Scheduler with these values, plus the current
  GPU list so a node's GPU metadata stays correct across restarts/upgrades.
  - The Scheduler replaces stored GPU metadata from each heartbeat.
  - An explicitly empty GPU list clears stale GPU metadata.
  - Workers that omit the GPU field leave stored metadata untouched.
- Logs warnings on failures but keeps running.

This allows the Scheduler to make better placement decisions and detect unhealthy nodes.

---

## 6. Shutdown Semantics

### 6.1 SIGTERM handling (`src/lifecycle/shutdown.ts`)

When `SIGTERM` is received:

1. `installSigTermHandler` invokes the registered handler once.
2. `gracefulShutdown` runs with:
   - `onSignal`:
     - Sets the worker pool to **draining** (`startDrain()`), so new jobs aren’t accepted.
     - Cancels the RabbitMQ consumer (`cancelConsumer`), stopping new messages.
     - Clears the heartbeat interval.
   - `drainWaiter`:
     - Polls `workerPool.hasActiveJobs()` until it returns `false` (all jobs done).
   - `steps`:
     - Final heartbeat send.
     - `closeConnection` (RabbitMQ connection close).

3. Once steps complete, the process exits with code `0`.
4. If anything in the shutdown pipeline throws, the process exits with code `1`.

Because per-job cleanup is in `runJob`’s `finally` block, **all cloned repos and containers are removed as part of normal job completion**, even during shutdown.

---

## 7. Summary

End-to-end, the worker agent does the following for each job:

1. **Accepts** a validated job from RabbitMQ, subject to resource and concurrency limits.
2. **Clones** the requested Git repo + branch into a temp workspace.
3. **Spawns** a locked-down Docker container named `computebay-<jobId>`:
   - Limited CPU/memory, non-root user.
   - Read-only root FS, tmpfs `/tmp` and `/var/tmp`.
   - Repo bind-mounted at `/workspace`; runs `sh -c <startCommand>`.
4. **Streams logs** in real time to both a local file and RabbitMQ `job.log.chunk` events.
5. **Enforces timeout/cancel** by racing `docker wait`, `docker kill` on timeout, and an abort signal.
6. **Aggregates final logs** from `docker logs` into a single canonical `full.log`.
7. **Uploads logs** to MinIO (if configured) under `logs/<jobId>/full.log` and includes that key in `job.completed`.
8. **Cleans up** the container and deletes the temp workspace, ensuring no job-specific disk state persists.

This architecture keeps the runtime **isolated, observable, and self-cleaning**, while giving the job service a simple contract: **"Git repo + branch + start command + resources" → fully managed container execution with logs and lifecycle events.**

