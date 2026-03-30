# ComputeBay Worker Agent: Codebase Anatomy & Documentation

The ComputeBay Worker Agent is a robust, resource-aware TypeScript/Node.js application responsible for executing distributed workloads on behalf of the ComputeBay platform. It securely pulls jobs from RabbitMQ, provisions Docker containers for execution, monitors host resources, orchestrates long-running services, and ships real-time logs and state transitions back to the central Scheduler.

This document provides a comprehensive, file-by-file breakdown of the repository to serve as an in-depth reference for developers modifying, debugging, or extending the agent's capabilities.

---

## 1. High-Level Architecture
The agent runs independently on worker node machines. At startup, it interrogates host physical resources and registers itself with the central **Scheduler** via REST to obtain RabbitMQ credentials. It listens to a designated node-specific queue for **Batch Jobs** (ephemeral executions) and **Service Jobs** (long-running servers). 

When a job arrives, the **Worker Pool** allocates required CPU and Memory. If accepted, the agent:
1. Allocates network ports (for services).
2. Clones target repositories via Git to a secure host directory.
3. Spawns heavily restricted Docker containers using the `node:child_process` execution model.
4. Streams execution logs to RabbitMQ while writing to temporary files.
5. Captures job termination states to upload artifacts (via S3/MinIO) and signals the Scheduler.

---

## 2. Root Files

* `package.json` / `bun.lock`: Defines the project configuration, `tsx` for execution, `prettier`/`eslint` for formatting, and dependencies such as `amqplib` (RabbitMQ), `pino` (logging), `@aws-sdk/client-s3` (Artifact storage), and `zod` (validation).
* `tsconfig.json`: TypeScript compiler configuration targeting Node/ESNext paths.
* `.env.example`: Template for environment variables spanning capabilities like scheduler URL, compute restrictions, and MinIO credentials.

---

## 3. The `src/` Directory File-by-File

The core logic resides under `src/` and is compartmentalized into domains.

### Entry Point
#### `src/main.ts`
The bootstrap file. It orchestrates the entire lifecycle of the agent:
- Detects system hardware.
- Registers the worker to the Scheduler if not already bootstrapped.
- Connects to RabbitMQ.
- Mounts the `WorkerPool` with resource thresholds and event emitters.
- Mounts the RabbitMQ Consumer and processes `ScheduledJobMessage`s.
- Sets up an interval heartbeat to keep the node alive in the central Scheduler.
- Installs `SIGTERM` handlers for graceful draining and stopping running services before exiting.

---

### Execution Engine (`src/execution/`)
Handles the lifecycle of jobs and services natively.

#### `src/execution/worker.pool.ts`
The concurrency and resource-aware dispatcher. It acts as the gatekeeper, accepting jobs from RabbitMQ only if the `ResourceMonitor` has available CPU and Memory. It routes Batch Jobs to `job.runner.ts` and Service Jobs to `service.runner.ts`, tracking `activeCount` and providing mechanisms to gracefully drain or forcefully terminate containers.

#### `src/execution/container.runtime.ts`
A pure wrapper around the `docker` CLI using Node's `child_process`. It intentionally leverages raw Docker CLI rather than an API client for flexibility over constraints: limits CPU (`--cpus`), drops capabilities (`--cap-drop ALL`), mounts explicit directories, exposes required ports, and runs tasks securely via explicit `exitCode` handlers. Includes tools to stream logs via `docker logs -f` and check true execution state.

#### `src/execution/job.runner.ts`
The ephemeral Batch Job executor. It creates isolated temporary workspaces, clones git repositories locally using `libs/git.clone.ts`, and executes the provided command in an ephemeral container. Upon completion or timeout, it safely captures stdout/stderr natively, uploads the artifact to S3/MinIO, and cleans up the working directory and Docker environment.

#### `src/execution/service.runner.ts`
The long-running Service Job executor. Distinct from batch jobs, services require network connectivity via HTTP. This fetches an open host port, optionally runs a "setup" container first (e.g., `npm install`), mounts a readable `tmpfs` to avoid Docker permissions mismatches, runs the user application entrypoint, and waits for a physical port TCP connection signal to mark the container "Ready".

#### `src/execution/service.registry.ts`
A tracking map specifically for Service Jobs. Because services live indefinitely until canceled, the registry holds state mapping `serviceId` -> `containerId`, `hostPort`, and active `logStreams`. Ensures that when a service is canceled or shutdown, its host constraints, ports, and temporary folders are thoroughly cleaned up.

---

### External Communication (`src/messaging/` & `src/bootstrap/` & `src/scheduler/`)
Handles interactions with external APIs and real-time message brokers.

#### `src/messaging/rabbit.connection.ts`
Establishes and provisions a unified `amqplib` channel mapped to the computebay rabbit-cluster.

#### `src/messaging/rabbit.consumer.ts`
Registers a pre-fetch based queue consumer listening for `.scheduled` jobs and `.cancelled` prompts. Enforces strict message acks/nacks on payload interpretation failure or unhandled exceptions.

#### `src/messaging/rabbit.publisher.ts`
Simple wrapper for emitting telemetry and event logs back over a wildcard AMQP exchange (e.g., `node.<nodeId>.events`).

#### `src/bootstrap/scheduler.client.ts`
The Node to external Scheduler REST API handler. It wraps API endpoints to `/nodes/register` (complete with exponential backoff on retries) and `/nodes/heartbeat` to keep resource allocations consistent.

#### `src/bootstrap/bootstrap.state.ts`
A small persistent state file storing Node AMQP credentials globally under `.computebay/bootstrap.json`. Ensures that if the worker agent restarts, it reconnects using the exact same Node ID instead of establishing itself as a phantom node.

#### `src/scheduler/heartbeat.service.ts` / `src/scheduler/job.listener.ts`
(Draft files intended for future dedicated decoupled scheduling routines / event loop processing, currently mostly handled in `main.ts`).

---

### Resource Monitoring (`src/monitoring/` & `src/network/`)

#### `src/monitoring/system.specs.ts`
Detects the exact physical CPU cores and MB memory via Node's `os` module.

#### `src/monitoring/capacity.ts`
Computes "Allocatable Capacity". Overrides physical limits if `CPU_LIMIT` and `MEMORY_LIMIT_MB` constants are defined in the configuration environment.

#### `src/monitoring/resource.monitor.ts`
A concurrent state machine. At any given time, maintains accurate `usedCpu` and `usedMemoryMb`. Synchronized synchronously to prevent the worker from accepting jobs when hardware is exhausted.

#### `src/network/port.manager.ts`
An in-memory port allocator starting at `:30000`. Includes sub-routines using `node:net` to probe actual TCP boundaries so Docker-Proxy pseudo-binds do not trigger false positive "Readiness" signals.

---

### Configuration & Utilities (`src/config/`, `src/utils/`, `src/libs/`, `src/types/`)

#### `src/config/config.ts`
Employs `zod` to perform strictly typed validation over `process.env`. If a variable like `SCHEDULER_BASE_URL` is missing, the application fast-fails upon booting.

#### `src/libs/git.clone.ts`
Uses raw `child_process.spawn` to cleanly executing `git clone` isolating depths and avoiding side-effects dynamically inside transient directories.

#### `src/libs/log.upload.ts` & `src/libs/s3.ts`
Responsible for moving completed offline compilation batches back up into object storage buckets. Connects exclusively using AWS S3 clients initialized towards MinIO URLs defined in the environment map.

#### `src/types/job.types.ts`
A vast source of truth dictating the communication protocol schemas. Transforms external `ScheduledJobMessage` payloads into deeply typed `ScheduledJob` structs containing guaranteed parameters like numeric CPUs and sanitized runtime strings. Maintains typing for events such as `NodeHeartbeatEvent`, `JobLifecycleEvent`, etc.

#### `src/utils/logger.ts`
Instantiates a highly optimized `pino` logger, leveraging `pino-pretty` for terminal output readability.

#### `src/lifecycle/shutdown.ts`
Encapsulates POSIX `SIGTERM`/`SIGINT` handling logic to ensure safe shutdown routines, stopping queues, un-claiming host allocations, and signaling fatal drops correctly.

#### `src/security/identity.ts`
Creates or accesses a secure Machine OS-based hardware identification string (Machine ID) to ensure that reboots keep workers bounded continuously.

## Execution Flow Recap
Whenever you add a new feature (like supporting new job variants), you typically follow this pipeline in the code:
1. Update boundaries inside `src/types/job.types.ts`.
2. Determine resource feasibility mapping inside `src/execution/worker.pool.ts`.
3. Scaffold specific operations on `Docker` utilizing `src/execution/container.runtime.ts`.
4. Run routines via an executor module (`src/execution/job.runner.ts` / `service.runner.ts`).
5. Emit results via Event Publishes hooked up to `src/main.ts` back into RabbitMQ.
