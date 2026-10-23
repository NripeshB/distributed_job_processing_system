# Distributed Job Processing System

A PostgreSQL-backed background job processor written in Go. The project is being built in versions to explore backend engineering, database concurrency, and eventually distributed-systems reliability.

**Current milestone: V1 — persistent job queue and independent worker.** V1 supports an HTTP API for submitting and inspecting jobs and a separate worker process that polls PostgreSQL, claims a job, runs a demonstration task, and records the result.

> This is a learning and engineering portfolio project, not a production-ready managed queue. Retries, crash recovery, leases, and distributed worker coordination are planned rather than implemented.

## Architecture

```text
Client (curl / Postman)
        |
        | HTTP
        v
   Go API (:8080) -----------+
                             |
                         PostgreSQL
                         jobs table
                             |
                             | atomic claim / status updates
                             v
                        Go worker
                             |
                       task executor
```

The API and worker are **independent executables**. The API can accept jobs while the worker is offline; queued jobs persist in PostgreSQL until a worker claims them.

## Current capabilities (V1)

- `POST /jobs` — validate and persist a job.
- `GET /jobs/{id}` — retrieve a job and its status.
- `GET /jobs` — list jobs.
- `GET /health` — API health response (not a database readiness probe).
- Independent worker process polling for pending jobs.
- Atomic PostgreSQL job claiming with `FOR UPDATE SKIP LOCKED`.
- Job lifecycle: `pending → processing → succeeded | failed`.
- Demonstration `email` task: logs a message and waits approximately two seconds; **no email is sent**.
- Context-aware polling and basic graceful shutdown.
- HTTP handler unit tests using a fake store.

## Tech stack

- Go (`net/http`, `database/sql`, contexts, standard testing tools)
- PostgreSQL and raw SQL
- `pgx` PostgreSQL driver via `database/sql`

## Project layout

```text
cmd/
  api/main.go            # HTTP API executable
  worker/main.go         # Worker executable
internals/
  httpapi/handlers.go    # Routes and request validation
  job/job.go             # Job domain model
  store/postgres.go      # PostgreSQL connection management
  store/jobs.go          # Job persistence and claiming
  executor/executor.go   # Demonstration task execution
```

Additional directories, including SQL migrations, may exist in the repository; this tree highlights the core application components.

## Prerequisites

- Go (a version supporting method-based `net/http` routes and `Request.PathValue`, i.e. Go 1.22 or later)
- PostgreSQL
- A PostgreSQL database with the project's `jobs` table migration applied

The application reads the connection string from `DATABASE_URL`. For example, if your local PostgreSQL setup uses a matching database and user:

```bash
export DATABASE_URL='postgres://nripesh@localhost:5432/jobqueue?sslmode=disable'
```

Replace the username, host, and database with your own settings. The example disables TLS **for local development only**; use appropriate TLS and secret management for deployments. Apply the SQL migration in the repository before starting the applications.

## Run locally

Open two terminals at the repository root (with PostgreSQL running and `DATABASE_URL` set in **both** terminals).

**Terminal 1 — API**

```bash
go run ./cmd/api
```

**Terminal 2 — worker**

```bash
go run ./cmd/worker
```

The API listens on `localhost:8080` by default. The worker checks PostgreSQL for pending jobs and executes them independently of the HTTP process.

## Try it

**1. Submit a job**

```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"email","payload":{"to":"demo@example.com"}}'
```

Copy the returned `id`.

**2. Inspect it**

```bash
curl -sS http://localhost:8080/jobs/YOUR_JOB_ID
```

You should observe the status progress from `pending` to `processing` to `succeeded` (some intermediate states may be too brief to observe). The demonstration executor waits about two seconds.

**3. Exercise the failure path**

```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"unsupported_task","payload":{}}'
```

The worker should mark this job `failed`, since that task type is not supported.

**4. Demonstrate process independence**

Stop the worker, submit a job, and check that it stays `pending`. Restart the worker and confirm that the job is processed.

## Data model

The `jobs` table tracks:

| Field | Purpose |
| --- | --- |
| `id` | Unique job identifier |
| `type` | Task type handled by the executor |
| `payload` | JSON task input |
| `status` | `pending`, `processing`, `succeeded`, or `failed` |
| `created_at` | Submission time |
| `started_at` | Time claimed for execution |
| `completed_at` | Time marked finished |
| `attempts` | Number of claims / execution attempts |

The worker claims jobs using a single PostgreSQL statement that selects a pending row with `FOR UPDATE SKIP LOCKED`, changes its status, and returns the claimed record. This prevents competing workers from claiming the same pending row simultaneously; it **does not** guarantee exactly-once execution or recovery after a crash.

## Tests and checks

```bash
go test ./...
go test ./... -race
go vet ./...
```

The existing handler tests use a fake in-memory store. They do not exercise PostgreSQL. Database integration tests and concurrent-claiming tests are next priorities; do not interpret a passing handler test suite as proof of crash safety.

## Limitations and delivery semantics

- A worker crash after claiming a job can leave it stuck in `processing`.
- Failed jobs do not retry automatically.
- The V1 worker executes one job at a time.
- No lease ownership, heartbeat, dead-letter handling, or idempotency protocol yet.
- No authentication or public-demo resource controls are provided by V1.
- This version does **not** claim exactly-once processing or production fault tolerance.

## Roadmap

- **V1 — Persistent processing:** HTTP API, PostgreSQL, separate worker, basic lifecycle. **Implemented; verification and packaging in progress.**
- **V2 — Reliability:** multiple workers, concurrent claiming tests, retries with exponential backoff, dead-letter behavior, leases/crash recovery, shutdown improvements.
- **V3 — Coordination:** gRPC where it solves a real coordination need, worker registration/heartbeats, idempotency design, backpressure.
- **V4 — Operations:** Redis where justified, structured logs, metrics, Docker Compose, integration/load tests, documented benchmark results, optional TypeScript dashboard.

## Engineering notes

The system deliberately begins with PostgreSQL as both persistent storage and a work queue. This makes job state observable and lets us learn transactions and row locking without adding a message broker prematurely. Later versions will introduce additional infrastructure only when there is a concrete limitation to solve.

## License

Add a license file when you decide how you want others to reuse the code. Until then, no open-source license is implied.

