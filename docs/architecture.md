# Architecture

This document records the significant design decisions behind Portrait Engine
and the reasoning (and trade-offs) behind each. It is kept honest: it describes
what the system does, not what it aspires to.

## Goals

- A realistic **asynchronous image-processing** backend.
- Correct, observable job lifecycle with honest delivery semantics.
- Runs locally via Docker Compose, **no GPU**, no heavyweight native deps.
- Simple enough that an experienced Go engineer understands it quickly.

## Topology

Two binaries over shared `internal/` packages:

- **`cmd/api`** — external REST surface. Creates jobs, issues presigned upload
  URLs, publishes work, exposes status. Stateless; horizontally scalable.
- **`cmd/worker`** — background processor. Consumes from JetStream, runs the
  pipeline, writes artifacts. Horizontally scalable; concurrency bounded.

Backing services: **PostgreSQL** (state), **NATS JetStream** (queue),
**S3/MinIO** (bytes).

We deliberately avoid microservices, gRPC, and Redis. The API talks REST to
clients and dispatches work asynchronously; there is no low-latency internal
RPC path that would justify gRPC, and Postgres + JetStream already cover state
and queueing that Redis might otherwise provide.

## Key decisions

### PostgreSQL is the source of truth for job state

Job state transitions must be correct under concurrency (an API cancel racing a
worker starting the same job). Relational transactions and constraints give us:

- Atomic, guarded state transitions (`UPDATE ... WHERE status = <expected>`).
- Uniqueness constraints for idempotency keys.
- Durable history (attempts, processing steps, artifacts).

The message queue is intentionally **not** authoritative — a queue is good at
"deliver this once-or-more", not at "hold the canonical state of an entity".

### NATS JetStream for the queue, at-least-once delivery

JetStream provides durable streams, durable consumers, explicit ACK,
redelivery, and a max-delivery cap. That is exactly what an async job system
needs. We use **at-least-once** delivery and say so plainly: a message may be
delivered more than once (redelivery after a crash, an ACK lost in flight).

We do **not** claim exactly-once. Instead, duplicate deliveries are made *safe*:

1. The message is just a pointer (`job_id`, `attempt_id`, pipeline coordinates).
2. The worker loads authoritative job state from Postgres.
3. If the job is already terminal (`COMPLETED`/`CANCELLED`/`FAILED`) or being
   processed by a live attempt, the worker ACKs and does nothing.
4. Otherwise it processes, and commits the terminal state transactionally.

This turns "at-least-once + idempotent worker" into effectively-once *effects*
without the distributed-systems cost of true exactly-once.

### Idempotent job creation

`POST /v1/jobs` accepts an `Idempotency-Key`. The key is persisted with a unique
constraint; concurrent duplicate requests resolve via the database
(`INSERT ... ON CONFLICT`), returning the same job rather than creating a second
one. This is enforced by the DB, not by in-memory state, so it holds across API
replicas.

### Bounded worker pool

Workers use a **fixed** number of goroutines pulling from a durable consumer —
never goroutine-per-message. This bounds memory and downstream pressure (DB
connections, storage bandwidth, CPU-heavy image ops) predictably. Backpressure
is natural: if all workers are busy, messages simply wait in JetStream.

Each job runs under a `context` with a per-job timeout; cancellation propagates
API → job → worker → pipeline → processor. A panic in processing is recovered at
the worker boundary so one bad job can't crash the pool; the message is left for
redelivery / retry accounting.

### Versioned processing pipeline

A job records its `pipeline` and `pipeline_version`. The worker resolves the
exact pipeline version from a registry, so deploying a new pipeline version does
not change how already-created jobs are processed — old jobs stay reproducible.
Each processor run records name, status, duration, and error.

### Pure-Go image processing (no cgo)

Face detection uses **Pigo** (pure Go, cascade-based) and image manipulation
uses **`disintegration/imaging`**. This keeps the build cgo-free: static
binaries, tiny distroless images, trivial cross-compilation, and deterministic
tests. The trade-off is less sophisticated enhancement than libvips would give —
acceptable, and the `Processor` interface leaves room to swap in a native or
ML-backed implementation later (see below).

### Input safety

Uploaded images are untrusted. Object keys are server-minted and validated
(no path traversal / foreign namespace). Before processing, the job's object is
`Stat`-ed to confirm existence and re-check size/type. Decoding is bounded on
both encoded byte count and decoded pixel dimensions (the image header is read
before the full bitmap is allocated) to prevent a decompression bomb — a small
file that expands into an enormous in-memory image — from exhausting memory.
These are permanent errors: retrying the same input cannot help.

### Retry classification

Errors are classified as **permanent** or **transient** (via `errors.As`).
Permanent errors (e.g. an invalid/unsupported image) fail the job immediately —
retrying cannot help. Transient errors (storage blip, DB timeout) drive a retry
with exponential backoff + jitter, up to `max_attempts`, after which the job is
marked `FAILED`.

## How real ML / GPU inference could integrate later

The `Processor` interface (`Process(ctx, input) (output, error)`) is the seam.
A future `MLEnhanceProcessor` could call out to a model server (e.g. an ONNX
Runtime / Triton service, or a hosted inference API) instead of doing pixel math
locally. Because processors already receive `context.Context`, get timeouts, and
are ordered by an explicit versioned pipeline, adding a GPU-backed step is a
matter of implementing the interface and registering a new pipeline version —
no changes to the queue, state machine, or worker lifecycle.

## Package layout

```
cmd/api, cmd/worker        binaries
internal/config            env-driven configuration
internal/observability     logging, metrics, tracing, lifecycle
internal/domain            job model + state machine + error taxonomy
internal/repository        PostgreSQL persistence
internal/storage           object store (S3/MinIO)
internal/messaging         NATS JetStream publish/consume
internal/jobs              job service (creation, idempotency, orchestration)
internal/worker            bounded worker pool
internal/pipeline          processor interface + registry
internal/image             image ops (resize/color/beauty/background)
internal/api               HTTP routing, middleware, handlers
```
