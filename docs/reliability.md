# Reliability

How Portrait Engine behaves under failure, and the guarantees it does — and does
not — make. This document is expanded as the corresponding features land; it
never documents behavior that isn't implemented.

## Delivery semantics: at-least-once

Job messages are delivered **at least once**. A message may be delivered more
than once because:

- A worker crashed after processing but before its ACK was recorded.
- An ACK was lost in transit and JetStream redelivered after `AckWait`.
- A transient failure triggered a retry.

We do **not** provide exactly-once delivery. Exactly-once across a queue, a
database, and object storage would require distributed transactions we
deliberately avoid. Instead we make duplicate processing *safe* (see below).

## Making duplicates safe

The queue message is only a pointer. Authoritative state lives in PostgreSQL.
Before doing work, a worker loads the job and its current attempt and decides:

- Job already terminal (`COMPLETED` / `FAILED` / `CANCELLED`) → ACK, do nothing.
- Job owned by another live attempt → ACK, do nothing.
- Otherwise → process, then commit the terminal transition transactionally.

Terminal transitions are guarded (`UPDATE ... WHERE status = <expected>`), so two
workers racing the same job cannot both "win".

## Idempotent job creation

`POST /v1/jobs` with an `Idempotency-Key` is safe to retry. The key is stored
under a unique constraint; a duplicate request returns the original job instead
of creating a new one. Enforced by the database, so it holds across API replicas
and truly concurrent requests.

## Retries and backoff

Errors are classified (`domain.Classify`, via `errors.As`):

- **Permanent** (invalid image, unsupported format, unknown pipeline): the job
  goes straight to `FAILED`, no retry.
- **Transient** (storage/DB/network blips) and unclassified errors: the worker
  walks the job `PROCESSING → FAILED → RETRYING → QUEUED` and negatively-acks
  the delivery with a backoff delay, so JetStream redelivers it and a worker
  re-claims the QUEUED job. This repeats while `attempt_count < max_attempts`;
  once exhausted the job is `FAILED`.

Backoff is exponential (base 2s, capped at 60s) with **equal jitter** — the
delay is uniformly drawn from `[exp/2, exp]` — so competing workers do not retry
in lockstep. The database `attempt_count` is the authority for the retry cap;
JetStream's `MaxDeliver` is configured higher as a secondary safety net.

## Recovering stuck QUEUED jobs

Because enqueue is state-first (see above), a publish failure can leave a job
`QUEUED` with no live message. On startup each worker runs a **requeue sweep**:
it lists QUEUED jobs untouched for longer than a grace period and republishes
them. Republishing is safe — the publisher dedups by job id and the worker
re-reads authoritative state — so a job whose message is merely in-flight is not
processed twice. (A transactional outbox would remove the window entirely; it is
noted as a future improvement.)

## Cancellation

Cancellation is cooperative and propagates through `context.Context`:

```
API cancel request → job state (CANCELLED) → worker watcher → context → pipeline → processor
```

The API marks the job `CANCELLED` (a guarded transition). While a worker
processes a job it polls the job's state; when it observes `CANCELLED` it
cancels the processing context, which the pipeline honors between stages and
each processor honors via `ctx`. In-flight work therefore stops promptly rather
than running to completion and being discarded. Even if processing finishes
first, the final `PROCESSING → COMPLETED` transition is guarded and fails
against a `CANCELLED` job, so a cancelled job is never promoted to completed and
partial artifacts are not published as a result. (Polling is a simple mechanism;
a pub/sub cancel signal is a possible future refinement.)

## Graceful shutdown

On `SIGTERM`/`SIGINT`:

- **API**: stops accepting new connections and drains in-flight requests within
  a timeout.
- **Worker**: stops pulling new messages, lets in-flight jobs finish within a
  timeout, ACKs those that complete, and leaves the rest for JetStream
  redelivery. No job is lost; at worst it is retried.

## What can still go wrong (honest limitations)

- A job may be processed twice with observable *side effects only if a processor
  is not idempotent*. Output objects use deterministic keys so re-processing
  overwrites rather than duplicates.
- If Postgres is unavailable, the worker cannot safely act and will NAK/retry;
  progress halts until the database returns (fail-safe, not fail-open).
