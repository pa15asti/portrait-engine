# Portrait Engine

Async photo-processing service. You upload a portrait, it runs through a
pipeline (face detection, resize, colour, light retouch, background blur) and
drops the result in object storage. Everything runs locally with Docker Compose,
no GPU.

I built it to have a realistic async system to point at — a REST API, a durable
queue, a bounded worker pool, and a job state machine that's actually correct
under concurrency, rather than a toy that falls over on the second request.

## How it fits together

```
Client
  │  1. POST /v1/uploads            → presigned PUT URL
  │  2. PUT bytes ─────────────────────────────────────► S3 / MinIO
  │  3. POST /v1/jobs {upload_id}
  ▼
REST API ──────────────► Postgres      (source of truth for job state)
  │  create + publish        ▲
  ▼                          │ workers re-read state before acting
NATS JetStream (work queue)  │
  ▼                          │
Worker pool (bounded) ───────┘
  ▼
Pipeline: resize → face-detect (Pigo) → colour → retouch → background
  ▼
S3 / MinIO  (artifacts)
```

Two binaries over shared `internal/` packages:

- `cmd/api` — REST surface. Persists state, hands out presigned upload URLs,
  publishes work. Never sees image bytes.
- `cmd/worker` — pulls jobs, loads state from Postgres, runs the pipeline,
  writes artifacts.

More detail in [docs/architecture.md](docs/architecture.md) and
[docs/reliability.md](docs/reliability.md).

## Stack, and why

| Thing            | Choice                   | Reason |
|------------------|--------------------------|--------|
| Job state        | Postgres (pgx)           | Transactions + constraints give a correct state machine and idempotency for free |
| Queue            | NATS JetStream           | Durable, at-least-once, redelivery and a delivery cap without running Kafka |
| Storage          | S3 / MinIO               | Presigned URLs keep big files off the API |
| Face detection   | Pigo                     | Pure Go, no cgo, no GPU; cascade is embedded |
| Image ops        | `disintegration/imaging` | Pure Go, so static binaries and reproducible tests |
| Metrics          | Prometheus               | Pull-based, boring, works |
| Tracing          | OpenTelemetry            | Propagated through the queue so a job is one trace end to end |

No Redis (Postgres + JetStream already cover state and queueing) and no gRPC
(there's no low-latency internal RPC to justify it).

## Running it

Need Docker + Compose. Go 1.26+ for tests.

```bash
make dev          # api, worker, postgres, nats, minio
make migrate-up   # apply migrations
```

- API: `localhost:8080`
- Worker metrics: `localhost:8081/metrics`
- MinIO console: `localhost:9001` (minioadmin / minioadmin)
- NATS monitoring: `localhost:8222`

## API

| Method | Path                       | |
|--------|----------------------------|--|
| POST   | `/v1/uploads`              | presigned upload URL |
| POST   | `/v1/jobs`                 | create a job (honours `Idempotency-Key`) |
| GET    | `/v1/jobs/{id}`            | status |
| POST   | `/v1/jobs/{id}/cancel`     | cancel |
| GET    | `/v1/jobs/{id}/artifacts`  | list outputs |
| GET    | `/health` `/ready` `/metrics` | ops |

```bash
# 1. ask for an upload slot
curl -sX POST localhost:8080/v1/uploads \
  -H 'content-type: application/json' \
  -d '{"content_type":"image/jpeg","size_bytes":204800}'
# → {"upload_id":"...","upload_url":"http://localhost:9000/...","expires_in_seconds":900}

# 2. PUT the bytes straight to storage (not through the API)
curl -sX PUT --upload-file portrait.jpg -H 'content-type: image/jpeg' "<upload_url>"

# 3. create the job; the Idempotency-Key makes it safe to retry
curl -sX POST localhost:8080/v1/jobs \
  -H 'content-type: application/json' -H 'Idempotency-Key: once-per-request' \
  -d '{"upload_id":"<upload_id>","pipeline":"portrait-enhance","pipeline_version":"v1"}'

# 4. poll
curl -s localhost:8080/v1/jobs/<job_id>
curl -s localhost:8080/v1/jobs/<job_id>/artifacts
```

## Pipeline

A pipeline is an ordered list of processors run under an explicit version. The
job stores `pipeline` + `pipeline_version`; the worker resolves that exact
version from a registry, so shipping a new version doesn't change how existing
jobs process.

`portrait-enhance@v1`: resize → face detection (Pigo) → colour adjust → light
Gaussian smoothing → background blur that keeps detected faces sharp. Every
stage records name/status/duration/error, so a failed run is still auditable.

## Reliability

- Postgres holds the truth; NATS just says "there's work". Workers re-read state
  before doing anything.
- At-least-once delivery. Duplicates are expected and made safe: the message is
  a pointer, and only the winner of a guarded `QUEUED → PROCESSING` claim
  processes. Not exactly-once — I don't claim it.
- Idempotent creation via `Idempotency-Key` on a unique constraint, so it holds
  under genuinely concurrent duplicates (there's a race test for that).
- Errors are permanent (bad/oversized image, unknown pipeline) → straight to
  FAILED, or transient → retry with exponential backoff + jitter up to the
  attempt cap.
- Cancellation propagates into a running pipeline; graceful shutdown drains
  in-flight jobs and leaves the rest for redelivery.

Details in [docs/reliability.md](docs/reliability.md).

## Observability

`/metrics` exposes HTTP latency/counts, jobs created/completed/failed/retried,
queue latency, processing duration, active workers, processor and storage
errors. Traces span HTTP → publish → claim → process → pipeline stages → S3,
with the trace context carried through JetStream headers so it's one trace.
Logs are structured (request/correlation/job ids); image bytes, credentials and
presigned URLs never get logged.

## Testing

```bash
make test              # unit
make test-race         # unit + race detector
make test-integration  # integration + e2e (needs Docker)
```

Unit tests cover the state machine, validation, idempotency, retry
classification and backoff, pipeline execution, cancellation, and the pool
(bounded concurrency, panic recovery, timeouts, drain — with goleak). The e2e
test drives the whole thing against real Postgres/NATS/MinIO: upload → job →
worker → artifact → COMPLETED, plus duplicate delivery, concurrent idempotency,
cancellation, and retry exhaustion. All green under `-race`.

## Scaling notes

API is stateless — scale it flat. Workers scale flat too; one durable consumer
spreads work and `WORKER_CONCURRENCY` caps in-flight jobs per worker, so
overload becomes queue latency rather than a crash. Postgres is the first thing
that'll hurt; the guarded updates are indexed and short. Image work is CPU-bound
— size the pool to cores.

## Known gaps

- The "AI" is classic CV — face detection is a real Pigo cascade, but "retouch"
  is a blur blend and "background" is a face-keyed blur, not segmentation. I'd
  rather say so than dress it up.
- Enqueue is a dual-write (state first, then publish). A publish failure can
  leave a job QUEUED with no live message; a startup requeue sweep recovers
  those. A transactional outbox would close the window — haven't needed it yet.
- A worker that hard-crashes mid-job can leave a job PROCESSING until someone
  intervenes. Wants a lease + reaper.
- Single region, single bucket, no auth yet.

## License

MIT.
