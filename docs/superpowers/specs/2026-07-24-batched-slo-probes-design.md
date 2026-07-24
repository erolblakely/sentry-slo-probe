# Batched SLO Probes — Design

**Date:** 2026-07-24
**Status:** Approved (pending spec review)

## Goal

Change each of the three SLO probes (trace ingestion, error ingestion, span
completeness) from sending **one** synthetic event per cycle to sending a
**batch of 100 events per type, paced across a ~2-minute cycle**, and to
measure both:

1. **Latency distribution** — p50 / p95 / p99 ingestion latency across the batch.
2. **Ingestion reliability** — how many of the sent events arrived (`received / sent`).

Backward compatible: `PROBE_BATCH_SIZE=1` reproduces today's single-event behavior.

## Why the polling model must change

Today each probe sends one event and polls for that single event — up to
`POLL_TIMEOUT / POLL_INTERVAL` ≈ 24 API queries per event. Scaling that
per-event approach to 100 events would be ~2,400 Sentry API queries per probe
per cycle, which hits API rate limits immediately.

**Chosen approach — batch-query polling:** tag all 100 events in a cycle with a
shared batch id, then run **one** paginated Discover query per poll that returns
every arrived event in the batch at once. Per-cycle API cost drops from ~2,400
to a few dozen queries.

**Rejected alternative — per-event polling:** accurate but infeasible at 100×
due to rate limits.

## Configuration

New/changed environment variables (all with backward-compatible defaults):

| Var | Default | Meaning |
|---|---|---|
| `PROBE_BATCH_SIZE` | `100` | events sent per type per cycle |
| `PROBE_INTERVAL_SECONDS` | `120` | cadence between cycle starts (was 60) |
| `PROBE_SEND_WINDOW_SECONDS` | `100` | pace the batch sends evenly across this window |
| `POLL_TIMEOUT_SECONDS` | `120` | how long to keep polling after the last send |
| `POLL_INTERVAL_SECONDS` | `5` | poll cadence; also the latency measurement resolution |
| `SPAN_COMPLETENESS_SAMPLE` | `20` | how many arrived transactions to fetch span detail for |

`PROBE_BATCH_SIZE=1` collapses the batch machinery back to single-event behavior.

## Send model

Per cycle, per type:

- A sender paces `PROBE_BATCH_SIZE` events evenly across `PROBE_SEND_WINDOW_SECONDS`
  (~1 event/sec at defaults).
- Each event is stamped with a unique sequence id and a shared
  `probe_batch:<cycleId>` tag; the sender records `sentAt[id]` on the local clock.
- A bounded worker pool performs the actual sends so a slow HTTP send cannot
  stall the pacing schedule.

`cycleId` is unique per batch (e.g. probe type + monotonically increasing
counter seeded from cycle start), so concurrent/overlapping batches never
collide in queries.

## Measurement model

A poller queries the **Discover events endpoint** filtered by
`probe_batch:<cycleId>` (`per_page=100`), once per `POLL_INTERVAL_SECONDS`:

- For each id seen for the first time: `latency = detectionTime − sentAt[id]`,
  both measured on the local clock (no cross-clock skew with Sentry).
- The poller runs until all sent ids are found or until `POLL_TIMEOUT_SECONDS`
  after the last send.
- At the end: `reliability = received / sent`; collected latencies →
  p50 / p95 / p99.

**Decision A — latency resolution:** latency is measured at poll granularity
(±`POLL_INTERVAL_SECONDS`, default 5s). This is acceptable for an ingestion SLO
measured in seconds. Finer resolution is available by lowering
`POLL_INTERVAL_SECONDS`. **Accepted default: 5s.**

## Span completeness at scale

Each of the 100 transactions carries 5 spans (500 sent). Fetching per-transaction
event detail for all 100 = 100 detail calls/cycle.

**Decision B — sampling:** measure completeness by fetching span detail for a
**sample of `SPAN_COMPLETENESS_SAMPLE` (default 20)** arrived transactions. A
transaction is "complete" if its event shows all expected spans. Reported as
`received_pct` = complete-in-sample / sample-size × 100, plus the batch-level
reliability (transactions arrived / sent). **Accepted default: sample 20.**

Rationale: keeps span-detail API cost bounded and constant regardless of batch
size, while still giving a statistically meaningful completeness signal.

## Datadog metrics (per type, per cycle)

Replaces today's single latency gauge with, for each `<signal>` in
`{ingestion, error_ingestion, span_completeness}`:

- `sentry.<signal>.latency_ms.p50` / `.p95` / `.p99` — gauges, computed in-process
- `sentry.<signal>.sent` — count
- `sentry.<signal>.received` — count
- `sentry.<signal>.success_rate` — gauge (percent)

For span completeness additionally:

- `sentry.span_completeness.received_pct` — gauge (sampled, as today)

Tags unchanged: `sentry_org`, `sentry_project`, `probe`. Percentiles are computed
in-process (simple sorted-slice percentile) and submitted via the existing v2
series client — no new Datadog dependency, no distribution-metric plumbing.

The metric-submission helper will log on non-202 responses (today the return
value is ignored — a silent-failure class we are closing here).

## Concurrency / overlap

**Decision C — overlap:** at defaults, a batch can still be draining when the
next cycle starts (every 120s). Batches are isolated by unique `cycleId`, so
overlap is harmless for correctness. A backstop caps **in-flight batches per
type at 2**; if that cap is hit, the new cycle is skipped and a warning logged
(so we never leak goroutines under sustained slowness). **Accepted default:
overlap allowed, cap 2.**

## Code structure

- **`batch.go` (new)** — the paced sender, the batch poller, and a percentile
  helper. One focused unit: given a "send one event" function, a "batch query"
  function, and timing config, it runs a full batch and returns
  `{sent, received, latencies}`.
- **`probe.go` / `probe_error.go` / `probe_spans.go`** — each reworked to
  express two small pieces: "send one event (returns id, sentAt)" and the batch
  query predicate/parameters, delegating the orchestration loop to `batch.go`.
  Span completeness adds the sampled detail-fetch step after the batch completes.
- **`sentry_api.go`** — add a batch query against the Discover events endpoint
  that filters by `probe_batch` and paginates up to the batch size; keep the
  existing single-event helpers (used when `PROBE_BATCH_SIZE=1` / for the
  span-detail fetch).
- **`datadog.go`** — add a helper to post the metric set for a batch result, and
  make `postMetric` failures log rather than be silently discarded.
- **`main.go`** — parse new config; the three probe loops keep their shape but
  each cycle now runs a batch and emits the metric set. Enforce the in-flight cap.
- **OTel tracing** — each batch cycle remains wrapped in a span; per-event child
  spans are summarized (counts/percentiles as span attributes) rather than one
  child span per event, to avoid 100× span volume.

## Testing

- Unit test the percentile helper (known inputs → known p50/p95/p99).
- Unit test the pacing schedule (N events across window → expected inter-send spacing, using an injected clock/ticker).
- Unit test the batch poller against a fake query function: events "arrive" over
  successive polls; assert `received`, per-event latency, and timeout behavior.
- Unit test config parsing (defaults + overrides, including `PROBE_BATCH_SIZE=1`).
- Manual end-to-end run against live Sentry/Datadog to confirm real percentiles,
  success rate, and clean transport (the verification loop used throughout this
  project).

## Out of scope

- Datadog distribution-metric type (client-side percentiles are sufficient).
- Fetching exact Sentry ingestion timestamps (detection-based latency is used).
- Changing the OTLP export configuration (already fixed).
