# sentry-slo-probe

Synthetic probes that measure **how well Sentry is ingesting your data**, and export the results to Datadog as SLO metrics.

Your error tracker is the thing you find out about outages from. This measures whether it's actually working — by continuously sending known synthetic events into Sentry, polling until they become queryable, and recording how long that took and how many never showed up at all.

## What it measures

| Probe | Question it answers | How |
|---|---|---|
| **Trace ingestion** | How long until a transaction is queryable, and how many never are? | Sends a batch of probe transactions tagged with a batch id, polls Discover for that tag |
| **Error ingestion** | How long until an error is queryable, and how many never are? | Sends a batch of probe errors, polls Discover for the batch tag |
| **Span completeness** | Do all the spans actually arrive? | Sends transactions with 5 child spans each, then checks how many of a sample of the arrived ones came back with all 5 |

Span completeness is the one that catches quiet data loss. A trace can arrive while some of its spans are silently dropped, so ingestion latency alone looks healthy.

## How it works

```
   send synthetic batch          poll Sentry API until they appear        emit
  ┌──────────────────┐          ┌─────────────────────────────┐    ┌──────────────┐
  │  Sentry SDK      │─────────▶│  GET /api/0/.../events/     │───▶│   Datadog    │
  │  (real ingest)   │          │  every POLL_INTERVAL         │    │  v2/series   │
  └──────────────────┘          └─────────────────────────────┘    └──────────────┘
        t=sentAt                          t=firstSeen                latency = Δ
```

Latency is wall-clock from send to the first successful query, so its **resolution is bounded by `POLL_INTERVAL_SECONDS`** (5s by default) — a 1-second ingestion and a 4-second ingestion may both report as one poll.

An OpenTelemetry exporter to Datadog's agentless OTLP intake (`otlp.<DD_SITE>`) is still configured, but **it currently emits no spans**. The single-event probes were the only things that created them, and the batch probes that replaced them are not self-instrumented — so there is nothing to see in APM. Its failure to initialise is logged and non-fatal.

## Metrics emitted

Each cycle sends a batch of `PROBE_BATCH_SIZE` events per probe, so every signal reports a distribution and a success rate rather than one sample. For each signal in `ingestion` (traces), `error_ingestion`, and `span_completeness`:

| Metric | Unit | Meaning |
|---|---|---|
| `sentry.<signal>.latency_ms.p50` | ms | Median send→queryable latency over the batch |
| `sentry.<signal>.latency_ms.p95` | ms | p95 — the series the monitors alert on |
| `sentry.<signal>.latency_ms.p99` | ms | p99 (a single event, at a batch size of 100) |
| `sentry.<signal>.sent` | count | Events that left the process successfully |
| `sentry.<signal>.received` | count | Events that came back queryable before the poll timeout |
| `sentry.<signal>.success_rate` | % | `received / sent`, published as `0` when `sent` is `0` |

Plus one metric from the span probe alone:

| Metric | Unit | Meaning |
|---|---|---|
| `sentry.span_completeness.received_pct` | % | Percentage of *sampled* transactions that arrived **complete** — all 5 child spans stored. **Not published at all** when the sample could not be taken — a gap in the series, rather than a fabricated `0%` that would read as total span loss. |

Two things about `received_pct` matter before you put a threshold on it, and neither is obvious from the name.

It is **all-or-nothing per transaction**, not a ratio of spans. `probe_spans.go` computes `complete / sample × 100`, where a transaction counts toward `complete` only if Sentry stored *all* of its 5 child spans. So 100 transactions each losing one span out of five read as **0%**, not 80%. A threshold of, say, 95 means "tolerate 5% of transactions losing spans", not "tolerate 5% span loss".

It is a **sample, not a census**: at most `SPAN_COMPLETENESS_SAMPLE` (20 by default) of the transactions that arrived have their stored span count fetched, one API call each. Fetches that fail shrink the sample rather than scoring as incomplete, so a partly failing Sentry API narrows the measurement instead of biasing it downward.

The trace signal is called `ingestion`, not `trace_ingestion`, so its metrics live under `sentry.ingestion.*` and its tag is `probe:ingestion`. All metrics are tagged `sentry_org:<org>`, `sentry_project:<project>`, and `probe:<signal>`.

**There is no `sentry.ingestion.error` metric.** The old per-cycle failure counter was retired along with the single-event probe, and has no successor. Failures surface in two other places: `sentry.<signal>.sent` short of `PROBE_BATCH_SIZE` means *our* send path failed, and a low `sentry.<signal>.success_rate` means Sentry did not return what we did send. Keeping those apart matters — a monitor on `success_rate` alone reports our own send outage as a Sentry breach, which is why the paging monitors in `scripts/setup_datadog_slos.sh` gate each one behind `sent > 0`, and why the reliability SLOs divide `received` by `sent` instead of thresholding `success_rate`.

If you are upgrading from the pre-batch metric names, note that Datadog does not roll a parent name up over its children: a query on `sentry.ingestion.latency_ms` does not match `sentry.ingestion.latency_ms.p95`. It goes permanently no-data instead of erroring, so nothing will tell you it broke.

## Quick start

Requires Go 1.25.6, a Sentry auth token with `event:read` + `project:read` + `org:read`, and a Datadog API key.

```bash
cp .env.example .env    # then fill in your real values
set -a && . ./.env && set +a
go run .
```

Or with Docker:

```bash
docker compose up --build
```

You should see a startup line, then a line per probe per cycle once its batch finishes draining:

```
Starting SLO probes (run=dktnmz0ub263, interval=2m0s, batch=100, send_window=1m40s, poll_timeout=2m0s, max_inflight=2)
[trace_ingestion] starting (interval=2m0s, batch=100)
[error_ingestion] starting (interval=2m0s, batch=100)
[span_completeness] starting (interval=2m0s, batch=100)
[trace_ingestion] batch=dktnmz0ub263-trace_ingestion-1 received=100/100 p50=3.1s p95=6.4s
[error_ingestion] batch=dktnmz0ub263-error_ingestion-1 received=99/100 p50=8.4s p95=14.2s
[span_completeness] batch=dktnmz0ub263-span_completeness-1 received=100/100 received_pct=100%
```

The `run=` component is regenerated on every start and prefixes every batch id, so a restart cannot re-query the previous process's events. `received_pct=unknown` means the completeness sample could not be taken this cycle; the metric is suppressed rather than reported as `0%`.

## Configuration

Required:

| Variable | Purpose |
|---|---|
| `SENTRY_DSN` | Where synthetic events are sent |
| `SENTRY_AUTH_TOKEN` | Reads them back; needs `event:read`, `project:read`, `org:read` |
| `SENTRY_ORG` | Org slug, used in API paths and metric tags |
| `SENTRY_PROJECT` | Project slug |
| `DD_API_KEY` | Metric submission and OTLP intake |

Optional:

| Variable | Default | Purpose |
|---|---|---|
| `DD_SITE` | `datadoghq.com` | e.g. `datadoghq.eu`, `us3.datadoghq.com` |
| `PROBE_INTERVAL_SECONDS` | `120` | How often each probe starts a batch |
| `POLL_TIMEOUT_SECONDS` | `120` | Give up waiting for the rest of a batch after this long |
| `POLL_INTERVAL_SECONDS` | `5` | How often to poll — also the latency resolution |
| `PROBE_BATCH_SIZE` | `100` | Events sent per probe per cycle |
| `PROBE_SEND_WINDOW_SECONDS` | `100` | Batch sends are paced evenly across this window rather than fired at once |
| `SPAN_COMPLETENESS_SAMPLE` | `20` | How many arrived transactions to fetch span counts for |

A batch is bounded at `PROBE_SEND_WINDOW_SECONDS + POLL_TIMEOUT_SECONDS + POLL_INTERVAL_SECONDS` (225s at defaults), which is longer than the 120s interval, so two batches per probe overlap in normal operation. That is expected.

A `skipping cycle` log line is not. It means a cycle outlived twice the interval (240s at defaults), which is past that ceiling. The send path is not what gets you there — its sends are paced across the window and bounded by a 10s flush timeout each. The work that can is what runs *after* the batch drains but still inside the same in-flight slot: the span probe's completeness sampling, up to `SPAN_COMPLETENESS_SAMPLE + 1` sequential Sentry calls at a 10s timeout apiece (~210s at defaults), and the six or seven Datadog submissions per signal, also 10s apiece (~60s). So a skip points at a slow Sentry event-detail API or a slow Datadog, not at a slow send.

Note that the probe writes real events into the target Sentry project and consumes quota. Point it at a dedicated project if that matters to you.

## Creating the Datadog SLOs

`scripts/setup_datadog_slos.sh` creates monitors plus SLOs over the metrics above. It needs a Datadog **application** key in addition to the API key, and `jq`:

```bash
export DD_API_KEY=... DD_APP_KEY=... SENTRY_ORG=... SENTRY_PROJECT=...
./scripts/setup_datadog_slos.sh
```

It is not idempotent — re-running creates duplicate monitors. The thresholds in it are starting points; tune them to your own ingestion behaviour once you have a few days of data.

**Six SLOs are created, of two different types, and both types are deliberate.**

- The three **latency and span-completeness** SLOs are **monitor-based**: a metric monitor with a threshold, plus an SLO measuring what fraction of the 30-day window that monitor was not alerting. Those signals genuinely are gauges — p95 latency and `received_pct` are levels, not tallies — so there is no good/total ratio to divide, and "how much of the month was this level acceptable?" is the only question available.
- The three **reliability** SLOs are **metric-based**: `sum(sentry.<signal>.received) / sum(sentry.<signal>.sent)`. Those two are real counts of events the batch probe tallies, so a good-events-over-total-events ratio is exactly what a metric-based SLO is for, and it measures every event rather than sampling whether a threshold monitor happened to be red.

Neither type is a leftover from the other. No SLO is backed by a composite.

Metric-based reliability gets the `sent > 0` gate structurally: a cycle that sent nothing contributes `0` to both numerator and denominator, so our own send failure cannot burn Sentry's error budget. Do not "simplify" those three into threshold SLOs over `success_rate` — `success_rate` is published as `0` when nothing sent, so such an SLO would record a full breach against Sentry for a local outage.

Reliability **paging** is a separate path, and that is where the composites live. Each signal gets a **composite** of two monitors: `success_rate` below threshold, AND `sent > 0`. They alert; they back no SLO. The gate is still not optional here, for the same reason — an ungated `success_rate` monitor pages against Sentry for our own send failure. The gate monitor sits in ALERT during normal operation by design; do not page on it, and do not fold the pair back into one monitor.

## Project status

This branch (`batched-slo-probes`) sends a paced batch of events per probe per cycle, and the batch path **is** wired into `main.go` — the legacy single-event path has been deleted. That is what turns the latency signal into real p50/p95/p99 percentiles plus a success rate rather than a single sample, and it is why the metric names in this README differ from the ones the released code emits.

The `main` branch still runs **one synthetic event per probe per cycle** and still emits the pre-batch metric names, so any dashboards built against it need updating before this branch merges. The design and task plan live in `docs/`.

## Layout

```
main.go          config + the three batch probe loops
probe.go         trace ingestion batch probe, Sentry send helpers
probe_error.go   error ingestion batch probe
probe_spans.go   span completeness batch probe
sentry_api.go    Sentry Discover / event-detail queries
datadog.go       Datadog v2 series submission
tracer.go        OTel → Datadog agentless OTLP setup (configured, emits no spans)
batch.go         batch engine: paced sends, batched polling, latency tracking
scripts/         Datadog monitor + SLO provisioning
docs/            design doc and implementation plan
```

## Development

```bash
go test ./...      # unit tests
go test ./... -race
go vet ./...
```

The pure logic — percentiles, send pacing, latency tracking, batch stop conditions, batch ids, the in-flight cap, the span-completeness suppression rule, and the Sentry/Datadog request shapes — is unit tested against `httptest` servers. The probe functions and `runBatchLoop` are network and process glue, marked as untested in the source, and verified by running against live Sentry and Datadog.
