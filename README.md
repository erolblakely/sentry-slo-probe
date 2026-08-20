# sentry-slo-probe

Synthetic probes that measure **how well Sentry is ingesting your data**, and export the results to Datadog as SLO metrics.

Your error tracker is the thing you find out about outages from. This measures whether it's actually working — by continuously sending known synthetic events into Sentry, polling until they become queryable, and recording how long that took and how many never showed up at all.

## What it measures

| Probe | Question it answers | How |
|---|---|---|
| **Trace ingestion** | How long until a transaction is queryable, and how many never are? | Sends a batch of probe transactions tagged with a batch id, polls Discover for that tag |
| **Error ingestion** | How long until an error is queryable, and how many never are? | Sends a batch of probe errors, polls Discover for the batch tag |
| **Span completeness** | Do all the spans actually arrive? | Sends transactions with 5 child spans each, then counts the spans Sentry stored on a sample of the ones that arrived |

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
| `sentry.span_completeness.received_pct` | % | Spans stored ÷ spans sent, sampled over the transactions that arrived. **Not published at all** when the sample could not be taken — a gap in the series, rather than a fabricated `0%` that would read as total span loss. |

The trace signal is called `ingestion`, not `trace_ingestion`, so its metrics live under `sentry.ingestion.*` and its tag is `probe:ingestion`. All metrics are tagged `sentry_org:<org>`, `sentry_project:<project>`, and `probe:<signal>`.

**There is no `sentry.ingestion.error` metric.** The old per-cycle failure counter was retired along with the single-event probe, and has no successor. Failures surface in two other places: `sentry.<signal>.sent` short of `PROBE_BATCH_SIZE` means *our* send path failed, and a low `sentry.<signal>.success_rate` means Sentry did not return what we did send. Keeping those apart matters — a monitor on `success_rate` alone reports our own send outage as a Sentry breach, which is why `scripts/setup_datadog_slos.sh` gates each one behind `sent > 0`.

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
Starting SLO probes (run=3w5e11264sgsf, interval=2m0s, batch=100, send_window=1m40s, poll_timeout=2m0s, max_inflight=2)
[trace_ingestion] starting (interval=2m0s, batch=100)
[error_ingestion] starting (interval=2m0s, batch=100)
[span_completeness] starting (interval=2m0s, batch=100)
[trace_ingestion] batch=3w5e11264sgsf-trace_ingestion-1 received=100/100 p50=3.1s p95=6.4s
[error_ingestion] batch=3w5e11264sgsf-error_ingestion-1 received=99/100 p50=8.4s p95=14.2s
[span_completeness] batch=3w5e11264sgsf-span_completeness-1 received=100/100 received_pct=100%
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

A batch is bounded at `PROBE_SEND_WINDOW_SECONDS + POLL_TIMEOUT_SECONDS + POLL_INTERVAL_SECONDS` (225s at defaults), which is longer than the 120s interval, so two batches per probe overlap in normal operation. That is expected. A `skipping cycle` log line is not: it means a batch outlived twice the interval, which is past that ceiling, and points at a slow send path.

Note that the probe writes real events into the target Sentry project and consumes quota. Point it at a dedicated project if that matters to you.

## Creating the Datadog SLOs

`scripts/setup_datadog_slos.sh` creates monitors plus monitor-based SLOs over the metrics above. It needs a Datadog **application** key in addition to the API key, and `jq`:

```bash
export DD_API_KEY=... DD_APP_KEY=... SENTRY_ORG=... SENTRY_PROJECT=...
./scripts/setup_datadog_slos.sh
```

It is not idempotent — re-running creates duplicate monitors. The thresholds in it are starting points; tune them to your own ingestion behaviour once you have a few days of data.

Each reliability SLO is backed by a **composite** of two monitors: `success_rate` below threshold, AND `sent > 0`. The gate is not optional — `success_rate` is published as `0` when nothing sent, so an ungated monitor turns a local send failure into a full SLO breach blamed on Sentry. The gate monitor sits in ALERT during normal operation by design; do not page on it, and do not fold the pair back into one monitor.

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
