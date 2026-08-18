# sentry-slo-probe

Synthetic probes that measure **how well Sentry is ingesting your data**, and export the results to Datadog as SLO metrics.

Your error tracker is the thing you find out about outages from. This measures whether it's actually working — by continuously sending known synthetic events into Sentry, polling until they become queryable, and recording how long that took and how many never showed up at all.

## What it measures

| Probe | Question it answers | How |
|---|---|---|
| **Trace ingestion** | How long until a transaction is queryable? | Sends a probe transaction, polls Discover for its trace ID |
| **Error ingestion** | How long until an error is queryable? | Sends a probe error, polls the project's issues for it |
| **Span completeness** | Do all the spans actually arrive? | Sends a transaction with 5 child spans, then counts the spans Sentry stored |

Span completeness is the one that catches quiet data loss. A trace can arrive while some of its spans are silently dropped, so ingestion latency alone looks healthy.

## How it works

```
   send synthetic event          poll Sentry API until it appears         emit
  ┌──────────────────┐          ┌─────────────────────────────┐    ┌──────────────┐
  │  Sentry SDK      │─────────▶│  GET /api/0/.../events/     │───▶│   Datadog    │
  │  (real ingest)   │          │  every POLL_INTERVAL         │    │  v2/series   │
  └──────────────────┘          └─────────────────────────────┘    └──────────────┘
        t=sentAt                          t=firstSeen                latency = Δ
```

Latency is wall-clock from send to the first successful query, so its **resolution is bounded by `POLL_INTERVAL_SECONDS`** (5s by default) — a 1-second ingestion and a 4-second ingestion may both report as one poll.

Each probe run is also traced with OpenTelemetry and exported straight to Datadog's agentless OTLP intake (`otlp.<DD_SITE>`), so you can see the probe's own send/poll breakdown in APM without running an agent.

## Metrics emitted

| Metric | Unit | Meaning |
|---|---|---|
| `sentry.ingestion.latency_ms` | ms | Transaction ingestion latency |
| `sentry.error_ingestion.latency_ms` | ms | Error ingestion latency |
| `sentry.span_completeness.received_pct` | % | Spans stored ÷ spans sent |
| `sentry.ingestion.error` | count | Posted as `1` whenever a probe fails outright |

All tagged `sentry_org:<org>`, `sentry_project:<project>`, and `probe:<name>`.

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

You should see a line per probe per cycle:

```
[trace_ingestion]   latency=3.1s
[error_ingestion]   latency=8.4s
[span_completeness] received=5/5 (100%)
```

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
| `PROBE_INTERVAL_SECONDS` | `120` | How often each probe runs |
| `POLL_TIMEOUT_SECONDS` | `120` | Give up waiting for an event after this long |
| `POLL_INTERVAL_SECONDS` | `5` | How often to poll — also the latency resolution |

Note that the probe writes real events into the target Sentry project and consumes quota. Point it at a dedicated project if that matters to you.

## Creating the Datadog SLOs

`scripts/setup_datadog_slos.sh` creates monitors plus monitor-based SLOs over the metrics above. It needs a Datadog **application** key in addition to the API key, and `jq`:

```bash
export DD_API_KEY=... DD_APP_KEY=... SENTRY_ORG=... SENTRY_PROJECT=...
./scripts/setup_datadog_slos.sh
```

It is not idempotent — re-running creates duplicate monitors. The thresholds in it are starting points; tune them to your own ingestion behaviour once you have a few days of data.

## Project status

`main` runs the probes as described above: **one synthetic event per probe per cycle**.

Work is in progress on `batched-slo-probes` to send a paced batch of events per cycle instead, which turns the latency signal into real p50/p95/p99 percentiles plus a success rate rather than a single sample. The batch engine (`batch.go`) and its Sentry and Datadog support are built and tested on that branch, but are **not yet wired into `main.go`** — so nothing in the running behaviour has changed yet. The design and task plan live in `docs/`.

## Layout

```
main.go          config + the three probe loops
probe.go         trace ingestion probe, Sentry send helpers, polling
probe_error.go   error ingestion probe
probe_spans.go   span completeness probe
sentry_api.go    Sentry Discover / issues / event-detail queries
datadog.go       Datadog v2 series submission
tracer.go        OTel → Datadog agentless OTLP setup
batch.go         batch engine (built, not yet wired in)
scripts/         Datadog monitor + SLO provisioning
docs/            design doc and implementation plan
```

## Development

```bash
go test ./...      # unit tests
go test ./... -race
go vet ./...
```

The pure logic — percentiles, send pacing, latency tracking, batch stop conditions, and the Sentry/Datadog request shapes — is unit tested against `httptest` servers. The probe functions themselves are network glue and are verified by running against live Sentry and Datadog.
