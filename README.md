# sentry-slo-probe

Synthetic probes that measure **how well Sentry is ingesting your data**, and export the results to Datadog as SLO metrics.

Your error tracker is the thing you find out about outages from. This measures whether it's actually working — by continuously sending known synthetic events into Sentry, polling until they become queryable, and recording how long that took and how many never showed up at all.

## What it measures

| Probe | Question it answers | How |
|---|---|---|
| **Trace ingestion** | How long until a transaction is queryable, and how many never are? | Sends a batch of probe transactions tagged with a batch id, polls Discover for that tag |
| **Error ingestion** | How long until an error is queryable, and how many never are? | Sends a batch of probe errors, polls Discover for the batch tag |
| **Span completeness** | Do all the spans actually arrive? | Sends transactions with 5 child spans each, then counts the stored child spans of a sample of the arrived ones in a single grouped query |
| **Trace stream** | Right now, minute by minute, how long is a trace taking to arrive, and did all its spans? | Sends one probe transaction **every second**, continuously, polls Sentry for the ones still in flight by trace id, then counts their child spans |
| **Trace stream (OTLP)** | The same, for traces sent with OpenTelemetry over OTLP | The same stream, but built with the OpenTelemetry SDK and exported to Sentry's OTLP endpoint, a separate ingest path |

The trace stream measures the same thing as trace ingestion, but continuously instead of in a batch every two minutes, so it reports every minute with no gaps between batches. It runs twice, once through the Sentry SDK and once through OpenTelemetry, so the two ingest paths can be compared side by side. See [The trace stream](#the-trace-stream).

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

**Expect traces to be far slower than errors — roughly 4-5x.** On the measured baseline (see [Project status](#project-status)) transactions came back at p50 57.1s / p95 66.1s against the error probe's p50 11.2s / p95 16.3s. That is surprising the first time you see it, and it is the single fact that most affects how you set thresholds: a latency threshold that is generous for the error probe sits *below* the healthy p95 of the trace probe and alerts forever. The two signals therefore get separate, deliberately non-comparable thresholds in `scripts/setup_datadog_slos.sh`.

An OpenTelemetry exporter to Datadog's agentless OTLP intake (`otlp.<DD_SITE>`) is still configured, but **it currently emits no spans**. The single-event probes were the only things that created them, and the batch probes that replaced them are not self-instrumented — so there is nothing to see in APM. Its failure to initialise is logged and non-fatal.

## Metrics emitted

Each cycle sends a batch of `PROBE_BATCH_SIZE` events per probe, so every signal reports a distribution and a success rate rather than one sample. For each signal in `ingestion` (traces), `error_ingestion`, and `span_completeness`:

| Metric | Unit | Datadog type | Meaning |
|---|---|---|---|
| `sentry.<signal>.latency_ms.p50` | ms | gauge | Median send→queryable latency over the batch |
| `sentry.<signal>.latency_ms.p95` | ms | gauge | p95 — the series the monitors alert on |
| `sentry.<signal>.latency_ms.p99` | ms | gauge | p99 (a single event, at a batch size of 100) |
| `sentry.<signal>.sent` | count | **count** | Events Sentry received: answered HTTP 2xx, or its own 5xx |
| `sentry.<signal>.received` | count | **count** | Events that came back queryable before the poll timeout |
| `sentry.<signal>.success_rate` | % | gauge | `received / sent`, published as `0` when `sent` is `0` |

Plus one metric from the span probe alone:

| Metric | Unit | Datadog type | Meaning |
|---|---|---|---|
| `sentry.span_completeness.received_pct` | % | gauge | Percentage of *sampled* transactions that arrived **complete** — all 5 child spans stored. **Not published at all** when the sample could not be taken — a gap in the series, rather than a fabricated `0%` that would read as total span loss. |

`sent` and `received` are submitted as Datadog **count** (type 1, with an `interval`) rather than gauges, because they are the numerator and denominator of the metric-based reliability SLOs and because two overlapping batches landing in one rollup bucket have to sum, not average. Everything else is a level and stays a gauge.

**Two things are deliberately never published, and both are gaps you should not fill in.** The three latency percentiles are omitted for any cycle in which nothing arrived: there is no sample to take a percentile of, and the `0` an empty percentile returns would render a total ingestion failure as *perfect* latency — well under the alert thresholds, so the latency SLOs would sit at 100% straight through the outage. And *every* metric for a signal is withheld for a cycle in which the probe could not query Sentry at all — an expired `SENTRY_AUTH_TOKEN` is the observed case, where sends keep succeeding against the DSN while every Discover poll returns 401. `received` is not `0` there, it is unknown, and publishing `sent` without it would hand the reliability SLOs a denominator with no numerator. A cycle that publishes nothing contributes `0/0` and cannot move an SLO; the absence itself is what re-arms `notify_no_data` on the latency monitors. There is no heartbeat metric standing in for either case.

Two things about `received_pct` matter before you put a threshold on it, and neither is obvious from the name.

It is **all-or-nothing per transaction**, not a ratio of spans. `probe_spans.go` computes `complete / sample × 100`, where a transaction counts toward `complete` only if Sentry stored *all* of its 5 child spans. So 100 transactions each losing one span out of five read as **0%**, not 80%. A threshold of, say, 95 means "tolerate 5% of transactions losing spans", not "tolerate 5% span loss".

It is a **sample, not a census** of the batch: at most `SPAN_COMPLETENESS_SAMPLE` (20 by default) of the transactions that arrived are measured. Those are measured in **one** request, not one per transaction — a `count()` query over the spans dataset filtered to `trace:[<sampled ids>] is_transaction:false`, grouped by trace, so it returns stored child spans per trace with the root transaction span excluded. `SPAN_COMPLETENESS_SAMPLE` still bounds the measurement: it caps how many trace ids go into that single query.

Because there is only one request, there is no per-transaction fetch left to fail. Either the query succeeds — in which case a sampled trace absent from the results genuinely has zero stored child spans, and is scored **incomplete** — or it fails outright, in which case nothing is known and the metric is suppressed for the cycle rather than published as a `0%` that would read as total span loss.

The trace signal is called `ingestion`, not `trace_ingestion`, so its metrics live under `sentry.ingestion.*` and its tag is `probe:ingestion`. All metrics are tagged `sentry_org:<org>`, `sentry_project:<project>`, and `probe:<signal>`.

**There is no `sentry.ingestion.error` metric.** The old per-cycle failure counter was retired along with the single-event probe, and has no successor. Failures surface in two other places: `sentry.<signal>.sent` short of `PROBE_BATCH_SIZE` means a send failed on *our* side (our network, a 4xx such as a bad DSN key, or Sentry rate limiting us with a 429), and a low `sentry.<signal>.success_rate` means Sentry did not return what it received. Keeping those apart matters — a monitor on `success_rate` alone reports our own send outage as a Sentry breach, which is why the paging monitors in `scripts/setup_datadog_slos.sh` gate each one behind `sent > 0`, and why the reliability SLOs divide `received` by `sent` instead of thresholding `success_rate`.

**What counts as sent** is decided per event, under the SDK's transport (`delivery.go`), by whose failure it is:

| Sentry's answer | Counts as sent? | Why |
|---|---|---|
| 2xx | yes | Accepted |
| 5xx | **yes** (logged) | Sentry's own ingest failed an event it received. Over HTTPS this cannot come from our network, and the SDK does not retry it, so a real customer's event is lost the same way. It never arrives and reads as **lost**, which is what the probe exists to catch |
| 429 | no | Sentry rate limiting us: quota, not ingestion. In the batch probes the Sentry SDK then drops further events without a request until `Retry-After` passes, and those are not sent either; in the streams every send is a fresh request and gets its own 429 |
| 2xx that rejects spans (OTLP `partial_success`) | no (logged) | Sentry explicitly refusing them, as with a 4xx. The response does not say which spans, so every trace in that request is excluded rather than charged to Sentry |
| other 4xx | no | Our fault: bad DSN key, malformed or oversized payload |
| no answer within 25s of the full request (**streams only**) | **yes** (logged, `probe.unanswered`) | Sentry's endpoint had the data and was too slow. Slowness is Sentry's, never ours, so it counts like a 5xx. See [When Sentry is slow](#when-sentry-is-slow) |
| connection reset, closed or lost; refused; unreachable; a write that failed | no | Not provably Sentry's: our egress dying, an idle connection closed under us, an HTTP/2 stream refused. Only the final attempt counts if net/http retries |
| no answer at all, in the batch probes | no | The batch probes' rule is unchanged: they wait 10s and any transport error is not sent |
| dropped before any request | no | The SDK dropped it (buffer full, or a `Retry-After` in force) |

This has to be checked per event because the SDK's own `Flush` cannot tell: it silently drops an event whose request failed, or that it held back during a `Retry-After`, and still reports success. Sends that do not count are logged with the reason.

If you are upgrading from the pre-batch metric names, note that Datadog does not roll a parent name up over its children: a query on `sentry.ingestion.latency_ms` does not match `sentry.ingestion.latency_ms.p95`. It goes permanently no-data instead of erroring, so nothing will tell you it broke.

## The trace stream

The trace stream (`stream.go`) sends one probe transaction every second, with the same payload as the trace ingestion probe (a root transaction plus 4 child spans). Every `TRACE_STREAM_POLL_SECONDS` (2s by default) it asks Sentry which of the traces still in flight have arrived. It names them by trace id, up to 100 per request, in a `trace:[…] is_transaction:true` query on the spans dataset. Every 60 seconds it reports the traces whose fate was decided in that minute.

Each trace's fate is decided exactly once:

- **arrived**: a successful query found it. Its latency runs from the local send to the response of that query, so it is an upper bound with a resolution of about `TRACE_STREAM_POLL_SECONDS`.
- **lost**: a successful query, asked at or after `TRACE_STREAM_TIMEOUT_SECONDS` (300s by default), did not find it.
- **dropped, unmeasured**: no successful query was asked at or after its deadline within 30s of it (every query failing on an expired `SENTRY_AUTH_TOKEN`, say). We never managed to look when it mattered, so it is counted on **neither** side of the success rate rather than charged to Sentry.

Only sends that count as sent are tracked at all (see the table in [Metrics emitted](#metrics-emitted)): a send that failed on our side is logged as `send_failed` and is never counted as lost, while one Sentry answered with a 5xx is tracked and, never arriving, reads as lost.

The stream's timeout is deliberately its own setting and much longer than the batch probes' `POLL_TIMEOUT_SECONDS`. Measurable latency is capped at roughly the timeout, so it has to sit well above the 120s trace latency alert threshold: at 120s, a Sentry that took 130s for every trace would read as a total loss with no latency at all. (A batch trace effectively gets 120-219s, because the batch drains from its *last* send.) A longer timeout costs nothing while Sentry is healthy, since the number of traces in flight follows the actual latency, about 60. It only means more traces are polled while Sentry is slow: up to 300 in flight (three requests per poll) if nothing arrives at all.

### Child-span completeness

About a minute after a trace's root arrives, the stream counts its stored child spans, in one `count()` query per minute over the spans dataset (`trace:[…] is_transaction:false`, grouped by trace, up to 100 traces per request). A trace is **complete** only if all 4 children are stored: all-or-nothing per trace, as in the span completeness probe. A trace missing from a successful count stored no children and is incomplete. The minute's wait lets children that land a little after their root be counted fairly. A count that fails scores nothing; its traces are retried each minute and dropped uncounted after 5 minutes of failures. Lost traces are never counted: they have no spans to count.

### Metrics

Each stream publishes the same six metrics as a batch signal, plus one, once a minute. The SDK stream publishes under `sentry.trace_stream.*` with tag `probe:trace_stream`; the OTLP stream under `sentry.trace_stream_otlp.*` with tag `probe:trace_stream_otlp`:

| Metric | Unit | Datadog type | Meaning |
|---|---|---|---|
| `sentry.trace_stream.latency_ms.p50` / `.p95` / `.p99` | ms | gauge | Over the traces that arrived this minute (~60 samples, so p99 is the minute's maximum) |
| `sentry.trace_stream.sent` | count | **count** | Traces **decided** this minute (arrived + lost), *not* traces sent this minute |
| `sentry.trace_stream.received` | count | **count** | Traces that arrived this minute |
| `sentry.trace_stream.success_rate` | % | gauge | `received / sent` over the same decided traces |
| `sentry.trace_stream.census.checked` | count | **count** | Arrived traces whose child spans were counted this minute |
| `sentry.trace_stream.census.complete` | count | **count** | Of those, traces with all 4 child spans stored. `sum(complete) / sum(checked)` is the completeness SLO ratio |
| `sentry.trace_stream.complete_pct` | % | gauge | `complete / checked` for this minute alone. Swings on small samples; use the counts for anything over time |

The three census metrics are **not published** in a minute when nothing was counted, so a failed count leaves a gap rather than a `0%` that would read as total span loss.

Each stream also reports on itself, every minute, even a minute that decided nothing, so a broken probe can be told apart from a slow or lossy Sentry:

| Metric | Datadog type | Meaning |
|---|---|---|
| `sentry.trace_stream.probe.send_failed` | count | Sends that did not count as sent (see the table in [Metrics emitted](#metrics-emitted)) |
| `sentry.trace_stream.probe.send_skipped` | count | Seconds on which every send worker was still busy |
| `sentry.trace_stream.probe.query_failed` | count | Sentry lookups that failed (an expired `SENTRY_AUTH_TOKEN` shows here) |
| `sentry.trace_stream.probe.abandoned` | count | Traces dropped unmeasured because no lookup succeeded in time |
| `sentry.trace_stream.probe.census_failed` | count | Child-span counts that failed |
| `sentry.trace_stream.probe.census_dropped` | count | Traces dropped uncounted after 5 minutes of failed counts |
| `sentry.trace_stream.probe.unanswered` | count | Sends Sentry left unanswered for 25s after the full request, counted as sent. A direct read of Sentry being slow to answer |
| `sentry.trace_stream.probe.http_retried` | count | Sends net/http retried on a new HTTP/2 attempt (a refused stream, a GOAWAY). Also how a request can reach Sentry twice |
| `sentry.trace_stream.probe.pending` | gauge | Traces in flight. Around 60 when healthy; it climbs when Sentry slows down |

All of a minute's metrics go to Datadog in one request, so a slow Datadog costs one timeout per minute rather than one per metric.

`sent` is the traces decided in the window, rather than the traces sent during it, because a trace sent in the last minute has usually not arrived yet. Dividing by sends would make `success_rate` read low every minute. Every trace is decided in exactly one window, so over any longer period the sum of `sent` still equals what was sent (less any dropped unmeasured).

A minute that decides nothing **publishes no latency or reliability**. That covers startup until the first trace arrives, and any minute in which every Sentry query failed. The first minute usually decides only the few traces sent in its first seconds, so its percentiles rest on a tiny sample. Publishing would post `sent=0` and `success_rate=0`, which reads as a total outage. The stream's counts carry a 60s `interval`, not `PROBE_INTERVAL_SECONDS`.

Sends are fired on a fixed one-second tick by a pool of 64 workers, so a slow Sentry cannot delay the next send. If all 64 are still busy, the tick is skipped and counted (`probe.send_skipped`). Each minute logs one line:

```
[trace_stream] window: arrived=60 lost=0 p50=57.4s p95=1m4.2s complete=59/59 pending=58 send_failed=0 send_skipped=0 query_ok=30 query_failed=0 abandoned=0 census_failed=0 census_dropped=0
```

`pending` is the traces still in flight. At about one per second and ~60s to arrive, a healthy value is around 60.

### When Sentry is slow

The streams treat slowness as Sentry's, never ours, so a slow Sentry shows up as slow (or lost) rather than quietly shrinking the sample, and our own queues and network never show up as Sentry's:

- **Each trace goes out at once, in its own request, on both paths.** The SDK stream sends each trace on its own client, because the Sentry SDK's transport sends one request at a time; the OTLP stream exports each trace directly, because a batch span processor runs its exports one at a time under a lock. Either way, a slow Sentry would otherwise build a queue on our side, and since latency runs from the send, our queue would be measured as Sentry's latency, and the slowest sends would fall out of the sample. 64 send workers per stream keep a slow Sentry from ever causing a skipped send.
- **Sentry gets 25 seconds to answer, on the probe's own clock.** The clock starts with the request (connecting included). A request written in full that is still unanswered when it runs out is abandoned and counts as sent, like a 5xx, and is published as `probe.unanswered`. That is the only way a request with no answer counts as sent: a reset, a closed or lost connection, a refused stream or a failed write is not provably Sentry's and counts as not sent. If net/http retries a request on a new attempt, only the final attempt counts (a refused attempt stops counting as written the moment net/http decides to retry it), and the retry is published as `probe.http_retried`.
- **A dead network path is ours, not Sentry's.** The streams use their own HTTP transport with HTTP/2 health pings: a connection whose path has died fails as a lost connection within about 15s (not sent), instead of leaving every send on it to run out the 25s and be charged to Sentry. A live Sentry answers pings at the protocol layer even while its ingest is slow, so a slow Sentry still reads as slow. HTTP/1.1 has no such pings: if a connection ever falls back to it, a path that dies after the write still runs out the 25s and reads as unanswered, which `probe.unanswered` makes visible.

The batch probes keep their original rule: they wait 10s for a flush, and any transport error is not sent.

Observed live on 2026-11-16: during a slowdown that Sentry's status page did not report, both streams' p95 rose from ~71s to 2m42s, in-flight traces grew from ~60 to ~150, and one trace's root was stored twice on each stream. Nothing was lost: all 200 traces per stream arrived, complete, and the lookups tolerate duplicate rows. The duplicates' cause is not established: Sentry storing a root twice, or net/http retrying a request on the shared HTTP/2 connection, which the probe now logs and counts.

### The OTLP stream

The OTLP stream (`otlp.go`) sends the same probe trace (a `probe.login` root with 4 child spans, `probe_batch` on the root) built with the OpenTelemetry Go SDK and exported over OTLP/HTTP (protobuf) straight to Sentry's OTLP traces endpoint. That endpoint is a separate ingest path from the SDK's envelope endpoint, and Sentry documents it as **open beta**. Everything after the send is shared with the SDK stream: the same trace-id lookups, the same child-span count, the same timeout, the same metrics under its own name.

It needs no extra configuration. The endpoint and key come from `SENTRY_DSN`: `https://<host>/api/<project>/integration/otlp/v1/traces`, authenticated with `x-sentry-auth: sentry sentry_key=<public key>`. Verified against a live project on 2026-11-16: the root is stored with `is_transaction:true` and the 4 children with `is_transaction:false`, exactly as for the SDK path, and became queryable in 51s.

It counts sends by the same rule as the SDK stream (the table in [Metrics emitted](#metrics-emitted)), checked per trace under the exporter's HTTP client. The exporter's retries are off, as the Sentry SDK has none, so each request's outcome is final. Each trace's 5 spans are exported together in their own request, directly rather than through a batch span processor (see [When Sentry is slow](#when-sentry-is-slow) for why), and the trace counts as sent only if all 5 were accepted. The sampler is pinned so `OTEL_TRACES_SAMPLER` in the environment cannot change what is sent.

It runs on a private OpenTelemetry provider and is unrelated to the Datadog OTLP exporter in `tracer.go`.

**Volume.** Each stream is one trace per second: 86,400 transactions (432,000 spans) a day, so both together double that, on top of the batch probes. Set `TRACE_STREAM_ENABLED=false` or `TRACE_STREAM_OTLP_ENABLED=false` to switch either off.

`scripts/setup_datadog_slos.sh` does not yet create monitors or SLOs for either stream.

## Quick start

Requires Go 1.25.6, a Sentry auth token with `event:read` + `project:read` + `org:read`, and a Datadog API key.

**Your Sentry org's transaction data must live in the spans (EAP) dataset.** The trace and span probes query `dataset=spans`; Sentry migrated transaction data there, and for a migrated org the older `transactions` dataset returns zero rows for every query at every stats period.

If your org does not match that assumption, the failure is silent and looks nothing like a failure: transactions send successfully, every query comes back empty, and both trace probes report `received=0/N` with a **0% success rate, no send errors and no query errors**. The error probe reads `dataset=errors` and keeps working throughout, so the recognisable signature is one healthy probe alongside two flatlined at zero with clean logs. That is exactly how this was found; if you see it, check which dataset holds your transactions before looking anywhere else.

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
Starting SLO probes (run=dktnmz0ub263, interval=2m0s, batch=100, send_window=1m40s, poll_timeout=2m0s, max_inflight=2, trace_stream=true)
[trace_stream] starting (send every 1s, poll every 2s, report every 1m0s, lost after 5m0s, census 1m0s after arrival, probe_batch=dktnmz0ub263-trace_stream)
[trace_stream_otlp] starting (send every 1s, poll every 2s, report every 1m0s, lost after 5m0s, census 1m0s after arrival, probe_batch=dktnmz0ub263-trace_stream_otlp)
[trace_ingestion] starting (interval=2m0s, batch=100)
[error_ingestion] starting (interval=2m0s, batch=100)
[span_completeness] starting (interval=2m0s, batch=100)
[trace_ingestion] batch=dktnmz0ub263-trace_ingestion-1 received=100/100 p50=58.4s p95=1m7.2s
[error_ingestion] batch=dktnmz0ub263-error_ingestion-1 received=99/100 p50=11.9s p95=16.8s
[span_completeness] batch=dktnmz0ub263-span_completeness-1 received=100/100 received_pct=100%
```

Those latencies are illustrative, but their *shape* is real: traces take roughly 5x as long as errors to become queryable. The measured baseline is in [Project status](#project-status).

The `run=` component is regenerated on every start and prefixes every batch id, so a restart cannot re-query the previous process's events. `received_pct=unknown` means the completeness sample could not be taken this cycle; the metric is suppressed rather than reported as `0%`.

`received=0/N` on the two trace probes, with no errors of any kind, is the signature of transaction data not being in the spans dataset — see the note in [Quick start](#quick-start).

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
| `PROBE_BATCH_SIZE` | `100` | Events sent per probe per cycle. **100 is also the maximum**; a larger value is rejected at startup |
| `PROBE_SEND_WINDOW_SECONDS` | `100` | Batch sends are paced evenly across this window rather than fired at once |
| `SPAN_COMPLETENESS_SAMPLE` | `20` | How many arrived transactions go into the single span-count query |
| `TRACE_STREAM_ENABLED` | `true` | Run the one-trace-per-second Sentry SDK stream. Must be `true` or `false`; anything else is rejected at startup, so a typo cannot leave it running |
| `TRACE_STREAM_OTLP_ENABLED` | `true` | Run the same stream over OpenTelemetry/OTLP. Same rules as `TRACE_STREAM_ENABLED`. Poll interval and timeout below apply to both streams |
| `TRACE_STREAM_POLL_SECONDS` | `2` | How often the stream asks Sentry about in-flight traces, and so its latency resolution |
| `TRACE_STREAM_TIMEOUT_SECONDS` | `300` | How long a stream trace may take before it counts as lost. Keep it well above your latency alert threshold. Maximum 1800, because the stream's lookups search the last hour |

The two `TRACE_STREAM_*_SECONDS` settings must be whole numbers of seconds, at least 1. Anything else is rejected at startup rather than silently replaced by the default.

**`PROBE_BATCH_SIZE` cannot exceed 100.** Each batch is looked up with a single unpaginated Discover request, and Sentry documents `per_page` as "Default and maximum allowed is 100" — a larger batch would send events the probe then cannot see, and report them as dropped by Sentry. The process refuses to start rather than measure something it cannot measure, and `findBatch` clamps `per_page` at 100 defensively as well.

A batch is bounded at `(PROBE_BATCH_SIZE - 1) / PROBE_BATCH_SIZE × PROBE_SEND_WINDOW_SECONDS + POLL_TIMEOUT_SECONDS + POLL_INTERVAL_SECONDS` (224s at defaults), which is longer than the 120s interval, so two batches per probe overlap in normal operation. That is expected. The first term is the *last scheduled send*, not the end of the send window: sends are paced at `window / size`, so the final one leaves one step before the window closes and its drain budget is measured from there. At `PROBE_BATCH_SIZE=1` that collapses to `POLL_TIMEOUT_SECONDS + POLL_INTERVAL_SECONDS`, matching what a single-event probe would take.

A `skipping cycle` log line is not. It means a cycle outlived twice the interval (240s at defaults), which is past that ceiling. The send path is not what gets you there — its sends are paced across the window and bounded by a 10s flush timeout each. The work that can is what runs *after* the batch drains but still inside the same in-flight slot: the span probe's completeness census, exactly two Sentry Discover calls at a 10s timeout apiece (~20s, and flat in `SPAN_COMPLETENESS_SAMPLE`), and the six or seven Datadog submissions per signal, also 10s apiece (~60s). So a skip points at slow Datadog submission or an unusually slow Sentry Discover query, not at a slow send. Post-batch work no longer scales with the sample size, so skips should be rare.

Note that the probe writes real events into the target Sentry project and consumes quota. Point it at a dedicated project if that matters to you.

## Creating the Datadog SLOs

`scripts/setup_datadog_slos.sh` creates monitors plus SLOs over the metrics above. It needs a Datadog **application** key in addition to the API key, and `jq`:

```bash
export DD_API_KEY=... DD_APP_KEY=... SENTRY_ORG=... SENTRY_PROJECT=...
./scripts/setup_datadog_slos.sh
```

It is not idempotent — re-running creates duplicate monitors.

The latency thresholds in it are set against the measured baseline below rather than guessed: trace p95 alerts above 120s (~2x the observed 66s), error p95 above 90s (~5.5x the observed 16s, left deliberately loose pending more data). Each carries a comment in the script recording the observation it was set from. Re-check both against a few days of your own org's data — and read those comments first, because the trace and error thresholds are not comparable to each other.

**Six SLOs are created, of two different types, and both types are deliberate.**

- The three **latency and span-completeness** SLOs are **monitor-based**: a metric monitor with a threshold, plus an SLO measuring what fraction of the 30-day window that monitor was not alerting. Those signals genuinely are gauges — p95 latency and `received_pct` are levels, not tallies — so there is no good/total ratio to divide, and "how much of the month was this level acceptable?" is the only question available.
- The three **reliability** SLOs are **metric-based**: `sum(sentry.<signal>.received) / sum(sentry.<signal>.sent)`. Those two are real counts of events the batch probe tallies, so a good-events-over-total-events ratio is exactly what a metric-based SLO is for, and it measures every event rather than sampling whether a threshold monitor happened to be red.

Neither type is a leftover from the other. No SLO is backed by a composite.

Metric-based reliability gets the `sent > 0` gate structurally: a cycle that sent nothing contributes `0` to both numerator and denominator, so our own send failure cannot burn Sentry's error budget. Do not "simplify" those three into threshold SLOs over `success_rate` — `success_rate` is published as `0` when nothing sent, so such an SLO would record a full breach against Sentry for a local outage.

Reliability **paging** is a separate path, and that is where the composites live. Each signal gets a **composite** of two monitors: `success_rate` below threshold, AND `sent > 0`. They alert; they back no SLO. The gate is still not optional here, for the same reason — an ungated `success_rate` monitor pages against Sentry for our own send failure. The gate monitor sits in ALERT during normal operation by design; do not page on it, and do not fold the pair back into one monitor.

## Project status

**Verified end to end against live Sentry and Datadog on 2026-08-20.** All three probes send, all three come back queryable, and all three publish to Datadog. Two clean cycles at `PROBE_BATCH_SIZE=10`:

| Probe | Received | p50 | p95 |
|---|---|---|---|
| `error_ingestion` | 10/10 | 11.204s | 16.261s |
| `trace_ingestion` | 10/10 | 57.122s | 66.129s |
| `span_completeness` | 10/10 | — | `received_pct=100%` |

Zero send errors, zero query errors, zero Datadog post errors, zero skipped cycles. The batch path is confirmed working: paced sends, batched polling, percentiles and success rates all arrive in Datadog.

**Those p95 figures are not really p95s.** Percentiles are nearest-rank, so at a batch size of 10 both p95 and p99 resolve to `ceil(0.95 × 10) = ceil(0.99 × 10) = 10` — index 9, the largest of the ten samples. Each number above is the **maximum observed latency**, and p95 and p99 were identical. At the default `PROBE_BATCH_SIZE=100` p95 becomes a genuine 95th percentile and will read **lower** than these figures for the same underlying distribution, so do not read that drop as Sentry getting faster. The direction is safe for the thresholds derived from them — a threshold set off a maximum is conservative.

Use those numbers as a **reference baseline, not a guarantee**. Two cycles at batch size 10 is a first measurement, not a soak test — it says nothing about the tail beyond the maximum, about behaviour at the default batch size of 100, or about how any of this drifts over days.

**The trace streams were verified live against Sentry on 2026-11-16, with Datadog posting switched off.** Several 200-second runs with both streams side by side: every trace sent reached Sentry (200 of 200 per stream, per run, counted independently in Sentry), every one with all 4 child spans, and the probe's own arrived/complete counts matched. Healthy latency was about 50s at p50 and 71–76s at p95 on both ingest paths. One run caught an undeclared Sentry slowdown (p95 2m42s) in which nothing was lost. **Not yet verified:** the streams' metrics arriving in Datadog (the wire format is unit tested), and the Docker image itself.

The batch path **is** wired into `main.go` — the legacy single-event path and its alert-webhook probe have been deleted, so no single-event sender remains in the tree. That is what turns the latency signal into real p50/p95/p99 percentiles plus a success rate rather than a single sample.

The batch rewrite is merged to `main`. If you have dashboards or monitors built against the pre-batch metric names, they need updating: the names changed, and Datadog will show them as permanently no-data rather than erroring. The design and task plan live in `docs/`.

## Layout

```
main.go          config + the three batch probe loops, starts the trace stream
stream.go        trace streams: one trace a second, polled by trace id, child spans counted, reported per minute
otlp.go          the OTLP stream's sender: OpenTelemetry SDK → Sentry's OTLP endpoint
probe.go         trace ingestion batch probe, Sentry send helpers
delivery.go      confirms each send was accepted by Sentry (HTTP 2xx), under the SDK transport
probe_error.go   error ingestion batch probe
probe_spans.go   span completeness batch probe
sentry_api.go    Sentry Discover queries (spans + errors datasets)
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

The pure logic — percentiles, send pacing, latency tracking, batch stop conditions, batch ids, the in-flight cap, the span-completeness suppression rule, the trace stream's arrived/lost/dropped rules, its child-span count and its run loop, the OTLP sender against a fake OTLP endpoint (one request per trace, `partial_success`, no queueing behind a slow Sentry), the unanswered-request rule against a scripted connection, send confirmation against a fake Sentry ingest endpoint (rejected, refused and rate-limited sends), and the Sentry/Datadog request shapes — is unit tested against `httptest` servers. The probe functions, `runBatchLoop`, `runStreamProbe`, `runTraceStreamProbe` and `runOTLPStreamProbe` are network and process glue, marked as untested in the source, and verified by running against live Sentry and Datadog.
