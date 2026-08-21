#!/usr/bin/env bash
#
# setup_datadog_slos.sh
#
# Creates the Datadog monitors + SLOs that track Sentry's ingestion
# performance, using the metrics emitted by this probe.
#
# The probe sends a paced *batch* of synthetic events per cycle, so for each
# signal in {ingestion, error_ingestion, span_completeness} it emits:
#
#   sentry.<signal>.latency_ms.p50        (batch latency percentiles, ms)
#   sentry.<signal>.latency_ms.p95
#   sentry.<signal>.latency_ms.p99
#   sentry.<signal>.sent                  (events that left the process)
#   sentry.<signal>.received              (events that came back queryable)
#   sentry.<signal>.success_rate          (received/sent as a %; 0 when sent==0)
#
# and, from the span probe only:
#
#   sentry.span_completeness.received_pct (% of a SAMPLE of arrived
#                                          transactions that had all their
#                                          spans — see the monitor below; this
#                                          is NOT spans stored / spans sent)
#
# The trace signal is named "ingestion", not "trace_ingestion", so its metrics
# are sentry.ingestion.* and its tag is probe:ingestion. The other two tag
# themselves probe:error_ingestion and probe:span_completeness.
#
# p95 is the alerting series for latency; p50 and p99 are emitted too and are
# worth graphing. Alerting on p50 hides the tail, and p99 of a 100-event batch
# is one event.
#
# THERE IS NO sentry.ingestion.error METRIC ANY MORE, and no successor. The old
# per-cycle error counter went away with the single-event probe. Failures now
# surface two ways: as a gap between sentry.<signal>.sent and PROBE_BATCH_SIZE
# (our own send path failed), and as a low sentry.<signal>.success_rate (we
# sent, Sentry did not give them back). The reliability paging monitors below
# watch the second, gated on the first; the reliability SLOs get the same
# separation for free by dividing received by sent.
#
# Datadog does NOT roll a parent metric name up over its children: a query on
# sentry.ingestion.latency_ms matches nothing now that the series is
# sentry.ingestion.latency_ms.p95. It does not error either — it just goes
# permanently no-data, which looks exactly like a healthy monitor. Any dashboard
# or monitor still on the pre-batch names needs the same treatment this script
# just got.
#
# TWO KINDS OF SLO LIVE IN THIS FILE, AND BOTH ARE DELIBERATE.
#
#   * Latency and span completeness are *monitor-based*: a metric monitor with
#     a threshold, plus an SLO tracking "% of time that monitor was NOT
#     alerting" over 30d. Those signals genuinely are gauges — p95 latency and
#     received_pct are levels, not tallies. There is no meaningful
#     good-events/total-events ratio to divide, so "how much of the month was
#     this level acceptable?" is the only question that can be asked, and
#     monitor-based is the SLO type that asks it.
#
#   * Reliability is *metric-based*: numerator sentry.<signal>.received over
#     denominator sentry.<signal>.sent. These two ARE tallies — the batch probe
#     counts the events it got out the door and the events that came back
#     (datadog.go, postBatchMetrics) — so the ratio is a real good/total event
#     ratio and metric-based measures it directly, event by event, instead of
#     sampling whether a threshold monitor happened to be red.
#
# An earlier version of this header claimed monitor-based was right for
# everything because "a metric-based SLO wants count numerator/denominator,
# which these gauges don't provide cleanly". That was true of the OLD
# single-event probe, which emitted only latency gauges. The batch rewrite made
# it false. Do not restore that reasoning: the two kinds coexist here on
# purpose, and neither is a leftover.
#
# PREREQUISITES
#   export DD_API_KEY=...    # same key the probe uses
#   export DD_APP_KEY=...    # Datadog *application* key — writes need this;
#                            # the API key alone is NOT sufficient.
#   export DD_SITE=datadoghq.com   # optional, defaults to datadoghq.com
#   jq must be installed.
#   The probe should already be running so these metrics exist in Datadog.
#
# NOTE: This does NOT upsert — re-running creates duplicates. Delete the old
# monitors/SLOs first if you need to re-run.
#
# THE LATENCY THRESHOLDS BELOW ARE NO LONGER GUESSES. They are set against a
# live baseline measured on 2026-08-20 — two clean cycles at
# PROBE_BATCH_SIZE=10, 10/10 received on every probe, zero send errors, zero
# query errors, zero Datadog post errors, zero skips:
#
#   error_ingestion      p50 11.204s   p95 16.261s
#   ingestion (trace)    p50 57.122s   p95 66.129s
#   span_completeness    received_pct 100%
#
# Each latency threshold below carries a comment saying how it stands against
# that baseline and whether it is meant to be tuned. Two cycles is a baseline,
# not a soak test — read those comments before changing a number, and re-check
# all of them against a few days of your own org's data.

set -euo pipefail

: "${DD_API_KEY:?set DD_API_KEY}"
: "${DD_APP_KEY:?set DD_APP_KEY (Datadog application key — SLO/monitor writes require it)}"
DD_SITE="${DD_SITE:-datadoghq.com}"
API="https://api.${DD_SITE}/api/v1"

command -v jq >/dev/null || { echo "jq is required"; exit 1; }

# Scope every query to this probe's tags. Change if your org/project differ.
SCOPE="sentry_org:${SENTRY_ORG:?set SENTRY_ORG},sentry_project:${SENTRY_PROJECT:?set SENTRY_PROJECT}"
TAGS='["service:sentry-slo-probe","source:sentry-ingestion-probe"]'

api_post() { # $1 = path, $2 = json body
  curl -sS -X POST "${API}${1}" \
    -H "DD-API-KEY: ${DD_API_KEY}" \
    -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
    -H "Content-Type: application/json" \
    -d "$2"
}

create_monitor() { # $1 = json body -> prints monitor id
  local resp id
  resp=$(api_post "/monitor" "$1")
  id=$(echo "$resp" | jq -r '.id // empty')
  if [[ -z "$id" ]]; then
    echo "  ERROR creating monitor: $resp" >&2
    exit 1
  fi
  echo "$id"
}

create_slo() { # $1 = json body
  local resp id
  resp=$(api_post "/slo" "$1")
  id=$(echo "$resp" | jq -r '.data[0].id // empty')
  if [[ -z "$id" ]]; then
    echo "  ERROR creating SLO: $resp" >&2
    exit 1
  fi
  echo "$id"
}

echo "==> Creating monitors on api.${DD_SITE}"

# Latency: p95 is the alerting series. The tag is probe:ingestion — the trace
# signal is named "ingestion" in the code, so probe:trace_ingestion matches
# nothing and would leave this monitor silently no-data.
#
# 120000 is roughly 2x the observed p95. Measured baseline 2026-08-20, two
# clean 10/10 cycles at PROBE_BATCH_SIZE=10: p50 57.122s, p95 66.129s.
#
# DO NOT COMPARE THIS THRESHOLD TO THE ERROR ONE BELOW. Transactions become
# queryable in the spans dataset far more slowly than errors do — about 4x at
# p95 (66.1s vs 16.3s) and 5x at p50 (57.1s vs 11.2s) — so they measure different
# physics and the gap between them is not slack to be tidied away. This monitor
# was previously 60000, i.e. *below* the healthy observed p95, so it would have
# sat in ALERT continuously against a fully working system.
M_TRACE=$(create_monitor '{
  "name": "Sentry trace ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.ingestion.latency_ms.p95{'"${SCOPE}"',probe:ingestion} > 120000",
  "message": "p95 of a synthetic trace batch took > 120s to become queryable in Sentry. Healthy observed p95 is ~66s (baseline measured 2026-08-20 at batch size 10); traces are inherently slower to become queryable than errors.",
  "tags": ["service:sentry-slo-probe","probe:ingestion"],
  "options": {"thresholds": {"critical": 120000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  trace-ingestion-latency monitor:   ${M_TRACE}"

# 90000 is DELIBERATELY LOOSE AND IS DELIBERATELY LEFT LOOSE, pending more data.
# Measured baseline 2026-08-20, two clean 10/10 cycles at PROBE_BATCH_SIZE=10:
# p50 11.204s, p95 16.261s — so this threshold carries roughly 5.5x slack over
# the observed p95.
#
# That slack is not an oversight and it was not tightened when the baseline
# landed. Two cycles is one observation of the tail, and tightening a threshold
# that is not firing toward a single observation manufactures false alerts,
# whereas raising a demonstrably-broken threshold (see the trace monitor above,
# which sat below its own healthy p95) cannot. The numbers are recorded here so
# that whoever tunes this against a few days of real data starts from a measured
# value rather than a guess.
M_ERROR=$(create_monitor '{
  "name": "Sentry error ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.error_ingestion.latency_ms.p95{'"${SCOPE}"',probe:error_ingestion} > 90000",
  "message": "p95 of a synthetic error batch took > 90s to appear in Sentry Issues.",
  "tags": ["service:sentry-slo-probe","probe:error_ingestion"],
  "options": {"thresholds": {"critical": 90000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  error-ingestion-latency monitor:   ${M_ERROR}"

# received_pct is unchanged by the batch rewrite — still emitted under the same
# name, so this query is correct as written.
#
# READ WHAT THE METRIC IS BEFORE TUNING THIS THRESHOLD. Despite the name, it is
# NOT spans stored over spans sent. probe_spans.go computes complete / sample *
# 100 over a SAMPLE of at most SPAN_COMPLETENESS_SAMPLE (20) of the transactions
# that arrived, where a transaction counts as complete only if Sentry stored ALL
# 5 of its child spans. It is all-or-nothing per transaction: 100 transactions
# each losing one span out of five read 0%, not 80%.
#
# Two consequences. "< 100" is the right threshold here — any sampled
# transaction short of its full span count trips it — and a *lower* threshold
# would mean "tolerate N% of transactions losing spans", not "tolerate N% span
# loss". And because it is a sample rather than a census, a single sampled
# transaction is worth 1/sample of the reading (5 points at the default 20), so
# the series is coarse by construction.
#
# The census behind it is ONE grouped count() query over the spans dataset for
# the whole sample, not a fetch per transaction, so there is no per-transaction
# failure left to shrink the sample. Either the query succeeds — in which case a
# sampled transaction absent from the results genuinely stored no child spans
# and scores as incomplete — or it fails outright and the cycle publishes
# nothing rather than 0.
#
# The metric name is fixed and must not change; the meaning is the one above.
M_SPANS=$(create_monitor '{
  "name": "Sentry span completeness (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.span_completeness.received_pct{'"${SCOPE}"',probe:span_completeness} < 100",
  "message": "At least one sampled probe transaction arrived in Sentry missing some of its 5 child spans (span drop). This is a sample of up to SPAN_COMPLETENESS_SAMPLE arrived transactions per cycle, scored all-or-nothing per transaction — see scripts/setup_datadog_slos.sh.",
  "tags": ["service:sentry-slo-probe","probe:span_completeness"],
  "options": {"thresholds": {"critical": 100}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  span-completeness monitor:         ${M_SPANS}"

# --- Reliability PAGING monitors: success_rate, gated on sent > 0 -------------
#
# These replace the old sentry.ingestion.error monitor, whose metric no longer
# exists.
#
# COMPOSITES PAGE, METRIC-BASED SLOs MEASURE. Read that before changing either.
# The composites built here are the *alerting* path: they are what wakes someone
# up when Sentry starts dropping events. They do NOT back the reliability SLOs
# any more — those are metric-based over received/sent, further down. The two
# express the same intent by different means on purpose: an alert has to make a
# yes/no call in a 10-minute window, while an SLO integrates every event over
# 30 days.
#
# WHY A COMPOSITE AND NOT A BARE success_rate MONITOR. Do not "simplify" this.
#
# success_rate is deliberately published as 0 when sent == 0 (received/sent is
# NaN there, and a metric that never arrives looks identical to a healthy one on
# a dashboard, so the probe posts an explicit 0 instead). But sent == 0 means
# *our* send path failed — bad DSN, no egress, the SDK refusing to flush. A bare
# "success_rate < N" monitor cannot tell that apart from Sentry dropping every
# event, so it would page against Sentry for our own outage. That is the single
# worst failure mode an SLO tool has: a confident false accusation.
#
# So each signal gets two metric monitors and one composite over them:
#
#   A  success_rate < threshold   "they did not come back"
#   B  sent > 0                   "we actually sent some"
#   C  composite, A && B          alert only when both hold
#
# Monitor B sits in ALERT state throughout normal healthy operation, by design.
# It is a gate, not a page: it carries no notification handles, and nothing
# should ever be routed off it directly. Only C is paged on.
#
# A and B are referenced only by their composite now that the SLOs no longer
# consume them. That is fine and they must not be deleted: C is defined in terms
# of their ids, so removing either breaks the composite.
#
# A probe that is not running at all leaves sent at no-data -> default_zero -> 0,
# so B is not alerting and C stays quiet. Deliberate: "the probe is down" is a
# different alert from "Sentry is dropping our events", and the latency
# monitors' notify_no_data already covers the first.
#
# create_reliability_monitor <metric signal> <probe tag> <threshold %> <label>
# Prints the composite monitor id on stdout; progress goes to stderr, because
# the caller captures stdout.
#
# The returned id is for routing notifications, not for an SLO. Nothing below
# passes it to create_slo.
create_reliability_monitor() {
  local signal="$1" probe_tag="$2" threshold="$3" label="$4"
  local m_rate m_sent m_comp

  # last_10m, not last_5m: one batch publishes one point roughly every
  # PROBE_INTERVAL_SECONDS (120s by default), and a window that spans several
  # batches keeps one unlucky batch from tripping the SLO.
  m_rate=$(create_monitor '{
  "name": "Sentry '"${label}"' success rate (alert part A of composite)",
  "type": "metric alert",
  "query": "avg(last_10m):avg:sentry.'"${signal}"'.success_rate{'"${SCOPE}"',probe:'"${probe_tag}"'} < '"${threshold}"'",
  "message": "Fewer than '"${threshold}"'% of the synthetic '"${label}"' events sent became queryable in Sentry. Gated by the sent>0 monitor through a composite — see scripts/setup_datadog_slos.sh.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'","slo-part:success-rate"],
  "options": {"thresholds": {"critical": '"${threshold}"'}, "notify_no_data": false, "renotify_interval": 0}
}')

  m_sent=$(create_monitor '{
  "name": "Sentry '"${label}"' sent>0 gate (alert part B of composite — DO NOT PAGE)",
  "type": "metric alert",
  "query": "avg(last_10m):default_zero(avg:sentry.'"${signal}"'.sent{'"${SCOPE}"',probe:'"${probe_tag}"'}) > 0",
  "message": "Gate only. ALERTING here is the NORMAL, HEALTHY state: it means the probe is successfully sending '"${label}"' events. Consumed by a composite monitor so a local send failure cannot be reported as a Sentry breach. Do not route notifications off this monitor and do not delete it without deleting its composite.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'","slo-part:sent-gate"],
  "options": {"thresholds": {"critical": 0}, "notify_no_data": false, "renotify_interval": 0}
}')

  m_comp=$(create_monitor '{
  "name": "Sentry '"${label}"' reliability (paging)",
  "type": "composite",
  "query": "'"${m_rate}"' && '"${m_sent}"'",
  "message": "Sentry returned fewer than '"${threshold}"'% of the synthetic '"${label}"' events we successfully sent. The sent>0 gate is satisfied, so this is Sentry dropping events, not the probe failing to send them. This monitor pages; the corresponding SLO is metric-based over received/sent and is measured independently of this alert.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'"],
  "options": {"renotify_interval": 0}
}')

  echo "  ${label} success-rate (A):  ${m_rate}" >&2
  echo "  ${label} sent>0 gate  (B):  ${m_sent}" >&2
  echo "  ${label} reliability  (C):  ${m_comp}" >&2
  echo "${m_comp}"
}

# Signal name vs probe tag: the trace signal is "ingestion" in the code, so its
# metrics are sentry.ingestion.* and its tag is probe:ingestion. It is NOT
# probe:trace_ingestion — that value is emitted nowhere and would leave these
# monitors silently no-data. The other two signals tag themselves with their own
# names. Verified against datadog.go (postBatchMetrics tags "probe:"+signal).
M_PAGE_TRACE=$(create_reliability_monitor "ingestion" "ingestion" 99 "trace ingestion")
M_PAGE_ERROR=$(create_reliability_monitor "error_ingestion" "error_ingestion" 99 "error ingestion")
M_PAGE_SPANS=$(create_reliability_monitor "span_completeness" "span_completeness" 99 "span-probe transaction")

echo "  reliability paging composites:     ${M_PAGE_TRACE}, ${M_PAGE_ERROR}, ${M_PAGE_SPANS}"
echo "     (route notifications here; the reliability SLOs below do not use them)"

echo "==> Creating SLOs (30-day rolling window)"

S1=$(create_slo '{
  "type": "monitor",
  "name": "Sentry trace ingestion latency",
  "description": "99% of the time, the p95 of a synthetic trace batch is queryable in Sentry within 120s. This is a batch percentile, not a per-event guarantee: the slowest 5% of a batch can exceed 120s without the monitor firing. 120s is ~2x the observed healthy p95 of 66s (baseline 2026-08-20); traces become queryable far more slowly than errors, so this target is not comparable to the error latency SLO.",
  "monitor_ids": ['"${M_TRACE}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO trace ingestion latency:       ${S1}"

S2=$(create_slo '{
  "type": "monitor",
  "name": "Sentry error ingestion latency",
  "description": "99% of the time, the p95 of a synthetic error batch appears in Sentry Issues within 90s. This is a batch percentile, not a per-event guarantee: the slowest 5% of a batch can exceed 90s without the monitor firing.",
  "monitor_ids": ['"${M_ERROR}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO error ingestion latency:       ${S2}"

S3=$(create_slo '{
  "type": "monitor",
  "name": "Sentry span completeness",
  "description": "99.5% of the time, every sampled probe transaction arrived in Sentry with all 5 of its child spans (no span drop). Measured over a sample of up to SPAN_COMPLETENESS_SAMPLE arrived transactions per cycle, scored all-or-nothing per transaction — not as a ratio of spans stored to spans sent.",
  "monitor_ids": ['"${M_SPANS}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.5, "warning": 99.9}],
  "tags": '"${TAGS}"'
}')
echo "  SLO span completeness:             ${S3}"

# --- Reliability SLOs: metric-based over received / sent ----------------------
#
# THE sent>0 GATE IS IMPLICIT AND STRUCTURAL HERE. THIS IS THE WHOLE POINT.
# Do not "simplify" these into threshold SLOs over sentry.<signal>.success_rate.
#
# A metric-based SLO is sum(good events) / sum(total events) across the whole
# 30-day window. When a batch fails to send, that cycle publishes sent=0 and
# received=0, so it contributes 0 to the numerator and 0 to the denominator. It
# cannot move the ratio. Our own send failure is arithmetically incapable of
# burning Sentry's error budget — not because we bolted a gate on, but because
# there is nothing to divide. Zero sends means zero opportunity for Sentry to
# fail us, and the SLO says so by construction.
#
# Contrast the thing not to build. success_rate is published as an explicit 0
# when sent == 0 (see the paging section above for why). An SLO thresholding
# "success_rate >= 99" would read that 0 as a total ingestion failure and record
# a full breach against Sentry for our own outage. Same false accusation the
# composite gate exists to prevent, silently reintroduced. The metric-based form
# below is immune to it for free, so keep it.
#
# It also measures the right thing. The retired monitor-based version asked "for
# what fraction of the month was the 10-minute success rate above 99%?" — a
# sample of a threshold, where one bad 10-minute window costs the same whether
# it dropped one event or every event. This asks "of every synthetic event we
# actually sent this month, what fraction did Sentry give back?" That is the
# real SLI, and it is why the targets changed shape: 99.0 here is a ratio of
# events, NOT the old 99.9 ratio of good minutes. The two numbers are not
# comparable and 99.9 must not be "restored" here.
#
# Targets match the paging composites' 99% threshold on purpose, so the alert
# and the SLO encode one intent rather than drifting apart.
#
# ONE THING TO CHECK ON THE FIRST REAL RUN. Datadog documents metric-based SLOs
# as supporting COUNT, RATE and percentile-enabled DISTRIBUTION metrics. The
# probe submits every metric as a gauge ("type": 3 in datadog.go's postMetric),
# including .sent and .received, which are semantically counts but are not
# typed as such. If Datadog rejects these three /slo calls, or accepts them and
# the resulting SLI looks wrong, the fix is in the probe rather than here:
# submit .sent and .received with the count type and leave these queries alone.
# create_slo prints the API error and exits non-zero, so a rejection is loud
# rather than a half-configured SLO.
S4=$(create_slo '{
  "type": "metric",
  "name": "Sentry trace ingestion reliability",
  "description": "Of the synthetic traces the probe successfully sent, 99% become queryable in Sentry. Cycles that sent nothing are absent from both numerator and denominator, so a local send failure cannot breach this SLO.",
  "query": {
    "numerator": "sum:sentry.ingestion.received{'"${SCOPE}"',probe:ingestion}",
    "denominator": "sum:sentry.ingestion.sent{'"${SCOPE}"',probe:ingestion}"
  },
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO trace ingestion reliability:   ${S4}"

S5=$(create_slo '{
  "type": "metric",
  "name": "Sentry error ingestion reliability",
  "description": "Of the synthetic errors the probe successfully sent, 99% become queryable in Sentry. Cycles that sent nothing are absent from both numerator and denominator, so a local send failure cannot breach this SLO.",
  "query": {
    "numerator": "sum:sentry.error_ingestion.received{'"${SCOPE}"',probe:error_ingestion}",
    "denominator": "sum:sentry.error_ingestion.sent{'"${SCOPE}"',probe:error_ingestion}"
  },
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO error ingestion reliability:   ${S5}"

# Distinct from the span completeness SLO above: this counts whole span-probe
# transactions arriving, that one asks whether the transactions that did arrive
# brought all their spans with them.
S6=$(create_slo '{
  "type": "metric",
  "name": "Sentry span-probe transaction reliability",
  "description": "Of the span-probe transactions the probe successfully sent, 99% become queryable in Sentry. Distinct from span completeness: this counts whole transactions arriving, that one asks whether the transactions that did arrive brought all their spans. Cycles that sent nothing are absent from both numerator and denominator, so a local send failure cannot breach this SLO.",
  "query": {
    "numerator": "sum:sentry.span_completeness.received{'"${SCOPE}"',probe:span_completeness}",
    "denominator": "sum:sentry.span_completeness.sent{'"${SCOPE}"',probe:span_completeness}"
  },
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO span-probe reliability:        ${S6}"

echo "==> Done. View them at https://app.${DD_SITE}/slo"
