#!/usr/bin/env bash
#
# setup_datadog_slos.sh
#
# Creates the Datadog monitors + monitor-based SLOs that track Sentry's
# ingestion performance, using the metrics emitted by this probe.
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
#   sentry.span_completeness.received_pct (% of sent spans that arrived)
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
# sent, Sentry did not give them back). The reliability monitors below watch the
# second, gated on the first.
#
# Datadog does NOT roll a parent metric name up over its children: a query on
# sentry.ingestion.latency_ms matches nothing now that the series is
# sentry.ingestion.latency_ms.p95. It does not error either — it just goes
# permanently no-data, which looks exactly like a healthy monitor. Any dashboard
# or monitor still on the pre-batch names needs the same treatment this script
# just got.
#
# Each SLO is *monitor-based*: we create a metric monitor with a threshold,
# then an SLO that tracks "% of time the monitor was NOT alerting" over 30d.
# This is the right SLO type for gauge metrics (a metric-based SLO wants
# count numerator/denominator, which these gauges don't provide cleanly).
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
# monitors/SLOs first if you need to re-run. Thresholds/targets below are
# sensible starting points; tune them to your real ingestion behavior.

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
M_TRACE=$(create_monitor '{
  "name": "Sentry trace ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.ingestion.latency_ms.p95{'"${SCOPE}"',probe:ingestion} > 60000",
  "message": "p95 of a synthetic trace batch took > 60s to become queryable in Sentry.",
  "tags": ["service:sentry-slo-probe","probe:ingestion"],
  "options": {"thresholds": {"critical": 60000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  trace-ingestion-latency monitor:   ${M_TRACE}"

M_ERROR=$(create_monitor '{
  "name": "Sentry error ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.error_ingestion.latency_ms.p95{'"${SCOPE}"',probe:error_ingestion} > 90000",
  "message": "p95 of a synthetic error batch took > 90s to appear in Sentry Issues.",
  "tags": ["service:sentry-slo-probe","probe:error_ingestion"],
  "options": {"thresholds": {"critical": 90000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  error-ingestion-latency monitor:   ${M_ERROR}"

# received_pct is unchanged by the batch rewrite — still emitted, still a
# percentage of spans stored over spans sent. This query is correct as written.
M_SPANS=$(create_monitor '{
  "name": "Sentry span completeness (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.span_completeness.received_pct{'"${SCOPE}"',probe:span_completeness} < 100",
  "message": "Sentry ingested fewer spans than were sent (span drop).",
  "tags": ["service:sentry-slo-probe","probe:span_completeness"],
  "options": {"thresholds": {"critical": 100}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  span-completeness monitor:         ${M_SPANS}"

# --- Reliability: success_rate, gated on sent > 0 -----------------------------
#
# This replaces the old sentry.ingestion.error monitor, whose metric no longer
# exists.
#
# WHY A COMPOSITE. Do not "simplify" this into a single success_rate monitor.
#
# success_rate is deliberately published as 0 when sent == 0 (received/sent is
# NaN there, and a metric that never arrives looks identical to a healthy one on
# a dashboard, so the probe posts an explicit 0 instead). But sent == 0 means
# *our* send path failed — bad DSN, no egress, the SDK refusing to flush. A bare
# "success_rate < N" monitor cannot tell that apart from Sentry dropping every
# event, so it would fire a full SLO breach against Sentry for our own outage.
# That is the single worst failure mode an SLO tool has: a confident false
# accusation.
#
# So each signal gets two metric monitors and one composite over them:
#
#   A  success_rate < threshold   "they did not come back"
#   B  sent > 0                   "we actually sent some"
#   C  composite, A && B          alert only when both hold
#
# Monitor B sits in ALERT state throughout normal healthy operation, by design.
# It is a gate, not a page: it carries no notification handles, and nothing
# should ever be routed off it directly. Only C is paged on, and only C backs
# the SLO.
#
# A probe that is not running at all leaves sent at no-data -> default_zero -> 0,
# so B is not alerting and C stays quiet. Deliberate: "the probe is down" is a
# different alert from "Sentry is dropping our events", and the latency
# monitors' notify_no_data already covers the first.
#
# create_reliability_monitor <metric signal> <probe tag> <threshold %> <label>
# Prints the composite monitor id on stdout; progress goes to stderr, because
# the caller captures stdout.
create_reliability_monitor() {
  local signal="$1" probe_tag="$2" threshold="$3" label="$4"
  local m_rate m_sent m_comp

  # last_10m, not last_5m: one batch publishes one point roughly every
  # PROBE_INTERVAL_SECONDS (120s by default), and a window that spans several
  # batches keeps one unlucky batch from tripping the SLO.
  m_rate=$(create_monitor '{
  "name": "Sentry '"${label}"' success rate (SLO source, part A of composite)",
  "type": "metric alert",
  "query": "avg(last_10m):avg:sentry.'"${signal}"'.success_rate{'"${SCOPE}"',probe:'"${probe_tag}"'} < '"${threshold}"'",
  "message": "Fewer than '"${threshold}"'% of the synthetic '"${label}"' events sent became queryable in Sentry. Gated by the sent>0 monitor through a composite — see scripts/setup_datadog_slos.sh.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'","slo-part:success-rate"],
  "options": {"thresholds": {"critical": '"${threshold}"'}, "notify_no_data": false, "renotify_interval": 0}
}')

  m_sent=$(create_monitor '{
  "name": "Sentry '"${label}"' sent>0 gate (SLO source, part B of composite — DO NOT PAGE)",
  "type": "metric alert",
  "query": "avg(last_10m):default_zero(avg:sentry.'"${signal}"'.sent{'"${SCOPE}"',probe:'"${probe_tag}"'}) > 0",
  "message": "Gate only. ALERTING here is the NORMAL, HEALTHY state: it means the probe is successfully sending '"${label}"' events. Consumed by a composite monitor so a local send failure cannot be reported as a Sentry breach. Do not route notifications off this monitor and do not delete it without deleting its composite.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'","slo-part:sent-gate"],
  "options": {"thresholds": {"critical": 0}, "notify_no_data": false, "renotify_interval": 0}
}')

  m_comp=$(create_monitor '{
  "name": "Sentry '"${label}"' reliability (SLO source)",
  "type": "composite",
  "query": "'"${m_rate}"' && '"${m_sent}"'",
  "message": "Sentry returned fewer than '"${threshold}"'% of the synthetic '"${label}"' events we successfully sent. The sent>0 gate is satisfied, so this is Sentry dropping events, not the probe failing to send them.",
  "tags": ["service:sentry-slo-probe","probe:'"${probe_tag}"'"],
  "options": {"renotify_interval": 0}
}')

  echo "  ${label} success-rate (A):  ${m_rate}" >&2
  echo "  ${label} sent>0 gate  (B):  ${m_sent}" >&2
  echo "  ${label} reliability  (C):  ${m_comp}" >&2
  echo "${m_comp}"
}

M_REL_TRACE=$(create_reliability_monitor "ingestion" "ingestion" 99 "trace ingestion")
M_REL_ERROR=$(create_reliability_monitor "error_ingestion" "error_ingestion" 99 "error ingestion")
M_REL_SPANS=$(create_reliability_monitor "span_completeness" "span_completeness" 99 "span-probe transaction")

echo "==> Creating monitor-based SLOs (30-day rolling window)"

S1=$(create_slo '{
  "type": "monitor",
  "name": "Sentry trace ingestion latency",
  "description": "99% of the time, a synthetic trace is queryable in Sentry within 60s.",
  "monitor_ids": ['"${M_TRACE}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO trace ingestion latency:       ${S1}"

S2=$(create_slo '{
  "type": "monitor",
  "name": "Sentry error ingestion latency",
  "description": "99% of the time, a synthetic error appears in Sentry Issues within 90s.",
  "monitor_ids": ['"${M_ERROR}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.0, "warning": 99.5}],
  "tags": '"${TAGS}"'
}')
echo "  SLO error ingestion latency:       ${S2}"

S3=$(create_slo '{
  "type": "monitor",
  "name": "Sentry span completeness",
  "description": "99.5% of the time, all sent spans are ingested (no span drop).",
  "monitor_ids": ['"${M_SPANS}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.5, "warning": 99.9}],
  "tags": '"${TAGS}"'
}')
echo "  SLO span completeness:             ${S3}"

# The reliability SLOs track the *composite* (C), never part A on its own —
# an SLO over the ungated success_rate would burn error budget every time our
# own send path failed, which is the whole reason the gate exists.
#
# Datadog's monitor-based SLO docs list metric, synthetic and service-check
# monitors as sources and do not mention composites. If the /slo call below is
# rejected for M_REL_*, create_slo prints the API error and the script exits
# non-zero — it will not create a half-configured SLO silently. In that case
# keep the composites as the paging monitors and build these three as
# metric-based SLOs over sentry.<signal>.received / sentry.<signal>.sent
# (numerator/denominator), which carries the same gate implicitly: a window with
# no sends has an empty denominator and is excluded rather than counted as a
# breach.
S4=$(create_slo '{
  "type": "monitor",
  "name": "Sentry trace ingestion reliability",
  "description": "99.9% of the time, at least 99% of the synthetic traces we sent were queryable in Sentry.",
  "monitor_ids": ['"${M_REL_TRACE}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.9, "warning": 99.95}],
  "tags": '"${TAGS}"'
}')
echo "  SLO trace ingestion reliability:   ${S4}"

S5=$(create_slo '{
  "type": "monitor",
  "name": "Sentry error ingestion reliability",
  "description": "99.9% of the time, at least 99% of the synthetic errors we sent were queryable in Sentry.",
  "monitor_ids": ['"${M_REL_ERROR}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.9, "warning": 99.95}],
  "tags": '"${TAGS}"'
}')
echo "  SLO error ingestion reliability:   ${S5}"

S6=$(create_slo '{
  "type": "monitor",
  "name": "Sentry span-probe transaction reliability",
  "description": "99.9% of the time, at least 99% of the span-probe transactions we sent were queryable in Sentry. Distinct from span completeness: this counts whole transactions arriving, that one counts spans within them.",
  "monitor_ids": ['"${M_REL_SPANS}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.9, "warning": 99.95}],
  "tags": '"${TAGS}"'
}')
echo "  SLO span-probe reliability:        ${S6}"

echo "==> Done. View them at https://app.${DD_SITE}/slo"
