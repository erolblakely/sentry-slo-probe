#!/usr/bin/env bash
#
# setup_datadog_slos.sh
#
# Creates the Datadog monitors + monitor-based SLOs that track Sentry's
# ingestion performance, using the metrics emitted by this probe:
#
#   sentry.ingestion.latency_ms          (trace ingestion latency)
#   sentry.error_ingestion.latency_ms    (error ingestion latency)
#   sentry.span_completeness.received_pct (% of sent spans that arrived)
#   sentry.ingestion.error               (posted as 1 whenever a probe fails)
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

M_TRACE=$(create_monitor '{
  "name": "Sentry trace ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.ingestion.latency_ms{'"${SCOPE}"',probe:trace_ingestion} > 60000",
  "message": "Synthetic trace took > 60s to become queryable in Sentry.",
  "tags": ["service:sentry-slo-probe","probe:trace_ingestion"],
  "options": {"thresholds": {"critical": 60000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  trace-ingestion-latency monitor:   ${M_TRACE}"

M_ERROR=$(create_monitor '{
  "name": "Sentry error ingestion latency (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.error_ingestion.latency_ms{'"${SCOPE}"',probe:error_ingestion} > 90000",
  "message": "Synthetic error took > 90s to appear in Sentry Issues.",
  "tags": ["service:sentry-slo-probe","probe:error_ingestion"],
  "options": {"thresholds": {"critical": 90000}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  error-ingestion-latency monitor:   ${M_ERROR}"

M_SPANS=$(create_monitor '{
  "name": "Sentry span completeness (SLO source)",
  "type": "metric alert",
  "query": "avg(last_5m):avg:sentry.span_completeness.received_pct{'"${SCOPE}"',probe:span_completeness} < 100",
  "message": "Sentry ingested fewer spans than were sent (span drop).",
  "tags": ["service:sentry-slo-probe","probe:span_completeness"],
  "options": {"thresholds": {"critical": 100}, "notify_no_data": true, "no_data_timeframe": 15, "renotify_interval": 0}
}')
echo "  span-completeness monitor:         ${M_SPANS}"

M_AVAIL=$(create_monitor '{
  "name": "Sentry ingestion probe failures (SLO source)",
  "type": "metric alert",
  "query": "sum(last_5m):default_zero(sum:sentry.ingestion.error{'"${SCOPE}"'}.as_count()) >= 1",
  "message": "One or more Sentry ingestion probes failed (timeout or API error).",
  "tags": ["service:sentry-slo-probe","probe:availability"],
  "options": {"thresholds": {"critical": 1}, "notify_no_data": false, "renotify_interval": 0}
}')
echo "  availability monitor:              ${M_AVAIL}"

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

S4=$(create_slo '{
  "type": "monitor",
  "name": "Sentry ingestion availability",
  "description": "99.9% of the time, the ingestion probes succeed (no timeouts/errors).",
  "monitor_ids": ['"${M_AVAIL}"'],
  "thresholds": [{"timeframe": "30d", "target": 99.9, "warning": 99.95}],
  "tags": '"${TAGS}"'
}')
echo "  SLO ingestion availability:        ${S4}"

echo "==> Done. View them at https://app.${DD_SITE}/slo"
