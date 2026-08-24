package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// Datadog v2 series metric types: 0 unspecified, 1 count, 2 rate, 3 gauge.
//
// Which one a metric carries is a contract, not a formatting detail.
//
//   - COUNT is required for the two tallies, .sent and .received. Datadog
//     documents metric-based SLOs as supporting COUNT, RATE and
//     percentile-enabled DISTRIBUTION, and those two are the numerator and
//     denominator of SLOs S4/S5/S6 (scripts/setup_datadog_slos.sh), so a gauge
//     there risks outright rejection at SLO creation. Worse if accepted: gauge
//     time-rollup defaults to avg, and two overlapping batches landing in one
//     bucket must SUM — overlap is the documented steady state at the default
//     interval (see maxInflightBatches), so this is the normal case, not an edge.
//
//   - GAUGE is right for the levels: the three latency percentiles, success_rate
//     and span_completeness.received_pct. Summing two batches' p95 would be
//     meaningless, and the monitor-based SLOs over them are correct with gauges.
const (
	ddTypeCount = 1
	ddTypeGauge = 3
)

type datadogClient struct {
	apiKey string
	site   string
	// baseURL is the API origin metrics are posted to. It defaults to the
	// site-derived Datadog endpoint and is overridable so tests can point at a
	// local server.
	baseURL string
	http    *http.Client
	// intervalSeconds is submitted alongside count metrics, which Datadog
	// requires in order to normalise them. It is the probe's own submission
	// cadence (PROBE_INTERVAL_SECONDS): one point per signal per cycle.
	intervalSeconds int64
}

func newDatadogClient(apiKey, site string, interval time.Duration) *datadogClient {
	// Floor at 1s. A count submitted with interval 0 is not normalisable, and
	// PROBE_INTERVAL_SECONDS is free-form env input.
	secs := int64(interval / time.Second)
	if secs < 1 {
		secs = 1
	}
	return &datadogClient{
		apiKey:          apiKey,
		site:            site,
		baseURL:         "https://api." + site,
		http:            &http.Client{Timeout: 10 * time.Second},
		intervalSeconds: secs,
	}
}

// postMetric sends a single metric of the given type to the Datadog API (v2
// series). See the ddType constants for which type belongs to which metric.
func (d *datadogClient) postMetric(name string, metricType int, value float64, tags []string) error {
	if tags == nil {
		tags = []string{}
	}

	series := map[string]any{
		"metric": name,
		"type":   metricType,
		"points": []map[string]any{
			{
				"timestamp": time.Now().Unix(),
				"value":     value,
			},
		},
		"tags": tags,
	}
	// interval applies to count and rate only; Datadog ignores it on a gauge, so
	// it is omitted there rather than sent as a meaningless number.
	if metricType == ddTypeCount {
		series["interval"] = d.intervalSeconds
	}

	payload := map[string]any{"series": []map[string]any{series}}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	url := d.baseURL + "/api/v2/series"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("DD-API-KEY", d.apiKey)
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.http.Do(req)
	if err != nil {
		return fmt.Errorf("datadog api request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("datadog api returned %d", resp.StatusCode)
	}
	return nil
}

// postMetricLogged posts a metric of the given type and logs any failure. Call
// sites that report SLO data have no way to recover from a rejected submission,
// but a silently dropped metric looks identical to a healthy zero on a
// dashboard, so the error must at least reach the logs.
func (d *datadogClient) postMetricLogged(name string, metricType int, value float64, tags []string) {
	if err := d.postMetric(name, metricType, value, tags); err != nil {
		log.Printf("[datadog] post %s: %v", name, err)
	}
}

// postBatchMetrics emits the latency percentiles + reliability metrics for one
// probe signal.
func (d *datadogClient) postBatchMetrics(signal string, res batchResult, baseTags []string) {
	// Measurement validity comes first, and it suppresses the WHOLE signal.
	//
	// res.measured false means no query() call ever succeeded (or no client was
	// ever built), so received carries no information — an expired
	// SENTRY_AUTH_TOKEN produces exactly this, with sends succeeding against the
	// DSN and every Discover poll returning 401. Publishing received=0 against a
	// real sent would accuse Sentry of a total outage that is actually our own
	// credential failure.
	//
	// Publishing sent alone is not the lesser evil, it is the greater one: the
	// metric-based reliability SLOs are sum(received)/sum(sent), so a denominator
	// with no numerator is a 0% ratio and a *harder* false breach than today's.
	// Posting nothing makes the cycle contribute 0/0 — the structural immunity
	// those SLOs are designed around (see scripts/setup_datadog_slos.sh).
	//
	// No heartbeat metric is emitted in its place; the metric-name list is fixed.
	// The alerting story is already covered: the composite's sent>0 gate goes
	// quiet, and the latency monitors' notify_no_data fires within 15 minutes, so
	// "the probe cannot measure" reads as a different alert from "Sentry is
	// dropping events" — the separation that script argues for at length.
	if !res.measured {
		log.Printf("[datadog] %s: batch not measured this cycle (no Sentry query succeeded — check SENTRY_AUTH_TOKEN and the Discover API), suppressing all sentry.%s.* metrics rather than publishing received=0 as a Sentry outage", signal, signal)
		return
	}
	// Copy baseTags before appending: callers share one baseTags slice across
	// every probe signal, so appending in place could scribble probe:<signal>
	// into a neighbour's tags.
	tags := append(append([]string{}, baseTags...), "probe:"+signal)
	// Latency percentiles are published ONLY when there is a latency sample.
	//
	// percentile() returns 0 for an empty slice — correct for a pure function,
	// and tested as such — but posting that 0 would render a total ingestion
	// failure as *perfect* latency. The latency monitors compare > 120000 and
	// > 90000 (scripts/setup_datadog_slos.sh), thresholds a zero can never trip,
	// so the latency SLOs would record 100% compliance across an outage in which
	// nothing was ingested at all. Both monitors also set notify_no_data with a
	// 15-minute timeframe: that net is armed by the series being ABSENT, and
	// filling it with a fabricated 0 is exactly what disables it.
	//
	// So the gap is the fix, not a shortcoming of it: "no event arrived, so no
	// latency was observed" is the honest reading, and it re-arms notify_no_data.
	// Reliability is unaffected — sent, received and success_rate below are
	// measured quantities even when received is 0, and must still be published so
	// a genuine total loss burns error budget.
	if len(res.latencies) > 0 {
		d.postMetricLogged("sentry."+signal+".latency_ms.p50", ddTypeGauge, float64(percentile(res.latencies, 50).Milliseconds()), tags)
		d.postMetricLogged("sentry."+signal+".latency_ms.p95", ddTypeGauge, float64(percentile(res.latencies, 95).Milliseconds()), tags)
		d.postMetricLogged("sentry."+signal+".latency_ms.p99", ddTypeGauge, float64(percentile(res.latencies, 99).Milliseconds()), tags)
	}
	d.postMetricLogged("sentry."+signal+".sent", ddTypeCount, float64(res.sent), tags)
	d.postMetricLogged("sentry."+signal+".received", ddTypeCount, float64(res.received), tags)
	// sent is the count of sends that actually succeeded, so it can be 0 when a
	// whole batch fails to send; guarding avoids submitting NaN.
	rate := 0.0
	if res.sent > 0 {
		rate = float64(res.received) / float64(res.sent) * 100
	}
	d.postMetricLogged("sentry."+signal+".success_rate", ddTypeGauge, rate, tags)
}
