package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type datadogClient struct {
	apiKey string
	site   string
	// baseURL is the API origin metrics are posted to. It defaults to the
	// site-derived Datadog endpoint and is overridable so tests can point at a
	// local server.
	baseURL string
	http    *http.Client
}

func newDatadogClient(apiKey, site string) *datadogClient {
	return &datadogClient{
		apiKey:  apiKey,
		site:    site,
		baseURL: "https://api." + site,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}

// postMetric sends a single gauge metric to the Datadog API (v2 series).
func (d *datadogClient) postMetric(name string, value float64, tags []string) error {
	if tags == nil {
		tags = []string{}
	}

	payload := map[string]any{
		"series": []map[string]any{
			{
				"metric": name,
				"type":   3, // 3 = gauge
				"points": []map[string]any{
					{
						"timestamp": time.Now().Unix(),
						"value":     value,
					},
				},
				"tags": tags,
			},
		},
	}

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

// postMetricLogged posts a metric and logs any failure. Call sites that report
// SLO data have no way to recover from a rejected submission, but a silently
// dropped metric looks identical to a healthy zero on a dashboard, so the error
// must at least reach the logs.
func (d *datadogClient) postMetricLogged(name string, value float64, tags []string) {
	if err := d.postMetric(name, value, tags); err != nil {
		log.Printf("[datadog] post %s: %v", name, err)
	}
}

// postBatchMetrics emits the latency percentiles + reliability metrics for one
// probe signal.
func (d *datadogClient) postBatchMetrics(signal string, res batchResult, baseTags []string) {
	// Copy baseTags before appending: callers share one baseTags slice across
	// every probe signal, so appending in place could scribble probe:<signal>
	// into a neighbour's tags.
	tags := append(append([]string{}, baseTags...), "probe:"+signal)
	d.postMetricLogged("sentry."+signal+".latency_ms.p50", float64(percentile(res.latencies, 50).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".latency_ms.p95", float64(percentile(res.latencies, 95).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".latency_ms.p99", float64(percentile(res.latencies, 99).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".sent", float64(res.sent), tags)
	d.postMetricLogged("sentry."+signal+".received", float64(res.received), tags)
	// sent is the count of sends that actually succeeded, so it can be 0 when a
	// whole batch fails to send; guarding avoids submitting NaN.
	rate := 0.0
	if res.sent > 0 {
		rate = float64(res.received) / float64(res.sent) * 100
	}
	d.postMetricLogged("sentry."+signal+".success_rate", rate, tags)
}
