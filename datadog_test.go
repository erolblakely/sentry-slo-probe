package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPostBatchMetrics(t *testing.T) {
	var mu sync.Mutex
	names := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAllTest(r)
		mu.Lock()
		for _, n := range extractMetricNames(body) {
			names[n] = true
		}
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"errors":[]}`))
	}))
	defer srv.Close()

	d := newDatadogClient("key", "datadoghq.com")
	d.baseURL = srv.URL
	res := batchResult{sent: 10, received: 8, latencies: []time.Duration{time.Second, 2 * time.Second}}
	d.postBatchMetrics("ingestion", res, []string{"sentry_org:o"})

	for _, want := range []string{
		"sentry.ingestion.latency_ms.p50", "sentry.ingestion.latency_ms.p95",
		"sentry.ingestion.latency_ms.p99", "sentry.ingestion.sent",
		"sentry.ingestion.received", "sentry.ingestion.success_rate",
	} {
		if !names[want] {
			t.Errorf("missing metric %q (got %v)", want, names)
		}
	}
}

func readAllTest(r *http.Request) (string, error) {
	b, err := io.ReadAll(r.Body)
	return string(b), err
}

func extractMetricNames(body string) []string {
	var out []string
	const key = "\"metric\":\""
	for i := 0; i+len(key) < len(body); i++ {
		if body[i:i+len(key)] == key {
			j := i + len(key)
			start := j
			for j < len(body) && body[j] != '"' {
				j++
			}
			out = append(out, body[start:j])
		}
	}
	return out
}
