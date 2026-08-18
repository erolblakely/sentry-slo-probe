package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"
)

// ddSeries is the part of a Datadog v2 series submission the SLO contract binds:
// the metric name, its tags and its value. Decoded with encoding/json rather
// than scanned for `"metric":"` so a payload that stops being valid JSON fails
// loudly instead of quietly matching nothing.
type ddSeries struct {
	Metric string   `json:"metric"`
	Tags   []string `json:"tags"`
	Points []struct {
		Value float64 `json:"value"`
	} `json:"points"`
}

// ddRecorder is a fake Datadog series endpoint that captures every series
// submitted to it.
type ddRecorder struct {
	mu     sync.Mutex
	series []ddSeries
	errs   []error
}

// newDDRecorder returns a recorder and a server that accepts submissions into
// it, mimicking the 202 + empty error list a real submission gets.
func newDDRecorder() (*ddRecorder, *httptest.Server) {
	rec := &ddRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"errors":[]}`))
	}))
	return rec, srv
}

func (rec *ddRecorder) record(r *http.Request) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		rec.errs = append(rec.errs, fmt.Errorf("read request body: %w", err))
		return
	}
	var payload struct {
		Series []ddSeries `json:"series"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		rec.errs = append(rec.errs, fmt.Errorf("decode %q: %w", body, err))
		return
	}
	rec.series = append(rec.series, payload.Series...)
}

// captured returns everything the endpoint received. A capture failure aborts
// the test with that one cause: reporting it as six separate "missing metric"
// errors would bury the actual reason. Safe to call once postBatchMetrics has
// returned — its posts are synchronous and each is recorded before its response
// is written.
func (rec *ddRecorder) captured(t *testing.T) []ddSeries {
	t.Helper()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.errs) > 0 {
		t.Fatalf("fake datadog endpoint failed on %d submission(s); first: %v", len(rec.errs), rec.errs[0])
	}
	return slices.Clone(rec.series)
}

// wantBatchMetrics is the complete set of metrics one postBatchMetrics call must
// emit for the "ingestion" signal, sorted. The assertion is on the exact set,
// not mere presence: a metric that silently stops being submitted (an unmarshal-
// able value, say) is invisible on a dashboard, so an extra or missing name has
// to break the build.
var wantBatchMetrics = []string{
	"sentry.ingestion.latency_ms.p50",
	"sentry.ingestion.latency_ms.p95",
	"sentry.ingestion.latency_ms.p99",
	"sentry.ingestion.received",
	"sentry.ingestion.sent",
	"sentry.ingestion.success_rate",
}

func metricNames(series []ddSeries) []string {
	names := make([]string, 0, len(series))
	for _, s := range series {
		names = append(names, s.Metric)
	}
	sort.Strings(names)
	return names
}

// metricValue returns the one submitted value for name, failing if the metric is
// absent or submitted more than once.
func metricValue(t *testing.T, series []ddSeries, name string) float64 {
	t.Helper()
	var found []float64
	for _, s := range series {
		if s.Metric == name {
			for _, p := range s.Points {
				found = append(found, p.Value)
			}
		}
	}
	if len(found) != 1 {
		t.Fatalf("want exactly one point for %q, got %d %v", name, len(found), found)
	}
	return found[0]
}

// assertTags checks every series carries exactly wantSorted. Datadog tags are a
// set, so the comparison is order-insensitive — but it is exact: a stray tag
// fragments the metric's time series just as badly as a missing one.
func assertTags(t *testing.T, series []ddSeries, wantSorted []string) {
	t.Helper()
	for _, s := range series {
		got := slices.Clone(s.Tags)
		sort.Strings(got)
		if !slices.Equal(got, wantSorted) {
			t.Errorf("%s tags = %v, want exactly %v", s.Metric, s.Tags, wantSorted)
		}
	}
}

func newTestDatadogClient(t *testing.T) (*datadogClient, *ddRecorder) {
	t.Helper()
	rec, srv := newDDRecorder()
	t.Cleanup(srv.Close)
	d := newDatadogClient("key", "datadoghq.com")
	d.baseURL = srv.URL
	return d, rec
}

func TestPostBatchMetrics(t *testing.T) {
	t.Run("full batch", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		res := batchResult{sent: 10, received: 8, latencies: []time.Duration{time.Second, 2 * time.Second}}
		d.postBatchMetrics("ingestion", res, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)

		if got := metricNames(series); !slices.Equal(got, wantBatchMetrics) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, wantBatchMetrics)
		}

		assertTags(t, series, []string{"probe:ingestion", "sentry_org:o", "sentry_project:p"})

		// The percentile values are what pin the unit named by latency_ms: the
		// same [1s, 2s] fixture submitted in nanoseconds would read 1e9 / 2e9.
		// success_rate is a percentage, not a fraction.
		for _, want := range []struct {
			metric string
			value  float64
		}{
			{"sentry.ingestion.latency_ms.p50", 1000},
			{"sentry.ingestion.latency_ms.p95", 2000},
			{"sentry.ingestion.latency_ms.p99", 2000},
			{"sentry.ingestion.sent", 10},
			{"sentry.ingestion.received", 8},
			{"sentry.ingestion.success_rate", 80},
		} {
			if got := metricValue(t, series, want.metric); got != want.value {
				t.Errorf("%s = %v, want %v", want.metric, got, want.value)
			}
		}
	})

	t.Run("zero sent still reports success_rate", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// A batch where every send failed. Computing received/sent unguarded
		// yields NaN, which json.Marshal rejects, so success_rate would never
		// leave the process — and a missing metric is indistinguishable from a
		// healthy one on a dashboard. It must arrive, and it must read 0.
		d.postBatchMetrics("ingestion", batchResult{sent: 0, received: 0}, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)

		if got := metricNames(series); !slices.Equal(got, wantBatchMetrics) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, wantBatchMetrics)
		}
		assertTags(t, series, []string{"probe:ingestion", "sentry_org:o", "sentry_project:p"})

		if got := metricValue(t, series, "sentry.ingestion.success_rate"); got != 0 {
			t.Errorf("success_rate = %v, want 0", got)
		}
	})

	t.Run("does not append into caller's tags", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// Callers share one baseTags slice across every probe signal. Appending
		// probe:<signal> to it in place would write into the caller's backing
		// array and leak that tag into the next signal's metrics. A len==cap
		// literal hides the bug (append reallocates), so hand over a slice with
		// spare capacity and watch the spare slot.
		backing := []string{"sentry_org:o", "sentry_project:p", "unwritten"}
		baseTags := backing[:2]

		d.postBatchMetrics("ingestion", batchResult{sent: 1, received: 1}, baseTags)

		if backing[2] != "unwritten" {
			t.Errorf("postBatchMetrics wrote %q into the caller's backing array; a shared baseTags would leak probe tags between signals", backing[2])
		}
		if !slices.Equal(baseTags, []string{"sentry_org:o", "sentry_project:p"}) {
			t.Errorf("caller's baseTags = %v, want it untouched", baseTags)
		}
		assertTags(t, rec.captured(t), []string{"probe:ingestion", "sentry_org:o", "sentry_project:p"})
	})
}
