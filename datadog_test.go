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
// the metric name, its type and interval, its tags and its value. Decoded with
// encoding/json rather than scanned for `"metric":"` so a payload that stops
// being valid JSON fails loudly instead of quietly matching nothing.
//
// Type is decoded because the metric TYPE is part of the contract, not an
// implementation detail. Datadog's metric-based SLOs accept COUNT, RATE and
// percentile-enabled DISTRIBUTION only, and .sent/.received back three such
// SLOs; a gauge there may be rejected at SLO creation, and its default time
// rollup is avg, which is actively wrong when two overlapping batches land in
// one bucket and should sum. This struct previously did not decode `type` at
// all, so nothing pinned it and every metric silently shipped as a gauge.
type ddSeries struct {
	Metric   string   `json:"metric"`
	Type     int      `json:"type"`
	Interval int64    `json:"interval"`
	Tags     []string `json:"tags"`
	Points   []struct {
		Value float64 `json:"value"`
	} `json:"points"`
}

// assertMetricTypes checks the submitted type (and, for counts, the interval) of
// every named metric. want maps metric name -> expected type. Every name in want
// must be present exactly once, and every submitted series must be named in want,
// so the mapping is pinned in both directions.
func assertMetricTypes(t *testing.T, series []ddSeries, want map[string]int) {
	t.Helper()
	seen := map[string]int{}
	for _, s := range series {
		wantType, ok := want[s.Metric]
		if !ok {
			t.Errorf("unexpected metric %q submitted; the type contract does not cover it", s.Metric)
			continue
		}
		seen[s.Metric]++
		if s.Type != wantType {
			t.Errorf("%s type = %d, want %d (1=count, 3=gauge)", s.Metric, s.Type, wantType)
		}
		// Datadog requires an interval alongside count and rate metrics and
		// ignores it for gauges. A count submitted without one is not normalisable.
		switch wantType {
		case ddTypeCount:
			if s.Interval <= 0 {
				t.Errorf("%s is a count with interval = %d, want the probe's submission interval in seconds", s.Metric, s.Interval)
			}
		case ddTypeGauge:
			if s.Interval != 0 {
				t.Errorf("%s is a gauge with interval = %d, want it omitted", s.Metric, s.Interval)
			}
		}
	}
	for name := range want {
		if seen[name] != 1 {
			t.Errorf("metric %q submitted %d times, want exactly 1", name, seen[name])
		}
	}
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

// testInterval is the submission cadence the test client reports for its count
// metrics. Deliberately not the 120s production default, so a hardcoded 120
// would fail rather than pass by coincidence.
const testInterval = 90 * time.Second

func newTestDatadogClient(t *testing.T) (*datadogClient, *ddRecorder) {
	t.Helper()
	rec, srv := newDDRecorder()
	t.Cleanup(srv.Close)
	d := newDatadogClient("key", "datadoghq.com", testInterval)
	d.baseURL = srv.URL
	return d, rec
}

// TestNewDatadogClientInterval pins that the count interval is the configured
// probe interval in seconds, and that a nonsensical interval cannot reach the
// wire as a zero (Datadog cannot normalise a count with interval 0).
func TestNewDatadogClientInterval(t *testing.T) {
	if got := newDatadogClient("k", "s", 120*time.Second).intervalSeconds; got != 120 {
		t.Errorf("intervalSeconds = %d, want 120", got)
	}
	if got := newDatadogClient("k", "s", 0).intervalSeconds; got != 1 {
		t.Errorf("intervalSeconds = %d for a zero interval, want the 1s floor", got)
	}
	if got := newDatadogClient("k", "s", -5*time.Second).intervalSeconds; got != 1 {
		t.Errorf("intervalSeconds = %d for a negative interval, want the 1s floor", got)
	}
}

// TestPostMetricCountCarriesInterval checks the interval that actually reaches
// the wire, not just the field on the struct.
func TestPostMetricCountCarriesInterval(t *testing.T) {
	d, rec := newTestDatadogClient(t)

	d.postMetricLogged("sentry.ingestion.sent", ddTypeCount, 10, []string{"probe:ingestion"})
	d.postMetricLogged("sentry.ingestion.success_rate", ddTypeGauge, 100, []string{"probe:ingestion"})

	series := rec.captured(t)
	if len(series) != 2 {
		t.Fatalf("got %d series, want 2", len(series))
	}
	for _, s := range series {
		switch s.Metric {
		case "sentry.ingestion.sent":
			if s.Type != ddTypeCount {
				t.Errorf("sent type = %d, want %d", s.Type, ddTypeCount)
			}
			if want := int64(testInterval / time.Second); s.Interval != want {
				t.Errorf("sent interval = %d, want %d (the probe's submission cadence)", s.Interval, want)
			}
		case "sentry.ingestion.success_rate":
			if s.Type != ddTypeGauge {
				t.Errorf("success_rate type = %d, want %d", s.Type, ddTypeGauge)
			}
			if s.Interval != 0 {
				t.Errorf("success_rate is a gauge with interval = %d, want it omitted", s.Interval)
			}
		}
	}
}

func TestPostBatchMetrics(t *testing.T) {
	t.Run("full batch", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		res := batchResult{sent: 10, received: 8, latencies: []time.Duration{time.Second, 2 * time.Second}, measured: true}
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

		// The metric TYPE is part of the SLO contract, per metric.
		//
		// .sent and .received are tallies of events and are the numerator and
		// denominator of three metric-based SLOs, which Datadog documents as
		// supporting COUNT, RATE and percentile-enabled DISTRIBUTION. As gauges
		// they may be rejected outright at SLO creation, and gauge time-rollup
		// defaults to avg — wrong here, because two overlapping batches landing in
		// one bucket should sum, not average.
		//
		// The percentiles and success_rate stay gauges deliberately: they are
		// levels, not tallies, and the monitor-based SLOs over them are correct
		// with gauges. Summing two batches' p95 would be meaningless.
		assertMetricTypes(t, series, map[string]int{
			"sentry.ingestion.latency_ms.p50": ddTypeGauge,
			"sentry.ingestion.latency_ms.p95": ddTypeGauge,
			"sentry.ingestion.latency_ms.p99": ddTypeGauge,
			"sentry.ingestion.sent":           ddTypeCount,
			"sentry.ingestion.received":       ddTypeCount,
			"sentry.ingestion.success_rate":   ddTypeGauge,
		})
	})

	t.Run("zero sent reports success_rate but no latency percentiles", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// A batch where every send failed, so no event was ever observed
		// arriving and there is no latency sample at all.
		//
		// Two rules meet here, and they pull in opposite directions.
		//
		// success_rate MUST arrive and MUST read 0. Computing received/sent
		// unguarded yields NaN, which json.Marshal rejects, so the metric would
		// never leave the process — and a missing metric is indistinguishable
		// from a healthy one on a dashboard.
		//
		// The three latency percentiles MUST NOT arrive. percentile() returns 0
		// for an empty slice, so posting them unconditionally publishes
		// p50=p95=p99=0ms — a total ingestion failure rendered as *perfect*
		// latency. The latency monitors compare > 120000 / > 90000, which a zero
		// can never trip, so the latency SLOs would record 100% compliance while
		// nothing at all was ingested. Both monitors set notify_no_data with a
		// 15-minute timeframe precisely to catch an absent series; publishing a
		// fabricated 0 fills the series and disables that net. A gap is the
		// honest reading of "no event arrived, so no latency was observed".
		d.postBatchMetrics("ingestion", batchResult{sent: 0, received: 0, measured: true}, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)

		want := []string{
			"sentry.ingestion.received",
			"sentry.ingestion.sent",
			"sentry.ingestion.success_rate",
		}
		if got := metricNames(series); !slices.Equal(got, want) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, want)
		}
		assertTags(t, series, []string{"probe:ingestion", "sentry_org:o", "sentry_project:p"})

		if got := metricValue(t, series, "sentry.ingestion.success_rate"); got != 0 {
			t.Errorf("success_rate = %v, want 0", got)
		}
	})

	// A batch that sent successfully and had every event time out is the same
	// latency situation as the zero-sent case above — no sample — but a very
	// different reliability situation. sent/received/success_rate must all be
	// published (this IS a real 0% breach and must burn error budget), and the
	// percentiles must still be absent.
	t.Run("measured but nothing arrived omits latency percentiles", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		d.postBatchMetrics("ingestion", batchResult{sent: 10, received: 0, measured: true}, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)

		want := []string{
			"sentry.ingestion.received",
			"sentry.ingestion.sent",
			"sentry.ingestion.success_rate",
		}
		if got := metricNames(series); !slices.Equal(got, want) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, want)
		}
		if got := metricValue(t, series, "sentry.ingestion.sent"); got != 10 {
			t.Errorf("sent = %v, want 10", got)
		}
		if got := metricValue(t, series, "sentry.ingestion.success_rate"); got != 0 {
			t.Errorf("success_rate = %v, want 0 (a real, measured total loss)", got)
		}
	})

	// The measurement-validity gate. When the probe could not query Sentry at
	// all — expired SENTRY_AUTH_TOKEN being the reproduced case — nothing about
	// this cycle is known, so nothing at all may be published.
	//
	// Note what is NOT acceptable here: publishing sent while withholding
	// received. That leaves the metric-based SLOs with a denominator and no
	// numerator, i.e. a 0% ratio — a *worse* false breach than the status quo.
	// Posting nothing makes the cycle contribute 0/0, which is precisely the
	// structural immunity those SLOs are built on, and it lets the latency
	// monitors' notify_no_data fire within 15 minutes so "the probe cannot
	// measure" pages differently from "Sentry is dropping events".
	t.Run("unmeasured cycle publishes nothing at all", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// sent is deliberately non-zero and latencies deliberably non-empty: the
		// guard must be the validity bit, not a side effect of empty data.
		res := batchResult{sent: 10, received: 0, latencies: []time.Duration{time.Second}, measured: false}
		d.postBatchMetrics("ingestion", res, []string{"sentry_org:o", "sentry_project:p"})

		if series := rec.captured(t); len(series) != 0 {
			t.Errorf("published %d series for an unmeasured cycle: %v — every one of these is a fabricated reading", len(series), metricNames(series))
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

		d.postBatchMetrics("ingestion", batchResult{sent: 1, received: 1, latencies: []time.Duration{time.Second}, measured: true}, baseTags)

		if backing[2] != "unwritten" {
			t.Errorf("postBatchMetrics wrote %q into the caller's backing array; a shared baseTags would leak probe tags between signals", backing[2])
		}
		if !slices.Equal(baseTags, []string{"sentry_org:o", "sentry_project:p"}) {
			t.Errorf("caller's baseTags = %v, want it untouched", baseTags)
		}
		assertTags(t, rec.captured(t), []string{"probe:ingestion", "sentry_org:o", "sentry_project:p"})
	})
}
