package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFindBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != "probe_batch:B1" {
			t.Errorf("query = %q, want probe_batch:B1", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "errors" {
			t.Errorf("dataset = %q, want errors", got)
		}
		// per_page carries the batch limit; if it were dropped the poll would
		// see only a page of arrived events and under-report the SLO.
		if got := r.URL.Query().Get("per_page"); got != "100" {
			t.Errorf("per_page = %q, want 100", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"probe_seq":"0"},{"probe_seq":"3"},{"probe_seq":"3"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	found, err := s.findBatch("errors", "B1", "probe_seq", 100)
	if err != nil {
		t.Fatalf("findBatch: %v", err)
	}
	if len(found) != 2 || !found["0"] || !found["3"] {
		t.Errorf("found = %v, want {0,3}", found)
	}
}

// TestFindTraceBatchQueriesSpansDataset pins the dataset both trace probes poll.
// Sentry has migrated this org's transaction data to the spans (EAP) dataset:
// dataset=transactions returns zero rows for any query, at any stats period, so
// a trace probe pointed there reports received=0 with no send error and no query
// error — the exact silent-blindness this test exists to prevent.
func TestFindTraceBatchQueriesSpansDataset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != "probe_batch:B1-trace" {
			t.Errorf("query = %q, want probe_batch:B1-trace", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "spans" {
			t.Errorf("dataset = %q, want spans", got)
		}
		// Slice form, not .Get(): .Get() returns only the first value, so a
		// stray Set that overwrites the field list would still read as "trace"
		// and pass. That exact mutation has been caught in this repo before.
		if got := r.URL.Query()["field"]; len(got) != 1 || got[0] != "trace" {
			t.Errorf("field = %v, want [trace]", got)
		}
		if got := r.URL.Query().Get("per_page"); got != "10" {
			t.Errorf("per_page = %q, want 10", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"trace":"T1"},{"trace":"T2"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	found, err := s.findTraceBatch("B1-trace", 10)
	if err != nil {
		t.Fatalf("findTraceBatch: %v", err)
	}
	if len(found) != 2 || !found["T1"] || !found["T2"] {
		t.Errorf("found = %v, want {T1,T2}", found)
	}
}

// TestFindBatchSpanCounts pins the one-request child-span census that replaced
// the per-event detail fetch. The predecessor could not work at all: the spans
// dataset's "id" is a 16-hex span id rather than an event id, and the
// events/{id}/ endpoint does not resolve transaction events in any case.
func TestFindBatchSpanCounts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// is_transaction:false isolates the children: a probe trace holds the
		// root slo.probe transaction plus its child spans, so without the
		// filter every trace counts one span too many and an incomplete trace
		// could pass the >= expectedSpans test.
		if got := r.URL.Query().Get("query"); got != "trace:[T1,T2] is_transaction:false" {
			t.Errorf("query = %q, want trace:[T1,T2] is_transaction:false", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "spans" {
			t.Errorf("dataset = %q, want spans", got)
		}
		// Slice form: dropping either field silently empties the result map —
		// no "trace" and the rows cannot be keyed, no "count()" and every
		// count reads as zero, i.e. total span loss.
		if got := r.URL.Query()["field"]; len(got) != 2 || got[0] != "trace" || got[1] != "count()" {
			t.Errorf("field = %v, want [trace count()]", got)
		}
		if got := r.URL.Query().Get("per_page"); got != "2" {
			t.Errorf("per_page = %q, want 2", got)
		}
		w.WriteHeader(200)
		// count() arrives as a JSON number, so it decodes as float64.
		w.Write([]byte(`{"data":[{"trace":"T1","count()":5},{"trace":"T2","count()":3}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	counts, err := s.findBatchSpanCounts([]string{"T1", "T2"}, 2)
	if err != nil {
		t.Fatalf("findBatchSpanCounts: %v", err)
	}
	if len(counts) != 2 || counts["T1"] != 5 || counts["T2"] != 3 {
		t.Errorf("counts = %v, want T1->5,T2->3", counts)
	}
}

// TestFindBatchSpanCountsErrorsOnNon2xx: a failed census must be reported as
// unknown, never as a map. An empty map reads as "no trace was complete" and
// publishes 0% — manufacturing a total-span-loss reading out of an API hiccup.
func TestFindBatchSpanCountsErrorsOnNon2xx(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"detail":"boom"}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	counts, err := s.findBatchSpanCounts([]string{"T1"}, 1)
	if err == nil {
		t.Fatalf("findBatchSpanCounts: err = nil, want error (counts = %v)", counts)
	}
	if counts != nil {
		t.Errorf("counts = %v, want nil on error", counts)
	}
}

// TestCompletenessPct pins the all-or-nothing rule: a trace counts as complete
// only when Sentry stored at least every child span we sent.
func TestCompletenessPct(t *testing.T) {
	sampled := []string{"T1", "T2", "T3", "T4"}
	counts := map[string]int{
		"T1": expectedSpans,     // exactly the expected count: complete
		"T2": expectedSpans - 1, // one span short: incomplete
		"T3": expectedSpans + 1, // more than expected: still complete
		// T4 absent: the census ran and found no child spans at all, which is
		// an incomplete trace, not an unknown one.
	}
	pct, known := completenessPct(sampled, counts, expectedSpans)
	if !known {
		t.Fatalf("known = false, want true for a non-empty sample")
	}
	if pct != 50 {
		t.Errorf("pct = %v, want 50 (T1 and T3 of 4 sampled)", pct)
	}
}

// TestCompletenessPctEmptySample: nothing sampled is "not measured", not 0%.
func TestCompletenessPctEmptySample(t *testing.T) {
	pct, known := completenessPct(nil, map[string]int{"T1": expectedSpans}, expectedSpans)
	if known {
		t.Errorf("known = true, want false for an empty sample")
	}
	if pct != 0 {
		t.Errorf("pct = %v, want 0 alongside known=false", pct)
	}
}

// TestSampleTraceIDs: SPAN_COMPLETENESS_SAMPLE bounds how many trace ids go
// into the census query. It bounds URL length (33 characters per id) and keeps
// the documented meaning of the setting.
func TestSampleTraceIDs(t *testing.T) {
	found := map[string]bool{"T1": true, "T2": true, "T3": true, "T4": true}
	got := sampleTraceIDs(found, 2)
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	for _, id := range got {
		if !found[id] {
			t.Errorf("sampled %q, which is not in the arrived set", id)
		}
	}
	if got, want := len(sampleTraceIDs(found, 10)), 4; got != want {
		t.Errorf("len = %d, want %d when the bound exceeds the arrived set", got, want)
	}
	if got := sampleTraceIDs(found, 0); len(got) != 0 {
		t.Errorf("sampleTraceIDs(_, 0) = %v, want empty", got)
	}
}
