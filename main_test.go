package main

import (
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestInflightCap(t *testing.T) {
	c := newInflightCap(2)
	if !c.try() || !c.try() {
		t.Fatal("first two acquisitions should succeed")
	}
	if c.try() {
		t.Fatal("third acquisition should fail at cap 2")
	}
	c.done()
	if !c.try() {
		t.Fatal("acquisition should succeed after done()")
	}
}

// TestInflightCapConcurrent is the test that actually exercises what the cap is
// for. try() and done() are called from the loop goroutine and the batch
// goroutines respectively, so a missing lock is a real data race; TestInflightCap
// alone would pass without one. Run under -race this fails on an unguarded
// counter, and the peak assertion fails if the cap ever hands out an extra slot.
//
// Holders keep their slot until the test releases them. That barrier is what
// makes the peak assertion mean anything: incrementing and decrementing
// back-to-back left peak reading 1 on almost every run, so the assertion could
// not fail even with the cap removed entirely. With the barrier, exactly max
// try() calls can succeed and all of them are live at once when peak is read, so
// peak is deterministically max — and any widening of the cap shows up.
func TestInflightCapConcurrent(t *testing.T) {
	const max = 2
	const goroutines = 100
	c := newInflightCap(max)

	var mu sync.Mutex
	live, peak := 0, 0

	release := make(chan struct{})
	// attempted counts every goroutine that has finished its try() *and* recorded
	// the result, so once it drains, live/peak are settled with the holders still
	// holding. No sleeps, so the assertion is deterministic rather than timing-dependent.
	var attempted, wg sync.WaitGroup
	attempted.Add(goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			ok := c.try()
			if ok {
				mu.Lock()
				live++
				if live > peak {
					peak = live
				}
				mu.Unlock()
			}
			attempted.Done()
			if !ok {
				return
			}
			<-release // hold the slot across the barrier
			mu.Lock()
			live--
			mu.Unlock()
			c.done()
		}()
	}

	attempted.Wait()
	mu.Lock()
	gotPeak := peak
	mu.Unlock()
	if gotPeak != max {
		t.Errorf("peak concurrent holders = %d, want exactly %d (cap %d, %d contenders, none released yet)",
			gotPeak, max, max, goroutines)
	}

	close(release)
	wg.Wait()

	// Every holder released, so the cap must be fully available again.
	for i := 0; i < max; i++ {
		if !c.try() {
			t.Fatalf("acquisition %d/%d failed: done() did not release every slot", i+1, max)
		}
	}
	if c.try() {
		t.Errorf("acquisition beyond cap %d succeeded: counter drifted below zero", max)
	}
}

// TestInflightCapDoneWithoutTry pins that an unmatched done() cannot push the
// counter negative and hand out extra slots.
func TestInflightCapDoneWithoutTry(t *testing.T) {
	c := newInflightCap(1)
	c.done()
	c.done()
	if !c.try() {
		t.Fatal("first acquisition should succeed")
	}
	if c.try() {
		t.Fatal("second acquisition should fail at cap 1: stray done() calls widened the cap")
	}
}

// spanBatchMetrics is the set postBatchMetrics emits for the span_completeness
// signal — everything reportSpanCompleteness publishes *except* received_pct,
// which is the one metric under test.
var spanBatchMetrics = []string{
	"sentry.span_completeness.latency_ms.p50",
	"sentry.span_completeness.latency_ms.p95",
	"sentry.span_completeness.latency_ms.p99",
	"sentry.span_completeness.received",
	"sentry.span_completeness.sent",
	"sentry.span_completeness.success_rate",
}

// TestReportSpanCompleteness pins the suppression rule: when the completeness
// sample is unknown, received_pct must not be published at all. Publishing 0
// instead would turn an API hiccup into a fabricated total-span-loss reading —
// the SLO would show a breach that never happened. This lived in an anonymous
// closure in main and so had no automated protection; the function exists to be
// testable, and this is the test.
func TestReportSpanCompleteness(t *testing.T) {
	res := batchResult{sent: 10, received: 10, latencies: []time.Duration{time.Second}}
	wantTags := []string{"probe:span_completeness", "sentry_org:o", "sentry_project:p"}

	t.Run("unknown completeness suppresses received_pct", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// pct is deliberately a value that would look catastrophic if published.
		reportSpanCompleteness(d, "run-span_completeness-1", res, 0, false, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)
		for _, s := range series {
			if s.Metric == "sentry.span_completeness.received_pct" {
				t.Fatalf("received_pct was published with value %v while the sample was unknown: a fabricated reading, not a measurement", s.Points)
			}
		}
		// The ingestion side of the probe is still measured, so the batch metrics
		// must all arrive — suppression is scoped to the one unknown metric.
		if got := metricNames(series); !slices.Equal(got, spanBatchMetrics) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, spanBatchMetrics)
		}
	})

	t.Run("known completeness publishes received_pct", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		reportSpanCompleteness(d, "run-span_completeness-1", res, 80, true, []string{"sentry_org:o", "sentry_project:p"})

		series := rec.captured(t)
		want := append(slices.Clone(spanBatchMetrics), "sentry.span_completeness.received_pct")
		sort.Strings(want)
		if got := metricNames(series); !slices.Equal(got, want) {
			t.Errorf("metric names\n got: %v\nwant: %v", got, want)
		}
		if got := metricValue(t, series, "sentry.span_completeness.received_pct"); got != 80 {
			t.Errorf("received_pct = %v, want 80", got)
		}
		// Every series, received_pct included, carries the org/project tags plus
		// the probe tag — a mistagged metric is invisible to the monitor scope.
		assertTags(t, series, wantTags)
	})

	t.Run("does not append into caller's tags", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)

		// The received_pct call site appends its probe tag by hand, so it is the
		// one place in the reporting path that can scribble into the baseTags
		// slice all three probe loops share. A len==cap literal would hide the
		// bug, so hand over a slice with spare capacity and watch the spare slot.
		backing := []string{"sentry_org:o", "sentry_project:p", "unwritten"}
		baseTags := backing[:2]

		reportSpanCompleteness(d, "run-span_completeness-1", res, 80, true, baseTags)

		if backing[2] != "unwritten" {
			t.Errorf("reportSpanCompleteness wrote %q into the caller's backing array; baseTags is shared across all three probe loops", backing[2])
		}
		assertTags(t, rec.captured(t), wantTags)
	})
}

func TestMakeBatchID(t *testing.T) {
	// Deterministic: no wall clock, no randomness, so the same inputs always
	// produce the same id.
	if got := makeBatchID("trace_ingestion", 7); got != "trace_ingestion-7" {
		t.Errorf("makeBatchID(trace_ingestion, 7) = %q, want %q", got, "trace_ingestion-7")
	}
	if a, b := makeBatchID("trace_ingestion", 1), makeBatchID("trace_ingestion", 1); a != b {
		t.Errorf("makeBatchID is not deterministic: %q != %q", a, b)
	}

	// Distinct per cycle and per signal. Two probe signals firing the same cycle
	// must not share a batch id, or their Discover queries would match each
	// other's events.
	seen := map[string]bool{}
	for _, signal := range []string{"trace_ingestion", "error_ingestion", "span_completeness"} {
		for cycle := 1; cycle <= 3; cycle++ {
			id := makeBatchID(signal, cycle)
			if seen[id] {
				t.Errorf("duplicate batch id %q", id)
			}
			seen[id] = true
		}
	}
	if len(seen) != 9 {
		t.Errorf("got %d distinct ids, want 9", len(seen))
	}
}

// TestQualifyBatchID pins the property the run component exists for: the value
// Sentry actually sees must differ between two processes that both start their
// cycle counter at 1, because findBatch queries a 1h window and the previous
// run's events would otherwise fill the page and hide the new run's own.
func TestQualifyBatchID(t *testing.T) {
	if got := qualifyBatchID("abc", "trace_ingestion", 1); got != "abc-trace_ingestion-1" {
		t.Errorf("qualifyBatchID = %q, want %q", got, "abc-trace_ingestion-1")
	}
	// Pure: same inputs, same output.
	if a, b := qualifyBatchID("abc", "trace_ingestion", 1), qualifyBatchID("abc", "trace_ingestion", 1); a != b {
		t.Errorf("qualifyBatchID is not deterministic: %q != %q", a, b)
	}
	// Different runs, same cycle 1: must not collide.
	if a, b := qualifyBatchID("run1", "trace_ingestion", 1), qualifyBatchID("run2", "trace_ingestion", 1); a == b {
		t.Errorf("cycle 1 of two runs shares a batch id %q: a restart would query the previous run's events", a)
	}
	// The run component must not swallow the per-cycle distinctions.
	seen := map[string]bool{}
	for _, run := range []string{"run1", "run2"} {
		for _, signal := range []string{"trace_ingestion", "error_ingestion", "span_completeness"} {
			for cycle := 1; cycle <= 3; cycle++ {
				id := qualifyBatchID(run, signal, cycle)
				if seen[id] {
					t.Errorf("duplicate batch id %q", id)
				}
				seen[id] = true
			}
		}
	}
	if len(seen) != 18 {
		t.Errorf("got %d distinct ids, want 18", len(seen))
	}
}

func TestNewRunID(t *testing.T) {
	base := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	// Pure in its argument: the clock is the caller's, so the id is testable.
	if a, b := newRunID(base), newRunID(base); a != b {
		t.Errorf("newRunID is not a function of its argument: %q != %q", a, b)
	}
	if newRunID(base) == "" {
		t.Fatal("newRunID returned an empty id: batch ids would lose their run component")
	}

	// Two process starts must differ. Nanosecond resolution is the point: a
	// container restarted inside the same second must still get a fresh id.
	if a, b := newRunID(base), newRunID(base.Add(time.Nanosecond)); a == b {
		t.Errorf("two starts 1ns apart share run id %q", a)
	}

	// The id is concatenated into a batch id with "-" as the separator, so it
	// must not contain one itself, and it must survive as a Sentry tag value.
	for _, at := range []time.Time{base, base.Add(time.Nanosecond), base.Add(400 * 24 * time.Hour)} {
		id := newRunID(at)
		if strings.ContainsAny(id, "-: ") {
			t.Errorf("newRunID(%s) = %q contains a separator or space", at, id)
		}
	}

	// Later starts sort after earlier ones, so ids order by run in a log or a
	// Sentry tag list. Base36 of a nanosecond count is a fixed 12 characters for
	// the rest of this century, so lexicographic order tracks time order — until
	// the count gains a 13th character around 2120. One century, not centuries;
	// nothing depends on the ordering.
	early, late := newRunID(base), newRunID(base.Add(time.Hour))
	if !(early < late) {
		t.Errorf("run ids not monotonic: %q (earlier) is not < %q (later)", early, late)
	}
}
