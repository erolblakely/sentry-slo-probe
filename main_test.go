package main

import (
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
// counter, and the peak assertion fails on a lost update.
func TestInflightCapConcurrent(t *testing.T) {
	const max = 2
	c := newInflightCap(max)

	var mu sync.Mutex
	live, peak := 0, 0

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !c.try() {
				return
			}
			mu.Lock()
			live++
			if live > peak {
				peak = live
			}
			mu.Unlock()

			mu.Lock()
			live--
			mu.Unlock()
			c.done()
		}()
	}
	wg.Wait()

	if peak > max {
		t.Errorf("peak concurrent holders = %d, want <= %d", peak, max)
	}
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
	// Sentry tag list. Base36 of a nanosecond count keeps a fixed width for
	// centuries, so lexicographic order tracks time order.
	early, late := newRunID(base), newRunID(base.Add(time.Hour))
	if !(early < late) {
		t.Errorf("run ids not monotonic: %q (earlier) is not < %q (later)", early, late)
	}
}
