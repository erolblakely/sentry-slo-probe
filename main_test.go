package main

import (
	"sync"
	"testing"
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

func TestBatchID(t *testing.T) {
	// Deterministic: no wall clock, no randomness, so the same inputs always
	// produce the same id.
	if got := batchID("trace_ingestion", 7); got != "trace_ingestion-7" {
		t.Errorf("batchID(trace_ingestion, 7) = %q, want %q", got, "trace_ingestion-7")
	}
	if a, b := batchID("trace_ingestion", 1), batchID("trace_ingestion", 1); a != b {
		t.Errorf("batchID is not deterministic: %q != %q", a, b)
	}

	// Distinct per cycle and per signal. Two probe signals firing the same cycle
	// must not share a batch id, or their Discover queries would match each
	// other's events.
	seen := map[string]bool{}
	for _, signal := range []string{"trace_ingestion", "error_ingestion", "span_completeness"} {
		for cycle := 1; cycle <= 3; cycle++ {
			id := batchID(signal, cycle)
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
