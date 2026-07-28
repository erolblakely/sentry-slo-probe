package main

import (
	"math"
	"sort"
	"time"
)

// percentile returns the nearest-rank percentile p (0-100) of latencies.
// It returns 0 for empty input and does not mutate the input slice.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// sendOffsets returns size offsets from the batch start, evenly spaced across
// window with step window/size (so the last send leaves headroom before the
// window closes). Returns nil for size <= 0.
func sendOffsets(size int, window time.Duration) []time.Duration {
	if size <= 0 {
		return nil
	}
	offsets := make([]time.Duration, size)
	step := window / time.Duration(size)
	for i := range offsets {
		offsets[i] = time.Duration(i) * step
	}
	return offsets
}

// batchResult is the outcome of one batch run.
type batchResult struct {
	sent      int
	received  int
	latencies []time.Duration
}

// latencyTracker records per-id send times and, on each poll, the latency of
// ids seen for the first time. Not safe for concurrent use; callers guard it.
type latencyTracker struct {
	sentAt    map[string]time.Time
	latencies map[string]time.Duration
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{
		sentAt:    make(map[string]time.Time),
		latencies: make(map[string]time.Duration),
	}
}

func (t *latencyTracker) markSent(id string, at time.Time) {
	t.sentAt[id] = at
}

func (t *latencyTracker) observe(found map[string]bool, now time.Time) int {
	n := 0
	for id := range found {
		if _, done := t.latencies[id]; done {
			continue
		}
		if sent, ok := t.sentAt[id]; ok {
			t.latencies[id] = now.Sub(sent)
			n++
		}
	}
	return n
}

func (t *latencyTracker) receivedCount() int { return len(t.latencies) }

func (t *latencyTracker) result(sent int) batchResult {
	out := make([]time.Duration, 0, len(t.latencies))
	for _, d := range t.latencies {
		out = append(out, d)
	}
	return batchResult{sent: sent, received: len(t.latencies), latencies: out}
}

// batchDone reports whether polling should stop: either every sent event has
// been received, or we are past the drain deadline (lastSendAt + pollTimeout).
func batchDone(received, size int, now, lastSendAt time.Time, pollTimeout time.Duration) bool {
	if received >= size {
		return true
	}
	return now.After(lastSendAt.Add(pollTimeout))
}
