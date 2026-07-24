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
