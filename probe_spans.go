package main

import (
	"context"
	"log"
)

const expectedSpans = 5

// probeSpansBatch runs a batch of transactions and, on a sample of arrived
// transactions, measures span completeness. The second return is the sampled
// completeness percentage; the third reports whether that percentage is known.
//
// A false third return means "not measured", not "0% complete": a failed
// event-id lookup or a sample in which no span count could be fetched carries
// no information about completeness, and publishing 0 for it would manufacture
// a total-failure reading out of an API hiccup. Callers must suppress the
// metric rather than publish the zero.
func probeSpansBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) (batchResult, float64, bool) {
	res := runTraceBatch(ctx, s, cfg, batchID, spansSignal, "[span_completeness]", expectedSpans)

	// Sampled completeness: fetch span detail for up to spanSample arrived txns.
	// One request per sampled event, so the sample bounds the API cost
	// independently of batch size. The batch key must be the same one
	// runTraceBatch sent and queried under, so it comes from the same signal.
	ids, err := s.findBatchEventIDs(batchKeyFor(batchID, spansSignal), cfg.batchSize)
	if err != nil {
		log.Printf("[span_completeness] event ids: %v", err)
		return res, 0, false
	}
	// attempts bounds the API cost at cfg.spanSample requests; sample counts
	// only the fetches that actually returned a span count. A failed fetch
	// therefore shrinks the sample instead of scoring as an incomplete event,
	// which would drag pct down for a reason that has nothing to do with span
	// completeness.
	attempts, sample, complete := 0, 0, 0
	for _, eventID := range ids {
		if attempts >= cfg.spanSample {
			break
		}
		attempts++
		n, err := s.fetchEventSpanCount(eventID)
		if err != nil {
			log.Printf("[span_completeness] span count: %v", err)
			continue
		}
		sample++
		if n >= expectedSpans {
			complete++
		}
	}
	if sample == 0 {
		return res, 0, false
	}
	return res, float64(complete) / float64(sample) * 100, true
}
