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
// A false third return means "not measured", not "0% complete": a failed trace
// lookup, a failed span census, or an empty sample carries no information about
// completeness, and publishing 0 for it would manufacture a total-failure
// reading out of an API hiccup. Callers must suppress the metric rather than
// publish the zero.
func probeSpansBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) (batchResult, float64, bool) {
	res := runTraceBatch(ctx, s, cfg, batchID, spansSignal, "[span_completeness]", expectedSpans)

	// Sampled completeness, in two requests regardless of batch size: one to
	// list the traces that arrived, one to count their child spans. The batch
	// key must be the same one runTraceBatch sent and queried under, so it comes
	// from the same signal.
	arrived, err := s.findTraceBatch(batchKeyFor(batchID, spansSignal), cfg.batchSize)
	if err != nil {
		log.Printf("[span_completeness] arrived traces: %v", err)
		return res, 0, false
	}

	// cfg.spanSample bounds the census, not the batch: it caps URL length (33
	// characters per trace id in the trace:[...] filter) and preserves the
	// documented meaning of SPAN_COMPLETENESS_SAMPLE. Completeness stays a
	// sample by design.
	sampled := sampleTraceIDs(arrived, cfg.spanSample)
	counts, err := s.findBatchSpanCounts(sampled, len(sampled))
	if err != nil {
		// The census failed, so no sampled trace has a known child-span count.
		// Unknown, not zero.
		log.Printf("[span_completeness] span counts: %v", err)
		return res, 0, false
	}
	pct, known := completenessPct(sampled, counts, expectedSpans)
	return res, pct, known
}

// sampleTraceIDs takes up to n trace ids from the arrived set. Map iteration
// order is randomised by the runtime and trace ids are random independently of
// how Sentry handled them, so the first n are an unbiased sample.
func sampleTraceIDs(arrived map[string]bool, n int) []string {
	if n > len(arrived) {
		n = len(arrived)
	}
	if n <= 0 {
		return nil
	}
	out := make([]string, 0, n)
	for id := range arrived {
		if len(out) == n {
			break
		}
		out = append(out, id)
	}
	return out
}

// completenessPct returns the percentage of sampled traces that arrived
// complete, and whether the percentage is known at all.
//
// Completeness is all-or-nothing per trace: a trace counts only when Sentry
// stored at least every child span the probe sent, so 100 traces each losing
// one span of five read as 0%, not 80%. A sampled trace missing from counts had
// no child spans stored — incomplete, and safe to score as such because the
// caller only reaches here when the census itself succeeded.
func completenessPct(sampled []string, counts map[string]int, expected int) (float64, bool) {
	if len(sampled) == 0 {
		// Nothing sampled: the batch may simply not have arrived. 0% here would
		// read as Sentry dropping every span it was sent.
		return 0, false
	}
	complete := 0
	for _, id := range sampled {
		if counts[id] >= expected {
			complete++
		}
	}
	return float64(complete) / float64(len(sampled)) * 100, true
}
