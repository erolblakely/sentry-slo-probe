package main

import (
	"fmt"
	"time"
)

const expectedSpans = 5

type spanResult struct {
	traceID       string
	sentSpans     int
	receivedSpans int
	pct           float64
	latency       time.Duration
}

// probeSpans sends a transaction with a known number of child spans, waits for
// it to be ingested, then fetches the event to verify how many spans arrived.
func probeSpans(s *sentryProbe, timeout, pollInterval time.Duration) (*spanResult, error) {
	traceID, sentAt, err := s.sendTraceWithSpans(expectedSpans)
	if err != nil {
		return nil, fmt.Errorf("send trace: %w", err)
	}

	var eventID string
	if err := pollUntil(timeout, pollInterval, func() (bool, error) {
		found, id, err := s.traceExistsWithEventID(traceID)
		if found {
			eventID = id
		}
		return found, err
	}); err != nil {
		return nil, fmt.Errorf("trace %s: %w", traceID, err)
	}

	received, err := s.fetchEventSpanCount(eventID)
	if err != nil {
		return nil, fmt.Errorf("fetch span count: %w", err)
	}

	pct := float64(received) / float64(expectedSpans) * 100

	return &spanResult{
		traceID:       traceID,
		sentSpans:     expectedSpans,
		receivedSpans: received,
		pct:           pct,
		latency:       time.Since(sentAt),
	}, nil
}
