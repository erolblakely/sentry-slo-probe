package main

import (
	"fmt"
	"time"
)

// probeError sends a Sentry error event, polls until it appears in the Issues
// API, and returns the measured ingestion latency.
func probeError(s *sentryProbe, timeout, pollInterval time.Duration) (*probeResult, error) {
	probeID, sentAt, err := s.sendError()
	if err != nil {
		return nil, fmt.Errorf("send error: %w", err)
	}

	if err := pollUntil(timeout, pollInterval, func() (bool, error) {
		return s.errorExists(probeID)
	}); err != nil {
		return nil, fmt.Errorf("error probe %s: %w", probeID, err)
	}

	return &probeResult{traceID: probeID, latency: time.Since(sentAt)}, nil
}
