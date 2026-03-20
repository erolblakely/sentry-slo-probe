package main

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// probeError measures Sentry error ingestion latency.
func probeError(ctx context.Context, s *sentryProbe, timeout, pollInterval time.Duration) (*probeResult, error) {
	ctx, span := tracer.Start(ctx, "probe.error_ingestion")
	defer span.End()

	probeID, sentAt, err := func() (string, time.Time, error) {
		_, s2 := tracer.Start(ctx, "sentry.send_error")
		defer s2.End()
		id, at, err := s.sendError()
		if err != nil {
			s2.RecordError(err)
			s2.SetStatus(codes.Error, err.Error())
		} else {
			s2.SetStatus(codes.Ok, "")
		}
		return id, at, err
	}()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("send error: %w", err)
	}
	span.SetAttributes(attribute.String("sentry.probe_id", probeID))

	attempts, err := pollWithSpan(ctx, "sentry.await_ingestion", timeout, pollInterval, func() (bool, error) {
		return s.errorExists(probeID)
	})
	span.SetAttributes(attribute.Int("sentry.poll_attempts", attempts))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("error probe %s: %w", probeID, err)
	}

	latency := time.Since(sentAt)
	span.SetAttributes(attribute.Int64("sentry.ingestion_latency_ms", latency.Milliseconds()))
	span.SetStatus(codes.Ok, "")
	return &probeResult{traceID: probeID, latency: latency}, nil
}
