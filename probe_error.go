package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
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

// probeErrorBatch sends a paced batch of probe messages and measures ingestion
// latency + reliability by per-event sequence number.
func probeErrorBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) batchResult {
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:         s.dsn,
		Environment: "probe",
		Release:     "sentry-slo-probe@1.0.0",
	})
	if err != nil {
		log.Printf("[error_ingestion] client: %v", err)
		// sent counts sends that actually succeeded. Nothing left the process,
		// so sent stays 0 — reporting cfg.batchSize here would read on the
		// dashboard as Sentry dropping a full batch it never received.
		return batchResult{}
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		// seqID is the batch's lookup key, so it must match byte-for-byte what
		// findBatch reads out of the "probe_seq" field of a Discover row. That
		// side decodes JSON into any and formats with fmt.Sprint, so a numeric
		// probe_seq arrives as float64: fmt.Sprint yields "0", "1", ... and
		// agrees with strconv.Itoa only below 1e6, above which Go switches to
		// "1e+06" formatting. Safe while batch sizes stay far below that; a
		// seq that could reach 1e6 needs a non-numeric key instead.
		seqID := strconv.Itoa(seq)
		hub := sentry.NewHub(client, sentry.NewScope())
		hub.Scope().SetTag("probe_batch", batchID)
		hub.Scope().SetTag("probe_seq", seqID)
		hub.CaptureMessage(fmt.Sprintf("SLO error probe batch %s seq %s", batchID, seqID))
		client.Flush(10 * time.Second)
		return seqID, time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("errors", batchID, "probe_seq", cfg.batchSize)
	}
	return runBatch(ctx, batchConfigFrom(cfg), send, query)
}
