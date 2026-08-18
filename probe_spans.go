package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const expectedSpans = 5

type spanResult struct {
	traceID       string
	sentSpans     int
	receivedSpans int
	pct           float64
	latency       time.Duration
}

// probeSpans measures span completeness — what % of sent spans actually arrived.
func probeSpans(ctx context.Context, s *sentryProbe, timeout, pollInterval time.Duration) (*spanResult, error) {
	ctx, span := tracer.Start(ctx, "probe.span_completeness")
	defer span.End()
	span.SetAttributes(attribute.Int("sentry.expected_spans", expectedSpans))

	traceID, sentAt, err := func() (string, time.Time, error) {
		_, s2 := tracer.Start(ctx, "sentry.send_trace")
		defer s2.End()
		id, at, err := s.sendTraceWithSpans(expectedSpans)
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
		return nil, fmt.Errorf("send trace: %w", err)
	}
	span.SetAttributes(attribute.String("sentry.trace_id", traceID))

	var eventID string
	attempts, err := pollWithSpan(ctx, "sentry.await_ingestion", timeout, pollInterval, func() (bool, error) {
		found, id, err := s.traceExistsWithEventID(traceID)
		if found {
			eventID = id
		}
		return found, err
	})
	span.SetAttributes(attribute.Int("sentry.poll_attempts", attempts))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("trace %s: %w", traceID, err)
	}

	var received int
	if _, fetchSpan := tracer.Start(ctx, "sentry.fetch_span_count"); true {
		received, err = s.fetchEventSpanCount(eventID)
		if err != nil {
			fetchSpan.RecordError(err)
			fetchSpan.SetStatus(codes.Error, err.Error())
			fetchSpan.End()
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return nil, fmt.Errorf("fetch span count: %w", err)
		}
		fetchSpan.SetAttributes(attribute.Int("sentry.received_spans", received))
		fetchSpan.SetStatus(codes.Ok, "")
		fetchSpan.End()
	}

	pct := float64(received) / float64(expectedSpans) * 100
	latency := time.Since(sentAt)
	span.SetAttributes(
		attribute.Int("sentry.received_spans", received),
		attribute.Float64("sentry.completeness_pct", pct),
		attribute.Int64("sentry.ingestion_latency_ms", latency.Milliseconds()),
	)
	span.SetStatus(codes.Ok, "")

	return &spanResult{
		traceID:       traceID,
		sentSpans:     expectedSpans,
		receivedSpans: received,
		pct:           pct,
		latency:       latency,
	}, nil
}

// probeSpansBatch runs a batch of transactions and, on a sample of arrived
// transactions, measures span completeness. The second return is the sampled
// completeness percentage.
func probeSpansBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) (batchResult, float64) {
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("[span_completeness] client: %v", err)
		// sent counts sends that actually succeeded. Nothing left the process,
		// so sent stays 0 — reporting cfg.batchSize here would read on the
		// dashboard as Sentry dropping a full batch it never received.
		return batchResult{}, 0
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return sendTraceTagged(client, batchID, seq, expectedSpans)
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("transactions", batchID, "trace", cfg.batchSize)
	}
	res := runBatch(ctx, batchConfigFrom(cfg), send, query)

	// Sampled completeness: fetch span detail for up to spanSample arrived txns.
	// One request per sampled event, so the sample bounds the API cost
	// independently of batch size.
	ids, err := s.findBatchEventIDs(batchID, cfg.batchSize)
	if err != nil {
		log.Printf("[span_completeness] event ids: %v", err)
		return res, 0
	}
	sample, complete := 0, 0
	for _, eventID := range ids {
		if sample >= cfg.spanSample {
			break
		}
		sample++
		n, err := s.fetchEventSpanCount(eventID)
		if err != nil {
			log.Printf("[span_completeness] span count: %v", err)
			continue
		}
		if n >= expectedSpans {
			complete++
		}
	}
	pct := 0.0
	if sample > 0 {
		pct = float64(complete) / float64(sample) * 100
	}
	return res, pct
}
