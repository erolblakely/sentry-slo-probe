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
