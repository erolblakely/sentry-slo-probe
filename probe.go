package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

type probeResult struct {
	traceID string
	latency time.Duration
}

// probe measures Sentry trace ingestion latency.
func probe(ctx context.Context, s *sentryProbe, timeout, pollInterval time.Duration) (*probeResult, error) {
	ctx, span := tracer.Start(ctx, "probe.trace_ingestion")
	defer span.End()

	var sentAt time.Time
	traceID, err := timedSpan(ctx, "sentry.send_trace", func() (string, error) {
		var id string
		var err error
		id, sentAt, err = s.sendTrace()
		return id, err
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("send trace: %w", err)
	}
	span.SetAttributes(attribute.String("sentry.trace_id", traceID))

	attempts, err := pollWithSpan(ctx, "sentry.await_ingestion", timeout, pollInterval, func() (bool, error) {
		return s.traceExists(traceID)
	})
	span.SetAttributes(attribute.Int("sentry.poll_attempts", attempts))
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, fmt.Errorf("trace %s: %w", traceID, err)
	}

	latency := time.Since(sentAt)
	span.SetAttributes(attribute.Int64("sentry.ingestion_latency_ms", latency.Milliseconds()))
	span.SetStatus(codes.Ok, "")
	return &probeResult{traceID: traceID, latency: latency}, nil
}

type sentryProbe struct {
	dsn       string
	authToken string
	org       string
	project   string
	baseURL   string
}

func newSentryProbe(dsn, authToken, org, project string) *sentryProbe {
	return &sentryProbe{dsn: dsn, authToken: authToken, org: org, project: project, baseURL: "https://sentry.io"}
}

func (s *sentryProbe) sendTrace() (traceID string, sentAt time.Time, err error) {
	return s.sendTraceWithSpans(4)
}

func (s *sentryProbe) sendTraceWithSpans(n int) (traceID string, sentAt time.Time, err error) {
	client, clientErr := sentry.NewClient(sentry.ClientOptions{
		Dsn:              s.dsn,
		EnableTracing:    true, // required in sentry-go v0.x; without it transactions are dropped (SampledFalse)
		TracesSampleRate: 1.0,
		Environment:      "probe",
		Release:          "sentry-slo-probe@1.0.0",
	})
	if clientErr != nil {
		return "", time.Time{}, fmt.Errorf("sentry client: %w", clientErr)
	}

	hub := sentry.NewHub(client, sentry.NewScope())
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	span := sentry.StartTransaction(ctx, "probe.login",
		sentry.WithOpName("slo.probe"),
		sentry.WithDescription("Synthetic login probe for SLO measurement"),
	)
	for i := 0; i < n; i++ {
		child := span.StartChild(fmt.Sprintf("probe.span.%d", i),
			sentry.WithDescription(fmt.Sprintf("Probe child span %d", i)),
		)
		sleep(5, 20)
		child.Status = sentry.SpanStatusOK
		child.Finish()
	}
	span.Finish()
	traceID = span.TraceID.String()
	sentAt = time.Now()
	client.Flush(10 * time.Second)
	return traceID, sentAt, nil
}

func (s *sentryProbe) sendError() (probeID string, sentAt time.Time, err error) {
	client, clientErr := sentry.NewClient(sentry.ClientOptions{
		Dsn:         s.dsn,
		Environment: "probe",
		Release:     "sentry-slo-probe@1.0.0",
	})
	if clientErr != nil {
		return "", time.Time{}, fmt.Errorf("sentry client: %w", clientErr)
	}

	probeID = fmt.Sprintf("slo-probe-%d", time.Now().UnixNano())
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.Scope().SetTag("probe_id", probeID)
	hub.CaptureMessage("SLO error probe: " + probeID)
	sentAt = time.Now()
	client.Flush(10 * time.Second)
	return probeID, sentAt, nil
}

// pollUntil retries checkFn every pollInterval until it returns true or the
// context deadline is exceeded. Returns the number of attempts made.
func pollUntil(ctx context.Context, timeout, pollInterval time.Duration, checkFn func() (bool, error)) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	attempts := 0
	var lastErr error
	for {
		select {
		case <-ctx.Done():
			if lastErr != nil {
				return attempts, fmt.Errorf("timed out after %s (%d attempts); last error: %w", timeout, attempts, lastErr)
			}
			return attempts, fmt.Errorf("timed out after %s (%d attempts)", timeout, attempts)
		case <-time.After(pollInterval):
			attempts++
			found, err := checkFn()
			if err != nil {
				lastErr = err
				// A permanent client error (bad auth, missing scope, bad
				// request) will never succeed — stop instead of polling to
				// the deadline and hiding it behind a timeout.
				var apiErr *apiError
				if errors.As(err, &apiErr) && apiErr.permanent() {
					return attempts, fmt.Errorf("permanent error after %d attempts: %w", attempts, err)
				}
				continue
			}
			if found {
				return attempts, nil
			}
		}
	}
}

// pollWithSpan wraps pollUntil in an OTel span.
func pollWithSpan(ctx context.Context, spanName string, timeout, pollInterval time.Duration, checkFn func() (bool, error)) (int, error) {
	ctx, span := tracer.Start(ctx, spanName)
	defer span.End()
	attempts, err := pollUntil(ctx, timeout, pollInterval, checkFn)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	return attempts, err
}

// timedSpan runs fn inside a new child span, returning its result.
func timedSpan(ctx context.Context, spanName string, fn func() (string, error)) (string, error) {
	_, span := tracer.Start(ctx, spanName)
	defer span.End()
	result, err := fn()
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	return result, err
}

// newTracingClient builds a client for transaction probes. EnableTracing is
// required in sentry-go v0.x; without it transactions are dropped
// (SampledFalse).
func newTracingClient(dsn string) (*sentry.Client, error) {
	return sentry.NewClient(sentry.ClientOptions{
		Dsn:              dsn,
		EnableTracing:    true,
		TracesSampleRate: 1.0,
		Environment:      "probe",
		Release:          "sentry-slo-probe@1.0.0",
	})
}

// sendTraceTagged sends one probe transaction with n child spans, tagged for
// batch identification, using the supplied client. Returns the trace id.
//
// The returned id is the batch's lookup key, so it must match byte-for-byte
// what findBatch reads out of the "trace" field of a Discover row. Both sides
// are 32 lowercase hex characters with no separators: TraceID.String() is
// hex.Encode of the raw 16 bytes, and Sentry's "trace" field is the same
// encoding. Do not reformat either side without changing the other.
func sendTraceTagged(client *sentry.Client, batchID string, seq, n int) (string, time.Time, error) {
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.Scope().SetTag("probe_batch", batchID)
	hub.Scope().SetTag("probe_seq", strconv.Itoa(seq))
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	span := sentry.StartTransaction(ctx, "probe.login",
		sentry.WithOpName("slo.probe"),
		sentry.WithDescription("Synthetic login probe for SLO measurement"),
	)
	// The transaction carries the tag itself as well as via the scope: findBatch
	// filters on probe_batch, and a scope tag alone is not guaranteed to land on
	// the transaction event.
	span.SetTag("probe_batch", batchID)
	for i := 0; i < n; i++ {
		child := span.StartChild(fmt.Sprintf("probe.span.%d", i))
		sleep(5, 20)
		child.Status = sentry.SpanStatusOK
		child.Finish()
	}
	span.Finish()
	traceID := span.TraceID.String()
	client.Flush(10 * time.Second)
	return traceID, time.Now(), nil
}

// probeBatch sends a paced batch of probe transactions and measures ingestion
// latency + reliability by trace id.
func probeBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) batchResult {
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("[trace_ingestion] client: %v", err)
		// sent counts sends that actually succeeded. Nothing left the process,
		// so sent stays 0 — reporting cfg.batchSize here would read on the
		// dashboard as Sentry dropping a full batch it never received.
		return batchResult{}
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return sendTraceTagged(client, batchID, seq, 4)
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("transactions", batchID, "trace", cfg.batchSize)
	}
	return runBatch(ctx, batchConfigFrom(cfg), send, query)
}
