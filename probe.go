package main

import (
	"context"
	"fmt"
	"time"

	"github.com/getsentry/sentry-go"
)

type probeResult struct {
	traceID string
	latency time.Duration
}

// probe sends a transaction to Sentry, then polls until it appears.
// Returns the measured ingestion latency.
func probe(s *sentryProbe, timeout, pollInterval time.Duration) (*probeResult, error) {
	traceID, sentAt, err := s.sendTrace()
	if err != nil {
		return nil, fmt.Errorf("send trace: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timed out waiting for trace %s to appear in Sentry (timeout=%s)", traceID, timeout)
		case <-time.After(pollInterval):
			found, err := s.traceExists(traceID)
			if err != nil {
				// Log but keep polling — transient API errors are common
				continue
			}
			if found {
				return &probeResult{
					traceID: traceID,
					latency: time.Since(sentAt),
				}, nil
			}
		}
	}
}

type sentryProbe struct {
	dsn       string
	authToken string
	org       string
	project   string
}

func newSentryProbe(dsn, authToken, org, project string) *sentryProbe {
	return &sentryProbe{dsn: dsn, authToken: authToken, org: org, project: project}
}

// sendTrace initialises a fresh Sentry client, sends one transaction with
// child spans, flushes it, then returns the trace ID and the time it was sent.
func (s *sentryProbe) sendTrace() (traceID string, sentAt time.Time, err error) {
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:              s.dsn,
		TracesSampleRate: 1.0,
		Environment:      "probe",
		Release:          "sentry-slo-probe@1.0.0",
	})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sentry client: %w", err)
	}

	hub := sentry.NewHub(client, sentry.NewScope())
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	span := sentry.StartTransaction(ctx, "probe.login",
		sentry.WithOpName("slo.probe"),
		sentry.WithDescription("Synthetic login probe for SLO measurement"),
	)

	// Simulate a login flow so the trace has realistic child spans
	simulateProbeLogin(span)

	span.Finish()
	traceID = span.TraceID.String()
	sentAt = time.Now()

	// Block until the SDK has delivered the envelope
	client.Flush(10 * time.Second)

	return traceID, sentAt, nil
}

func simulateProbeLogin(parent *sentry.Span) {
	steps := []struct {
		op      string
		desc    string
		minMs   int
		maxMs   int
	}{
		{"validate.input", "Validate email format", 5, 15},
		{"db.query", "SELECT user by email", 30, 80},
		{"auth.verify_password", "bcrypt comparison", 60, 120},
		{"auth.generate_token", "Generate session token", 10, 30},
	}

	for _, step := range steps {
		child := parent.StartChild(step.op, sentry.WithDescription(step.desc))
		sleep(step.minMs, step.maxMs)
		child.Status = sentry.SpanStatusOK
		child.Finish()
	}
}
