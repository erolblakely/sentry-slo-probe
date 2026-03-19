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
func probe(s *sentryProbe, timeout, pollInterval time.Duration) (*probeResult, error) {
	traceID, sentAt, err := s.sendTrace()
	if err != nil {
		return nil, fmt.Errorf("send trace: %w", err)
	}
	if err := pollUntil(timeout, pollInterval, func() (bool, error) {
		return s.traceExists(traceID)
	}); err != nil {
		return nil, fmt.Errorf("trace %s: %w", traceID, err)
	}
	return &probeResult{traceID: traceID, latency: time.Since(sentAt)}, nil
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

// sendTrace sends one transaction with child spans and returns the trace ID and send time.
func (s *sentryProbe) sendTrace() (traceID string, sentAt time.Time, err error) {
	return s.sendTraceWithSpans(4)
}

// sendTraceWithSpans sends a transaction with exactly n child spans.
func (s *sentryProbe) sendTraceWithSpans(n int) (traceID string, sentAt time.Time, err error) {
	client, clientErr := sentry.NewClient(sentry.ClientOptions{
		Dsn:              s.dsn,
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

// sendError sends a captured message event with a unique probe ID tag.
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

// pollUntil retries checkFn every pollInterval until it returns true or timeout elapses.
func pollUntil(timeout, pollInterval time.Duration, checkFn func() (bool, error)) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out after %s", timeout)
		case <-time.After(pollInterval):
			found, err := checkFn()
			if err != nil {
				continue // transient — keep polling
			}
			if found {
				return nil
			}
		}
	}
}
