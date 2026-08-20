package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
)

// probeResult is the outcome of a single-event probe. Still used by probeAlert,
// which measures alert-rule firing latency from one error event rather than a
// batch.
type probeResult struct {
	traceID string
	latency time.Duration
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

// batchFlushTimeout bounds how long a batch send waits for the transport to
// drain. Matches the legacy single-probe senders.
const batchFlushTimeout = 10 * time.Second

// Batch signals. Each trace probe namespaces the caller's per-cycle batch ID
// with its own signal (see batchKeyFor) so the two never share a probe_batch
// tag value.
const (
	traceSignal = "trace"
	spansSignal = "spans"
)

// traceProbeSpans is the child-span count for the trace-ingestion probe. It is
// deliberately its own constant rather than expectedSpans: the completeness
// probe treats expectedSpans as the threshold an arrived transaction must meet,
// so tying the ingestion probe's payload to it would make one number mean two
// different things. Changing this value does not affect span_completeness —
// the two probes run under disjoint batch keys (batchKeyFor).
const traceProbeSpans = 4

// batchKeyFor namespaces a cycle's batch ID per signal. Callers hand every
// probe the same batchID, so without this the trace-ingestion and
// span-completeness probes would tag and query the identical probe_batch value:
// each probe's findBatch would then match both probes' events (2x batchSize
// rows) against a per_page of batchSize and see only about half of its own
// events, reporting ~50% success_rate on both signals against a healthy Sentry.
// The two probes also send different span counts, so a shared pool would drag
// completeness to ~50% as well. Derived inside the probes, never passed in, so
// a caller cannot get it wrong.
func batchKeyFor(batchID, signal string) string { return batchID + "-" + signal }

// sendTraceTagged sends one probe transaction with n child spans, tagged for
// batch identification, using the supplied client. Returns the trace id.
//
// The returned id is the batch's lookup key, so it must match byte-for-byte
// what findBatch reads out of the "trace" field of a Discover row. Both sides
// are 32 lowercase hex characters with no separators: TraceID.String() is
// hex.Encode of the raw 16 bytes, and Sentry's "trace" field is the same
// encoding. Do not reformat either side without changing the other.
func sendTraceTagged(client *sentry.Client, batchKey string, seq, n int) (string, time.Time, error) {
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.Scope().SetTag("probe_batch", batchKey)
	hub.Scope().SetTag("probe_seq", strconv.Itoa(seq))
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	span := sentry.StartTransaction(ctx, "probe.login",
		sentry.WithOpName("slo.probe"),
		sentry.WithDescription("Synthetic login probe for SLO measurement"),
	)
	// Belt and braces. The scope tag alone is enough for findBatch: Span.Finish
	// captures the transaction through the hub on the context, and
	// Scope.ApplyToEvent merges scope tags into the event's tags — which is
	// exactly what probeErrorBatch relies on. Setting it on the span too keeps
	// the tag on the transaction itself, so it survives even if the transaction
	// is ever finished without this hub on its context.
	span.SetTag("probe_batch", batchKey)
	for i := 0; i < n; i++ {
		child := span.StartChild(fmt.Sprintf("probe.span.%d", i))
		sleep(5, 20)
		child.Status = sentry.SpanStatusOK
		child.Finish()
	}
	span.Finish()
	traceID := span.TraceID.String()
	// Stamp before the flush, as the retired single-event sender did:
	// PROBE_BATCH_SIZE=1 must
	// reproduce single-event behaviour, and a post-flush stamp would report a
	// systematically smaller latency for the identical event. Flush also drains
	// the whole shared client buffer, so a worker whose own event left at t=0
	// but whose Flush waits on a peer until t=0.5s would stamp 0.5s and
	// under-report — the metric would turn optimistic exactly when Sentry is
	// degrading.
	sentAt := time.Now()
	if !client.Flush(batchFlushTimeout) {
		// Flush reports false when the transport did not drain in time, so the
		// event may never have left the process. Returning an error (rather than
		// logging and reporting success) keeps sent honest: runBatch skips
		// markSent on error and observe only credits ids already in sentAt, so
		// the event leaves BOTH numerator and denominator. The sample shrinks
		// but success_rate stays unbiased, and the shortfall stays visible as
		// the gap between sent and PROBE_BATCH_SIZE.
		return "", time.Time{}, fmt.Errorf("flush transaction %s: transport did not drain within %s", traceID, batchFlushTimeout)
	}
	return traceID, sentAt, nil
}

// runTraceBatch sends a paced batch of tagged probe transactions, each with
// spans child spans, and measures ingestion latency + reliability by trace id.
// Shared by probeBatch and probeSpansBatch so send/query stay in lockstep: the
// probe_batch tag written when sending and the probe_batch filter used when
// querying are both batchKeyFor(batchID, signal), derived once here.
func runTraceBatch(ctx context.Context, s *sentryProbe, cfg config, batchID, signal, logPrefix string, spans int) batchResult {
	batchKey := batchKeyFor(batchID, signal)
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("%s client: %v", logPrefix, err)
		// sent counts sends that actually succeeded. Nothing left the process,
		// so sent stays 0 — reporting cfg.batchSize here would read on the
		// dashboard as Sentry dropping a full batch it never received.
		return batchResult{}
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return sendTraceTagged(client, batchKey, seq, spans)
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findTraceBatch(batchKey, cfg.batchSize)
	}
	return runBatch(ctx, batchConfigFrom(cfg), send, query)
}

// probeBatch sends a paced batch of probe transactions and measures ingestion
// latency + reliability by trace id.
func probeBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) batchResult {
	return runTraceBatch(ctx, s, cfg, batchID, traceSignal, "[trace_ingestion]", traceProbeSpans)
}
