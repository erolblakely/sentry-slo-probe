package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// The OTLP stream sends the same probe trace as the Sentry SDK stream, but
// built with the OpenTelemetry SDK and exported over OTLP/HTTP straight to
// Sentry's OTLP traces endpoint, a separate ingest path from the SDK's
// envelope endpoint. Running both side by side shows whether the two paths
// differ in latency, loss or span completeness. Sentry documents direct OTLP
// trace ingestion as open beta.
//
// This is unrelated to tracer.go, which exports the probe's own (currently
// empty) OTel instrumentation to Datadog through the global provider. The OTLP
// stream uses a private TracerProvider and never touches the global one.

// otlpEndpointFromDSN derives Sentry's OTLP traces endpoint and public key
// from a DSN. The path mirrors the SDK's envelope URL (scheme, host, any path
// prefix, then /api/<project>/) with the suffix Sentry documents for OTLP:
// integration/otlp/v1/traces. The key authenticates via the x-sentry-auth
// header.
func otlpEndpointFromDSN(dsn string) (endpoint, key string, err error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return "", "", fmt.Errorf("parse DSN for OTLP: not a URL")
	}
	key = u.User.Username()
	if key == "" {
		return "", "", fmt.Errorf("parse DSN for OTLP: no public key")
	}
	path := strings.Trim(u.Path, "/")
	prefix, project := "", path
	if i := strings.LastIndex(path, "/"); i >= 0 {
		prefix, project = "/"+path[:i], path[i+1:]
	}
	if project == "" {
		return "", "", fmt.Errorf("parse DSN for OTLP: no project id")
	}
	return fmt.Sprintf("%s://%s%s/api/%s/integration/otlp/v1/traces", u.Scheme, u.Host, prefix, project), key, nil
}

// otlpTraceIDs returns trace id -> number of spans for every trace in an
// OTLP/HTTP protobuf export request, without consuming the request body. The
// send check keys OTLP outcomes by trace and requires all of a trace's spans to
// have been accepted. Returns nil if the body cannot be decoded, which makes
// the send unconfirmable and so a failure, never a silent success.
func otlpTraceIDs(r *http.Request) map[string]int {
	if r.GetBody == nil {
		return nil
	}
	body, err := r.GetBody()
	if err != nil {
		return nil
	}
	defer body.Close()
	var reader io.Reader = io.LimitReader(body, 16<<20)
	if r.Header.Get("Content-Encoding") == "gzip" {
		zr, err := gzip.NewReader(reader)
		if err != nil {
			return nil
		}
		defer zr.Close()
		reader = zr
	}
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil
	}
	var req coltracepb.ExportTraceServiceRequest
	if proto.Unmarshal(raw, &req) != nil {
		return nil
	}
	spans := map[string]int{}
	for _, rs := range req.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				if len(sp.TraceId) > 0 {
					spans[hex.EncodeToString(sp.TraceId)]++
				}
			}
		}
	}
	return spans
}

// otlpPartialSuccess reads a 2xx OTLP response for spans Sentry rejected
// (partial_success.rejected_spans). Rejected spans are Sentry explicitly
// refusing them, as with a 4xx, so the request does not count as sent. The
// response does not say which spans were rejected, so every trace in the
// request is excluded rather than charged to Sentry. A message with nothing
// rejected is a warning and is only logged. The body is restored for the
// exporter, which reads it too.
func otlpPartialSuccess(resp *http.Response) error {
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	if err != nil || len(raw) == 0 {
		return nil
	}
	var out coltracepb.ExportTraceServiceResponse
	if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		err = protojson.Unmarshal(raw, &out)
	} else {
		err = proto.Unmarshal(raw, &out)
	}
	ps := out.GetPartialSuccess()
	if err != nil || ps == nil {
		return nil
	}
	if ps.RejectedSpans > 0 {
		return fmt.Errorf("sentry accepted the request but rejected %d spans: %s", ps.RejectedSpans, ps.ErrorMessage)
	}
	if ps.ErrorMessage != "" {
		log.Printf("[%s] sentry accepted every span with a warning: %s", traceStreamOTLPSignal, ps.ErrorMessage)
	}
	return nil
}

// otlpSender sends probe traces over OTLP. One per stream, shared by its send
// workers.
type otlpSender struct {
	tp        *sdktrace.TracerProvider
	tracer    trace.Tracer
	exporter  sdktrace.SpanExporter
	collected *traceCollector
	delivery  *deliveryLog
}

// newOTLPSender builds an OTLP sender for the project in dsn, counting
// unanswered requests and retries into stats.
func newOTLPSender(dsn string, stats *deliveryStats) (*otlpSender, error) {
	return newOTLPSenderWith(dsn, stats, sdktrace.AlwaysSample())
}

// newOTLPSenderWith is newOTLPSender with the sampler as a parameter. The probe
// always samples everything; the parameter lets a test stand in for spans lost
// on our side.
//
// Each trace is exported in its own request, directly, not through a batch
// span processor. A processor flushed per send runs its exports one at a time
// under a lock, a few traces each, so a slow Sentry built a queue on our side
// whose wait was measured as Sentry's latency, and the slowest sends fell out
// of the sample. One request per trace, concurrently, mirrors the SDK stream's
// client per send: neither path can queue behind itself, and each send's
// outcome is its own request's.
//
// The exporter's retries are off, as the Sentry SDK has none, so a request's
// outcome is final. The HTTP client's timeout, streamClientTimeout, sits beyond
// the delivery log's answer timer, which is what decides an unanswered request.
// (otlptracehttp's WithTimeout applies only to a client it builds itself, so it
// is not set.)
//
// Concurrent sends share one exporter. otlptracehttp v1.42's ExportSpans takes
// no lock and builds a fresh request per call, so that is safe, but it is that
// implementation's behaviour, not a SpanExporter guarantee: the -race barrier
// test (TestOTLPSenderDoesNotQueueBehindASlowSentry) guards it across upgrades.
func newOTLPSenderWith(dsn string, stats *deliveryStats, sampler sdktrace.Sampler) (*otlpSender, error) {
	endpoint, key, err := otlpEndpointFromDSN(dsn)
	if err != nil {
		return nil, err
	}
	d := newDeliveryLog(streamTransport, otlpTraces, streamDeliveryPolicy(stats))
	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(map[string]string{"x-sentry-auth": "sentry sentry_key=" + key}),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
		otlptracehttp.WithHTTPClient(&http.Client{Transport: d, Timeout: streamClientTimeout}),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	c := &traceCollector{spans: make(map[trace.TraceID][]sdktrace.ReadOnlySpan)}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sampler),
		sdktrace.WithSpanProcessor(c),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "sentry-slo-probe"))),
	)
	return &otlpSender{tp: tp, tracer: tp.Tracer("sentry-slo-probe"), exporter: exp, collected: c, delivery: d}, nil
}

// send sends one probe trace (a probe.login root with n child spans, the same
// shape as sendTraceTagged) in one export request, and returns its trace id and
// send stamp. It returns once that request has been answered or abandoned, so
// nothing it started outlives it.
//
// The trace counts as sent only if all n+1 spans were accepted
// (deliveryLog.confirm), which also catches spans lost on our side before the
// export. The exporter's own error does not decide the send: it reports a
// Sentry 5xx, which counts as sent. It is logged for the detail it carries
// (such as a 4xx body).
func (o *otlpSender) send(batchKey string, seq, n int) (string, time.Time, error) {
	ctx, root := o.tracer.Start(context.Background(), "probe.login",
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("probe_batch", batchKey),
			attribute.String("probe_seq", strconv.Itoa(seq)),
		))
	for i := 0; i < n; i++ {
		_, child := o.tracer.Start(ctx, fmt.Sprintf("probe.span.%d", i))
		sleep(5, 20)
		child.End()
	}
	root.End()
	tid := root.SpanContext().TraceID()
	traceID := tid.String()
	spans := o.collected.take(tid)
	// Stamped just before the request goes out, as sendTraceTagged stamps
	// before its flush, so both streams measure from the same point.
	sentAt := time.Now()
	if err := o.exporter.ExportSpans(context.Background(), spans); err != nil {
		log.Printf("[%s] export trace %s: %v", traceStreamOTLPSignal, traceID, err)
	}
	if err := o.delivery.confirm(traceID, n+1, "otlp trace"); err != nil {
		return "", time.Time{}, err
	}
	return traceID, sentAt, nil
}

// Close shuts down the sender's provider and exporter.
func (o *otlpSender) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = o.tp.Shutdown(ctx)
	_ = o.exporter.Shutdown(ctx)
}

// traceCollector is a span processor that holds each trace's ended spans until
// its send takes them for export, so one trace goes out as one request.
type traceCollector struct {
	mu    sync.Mutex
	spans map[trace.TraceID][]sdktrace.ReadOnlySpan
}

func (c *traceCollector) OnStart(context.Context, sdktrace.ReadWriteSpan) {}

func (c *traceCollector) OnEnd(s sdktrace.ReadOnlySpan) {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := s.SpanContext().TraceID()
	c.spans[id] = append(c.spans[id], s)
}

// take returns and forgets the ended spans of trace id.
func (c *traceCollector) take(id trace.TraceID) []sdktrace.ReadOnlySpan {
	c.mu.Lock()
	defer c.mu.Unlock()
	spans := c.spans[id]
	delete(c.spans, id)
	return spans
}

func (c *traceCollector) Shutdown(context.Context) error   { return nil }
func (c *traceCollector) ForceFlush(context.Context) error { return nil }
