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
	tp       *sdktrace.TracerProvider
	tracer   trace.Tracer
	delivery *deliveryLog
	// flushTimeout is how long a send waits for its spans to be exported.
	flushTimeout time.Duration
}

// newOTLPSender builds an OTLP sender for the project in dsn.
//
// The exporter's retries are off, as the Sentry SDK has none, so each request's
// outcome is final and deliveryLog classifies it exactly as for the SDK path.
// A batch processor, as a real OTel app would use, flushed after each trace.
// The sampler and batch settings are pinned so OTEL_TRACES_SAMPLER and
// OTEL_BSP_* in the environment cannot change what the probe sends: a sampler
// would silently drop traces, which would read as sends that never left.
func newOTLPSender(dsn string) (*otlpSender, error) {
	endpoint, key, err := otlpEndpointFromDSN(dsn)
	if err != nil {
		return nil, err
	}
	d := newDeliveryLog(nil, otlpTraces)
	exp, err := otlptracehttp.New(context.Background(),
		otlptracehttp.WithEndpointURL(endpoint),
		otlptracehttp.WithHeaders(map[string]string{"x-sentry-auth": "sentry sentry_key=" + key}),
		otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}),
		otlptracehttp.WithHTTPClient(&http.Client{Transport: d, Timeout: batchFlushTimeout}),
	)
	if err != nil {
		return nil, fmt.Errorf("otlp exporter: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithBatcher(exp,
			sdktrace.WithBatchTimeout(5*time.Second),
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithExportTimeout(30*time.Second),
		),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "sentry-slo-probe"))),
	)
	return &otlpSender{tp: tp, tracer: tp.Tracer("sentry-slo-probe"), delivery: d, flushTimeout: batchFlushTimeout}, nil
}

// send sends one probe trace (a probe.login root with n child spans, the same
// shape as sendTraceTagged) and returns its trace id and send stamp.
//
// When ForceFlush returns, every span ended before it has been exported: the
// batch processor moves spans queued ahead of the flush marker into its batch
// before the flush proceeds, and both its own exports and the flush's export
// run under the same batch lock, so whichever requests carried this trace's
// spans have completed and been recorded. The trace counts as sent only if all
// n+1 of its spans were accepted (deliveryLog.confirm): its children can go
// out in an earlier request than its root, and a span our side dropped
// without any request counts against it too.
//
// ForceFlush runs on a background context, and the send waits for it on its
// own timer. Its context becomes the export request's, and one request carries
// every worker's queued spans: a per-send deadline would let one send's timeout
// cancel a request carrying other traces, or leave an export that runs later
// with an expired context and discards the batch without sending it. Requests
// are bounded by the HTTP client's timeout instead. A send that gives up leaves
// its flush running; it ends within that timeout, or at Close.
//
// ForceFlush's error does not decide the send: it can belong to another
// trace's spans, and it reports a Sentry 5xx, which counts as sent. It is
// logged for the detail it carries (such as a 4xx body).
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
	traceID := root.SpanContext().TraceID().String()
	// Stamped before the flush, as sendTraceTagged does, so both streams
	// measure from the same point and a flush that waits on a peer's spans
	// cannot shorten this trace's latency.
	sentAt := time.Now()

	flushed := make(chan error, 1)
	go func() { flushed <- o.tp.ForceFlush(context.Background()) }()
	timer := time.NewTimer(o.flushTimeout)
	defer timer.Stop()
	select {
	case err := <-flushed:
		if err != nil {
			log.Printf("[%s] flush after trace %s: %v", traceStreamOTLPSignal, traceID, err)
		}
	case <-timer.C:
		return "", time.Time{}, fmt.Errorf("flush otlp trace %s: export did not finish within %s", traceID, o.flushTimeout)
	}
	if err := o.delivery.confirm(traceID, n+1, "otlp trace"); err != nil {
		return "", time.Time{}, err
	}
	return traceID, sentAt, nil
}

// Close flushes and shuts down the sender's provider and exporter.
func (o *otlpSender) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = o.tp.Shutdown(ctx)
}
