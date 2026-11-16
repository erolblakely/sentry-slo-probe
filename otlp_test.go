package main

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

// TestOTLPEndpointFromDSN: Sentry's OTLP traces endpoint and auth key are
// derived from the DSN the probe already has, so the OTLP stream needs no new
// configuration. The path mirrors the SDK's own envelope URL (scheme, host,
// any path prefix, then /api/<project>/...), with the OTLP suffix Sentry
// documents: /api/<project>/integration/otlp/v1/traces.
func TestOTLPEndpointFromDSN(t *testing.T) {
	for _, c := range []struct {
		dsn, wantURL, wantKey string
	}{
		{"https://abc123@o4510.ingest.us.sentry.io/4511", "https://o4510.ingest.us.sentry.io/api/4511/integration/otlp/v1/traces", "abc123"},
		{"http://pub@127.0.0.1:9000/1", "http://127.0.0.1:9000/api/1/integration/otlp/v1/traces", "pub"},
		{"https://key@sentry.example.com/prefix/7", "https://sentry.example.com/prefix/api/7/integration/otlp/v1/traces", "key"},
	} {
		gotURL, gotKey, err := otlpEndpointFromDSN(c.dsn)
		if err != nil || gotURL != c.wantURL || gotKey != c.wantKey {
			t.Errorf("otlpEndpointFromDSN(%q) = %q, %q, %v; want %q, %q", c.dsn, gotURL, gotKey, err, c.wantURL, c.wantKey)
		}
	}
	for _, bad := range []string{"", "not a url", "https://o1.ingest.sentry.io/5", "https://key@o1.ingest.sentry.io/"} {
		if u, k, err := otlpEndpointFromDSN(bad); err == nil {
			t.Errorf("otlpEndpointFromDSN(%q) = %q, %q and no error, want an error", bad, u, k)
		}
	}
}

func traceIDBytes(t *testing.T, h string) []byte {
	t.Helper()
	b, err := hex.DecodeString(h)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestOTLPTraceIDs: the send check keys OTLP outcomes by trace id, read from
// the protobuf export request. One request can carry several traces (the batch
// processor flushes whatever is queued), and each is keyed.
func TestOTLPTraceIDs(t *testing.T) {
	a := "0af7651916cd43dd8448eb211c80319c"
	b := "4bf92f3577b34da6a3ce929d0e0e4736"
	req := &coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: []*tracepb.Span{
			{TraceId: traceIDBytes(t, a), Name: "root"},
			{TraceId: traceIDBytes(t, a), Name: "child"},
			{TraceId: traceIDBytes(t, b), Name: "other"},
		}}},
	}}}
	body, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	plain, _ := http.NewRequest(http.MethodPost, "http://x/v1/traces", bytes.NewReader(body))
	// Span counts per trace, so the send check can require every span of a
	// trace to have been accepted.
	if got, want := otlpTraceIDs(plain), map[string]int{a: 2, b: 1}; !maps.Equal(got, want) {
		t.Errorf("otlpTraceIDs = %v, want %v", got, want)
	}

	var zipped bytes.Buffer
	zw := gzip.NewWriter(&zipped)
	zw.Write(body)
	zw.Close()
	gz, _ := http.NewRequest(http.MethodPost, "http://x/v1/traces", bytes.NewReader(zipped.Bytes()))
	gz.Header.Set("Content-Encoding", "gzip")
	if got := otlpTraceIDs(gz); len(got) != 2 {
		t.Errorf("otlpTraceIDs on a gzip body = %v, want both trace ids", got)
	}

	junk, _ := http.NewRequest(http.MethodPost, "http://x/v1/traces", strings.NewReader("not protobuf"))
	if got := otlpTraceIDs(junk); len(got) != 0 {
		t.Errorf("otlpTraceIDs on garbage = %v, want none (unconfirmable, so a failed send)", got)
	}
}

// fakeOTLP is a fake Sentry OTLP traces endpoint. It answers every request
// with status and records the spans and auth header it was sent.
type fakeOTLP struct {
	srv   *httptest.Server
	mu    sync.Mutex
	spans []*tracepb.Span
	auth  []string
}

func newFakeOTLP(t *testing.T, status int) *fakeOTLP {
	t.Helper()
	f := &fakeOTLP{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/1/integration/otlp/v1/traces" {
			t.Errorf("OTLP request to %s, want /api/1/integration/otlp/v1/traces", r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(body, &req); err != nil {
			t.Errorf("decode OTLP request: %v", err)
		}
		f.mu.Lock()
		f.auth = append(f.auth, r.Header.Get("x-sentry-auth"))
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				f.spans = append(f.spans, ss.Spans...)
			}
		}
		f.mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeOTLP) dsn() string {
	return "http://public@" + strings.TrimPrefix(f.srv.URL, "http://") + "/1"
}

func newTestOTLPSender(t *testing.T, dsn string) *otlpSender {
	t.Helper()
	o, err := newOTLPSender(dsn, &deliveryStats{})
	if err != nil {
		t.Fatalf("newOTLPSender: %v", err)
	}
	t.Cleanup(o.Close)
	return o
}

func stringAttr(attrs []*commonpb.KeyValue, key string) string {
	for _, kv := range attrs {
		if kv.Key == key {
			return kv.Value.GetStringValue()
		}
	}
	return ""
}

// TestOTLPSenderSendsProbeTrace: an OTLP probe trace has the same shape as the
// SDK stream's (a probe.login root with n child spans), carries probe_batch on
// the root, authenticates with the DSN key, and returns the trace id that
// reached Sentry, which is what the stream looks it up by.
func TestOTLPSenderSendsProbeTrace(t *testing.T) {
	ingest := newFakeOTLP(t, http.StatusOK)
	o := newTestOTLPSender(t, ingest.dsn())

	id, _, err := o.send("B1", 3, 4)
	if err != nil {
		t.Fatalf("send against a 200: %v", err)
	}

	ingest.mu.Lock()
	defer ingest.mu.Unlock()
	if len(ingest.spans) != 5 {
		t.Fatalf("ingest saw %d spans, want 5 (root + 4 children)", len(ingest.spans))
	}
	var root *tracepb.Span
	for _, sp := range ingest.spans {
		if hex.EncodeToString(sp.TraceId) != id {
			t.Errorf("span %q has trace id %x, want %s", sp.Name, sp.TraceId, id)
		}
		if len(sp.ParentSpanId) == 0 {
			root = sp
		}
	}
	if root == nil || root.Name != "probe.login" {
		t.Fatalf("no probe.login root span among %d spans", len(ingest.spans))
	}
	if got := stringAttr(root.Attributes, "probe_batch"); got != "B1" {
		t.Errorf("root probe_batch = %q, want B1", got)
	}
	for _, sp := range ingest.spans {
		if sp != root && !bytes.Equal(sp.ParentSpanId, root.SpanId) {
			t.Errorf("child %q is not parented on the root", sp.Name)
		}
	}
	if len(ingest.auth) == 0 || ingest.auth[0] != "sentry sentry_key=public" {
		t.Errorf("x-sentry-auth = %v, want \"sentry sentry_key=public\"", ingest.auth)
	}
}

// TestOTLPSenderDeliveryRules: the OTLP path counts sends by the same rule as
// the SDK path (see deliveryLog.RoundTrip). ForceFlush alone cannot tell:
// spans exported by the batch processor's own timer report their errors
// nowhere, and a flush's error can belong to another trace's spans.
func TestOTLPSenderDeliveryRules(t *testing.T) {
	for _, c := range []struct {
		status   int
		wantSent bool
	}{
		{http.StatusOK, true},
		{http.StatusServiceUnavailable, true}, // Sentry failed an event it received: reads as lost
		{http.StatusBadRequest, false},
		{http.StatusTooManyRequests, false},
	} {
		t.Run(http.StatusText(c.status), func(t *testing.T) {
			ingest := newFakeOTLP(t, c.status)
			o := newTestOTLPSender(t, ingest.dsn())
			_, _, err := o.send("B1", 0, 1)
			if gotSent := err == nil; gotSent != c.wantSent {
				t.Errorf("send against a %d: err = %v, want counted as sent = %t", c.status, err, c.wantSent)
			}
		})
	}

	t.Run("connection refused", func(t *testing.T) {
		o := newTestOTLPSender(t, refusedDSN(t))
		if _, _, err := o.send("B1", 0, 1); err == nil {
			t.Error("send returned no error although nothing was listening")
		}
	})
}

// otlpRequest is what a slowOTLP endpoint saw in one request.
type otlpRequest struct {
	status int
	spans  map[string]int // trace id -> spans in this request
}

// slowOTLP is a fake OTLP endpoint that answers after latency with the status
// statusFor(n) for its n-th request (from 0), and records every request.
type slowOTLP struct {
	srv  *httptest.Server
	mu   sync.Mutex
	reqs []otlpRequest
}

func newSlowOTLP(t *testing.T, latency time.Duration, statusFor func(n int) int, body []byte) *slowOTLP {
	t.Helper()
	f := &slowOTLP{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req coltracepb.ExportTraceServiceRequest
		if err := proto.Unmarshal(raw, &req); err != nil {
			t.Errorf("decode OTLP request: %v", err)
		}
		spans := map[string]int{}
		for _, rs := range req.ResourceSpans {
			for _, ss := range rs.ScopeSpans {
				for _, sp := range ss.Spans {
					spans[hex.EncodeToString(sp.TraceId)]++
				}
			}
		}
		f.mu.Lock()
		status := statusFor(len(f.reqs))
		f.reqs = append(f.reqs, otlpRequest{status: status, spans: spans})
		f.mu.Unlock()
		time.Sleep(latency)
		if body != nil {
			w.Header().Set("Content-Type", "application/x-protobuf")
		}
		w.WriteHeader(status)
		w.Write(body)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *slowOTLP) dsn() string {
	return "http://public@" + strings.TrimPrefix(f.srv.URL, "http://") + "/1"
}

func (f *slowOTLP) requests() []otlpRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.reqs)
}

// sendConcurrently runs workers goroutines each sending perWorker traces of 1
// root + 4 children, and returns each send's trace id (empty on error) and
// error.
func sendConcurrently(o *otlpSender, workers, perWorker int) (ids []string, errs []error) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				id, _, err := o.send("B1", g*100+i, 4)
				mu.Lock()
				ids, errs = append(ids, id), append(errs, err)
				mu.Unlock()
			}
		}(g)
	}
	wg.Wait()
	return ids, errs
}

// TestOTLPSenderPartialSuccess: OTLP lets a 200 reject spans in its body
// (partial_success). Sentry explicitly refused them, as with a 4xx, so the
// request's traces do not count as sent. The response does not say which
// spans were rejected, so every trace in it is excluded rather than charged
// to Sentry.
func TestOTLPSenderPartialSuccess(t *testing.T) {
	rejecting, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{
		PartialSuccess: &coltracepb.ExportTracePartialSuccess{RejectedSpans: 5, ErrorMessage: "quota exceeded"},
	})
	ingest := newSlowOTLP(t, 0, func(int) int { return http.StatusOK }, rejecting)
	o := newTestOTLPSender(t, ingest.dsn())
	if _, _, err := o.send("B1", 0, 4); err == nil || !strings.Contains(err.Error(), "quota exceeded") {
		t.Errorf("send = %v, want a failure naming Sentry's rejection message", err)
	}

	warningOnly, _ := proto.Marshal(&coltracepb.ExportTraceServiceResponse{
		PartialSuccess: &coltracepb.ExportTracePartialSuccess{ErrorMessage: "deprecated field"},
	})
	ingest2 := newSlowOTLP(t, 0, func(int) int { return http.StatusOK }, warningOnly)
	o2 := newTestOTLPSender(t, ingest2.dsn())
	if _, _, err := o2.send("B1", 0, 4); err != nil {
		t.Errorf("send with a warning but no rejected spans: %v", err)
	}
}

// dropChildren samples the probe's root span and drops its children, standing
// in for any way our side can lose spans without a request (a stale export
// discarding a batch, a sampler, a full queue).
type dropChildren struct{}

func (dropChildren) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	if strings.HasPrefix(p.Name, "probe.span.") {
		return sdktrace.SamplingResult{Decision: sdktrace.Drop}
	}
	return sdktrace.SamplingResult{Decision: sdktrace.RecordAndSample}
}

func (dropChildren) Description() string { return "dropChildren" }

// TestOTLPSenderRequiresEverySpan: a trace counts as sent only if all n+1 of
// its spans reached Sentry. If our side lost the children without any request,
// the root alone being accepted must not count: the census would then score
// the trace incomplete and charge Sentry for spans it never received.
func TestOTLPSenderRequiresEverySpan(t *testing.T) {
	ingest := newSlowOTLP(t, 0, func(int) int { return http.StatusOK }, nil)
	o, err := newOTLPSenderWith(ingest.dsn(), &deliveryStats{}, dropChildren{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(o.Close)

	if _, _, err := o.send("B1", 0, 4); err == nil {
		t.Error("send counted as sent although only the root of 5 spans reached Sentry")
	}
	if reqs := ingest.requests(); len(reqs) != 1 {
		t.Fatalf("ingest saw %d requests, want 1 carrying the root alone", len(reqs))
	}
}

// otlpBody is an OTLP export request carrying spans spans of one trace.
func otlpBody(t *testing.T, traceID string, spans int) []byte {
	t.Helper()
	var ss []*tracepb.Span
	for i := 0; i < spans; i++ {
		ss = append(ss, &tracepb.Span{TraceId: traceIDBytes(t, traceID), Name: "s"})
	}
	b, err := proto.Marshal(&coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{ScopeSpans: []*tracepb.ScopeSpans{{Spans: ss}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestOTLPSenderSendsEachTraceAlone: each send is its own export request
// carrying exactly its own trace, so its outcome is that request's alone, and
// per-request outcomes map one to one onto sends.
func TestOTLPSenderSendsEachTraceAlone(t *testing.T) {
	statusFor := func(n int) int {
		if n%2 == 1 {
			return http.StatusBadRequest
		}
		return http.StatusOK
	}
	ingest := newSlowOTLP(t, 20*time.Millisecond, statusFor, nil)
	o := newTestOTLPSender(t, ingest.dsn())
	ids, errs := sendConcurrently(o, 8, 4)

	accepted := map[string]bool{}
	for _, r := range ingest.requests() {
		if len(r.spans) != 1 {
			t.Errorf("a request carried %d traces, want exactly 1", len(r.spans))
		}
		for id, n := range r.spans {
			if n != 5 {
				t.Errorf("trace %s went out as %d spans in one request, want all 5", id, n)
			}
			accepted[id] = r.status == http.StatusOK
		}
	}
	sent := 0
	for i, err := range errs {
		if err == nil {
			sent++
			if !accepted[ids[i]] {
				t.Errorf("trace %s counted as sent, but its request was rejected", ids[i])
			}
		}
	}
	want := 0
	for _, ok := range accepted {
		if ok {
			want++
		}
	}
	if sent != want || want == 0 || want == len(errs) {
		t.Errorf("%d of %d sends counted as sent, want %d (the accepted requests), and a mix", sent, len(errs), want)
	}
}

// TestOTLPSenderDoesNotQueueBehindASlowSentry is the regression test for a
// queue that used to form on our side. A batch span processor flushed per
// send runs its exports one at a time under a lock, a few traces each, so a
// slow Sentry made sends wait behind each other: that wait was measured as
// Sentry's latency, and the slowest sends fell out of the sample. The barrier
// answers only once every send's request is in flight at the same time, so any
// serialization on our side fails this test outright.
func TestOTLPSenderDoesNotQueueBehindASlowSentry(t *testing.T) {
	const sends = 4
	srv, stuck := newBarrierServer(t, sends)
	o := newTestOTLPSender(t, "http://public@"+strings.TrimPrefix(srv.URL, "http://")+"/1")
	_, errs := sendConcurrently(o, sends, 1)
	for _, err := range errs {
		if err != nil {
			t.Errorf("send: %v", err)
		}
	}
	if n := stuck.Load(); n > 0 {
		t.Errorf("%d requests waited at the barrier for the others: sends were queued behind each other", n)
	}
}

// newBarrierServer answers every request only once n requests are in flight
// together, and counts those that gave up waiting (answering 400) in stuck.
// Sends that are serialized on our side can never fill it.
func newBarrierServer(t *testing.T, n int) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var mu sync.Mutex
	arrived := 0
	all := make(chan struct{})
	stuck := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		mu.Lock()
		arrived++
		if arrived == n {
			close(all)
		}
		mu.Unlock()
		select {
		case <-all:
			w.WriteHeader(http.StatusOK)
		case <-time.After(2 * time.Second):
			stuck.Add(1)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, stuck
}
