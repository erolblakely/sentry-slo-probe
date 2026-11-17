package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsentry/sentry-go"
)

// fakeIngest is a fake Sentry envelope endpoint. It answers every request with
// status (plus headers) and records each envelope header it was sent.
type fakeIngest struct {
	srv     *httptest.Server
	mu      sync.Mutex
	headers []envelopeHeader
}

type envelopeHeader struct {
	EventID string            `json:"event_id"`
	Trace   map[string]string `json:"trace"`
}

func newFakeIngest(t *testing.T, status int, header http.Header) *fakeIngest {
	t.Helper()
	f := &fakeIngest{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		line, err := bufio.NewReader(r.Body).ReadBytes('\n')
		if err != nil {
			t.Errorf("read envelope header: %v", err)
		}
		var h envelopeHeader
		if err := json.Unmarshal(line, &h); err != nil {
			t.Errorf("decode envelope header %q: %v", line, err)
		}
		f.mu.Lock()
		f.headers = append(f.headers, h)
		f.mu.Unlock()
		for k, vs := range header {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeIngest) dsn() string {
	return "http://public@" + strings.TrimPrefix(f.srv.URL, "http://") + "/1"
}

func (f *fakeIngest) received() []envelopeHeader {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]envelopeHeader(nil), f.headers...)
}

// refusedDSN returns a DSN for a port nothing listens on.
func refusedDSN(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := l.Addr().String()
	l.Close()
	return "http://public@" + addr + "/1"
}

func newTestTracingClient(t *testing.T, dsn string) probeClient {
	t.Helper()
	c, err := newTracingClient(dsn)
	if err != nil {
		t.Fatalf("newTracingClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

// TestSendTraceTaggedConfirmsDelivery: a send Sentry accepted succeeds, and the
// id it returns is the trace id that reached Sentry, which is what the stream
// and batch probes look the trace up by.
func TestSendTraceTaggedConfirmsDelivery(t *testing.T) {
	ingest := newFakeIngest(t, http.StatusOK, nil)
	c := newTestTracingClient(t, ingest.dsn())

	id, _, err := sendTraceTagged(c, "B1", 0, 2)
	if err != nil {
		t.Fatalf("sendTraceTagged against a 200: %v", err)
	}
	got := ingest.received()
	if len(got) != 1 || got[0].Trace["trace_id"] != id {
		t.Errorf("ingest saw %+v, want one envelope for trace %s", got, id)
	}
}

// The SDK's transport worker drops an event when its request fails, and when
// Sentry has told it to back off, and Flush still reports success. These pin
// that a send only counts once Sentry acknowledged it: otherwise the event is
// counted as sent, never arrives, and success_rate charges Sentry for our own
// network or quota trouble.
func TestSendTraceTaggedUndeliveredIsAnError(t *testing.T) {
	// 4xx other than 429 is our own fault: a bad DSN key, a malformed or
	// oversized payload. Sentry never had the event to lose.
	for _, status := range []int{http.StatusBadRequest, http.StatusForbidden, http.StatusRequestEntityTooLarge} {
		t.Run(fmt.Sprintf("rejected with %d", status), func(t *testing.T) {
			ingest := newFakeIngest(t, status, nil)
			c := newTestTracingClient(t, ingest.dsn())
			if _, _, err := sendTraceTagged(c, "B1", 0, 2); err == nil {
				t.Errorf("sendTraceTagged returned no error although Sentry answered %d", status)
			}
		})
	}

	t.Run("connection refused", func(t *testing.T) {
		c := newTestTracingClient(t, refusedDSN(t))
		if _, _, err := sendTraceTagged(c, "B1", 0, 2); err == nil {
			t.Error("sendTraceTagged returned no error although nothing was listening")
		}
	})

	// After a 429 the SDK drops every event of that category without a request
	// until Retry-After passes. The trace stream keeps one client for its whole
	// life, so without this a single 429 would turn a minute of sends into
	// "lost" traces.
	t.Run("rate limited", func(t *testing.T) {
		ingest := newFakeIngest(t, http.StatusTooManyRequests, http.Header{"Retry-After": {"60"}})
		c := newTestTracingClient(t, ingest.dsn())
		for seq := 0; seq < 3; seq++ {
			if _, _, err := sendTraceTagged(c, "B1", seq, 2); err == nil {
				t.Errorf("send %d returned no error while Sentry was rate limiting", seq)
			}
		}
		if n := len(ingest.received()); n != 1 {
			t.Errorf("ingest saw %d requests, want 1: the later sends should be dropped by the SDK, and still reported as failures", n)
		}
	})
}

// TestSendTraceTaggedSentry5xxCountsAsSent: a 5xx is Sentry's own ingest
// endpoint failing an event it received. Over HTTPS it cannot come from our
// network (a proxy failure is a transport error, not a 5xx), and the SDK does
// not retry it, so a real customer's event is lost exactly the same way. That
// is what the probes exist to measure: the send counts as sent and, never
// arriving, reads as lost. Treating it as our send failure would drop it from
// both sides of success_rate and hide a partial Sentry outage completely.
func TestSendTraceTaggedSentry5xxCountsAsSent(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusServiceUnavailable} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			ingest := newFakeIngest(t, status, nil)
			c := newTestTracingClient(t, ingest.dsn())
			id, _, err := sendTraceTagged(c, "B1", 0, 2)
			if err != nil {
				t.Fatalf("sendTraceTagged against a %d: %v; a Sentry-side failure must count as sent", status, err)
			}
			if got := ingest.received(); len(got) != 1 || got[0].Trace["trace_id"] != id {
				t.Errorf("ingest saw %+v, want one envelope for trace %s", got, id)
			}
		})
	}
}

// TestSendErrorEventConfirmsDelivery: the error probe has the same transport
// and the same failure mode.
func TestSendErrorEventConfirmsDelivery(t *testing.T) {
	newClient := func(t *testing.T, dsn string) probeClient {
		c, err := newProbeClient(sentry.ClientOptions{Dsn: dsn})
		if err != nil {
			t.Fatalf("newProbeClient: %v", err)
		}
		t.Cleanup(c.Close)
		return c
	}

	t.Run("accepted", func(t *testing.T) {
		ingest := newFakeIngest(t, http.StatusOK, nil)
		id, _, err := sendErrorEvent(newClient(t, ingest.dsn()), "B1", 7)
		if err != nil {
			t.Fatalf("sendErrorEvent against a 200: %v", err)
		}
		if id != "7" {
			t.Errorf("id = %q, want the probe_seq lookup key 7", id)
		}
	})

	t.Run("rejected with 400", func(t *testing.T) {
		ingest := newFakeIngest(t, http.StatusBadRequest, nil)
		if _, _, err := sendErrorEvent(newClient(t, ingest.dsn()), "B1", 7); err == nil {
			t.Error("sendErrorEvent returned no error although Sentry answered 400")
		}
	})

	t.Run("Sentry 503 counts as sent", func(t *testing.T) {
		ingest := newFakeIngest(t, http.StatusServiceUnavailable, nil)
		if _, _, err := sendErrorEvent(newClient(t, ingest.dsn()), "B1", 7); err != nil {
			t.Errorf("sendErrorEvent against a 503: %v; a Sentry-side failure must count as sent", err)
		}
	})
}

// TestDeliveryLogForgetsStaleOutcomes: an outcome nobody collects (its sender
// gave up on a flush timeout first, or a transaction's event-id key, which is
// never confirmed) must not accumulate for the life of a stream's client.
func TestDeliveryLogForgetsStaleOutcomes(t *testing.T) {
	clk := &fakeClock{t: streamT0}
	d := newDeliveryLog(nil, sentryEnvelopes, deliveryPolicy{})
	d.now = clk.now

	d.record(map[string]int{"old": 1}, nil)
	clk.set(streamT0.Add(deliveryRetention + time.Second))
	d.record(map[string]int{"new": 1}, nil)

	if err := d.confirm("old", 1, "event"); err == nil {
		t.Error("an outcome older than the retention window was still held")
	}
	if err := d.confirm("new", 1, "event"); err != nil {
		t.Errorf("fresh outcome: %v", err)
	}
	if err := d.confirm("new", 1, "event"); err == nil {
		t.Error("an outcome was confirmed twice; confirm must consume it")
	}
}

// TestDeliveryLogSharedRequestConfirmsEachTrace: one OTLP request carries the
// spans of every trace queued when it was exported. Confirming one trace must
// leave the others' outcomes alone; otherwise every other sender in that
// request reads "never reached Sentry" for a send Sentry accepted, and the
// sample shrinks exactly when Sentry is slow enough for traces to share a
// request.
func TestDeliveryLogSharedRequestConfirmsEachTrace(t *testing.T) {
	d := newDeliveryLog(nil, otlpTraces, deliveryPolicy{})
	d.record(map[string]int{"a": 5, "b": 5, "c": 5}, nil)
	for _, k := range []string{"a", "b", "c"} {
		if err := d.confirm(k, 5, "otlp trace"); err != nil {
			t.Errorf("confirm(%s) after a shared accepted request: %v", k, err)
		}
	}
}

// TestDeliveryLogMergesATraceSplitAcrossRequests: an OTLP trace's children can
// be exported in an earlier request than its root. The trace counts as sent
// only if every one of its spans was accepted, whatever order the requests
// finished in. Otherwise a rejected set of children followed by an accepted
// root would count as sent, and the census would charge Sentry for children it
// never received.
func TestDeliveryLogMergesATraceSplitAcrossRequests(t *testing.T) {
	rejected := errors.New("sentry answered HTTP 429")
	for _, c := range []struct {
		name     string
		requests []struct {
			spans int
			err   error
		}
		wantSent bool
	}{
		{"all accepted across two requests", []struct {
			spans int
			err   error
		}{{4, nil}, {1, nil}}, true},
		{"children rejected, root accepted", []struct {
			spans int
			err   error
		}{{4, rejected}, {1, nil}}, false},
		{"root accepted first, children rejected after", []struct {
			spans int
			err   error
		}{{1, nil}, {4, rejected}}, false},
		{"children never sent", []struct {
			spans int
			err   error
		}{{1, nil}}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			d := newDeliveryLog(nil, otlpTraces, deliveryPolicy{})
			for _, r := range c.requests {
				d.record(map[string]int{"tr": r.spans}, r.err)
			}
			err := d.confirm("tr", 5, "otlp trace")
			if gotSent := err == nil; gotSent != c.wantSent {
				t.Errorf("confirm = %v, want counted as sent = %t", err, c.wantSent)
			}
		})
	}
}

// TestNewProbeClientRefusesBypassedDelivery: the SDK swaps out HTTPTransport
// when SENTRYGODEBUG asks for HTTP dumps or traces. Every send would then be
// unconfirmable and fail with a misleading "dropped by the SDK" message, so the
// client refuses to build instead, naming the cause.
func TestNewProbeClientRefusesBypassedDelivery(t *testing.T) {
	ingest := newFakeIngest(t, http.StatusOK, nil)
	t.Setenv("SENTRYGODEBUG", "httpdump=1")
	c, err := newTracingClient(ingest.dsn())
	if err == nil {
		c.Close()
		t.Fatal("newTracingClient succeeded although the SDK bypassed the delivery log")
	}
}

// TestDeliveryConfirmationUnderConcurrentFlushes characterises the SDK
// behaviour deliveryLog.confirm relies on, so an SDK upgrade that changes it
// fails here rather than in production numbers. confirm assumes a true Flush
// means this event's request has completed and been recorded, even when other
// goroutines' Flushes rotate the transport's batch in between. The fake
// ingest accepts or rejects each trace by its id; every sender's confirmation
// must agree with what the server decided.
func TestDeliveryConfirmationUnderConcurrentFlushes(t *testing.T) {
	var mu sync.Mutex
	decided := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		line, _ := bufio.NewReader(r.Body).ReadBytes('\n')
		var h envelopeHeader
		json.Unmarshal(line, &h)
		id := h.Trace["trace_id"]
		status := http.StatusOK
		if id != "" && id[len(id)-1]%2 == 1 { // odd last hex digit: reject
			status = http.StatusBadRequest
		}
		mu.Lock()
		decided[id] = status
		mu.Unlock()
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	c := newTestTracingClient(t, "http://public@"+strings.TrimPrefix(srv.URL, "http://")+"/1")
	if _, ok := c.Client.Transport.(*sentry.HTTPTransport); !ok {
		t.Fatalf("client transport is %T; deliveryLog's ordering argument is written for *sentry.HTTPTransport", c.Client.Transport)
	}

	type result struct {
		id  string
		err error
	}
	results := make(chan result, 160)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				id, _, err := sendTraceTagged(c, "B1", g*100+i, 0)
				results <- result{id, err}
			}
		}(g)
	}
	wg.Wait()
	close(results)

	accepted, rejected := 0, 0
	for r := range results {
		if r.err == nil {
			accepted++
			mu.Lock()
			status := decided[r.id]
			mu.Unlock()
			if status != http.StatusOK {
				t.Errorf("trace %s confirmed as sent, but the server answered %d", r.id, status)
			}
		} else {
			rejected++
		}
	}
	mu.Lock()
	wantRejected := 0
	for _, status := range decided {
		if status != http.StatusOK {
			wantRejected++
		}
	}
	mu.Unlock()
	if rejected != wantRejected || accepted+rejected != 160 {
		t.Errorf("accepted = %d, rejected = %d, want %d rejected of 160 (what the server rejected)", accepted, rejected, wantRejected)
	}
}

// TestDeliveryNoAnswerAfterSendingCountsAsSent pins where our failure ends and
// Sentry's begins when there is no response at all. Once the whole request
// has been handed to Sentry's endpoint, a missing answer is Sentry's ingest
// failing: a slow Sentry must read as slow or lost, never be quietly dropped
// from the sample as if we had failed to send. A request that never got that
// far (refused, unreachable) is still ours.
func TestDeliveryNoAnswerAfterSendingCountsAsSent(t *testing.T) {
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		<-release
	}))
	// Cleanups run last-in first-out: release the handler before Close, which
	// waits for in-flight handlers.
	t.Cleanup(hang.Close)
	t.Cleanup(func() { close(release) })

	post := func(url string) *deliveryLog {
		d := newDeliveryLog(nil, otlpTraces, deliveryPolicy{answerTimeout: 100 * time.Millisecond})
		c := &http.Client{Transport: d, Timeout: 5 * time.Second}
		body := otlpBody(t, "0af7651916cd43dd8448eb211c80319c", 5)
		req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
		if resp, err := c.Do(req); err == nil {
			resp.Body.Close()
			t.Fatalf("request to %s unexpectedly succeeded", url)
		}
		return d
	}

	if err := post(hang.URL).confirm("0af7651916cd43dd8448eb211c80319c", 5, "otlp trace"); err != nil {
		t.Errorf("no answer after the request was sent: %v; Sentry's silence must count as sent", err)
	}
	if err := post("http://"+strings.TrimPrefix(refusedDSN(t), "http://public@")).confirm("0af7651916cd43dd8448eb211c80319c", 5, "otlp trace"); err == nil {
		t.Error("a refused connection counted as sent; the request never reached Sentry")
	}
}

// scriptedRT is a fake connection that drives net/http's trace hooks the way a
// real transport does: one GotConn per attempt, WroteRequest when an attempt's
// write finishes, then an answer, an error, or silence until the request's
// context ends.
type scriptedRT struct {
	attempts []attempt
	status   int   // final answer, if err is nil and hang is false
	err      error // final transport error
	hang     bool  // never answer; return the context's error when it ends
	// rewound: after the attempts, rewind the body (r.GetBody), as net/http
	// does the moment it decides to retry, before any backoff or redial.
	rewound bool
}

type attempt struct {
	written  bool  // the write finished
	writeErr error // the write failed partway
}

func (s scriptedRT) RoundTrip(r *http.Request) (*http.Response, error) {
	tr := httptrace.ContextClientTrace(r.Context())
	for _, a := range s.attempts {
		tr.GotConn(httptrace.GotConnInfo{})
		if a.written || a.writeErr != nil {
			tr.WroteRequest(httptrace.WroteRequestInfo{Err: a.writeErr})
		}
	}
	if s.rewound {
		if body, err := r.GetBody(); err == nil {
			body.Close()
		}
	}
	if s.hang {
		<-r.Context().Done()
		return nil, r.Context().Err()
	}
	if s.err != nil {
		return nil, s.err
	}
	return &http.Response{StatusCode: s.status, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}}, nil
}

// deliver sends one scripted request for trace "tr" (5 spans) through a
// delivery log with policy, and reports whether it counted as sent.
func deliver(t *testing.T, rt scriptedRT, policy deliveryPolicy) bool {
	t.Helper()
	d := newDeliveryLog(rt, otlpTraces, policy)
	req, _ := http.NewRequest(http.MethodPost, "http://x/v1/traces", bytes.NewReader(otlpBody(t, "0af7651916cd43dd8448eb211c80319c", 5)))
	if resp, err := d.RoundTrip(req); err == nil {
		resp.Body.Close()
	}
	return d.confirm("0af7651916cd43dd8448eb211c80319c", 5, "otlp trace") == nil
}

var streamPolicyForTest = deliveryPolicy{answerTimeout: 50 * time.Millisecond}

// TestDeliveryUnansweredRule pins which missing answers are Sentry's. Under
// the streams' policy, only one is: the whole request was written and Sentry
// had not answered when the probe's own answer timer ran out. Everything else
// that ends without an answer is ours, or at least not provably Sentry's: a
// write that failed, a reset or close after the write (our egress dying, an
// idle connection closed under us), a refused attempt. The batch probes keep
// the old rule: any transport error is not sent.
func TestDeliveryUnansweredRule(t *testing.T) {
	for _, c := range []struct {
		name     string
		rt       scriptedRT
		policy   deliveryPolicy
		wantSent bool
	}{
		{"written, then no answer before the stream's timer", scriptedRT{attempts: []attempt{{written: true}}, hang: true}, streamPolicyForTest, true},
		{"written, then reset", scriptedRT{attempts: []attempt{{written: true}}, err: errors.New("read: connection reset by peer")}, streamPolicyForTest, false},
		{"written, then EOF", scriptedRT{attempts: []attempt{{written: true}}, err: io.ErrUnexpectedEOF}, streamPolicyForTest, false},
		{"write failed partway, then no answer", scriptedRT{attempts: []attempt{{writeErr: errors.New("broken pipe")}}, hang: true}, streamPolicyForTest, false},
		{"never connected, timer ran out", scriptedRT{hang: true}, streamPolicyForTest, false},
		// net/http retries some fully written HTTP/2 requests on a new attempt
		// (REFUSED_STREAM, GOAWAY). Only the final attempt's write counts.
		{"earlier attempt written, final attempt not", scriptedRT{attempts: []attempt{{written: true}, {}}, hang: true}, streamPolicyForTest, false},
		{"retried, final attempt written and answered", scriptedRT{attempts: []attempt{{written: true}, {written: true}}, status: 200}, streamPolicyForTest, true},
		// A refused stream that net/http has decided to retry: the timer can run
		// out during the retry backoff or the redial, before the next attempt
		// connects. The refused attempt must not count as written.
		{"written, refused, timer ran out while the retry was pending", scriptedRT{attempts: []attempt{{written: true}}, rewound: true, hang: true}, streamPolicyForTest, false},
		// The batch probes' rule is unchanged: no answer is never sent.
		{"batch policy: written, then no answer", scriptedRT{attempts: []attempt{{written: true}}, err: context.DeadlineExceeded}, deliveryPolicy{}, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := deliver(t, c.rt, c.policy); got != c.wantSent {
				t.Errorf("counted as sent = %t, want %t", got, c.wantSent)
			}
		})
	}
}

// TestDeliveryStatsCountUnansweredAndRetries: both are published as probe
// health, so a slow Sentry (unanswered) and net/http retrying on our shared
// connection (a possible source of duplicates) are visible, not inferred.
func TestDeliveryStatsCountUnansweredAndRetries(t *testing.T) {
	stats := &deliveryStats{}
	policy := deliveryPolicy{answerTimeout: 50 * time.Millisecond, stats: stats}
	deliver(t, scriptedRT{attempts: []attempt{{written: true}}, hang: true}, policy)
	deliver(t, scriptedRT{attempts: []attempt{{written: true}, {written: true}}, status: 200}, policy)
	deliver(t, scriptedRT{attempts: []attempt{{written: true}}, status: 200}, policy)

	if unanswered, retried := stats.take(); unanswered != 1 || retried != 1 {
		t.Errorf("take() = %d unanswered, %d retried; want 1 and 1", unanswered, retried)
	}
	if unanswered, retried := stats.take(); unanswered != 0 || retried != 0 {
		t.Errorf("second take() = %d, %d; want 0, 0 (take resets)", unanswered, retried)
	}
}

// TestOnlyStreamClientsTreatUnansweredAsSent: the streams' rule for unanswered
// requests (and their transport) must not leak into the batch probes, whose
// rule is unchanged: any transport error is not sent.
func TestOnlyStreamClientsTreatUnansweredAsSent(t *testing.T) {
	dsn := "http://public@127.0.0.1:9/1"
	batch, err := newTracingClient(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer batch.Close()
	stream, err := newStreamTracingClient(dsn, &deliveryStats{})
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	o, err := newOTLPSender(dsn, &deliveryStats{})
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()

	if p := batch.delivery.policy; p.answerTimeout != 0 || batch.delivery.next != http.DefaultTransport {
		t.Errorf("batch client policy = %+v over %T, want the zero policy over http.DefaultTransport", p, batch.delivery.next)
	}
	for name, d := range map[string]*deliveryLog{"sdk stream": stream.delivery, "otlp stream": o.delivery} {
		if d.policy.answerTimeout != streamAnswerTimeout || d.policy.stats == nil || d.next != http.RoundTripper(streamTransport) {
			t.Errorf("%s policy = %+v over %T, want streamAnswerTimeout with stats over streamTransport", name, d.policy, d.next)
		}
	}
}
