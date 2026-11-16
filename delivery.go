package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"time"
)

// deliveryLog records, per envelope, whether the send counts as sent: Sentry
// answered it, with a 2xx or with its own 5xx failure (see RoundTrip for where
// the line is drawn and why).
//
// It exists because the SDK cannot say. sentry-go's HTTPTransport worker
// (v0.43.0, transport.go) drops an event when its request errors, and drops it
// without a request while Sentry's Retry-After back-off is in force, and either
// way still closes the batch, so Flush returns true. A probe that trusted Flush
// would count those events as sent; they never arrive, and success_rate =
// received/sent would charge Sentry for our own network trouble or quota. That
// breaks the invariant every probe here is built on: sent excludes our own
// send failures, and only those.
//
// It sits between the SDK and the network: as ClientOptions.HTTPTransport for
// the Sentry SDK, and as the OTLP exporter's HTTP client transport for the
// OpenTelemetry stream. keys says which outcomes a request settles. For Sentry
// envelopes (envelopeKeys) that is the header's event_id, plus its
// trace.trace_id when present, because hub.CaptureEvent does not hand a
// transaction's event id back to the caller: transaction sends confirm by
// trace id, error sends by event id. For OTLP (otlpTraceIDs) it is every trace
// id in the export request.
type deliveryLog struct {
	next   http.RoundTripper
	p      deliveryProtocol
	policy deliveryPolicy
	now    func() time.Time

	mu       sync.Mutex
	outcomes map[string]*deliveryOutcome
}

// deliveryProtocol is what deliveryLog needs to know about one wire protocol.
type deliveryProtocol struct {
	// keys says which sends a request carries, and how many units of each: a
	// Sentry envelope carries one event, an OTLP request any number of spans of
	// any number of traces.
	keys func(*http.Request) map[string]int
	// inspect, if set, reads a 2xx response for a rejection the status code
	// does not show (OTLP partial_success). It must leave resp.Body readable.
	inspect func(*http.Response) error
	// dropped explains, for this protocol, a send no request ever carried.
	dropped string
}

// sentryEnvelopes is the Sentry SDK's envelope protocol.
var sentryEnvelopes = deliveryProtocol{
	keys:    envelopeKeys,
	dropped: "the SDK dropped it before sending (rate limited by Sentry, or its buffer was full)",
}

// otlpTraces is OTLP/HTTP trace export, as the OTLP stream uses it.
var otlpTraces = deliveryProtocol{
	keys:    otlpTraceIDs,
	inspect: otlpPartialSuccess,
	dropped: "no export request carried them (dropped before export)",
}

// deliveryOutcome accumulates every request that carried one key. The OTLP
// stream sends each trace in one request, but OTLP itself allows a trace's
// spans to be split across requests, so outcomes merge rather than overwrite:
// accepted adds up the units carried by requests that count as sent, the first
// failure of any request sticks, and confirm requires every unit.
type deliveryOutcome struct {
	accepted int
	err      error
	at       time.Time // last update, for retention
}

// deliveryRetention bounds how long an uncollected outcome is kept: one whose
// sender gave up first (a flush timeout), or a key nobody confirms (a
// transaction envelope's event id). Stream clients live as long as the process.
const deliveryRetention = 10 * time.Minute

// deliveryPolicy is how a probe treats a request Sentry did not answer.
type deliveryPolicy struct {
	// answerTimeout is the streams' rule, and zero is the batch probes'.
	//
	// When set, the probe gives each request this long, on its own clock and
	// from the moment it starts (connecting included), to be answered. If the
	// final attempt's request was written in full and the timer ran out with no
	// answer, the request is abandoned and counts as sent: Sentry had
	// the data and was too slow, and slowness is Sentry's, never ours. Nothing
	// else without an answer counts as sent: a write that failed, a reset or a
	// close after the write (our egress dying, an idle connection closed under
	// us, an HTTP/2 stream refused), none of which is provably Sentry's.
	//
	// When zero, any transport error is not sent, as the batch probes have
	// always had it.
	answerTimeout time.Duration
	// stats, if set, counts unanswered requests and net/http retries.
	stats *deliveryStats
}

// deliveryStats counts what the streams publish as probe health.
type deliveryStats struct {
	unanswered atomic.Int64 // requests abandoned unanswered, counted as sent
	retried    atomic.Int64 // requests net/http attempted more than once
}

// take returns the counts since the last take and resets them.
func (s *deliveryStats) take() (unanswered, retried int) {
	return int(s.unanswered.Swap(0)), int(s.retried.Swap(0))
}

// newDeliveryLog wraps next, or http.DefaultTransport when next is nil, for
// protocol p under policy.
func newDeliveryLog(next http.RoundTripper, p deliveryProtocol, policy deliveryPolicy) *deliveryLog {
	if next == nil {
		next = http.DefaultTransport
	}
	return &deliveryLog{next: next, p: p, policy: policy, now: time.Now, outcomes: make(map[string]*deliveryOutcome)}
}

// RoundTrip sends one request and records whether what it carried counts as
// sent.
//
// The line is drawn by whose failure it is, because sent must exclude our own
// send failures and nothing else:
//   - 2xx: Sentry accepted it. Sent. Unless the body rejects some of it (OTLP
//     partial_success), which is Sentry explicitly refusing, as with a 4xx.
//   - 5xx: Sentry's own ingest endpoint received the data and failed it. Over
//     HTTPS this cannot come from our network (a proxy failure is a transport
//     error, not a 5xx), and neither SDK retries here, so a real customer's
//     data is lost the same way. That is the failure the probes exist to
//     measure: it counts as sent, never arrives, and reads as lost. Logged.
//   - 429: Sentry rate limiting us, a quota matter rather than an ingestion
//     failure. Not sent. (While its Retry-After lasts, the Sentry SDK drops
//     further events without a request; confirm reports those as never sent.
//     The OTLP exporter has no back-off: every send gets its own 429.)
//   - any other status (4xx: bad DSN key, malformed or oversized payload; an
//     unfollowed 3xx): our fault. Not sent.
//   - no answer at all: see deliveryPolicy. Under the streams' policy, a
//     request written in full that Sentry left unanswered past the probe's
//     answer timer counts as sent (logged, and counted as unanswered); any
//     other ending without an answer does not. Under the batch probes' policy,
//     none does.
//
// Only the final attempt counts. net/http retries some fully written HTTP/2
// requests on a fresh attempt (REFUSED_STREAM, GOAWAY), so the written flag is
// reset on every connection the request gets, and a retry is logged and
// counted: it is also how a request can reach Sentry twice.
func (d *deliveryLog) RoundTrip(r *http.Request) (*http.Response, error) {
	keys := d.p.keys(r)
	var written, timedOut atomic.Bool
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(r.Context())
	var timer *time.Timer
	if d.policy.answerTimeout > 0 {
		timer = time.AfterFunc(d.policy.answerTimeout, func() {
			timedOut.Store(true)
			cancel()
		})
	}
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			attempts.Add(1)
			written.Store(false)
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			written.Store(info.Err == nil)
		},
	})
	r2 := r.WithContext(ctx)
	// net/http rewinds the body (GetBody) the moment it decides to retry, before
	// any backoff or redial. A retried attempt was refused or lost, so it no
	// longer counts as written, even if the timer runs out before the next
	// attempt connects. keys() rewound the original r, not this copy, so that
	// does not trip it.
	if get := r.GetBody; get != nil {
		r2.GetBody = func() (io.ReadCloser, error) {
			written.Store(false)
			return get()
		}
	}
	resp, err := d.next.RoundTrip(r2)
	if timer != nil {
		timer.Stop()
	}
	if err != nil || resp == nil {
		cancel()
	} else {
		// The caller still reads the body; cancel once it is done with it.
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	}
	if n := attempts.Load(); n > 1 {
		log.Printf("[delivery] net/http attempted %v %d times (retried on a new attempt)", keys, n)
		if d.policy.stats != nil {
			d.policy.stats.retried.Add(1)
		}
	}

	var outcome error
	switch {
	case err != nil && written.Load() && timedOut.Load():
		log.Printf("[delivery] sentry did not answer %v within %s, though the full request was sent: counted as sent, so it reads as lost by Sentry if it never arrives", keys, d.policy.answerTimeout)
		if d.policy.stats != nil {
			d.policy.stats.unanswered.Add(1)
		}
	case err != nil:
		outcome = err
	case resp.StatusCode >= 200 && resp.StatusCode <= 299:
		if d.p.inspect != nil {
			outcome = d.p.inspect(resp)
		}
	case resp.StatusCode >= 500:
		log.Printf("[delivery] sentry answered HTTP %d for %v: counted as sent, so it reads as lost by Sentry if it never arrives", resp.StatusCode, keys)
	default:
		outcome = fmt.Errorf("sentry answered HTTP %d", resp.StatusCode)
	}
	d.record(keys, outcome)
	return resp, err
}

// cancelOnClose releases a request's context once its response body is closed.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()
	return err
}

// record merges one request's outcome into each key it carried, and forgets
// outcomes older than deliveryRetention.
func (d *deliveryLog) record(keys map[string]int, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.now()
	for k, o := range d.outcomes {
		if now.Sub(o.at) > deliveryRetention {
			delete(d.outcomes, k)
		}
	}
	for k, n := range keys {
		o := d.outcomes[k]
		if o == nil {
			o = &deliveryOutcome{}
			d.outcomes[k] = o
		}
		o.at = now
		if err != nil {
			if o.err == nil {
				o.err = err
			}
		} else {
			o.accepted += n
		}
	}
}

// confirm reports whether the send keyed by key counts as sent: every request
// that carried it counted as sent, and together they carried all want units.
// It consumes key's outcome and only key's: an OTLP request can carry several
// traces, and each of their senders must still find its own.
//
// Call it only after a successful flush, when every request carrying the send
// has completed and been recorded:
//   - Sentry SDK: within a batch, the single transport worker handles items in
//     order, and RoundTrip records each outcome before Client.Do returns, so
//     before the batch is closed as done. Across batches, our Flush waits on a
//     batch at least as new as the one holding our event (our SendEvent
//     released the buffer before our Flush took it, though another goroutine's
//     Flush may have rotated it since), and the worker finishes batches in
//     order, so a newer batch done means ours is done too.
//     TestDeliveryConfirmationUnderConcurrentFlushes pins this for sentry-go
//     v0.43.0's HTTPTransport.
//   - OTLP: see otlpSender.send.
func (d *deliveryLog) confirm(key string, want int, what string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	o, ok := d.outcomes[key]
	delete(d.outcomes, key)
	switch {
	case !ok:
		return fmt.Errorf("%s %s never reached Sentry: %s", what, key, d.p.dropped)
	case o.err != nil:
		return fmt.Errorf("deliver %s %s: %w", what, key, o.err)
	case o.accepted < want:
		return fmt.Errorf("deliver %s %s: only %d of %d spans reached Sentry; the rest: %s", what, key, o.accepted, want, d.p.dropped)
	}
	return nil
}

// envelopeKeys reads the event id and trace id from an envelope's header line,
// without consuming the request body (GetBody returns a fresh copy; the SDK
// builds requests from a bytes.Buffer, for which net/http always sets it).
// Returns nil if the header cannot be read, which makes the send unconfirmable
// and so a failure: never a silent success.
func envelopeKeys(r *http.Request) map[string]int {
	if r.GetBody == nil {
		return nil
	}
	body, err := r.GetBody()
	if err != nil {
		return nil
	}
	defer body.Close()
	line, err := bufio.NewReader(io.LimitReader(body, 64<<10)).ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return nil
	}
	var h struct {
		EventID string `json:"event_id"`
		Trace   struct {
			TraceID string `json:"trace_id"`
		} `json:"trace"`
	}
	if json.Unmarshal(line, &h) != nil {
		return nil
	}
	keys := map[string]int{}
	for _, k := range []string{h.EventID, h.Trace.TraceID} {
		if k != "" {
			keys[k] = 1
		}
	}
	return keys
}
