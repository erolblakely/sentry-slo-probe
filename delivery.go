package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
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
	next http.RoundTripper
	p    deliveryProtocol
	now  func() time.Time

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

// deliveryOutcome accumulates every request that carried one key. A send can
// be split across requests (an OTLP trace's children can be exported before
// its root), so outcomes merge rather than overwrite: accepted adds up the
// units carried by requests that count as sent, and the first failure of any
// request sticks.
type deliveryOutcome struct {
	accepted int
	err      error
	at       time.Time // last update, for retention
}

// deliveryRetention bounds how long an uncollected outcome is kept: one whose
// sender gave up first (a flush timeout), or a key nobody confirms (a
// transaction envelope's event id). Stream clients live as long as the process.
const deliveryRetention = 10 * time.Minute

// newDeliveryLog wraps next, or http.DefaultTransport when next is nil, for
// protocol p. The default transport is what the SDKs would otherwise build for
// these probes: they set no proxy or CA options.
func newDeliveryLog(next http.RoundTripper, p deliveryProtocol) *deliveryLog {
	if next == nil {
		next = http.DefaultTransport
	}
	return &deliveryLog{next: next, p: p, now: time.Now, outcomes: make(map[string]*deliveryOutcome)}
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
//   - transport error: the request never completed. Not sent.
func (d *deliveryLog) RoundTrip(r *http.Request) (*http.Response, error) {
	keys := d.p.keys(r)
	resp, err := d.next.RoundTrip(r)
	var outcome error
	switch {
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
// It consumes key's outcome and only key's, because one request can carry
// several sends (an OTLP export holds every trace queued when it ran), and
// each of their senders must still find its own.
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
