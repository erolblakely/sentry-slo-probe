package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"time"
)

// The trace stream is the continuous counterpart to the batch probes. Instead of
// a paced batch every PROBE_INTERVAL_SECONDS, it sends one probe trace every
// second, forever, polls Sentry for the traces still in flight, and reports
// latency and reliability for the traces whose fate was decided in each
// reporting window.
//
// A trace is pending only once Sentry acknowledged its send (see deliveryLog),
// and its fate is then decided exactly once:
//   - arrived: a successful Sentry query found it. latency = time of that query's
//     response - local send stamp, so it is an upper bound, never optimistic.
//   - lost: a successful query asked at or past sentAt+timeout did not find it.
//   - abandoned: no successful query was asked at or past its deadline by
//     sentAt+timeout+grace. We never managed to look when it mattered, so it
//     is dropped from both numerator and denominator rather than charged to
//     Sentry.
//
// The timeout is TRACE_STREAM_TIMEOUT_SECONDS (300s), not the batch probes'
// POLL_TIMEOUT_SECONDS: measurable latency is capped at roughly the timeout,
// and it has to sit well above the 120s trace alert threshold.
//
// The metric `sent` for a window is therefore arrived+lost: the traces decided
// in that window, not the traces sent during it. That is what keeps
// success_rate = received/sent a ratio over the same traces. Every sent trace
// is decided in exactly one window, so over time the sums still match sends.

// traceQueryFunc reports which of ids Sentry has ingested. ids is at most
// maxSentryPerPage long.
type traceQueryFunc func(ctx context.Context, ids []string) (map[string]bool, error)

// spanCountFunc returns trace id -> stored child-span count for ids, in one
// request. A trace with no stored children is absent from the map. ids is at
// most maxSentryPerPage long.
type spanCountFunc func(ctx context.Context, ids []string) (map[string]int, error)

// streamWindow is what one reporting window decided.
type streamWindow struct {
	arrived     []time.Duration // latency of every trace that arrived
	lost        int             // traces a successful query did not find by the deadline
	sendFailed  int             // sends that did not leave the process
	sendSkipped int             // ticks with every send worker still busy
	queryOK     int             // Sentry queries that succeeded
	queryFailed int             // Sentry queries that returned an error
	abandoned   int             // traces dropped unmeasured (no successful look in time)
	pending     int             // traces still in flight when the window closed

	// The child-span census (see traceStream.census).
	censused      int // arrived traces whose child spans were counted
	complete      int // of those, traces with every child span stored
	censusFailed  int // census requests that returned an error
	censusDropped int // traces dropped uncounted after the census kept failing

	// From the stream's delivery log (deliveryStats), filled in at report time.
	unanswered int // sends Sentry left unanswered past the answer timer, counted as sent
	retried    int // sends net/http retried on a new attempt
}

// result converts a window into the batchResult postBatchMetrics publishes.
// measured keeps batchResult's meaning: at least one Sentry query succeeded.
// It is not the same as "something was decided": the first window after
// startup queries successfully and decides nothing, and reportTraceStream
// handles that case itself.
func (w streamWindow) result() batchResult {
	return batchResult{
		sent:      len(w.arrived) + w.lost,
		received:  len(w.arrived),
		latencies: w.arrived,
		measured:  w.queryOK > 0,
	}
}

// traceStream tracks in-flight traces and accumulates the current window. Safe
// for concurrent use: sends record from worker goroutines while poll runs on
// its own.
type traceStream struct {
	timeout time.Duration
	grace   time.Duration
	now     func() time.Time

	// Child-span census settings; expectedChildren 0 means no census.
	expectedChildren int
	censusSettle     time.Duration
	censusGiveUp     time.Duration

	mu      sync.Mutex
	pending map[string]time.Time // trace id -> local send stamp
	due     map[string]time.Time // arrived trace id -> arrival, awaiting census
	w       streamWindow
}

func newTraceStream(timeout, grace time.Duration, now func() time.Time) *traceStream {
	return &traceStream{
		timeout: timeout,
		grace:   grace,
		now:     now,
		pending: make(map[string]time.Time),
		due:     make(map[string]time.Time),
	}
}

// withCensus turns on the child-span census: every arrived trace is checked,
// once, for expected stored child spans, no sooner than settle after its root
// arrived. A trace whose census keeps failing is dropped uncounted once it is
// giveUp past settle.
func (s *traceStream) withCensus(expected int, settle, giveUp time.Duration) *traceStream {
	s.expectedChildren = expected
	s.censusSettle = settle
	s.censusGiveUp = giveUp
	return s
}

// recordSend registers the outcome of one send. Only a send that succeeded
// becomes pending: a failed send is never polled for, so it can never time out
// and read as Sentry losing a trace it never received.
func (s *traceStream) recordSend(id string, at time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.w.sendFailed++
		return
	}
	s.pending[id] = at
}

// recordSkip registers a tick on which no send was attempted.
func (s *traceStream) recordSkip() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.w.sendSkipped++
}

// poll asks Sentry about every pending trace, oldest first, at most
// maxSentryPerPage per request, and decides what it can. The lock is not held
// across a query, so sends keep landing while Sentry is slow.
func (s *traceStream) poll(ctx context.Context, query traceQueryFunc) {
	s.mu.Lock()
	ids := s.pendingOldestFirst()
	s.mu.Unlock()

	for start := 0; start < len(ids); start += maxSentryPerPage {
		if ctx.Err() != nil {
			return
		}
		chunk := ids[start:min(start+maxSentryPerPage, len(ids))]
		asked := s.now()
		found, err := query(ctx, chunk)
		answered := s.now()
		s.mu.Lock()
		if err != nil {
			s.w.queryFailed++
			log.Printf("[trace_stream] query %d traces: %v", len(chunk), err)
		} else {
			s.w.queryOK++
			s.observe(chunk, found, asked, answered)
		}
		s.mu.Unlock()
	}

	s.mu.Lock()
	s.abandon(s.now())
	s.mu.Unlock()
}

// census counts the stored child spans of arrived traces that have settled,
// oldest arrivals first, at most maxSentryPerPage per request, and scores each
// complete only if every expected child is stored: all-or-nothing per trace,
// as in the batch span probe. A trace absent from a successful census stored
// no children and is incomplete. A failed request scores nothing; its traces
// are retried on the next call, and dropped uncounted once they are
// censusGiveUp past settling. No-op without withCensus.
//
// It runs on the report goroutine, just before the window is taken, so a slow
// census never delays a send or a poll.
func (s *traceStream) census(ctx context.Context, count spanCountFunc) {
	if s.expectedChildren <= 0 {
		return
	}
	s.mu.Lock()
	now := s.now()
	var ids []string
	for id, arrived := range s.due {
		if now.Sub(arrived) >= s.censusSettle+s.censusGiveUp {
			s.w.censusDropped++
			delete(s.due, id)
		} else if now.Sub(arrived) >= s.censusSettle {
			ids = append(ids, id)
		}
	}
	sortByTime(ids, s.due)
	s.mu.Unlock()

	for start := 0; start < len(ids); start += maxSentryPerPage {
		if ctx.Err() != nil {
			return
		}
		chunk := ids[start:min(start+maxSentryPerPage, len(ids))]
		counts, err := count(ctx, chunk)
		s.mu.Lock()
		if err != nil {
			s.w.censusFailed++
			log.Printf("[trace_stream] span census of %d traces: %v", len(chunk), err)
		} else {
			for _, id := range chunk {
				s.w.censused++
				if counts[id] >= s.expectedChildren {
					s.w.complete++
				}
				delete(s.due, id)
			}
		}
		s.mu.Unlock()
	}
}

// sortByTime orders ids by their time in at, oldest first, breaking ties by
// id so the order is deterministic.
func sortByTime(ids []string, at map[string]time.Time) {
	sort.Slice(ids, func(i, j int) bool {
		a, b := at[ids[i]], at[ids[j]]
		if !a.Equal(b) {
			return a.Before(b)
		}
		return ids[i] < ids[j]
	})
}

// pendingOldestFirst lists pending ids by send stamp, so the traces nearest
// their deadline are checked first. Callers must hold mu.
func (s *traceStream) pendingOldestFirst() []string {
	ids := make([]string, 0, len(s.pending))
	for id := range s.pending {
		ids = append(ids, id)
	}
	sortByTime(ids, s.pending)
	return ids
}

// observe applies one successful query over checked, asked at asked and
// answered at answered.
//
// Latency runs to answered, the latest moment the trace could have become
// queryable, so it is an upper bound. The deadline is judged at asked: a query
// asked before the deadline says nothing about the trace at the deadline, even
// if its response arrives after it.
//
// A found trace arrives before the deadline is considered, so the first
// successful look past the deadline still credits a trace Sentry holds
// (runBatch does the same). Ids in found that were not asked about, or are no
// longer pending, are ignored. Callers must hold mu.
func (s *traceStream) observe(checked []string, found map[string]bool, asked, answered time.Time) {
	for _, id := range checked {
		sentAt, ok := s.pending[id]
		if !ok {
			continue
		}
		switch {
		case found[id]:
			s.w.arrived = append(s.w.arrived, answered.Sub(sentAt))
			delete(s.pending, id)
			if s.expectedChildren > 0 {
				s.due[id] = answered
			}
		case !asked.Before(sentAt.Add(s.timeout)):
			s.w.lost++
			delete(s.pending, id)
		}
	}
}

// abandon drops traces that are past timeout+grace without a successful look.
// It runs after observe in the same poll, so a trace that a successful query
// just decided is never also abandoned. Callers must hold mu.
func (s *traceStream) abandon(now time.Time) {
	for id, sentAt := range s.pending {
		if !now.Before(sentAt.Add(s.timeout + s.grace)) {
			s.w.abandoned++
			delete(s.pending, id)
		}
	}
}

// streamConfig is the stream's timing. The send interval sets the probe rate;
// sendWorkers only absorbs slow flushes so one stuck send cannot delay the next.
type streamConfig struct {
	sendInterval   time.Duration
	pollInterval   time.Duration
	reportInterval time.Duration
	sendWorkers    int
}

const (
	// traceStreamSendInterval is the probe rate: one trace per second.
	traceStreamSendInterval = time.Second
	// traceStreamReportInterval is the reporting window, and therefore the
	// interval the stream's Datadog client submits its count metrics with.
	traceStreamReportInterval = 60 * time.Second
	// traceStreamGrace is how long past its timeout an unchecked trace is held
	// in case the Sentry API recovers, before it is dropped unmeasured. It also
	// caps the pessimistic latency a polling outage can produce.
	traceStreamGrace = 30 * time.Second
	// traceStreamSendWorkers bounds a stream's in-flight sends. Each send waits
	// for Sentry's answer, up to streamAnswerTimeout, so at one send a second a
	// slow Sentry can hold ~25 at once; 64 keeps a slow Sentry from ever
	// causing a skipped send, which would shrink the sample exactly when it
	// matters.
	traceStreamSendWorkers = 64
	// traceStreamCensusSettle is how long after its root arrives a trace's
	// child spans are counted. On both paths children travel in the same
	// request as the root, so a minute is generous: it only has to keep a
	// healthy trace from being scored before its children are queryable.
	traceStreamCensusSettle = 60 * time.Second
	// traceStreamCensusGiveUp is how long a failing census keeps retrying a
	// trace before dropping it uncounted.
	traceStreamCensusGiveUp = 5 * time.Minute
)

// streamConfigFrom derives the stream's timing from the process config.
func streamConfigFrom(cfg config) streamConfig {
	return streamConfig{
		sendInterval:   traceStreamSendInterval,
		pollInterval:   cfg.streamPollInterval,
		reportInterval: traceStreamReportInterval,
		sendWorkers:    traceStreamSendWorkers,
	}
}

// streamIO is how a stream talks to Sentry: send one probe trace, look up which
// pending traces have arrived, and count stored child spans (countSpans nil
// means no census). Each stream supplies its own send; lookups and the census
// are the same Discover queries for both ingest paths.
type streamIO struct {
	send       sendFunc
	query      traceQueryFunc
	countSpans spanCountFunc
}

// runTraceStream sends, polls and reports until ctx is cancelled. The three run
// on their own goroutines so that a slow Sentry query or Datadog submission can
// never delay a send. It returns only after every in-flight send has finished,
// and does not report the partial window it was cancelled in: that window would
// carry a count interval it did not cover.
func runTraceStream(ctx context.Context, cfg streamConfig, ts *traceStream, io streamIO, report func(streamWindow)) {
	workers := max(cfg.sendWorkers, 1)
	sem := make(chan struct{}, workers)
	var sends sync.WaitGroup
	var loops sync.WaitGroup
	loops.Add(3)

	go func() {
		defer loops.Done()
		seq := 0
		fire := func() {
			// Incremented on a skip too, so the gap in probe_seq records it.
			seq++
			select {
			case sem <- struct{}{}:
			default:
				ts.recordSkip()
				return
			}
			sends.Add(1)
			go func(seq int) {
				defer sends.Done()
				defer func() { <-sem }()
				id, at, err := io.send(ctx, seq)
				if err != nil {
					log.Printf("[trace_stream] send seq=%d: %v", seq, err)
				}
				ts.recordSend(id, at, err)
			}(seq)
		}
		fire()
		every(ctx, cfg.sendInterval, fire)
	}()

	go func() {
		defer loops.Done()
		every(ctx, cfg.pollInterval, func() { ts.poll(ctx, io.query) })
	}()

	go func() {
		defer loops.Done()
		every(ctx, cfg.reportInterval, func() {
			// The census runs here, just before the window is taken, so its
			// results land in this window and a slow census never delays a
			// send or a poll.
			if io.countSpans != nil {
				ts.census(ctx, io.countSpans)
			}
			report(ts.takeWindow())
		})
	}()

	loops.Wait()
	sends.Wait()
}

// every calls f on each tick of interval until ctx is cancelled. A tick that
// races with cancellation is dropped rather than run.
func every(ctx context.Context, interval time.Duration, f func()) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if ctx.Err() != nil {
				return
			}
			f()
		}
	}
}

// The streams' signals, which name their metrics (sentry.<signal>.*) and their
// probe:<signal> tag.
const (
	traceStreamSignal     = "trace_stream"      // Sentry SDK path
	traceStreamOTLPSignal = "trace_stream_otlp" // OpenTelemetry OTLP path
)

// reportTraceStream logs one window of the stream named signal and publishes
// it.
//
// Latency and reliability: a window that decided nothing publishes none. That
// covers startup until the first trace arrives, and any window in which every
// Sentry query failed. Publishing either would post sent=0 and success_rate=0,
// which reads as a total outage; the gap in the series is the honest signal.
// (The first window usually decides only the few traces sent in its first
// seconds, so its percentiles rest on a tiny sample.) Once anything is decided,
// batchPoints applies the same rules as the batch probes, including no latency
// percentiles when nothing arrived.
//
// Completeness: census.checked and census.complete (counts, so a metric-based
// SLO can divide their sums) and complete_pct (a gauge, the window's level) are
// published whenever the census counted anything, independently of the above,
// because the census looks at traces that arrived a minute or more earlier. A
// failed census publishes none of them rather than a 0% that would read as
// total span loss.
//
// Probe health: sentry.<signal>.probe.* (failed and skipped sends, failed
// queries, abandoned traces, census failures and drops, and pending) every
// window, even one that decided nothing. They describe the probe rather than
// Sentry, so a broken probe can be told apart from a slow or lossy Sentry on a
// dashboard.
func reportTraceStream(dd *datadogClient, signal string, w streamWindow, baseTags []string) {
	p50, p95 := "n/a", "n/a"
	if len(w.arrived) > 0 {
		p50 = percentile(w.arrived, 50).String()
		p95 = percentile(w.arrived, 95).String()
	}
	complete := "n/a"
	if w.censused > 0 {
		complete = fmt.Sprintf("%d/%d", w.complete, w.censused)
	}
	log.Printf("[%s] window: arrived=%d lost=%d p50=%s p95=%s complete=%s pending=%d send_failed=%d send_skipped=%d unanswered=%d http_retried=%d query_ok=%d query_failed=%d abandoned=%d census_failed=%d census_dropped=%d",
		signal, len(w.arrived), w.lost, p50, p95, complete, w.pending, w.sendFailed, w.sendSkipped, w.unanswered, w.retried, w.queryOK, w.queryFailed, w.abandoned, w.censusFailed, w.censusDropped)

	tags := append(append([]string{}, baseTags...), "probe:"+signal)
	// Probe health, every window: these describe the probe, not Sentry, and a
	// window of failing queries is exactly when they matter.
	pts := []metricPoint{
		{"sentry." + signal + ".probe.send_failed", ddTypeCount, float64(w.sendFailed)},
		{"sentry." + signal + ".probe.send_skipped", ddTypeCount, float64(w.sendSkipped)},
		{"sentry." + signal + ".probe.query_failed", ddTypeCount, float64(w.queryFailed)},
		{"sentry." + signal + ".probe.abandoned", ddTypeCount, float64(w.abandoned)},
		{"sentry." + signal + ".probe.census_failed", ddTypeCount, float64(w.censusFailed)},
		{"sentry." + signal + ".probe.census_dropped", ddTypeCount, float64(w.censusDropped)},
		{"sentry." + signal + ".probe.unanswered", ddTypeCount, float64(w.unanswered)},
		{"sentry." + signal + ".probe.http_retried", ddTypeCount, float64(w.retried)},
		{"sentry." + signal + ".probe.pending", ddTypeGauge, float64(w.pending)},
	}
	if res := w.result(); res.sent > 0 {
		pts = append(pts, batchPoints(signal, res)...)
	} else {
		log.Printf("[%s] no trace was decided this window, so no latency or reliability is published (query_failed=%d: if that is non-zero every cycle, check SENTRY_AUTH_TOKEN and SENTRY_ORG)", signal, w.queryFailed)
	}
	if w.censused > 0 {
		pts = append(pts,
			metricPoint{"sentry." + signal + ".census.checked", ddTypeCount, float64(w.censused)},
			metricPoint{"sentry." + signal + ".census.complete", ddTypeCount, float64(w.complete)},
			metricPoint{"sentry." + signal + ".complete_pct", ddTypeGauge, float64(w.complete) / float64(w.censused) * 100},
		)
	}
	// One request per window: a slow Datadog then costs one timeout per
	// window, not one per metric, and cannot push the next window back.
	dd.postPointsLogged(signal, pts, tags)
}

// takeWindow returns what the current window decided and starts a new one.
// Pending traces carry over: they are decided in whichever window they resolve.
func (s *traceStream) takeWindow() streamWindow {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.w
	w.pending = len(s.pending)
	s.w = streamWindow{}
	return w
}

// runStreamProbe runs the stream named signal until ctx is cancelled, sending
// with send and tagging with streamKey. Both ingest paths share everything
// else: the trace-id lookups, the child-span census, the timing, and the
// reporting. Their Sentry queries are identical because both paths store a
// root span with is_transaction:true and its children with
// is_transaction:false (verified live for OTLP on 2026-11-16).
//
// PROCESS GLUE, like runTraceBatch: it composes pieces that are each tested
// (findTraces, findBatchSpanCounts, traceStream, runTraceStream,
// reportTraceStream) and is verified end to end by a live run.
func runStreamProbe(ctx context.Context, s *sentryProbe, cfg config, signal, streamKey string, send sendFunc, stats *deliveryStats, baseTags []string) {
	io := streamIO{
		send: send,
		query: func(ctx context.Context, ids []string) (map[string]bool, error) {
			return s.findTraces(ids)
		},
		countSpans: func(ctx context.Context, ids []string) (map[string]int, error) {
			return s.findBatchSpanCounts(ids, len(ids))
		},
	}
	// Its own Datadog client because count metrics carry their submission
	// interval, and the stream submits every reporting window, not every
	// PROBE_INTERVAL_SECONDS.
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite, traceStreamReportInterval)
	ts := newTraceStream(cfg.streamTimeout, traceStreamGrace, time.Now).
		withCensus(traceProbeSpans, traceStreamCensusSettle, traceStreamCensusGiveUp)

	log.Printf("[%s] starting (send every %s, poll every %s, report every %s, lost after %s, census %s after arrival, probe_batch=%s)",
		signal, traceStreamSendInterval, cfg.streamPollInterval, traceStreamReportInterval, cfg.streamTimeout, traceStreamCensusSettle, streamKey)
	runTraceStream(ctx, streamConfigFrom(cfg), ts, io, func(w streamWindow) {
		w.unanswered, w.retried = stats.take()
		reportTraceStream(dd, signal, w, baseTags)
	})
}

// The streams' timing for Sentry's answer.
//
// streamAnswerTimeout is the decider: the delivery log gives Sentry this long,
// on its own clock, to answer a request written in full, and abandons it as
// unanswered (counted as sent) when it runs out. The clients' own timeouts,
// streamClientTimeout, sit beyond it so they never pre-empt it, and
// streamAnswerBackstop (the SDK stream's Flush wait) beyond those, only to
// catch a client stuck for some other reason.
const (
	streamAnswerTimeout  = 25 * time.Second
	streamClientTimeout  = 30 * time.Second // the Sentry SDK's own HTTP timeout
	streamAnswerBackstop = 45 * time.Second
)

// streamDeliveryPolicy is the streams' rule for unanswered requests (see
// deliveryPolicy), counting into stats.
func streamDeliveryPolicy(stats *deliveryStats) deliveryPolicy {
	return deliveryPolicy{answerTimeout: streamAnswerTimeout, stats: stats}
}

// streamTransport is the streams' HTTP transport, shared by both paths.
//
// HTTP/2 health pings make a dead network path fail as a lost connection
// within about 15s (not sent: ours), rather than leave every send on it to
// run out the answer timer and be charged to Sentry as unanswered. A live
// Sentry answers pings at the protocol layer even while its ingest is slow, so
// a slow Sentry still reads as slow. MaxIdleConnsPerHost covers the stream's
// 64 concurrent sends, so a burst does not churn through new TLS handshakes
// inside measured sends.
var streamTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = traceStreamSendWorkers
	t.HTTP2 = &http.HTTP2Config{SendPingTimeout: 10 * time.Second, PingTimeout: 5 * time.Second}
	return t
}()

// streamSDKSend returns the Sentry SDK stream's send: one probe trace on its
// own client, waiting for Sentry's answer.
//
// A client per send, because the SDK's transport sends one request at a time:
// with a shared client, a slow Sentry would queue each trace behind the
// previous ones, and our own queue would be measured as Sentry's latency
// (latency runs from the send). Each client is closed when its send returns,
// so its transport goroutine does not outlive it (see runTraceBatch). A fresh
// client also forgets any Retry-After, so during a rate limit every stream send
// gets its own 429 and counts as not sent.
func streamSDKSend(dsn, streamKey string, stats *deliveryStats) sendFunc {
	return func(ctx context.Context, seq int) (string, time.Time, error) {
		client, err := newStreamTracingClient(dsn, stats)
		if err != nil {
			return "", time.Time{}, fmt.Errorf("sentry client: %w", err)
		}
		defer client.Close()
		return sendTraceWithin(client, streamKey, seq, traceProbeSpans, streamAnswerBackstop)
	}
}

// runTraceStreamProbe runs the Sentry SDK stream until ctx is cancelled.
func runTraceStreamProbe(ctx context.Context, s *sentryProbe, cfg config, runID string, baseTags []string) {
	// Build one client up front only to fail fast on a bad DSN; each send
	// builds its own (streamSDKSend).
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("[%s] client: %v — trace stream disabled for this run", traceStreamSignal, err)
		return
	}
	client.Close()

	// Tagged like the batch probes so a stream trace can be found in Sentry and
	// traced back to the process that sent it. Lookups go by trace id, not tag.
	streamKey := runID + "-" + traceStreamSignal
	stats := &deliveryStats{}
	runStreamProbe(ctx, s, cfg, traceStreamSignal, streamKey, streamSDKSend(s.dsn, streamKey, stats), stats, baseTags)
}

// runOTLPStreamProbe runs the OpenTelemetry/OTLP stream until ctx is cancelled.
func runOTLPStreamProbe(ctx context.Context, s *sentryProbe, cfg config, runID string, baseTags []string) {
	stats := &deliveryStats{}
	o, err := newOTLPSender(s.dsn, stats)
	if err != nil {
		log.Printf("[%s] sender: %v — OTLP stream disabled for this run", traceStreamOTLPSignal, err)
		return
	}
	// Close runs after runStreamProbe returns, by which time runTraceStream
	// has joined every in-flight send.
	defer o.Close()

	streamKey := runID + "-" + traceStreamOTLPSignal
	runStreamProbe(ctx, s, cfg, traceStreamOTLPSignal, streamKey, func(ctx context.Context, seq int) (string, time.Time, error) {
		return o.send(streamKey, seq, traceProbeSpans)
	}, stats, baseTags)
}
