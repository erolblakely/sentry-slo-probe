package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"
)

func main() {
	cfg, err := configFromEnv()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx := context.Background()

	// Non-fatal on purpose. The batch probes emit no OTel spans of their own —
	// the legacy single-event probes were the only producers, and per-batch
	// self-instrumentation was deliberately dropped — so a failed exporter would
	// carry nothing. Exiting here would take the SLO measurement itself down for
	// zero benefit, which is the worst possible trade for a probe.
	tp, err := initTracer(ctx, cfg.ddAPIKey, cfg.ddSite)
	if err != nil {
		log.Printf("warning: init tracer: %v — continuing without OTel export", err)
	}
	if tp != nil {
		// Guarded: initTracer returns a nil provider alongside its error, and
		// Shutdown on a nil *TracerProvider would panic.
		defer tp.Shutdown(ctx)
	}

	s := newSentryProbe(cfg.sentryDSN, cfg.sentryAuthToken, cfg.sentryOrg, cfg.sentryProject)
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite)

	baseTags := []string{
		fmt.Sprintf("sentry_org:%s", cfg.sentryOrg),
		fmt.Sprintf("sentry_project:%s", cfg.sentryProject),
	}

	// One run id for the whole process, computed once. Logged here so an operator
	// who finds a probe_batch tag in Sentry can trace it back to the process that
	// wrote it.
	runID := newRunID(time.Now())

	log.Printf("Starting SLO probes (run=%s, interval=%s, batch=%d, send_window=%s, poll_timeout=%s, max_inflight=%d)",
		runID, cfg.interval, cfg.batchSize, cfg.sendWindow, cfg.pollTimeout, maxInflightBatches)

	var wg sync.WaitGroup
	wg.Add(3)

	// Each probe signal gets its own cap: one signal backing up behind a slow
	// Sentry must not stop the others from firing.

	// Probe 1: trace ingestion latency + reliability.
	go runBatchLoop("trace_ingestion", cfg, runID, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res := probeBatch(ctx, s, cfg, id)
		log.Printf("[trace_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		// The signal name is "ingestion", not "trace_ingestion". That choice
		// preserves the metric *prefix* sentry.ingestion.* — and nothing more. No
		// pre-batch dashboard query survives it: latency_ms became
		// latency_ms.p50/.p95/.p99, sent/received/success_rate are new, and
		// sentry.ingestion.error was retired with no successor, and Datadog does
		// not match a query on a parent name against sentry.ingestion.latency_ms.p95.
		// Every consumer had to be rewritten; see README "Metrics emitted" and
		// scripts/setup_datadog_slos.sh, which were.
		// postBatchMetrics copies baseTags before appending its probe tag.
		dd.postBatchMetrics("ingestion", res, baseTags)
	})

	// Probe 2: error ingestion latency + reliability.
	go runBatchLoop("error_ingestion", cfg, runID, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res := probeErrorBatch(ctx, s, cfg, id)
		log.Printf("[error_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		dd.postBatchMetrics("error_ingestion", res, baseTags)
	})

	// Probe 3: span completeness (ingestion of the batch, plus sampled
	// completeness over the transactions that arrived).
	go runBatchLoop("span_completeness", cfg, runID, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res, pct, known := probeSpansBatch(ctx, s, cfg, id)
		reportSpanCompleteness(dd, id, res, pct, known, baseTags)
	})

	wg.Wait()
}

// reportSpanCompleteness logs one span-completeness cycle and publishes its
// metrics, suppressing sentry.span_completeness.received_pct when the sample is
// unknown.
//
// A named function rather than a closure inside main because the suppression
// rule below is the most consequential decision in the batch reporting path,
// and a closure in main cannot be reached by a test. Pinned by
// TestReportSpanCompleteness.
func reportSpanCompleteness(dd *datadogClient, id string, res batchResult, pct float64, known bool, baseTags []string) {
	pctText := "unknown"
	if known {
		pctText = fmt.Sprintf("%.0f%%", pct)
	}
	log.Printf("[span_completeness] batch=%s received=%d/%d received_pct=%s",
		id, res.received, res.sent, pctText)
	// postBatchMetrics copies baseTags before appending its probe tag. It runs
	// either way: the ingestion side of this probe is measured even when the
	// completeness sample is not.
	dd.postBatchMetrics("span_completeness", res, baseTags)
	if !known {
		// known=false means the completeness sample is missing, not that no
		// spans arrived: the arrived-trace lookup failed, the span census
		// failed, or nothing arrived to sample. A sampled trace merely absent
		// from a *successful* census is not one of these — it genuinely stored
		// no child spans, and probe_spans.go scores it incomplete. Publishing 0
		// here would manufacture a total-failure reading out of an API hiccup,
		// so the metric is suppressed for this cycle and the gap stays visible
		// as a gap.
		log.Printf("[span_completeness] batch=%s: completeness not measured this cycle, suppressing sentry.span_completeness.received_pct", id)
		return
	}
	// Copy baseTags before appending. All three loops share one baseTags slice;
	// appending in place would be a concurrent write to a shared backing array
	// the moment baseTags has spare capacity, and would scribble one probe's tag
	// into another's metrics.
	dd.postMetricLogged("sentry.span_completeness.received_pct", pct,
		append(append([]string{}, baseTags...), "probe:span_completeness"))
}

// maxInflightBatches bounds concurrent batches per probe signal.
//
// This is load-bearing, not decorative. At defaults one batch is bounded at
// sendWindow (100s) + pollTimeout (120s) + pollInterval (5s) = 225s against an
// interval of 120s, so consecutive batches overlap in normal healthy operation:
// two in flight is the expected steady state.
//
// A *skip* is a different thing, and is not routine. Skipping needs two batches
// still alive at one tick, which means a batch that has outlived 2 x interval =
// 240s — 15s past runBatch's own 225s ceiling.
//
// The send path cannot get you there, so do not read a skip as a slow flush:
// runBatch's sender runs cfg.size sends over 8 workers at a 10s
// batchFlushTimeout (probe.go), which bounds it near the send window itself.
// The reachable causes all sit *outside* runBatch but *inside* the in-flight
// slot, because limiter.done() is deferred around the whole run(id) closure:
//
//   - span-completeness sampling: exactly two sequential Sentry calls at the
//     10s sentryHTTP timeout (probe_spans.go, sentry_api.go) — one Discover
//     query for the traces that arrived, one grouped count() census over the
//     sample — so ~20s worst case, and flat in SPAN_COMPLETENESS_SAMPLE;
//   - the Datadog submissions: six per signal, seven for span completeness, at
//     the client's 10s timeout each (datadog.go) — ~60s.
//
// So overlap is expected, and a skip points at slow Datadog submission or an
// unusually slow Sentry Discover query, not at the send path. The cap exists so
// that neither can grow the backlog without bound.
//
// An observation, not a TODO: span-completeness sampling used to dominate this
// arithmetic by a wide margin, because it scaled with the sample size. The
// single census removed that term, leaving Datadog as the only substantial
// out-of-runBatch cost, so skips should be markedly rarer than the cap was
// sized for and a cap of 2 may now be over-provisioned. Raising or lowering it
// is a behaviour change that wants its own measurement, and is deliberately not
// made here.
const maxInflightBatches = 2

// inflightCap bounds concurrent in-flight batches for one probe type.
type inflightCap struct {
	mu  sync.Mutex
	n   int
	max int // set once at construction; never written again, so safe to read unlocked
}

func newInflightCap(max int) *inflightCap { return &inflightCap{max: max} }

// try reserves a slot, reporting whether one was free.
func (c *inflightCap) try() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n >= c.max {
		return false
	}
	c.n++
	return true
}

// done releases a slot taken by try. The floor at zero keeps a stray release
// from widening the cap rather than being a no-op.
func (c *inflightCap) done() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n > 0 {
		c.n--
	}
}

// makeBatchID builds a deterministic per-cycle batch identifier (no wall clock
// and no randomness, so it stays testable): probe signal + cycle counter. It is
// named makeBatchID rather than batchID because the four batch probe functions
// already take a `batchID string` parameter, which would shadow the function
// inside their bodies. Process uniqueness is layered on by qualifyBatchID; the
// trace probes namespace the result further per Sentry signal (batchKeyFor)
// before tagging events.
func makeBatchID(signal string, n int) string {
	return fmt.Sprintf("%s-%d", signal, n)
}

// qualifyBatchID scopes a per-cycle batch id to one process run. Still pure —
// the run id is computed once in main and passed down — so it stays unit-tested.
//
// Load-bearing: the cycle counter restarts at 1 on every boot, while findBatch
// queries a statsPeriod of 1h with per_page = batchSize. Without the run
// component, a probe restarted inside that hour would re-use the previous run's
// batch id, whose events could fill the Discover page and hide the new run's
// own. observe() only credits ids already in sentAt, so received can never be
// inflated by this — the failure mode is a false reliability *drop*, for up to
// an hour, which for an SLO tool is the worst kind of wrong.
func qualifyBatchID(runID, signal string, n int) string {
	return runID + "-" + makeBatchID(signal, n)
}

// newRunID identifies one process run inside a batch id. Takes the clock as an
// argument so it stays a pure function of its input, and is called exactly once
// (in main) so every batch id from one process shares a run.
//
// Base36 nanoseconds: short, and [0-9a-z] only, so it never collides with the
// "-" that joins the id's parts. Nanoseconds rather than seconds because the
// whole point is surviving a restart, and a container relaunched inside the same
// second must still get a fresh id.
//
// Ids are 12 characters and so sort lexicographically by start time for the rest
// of this century; the nanosecond count gains a 13th character around 2120,
// after which newer ids sort before older ones. One century, not centuries —
// nothing depends on the ordering, it is only a convenience when scanning logs.
func newRunID(now time.Time) string {
	return strconv.FormatInt(now.UTC().UnixNano(), 36)
}

// runBatchLoop fires a batch immediately and then every cfg.interval, capping
// how many batches of this signal may be in flight at once. Each batch runs in
// its own goroutine because a batch routinely outlives the interval; the loop
// must keep ticking rather than block on the previous cycle.
//
// UNTESTED PROCESS GLUE — marked as such deliberately, per the plan's TDD rule.
// `for range ticker.C` has no termination path, so the loop cannot be driven to
// completion from a test without adding a stop channel, which would be a
// behaviour change. It is verified only by the live end-to-end run. What no
// automated test therefore covers: that a skipped cycle does not invoke `run`;
// that `cycle` increments even on a skip (so ids stay unique and the gap in the
// sequence records the skip); that the id handed to `run` is the run-qualified
// one, not the bare per-cycle id; and that the first fire precedes the ticker's
// first tick. The parts it composes — inflightCap, qualifyBatchID, newRunID —
// are each unit tested on their own.
func runBatchLoop(name string, cfg config, runID string, limiter *inflightCap, wg *sync.WaitGroup, run func(id string)) {
	defer wg.Done()
	log.Printf("[%s] starting (interval=%s, batch=%d)", name, cfg.interval, cfg.batchSize)
	cycle := 0
	fire := func() {
		// Incremented before the cap check so a skipped cycle still consumes its
		// number: batch ids stay unique, and the gaps in the sequence show up in
		// the logs as the skips they were.
		cycle++
		id := qualifyBatchID(runID, name, cycle)
		if !limiter.try() {
			// Two batches overlapping is normal (see maxInflightBatches); a skip
			// is not. It means a batch has outlived 2x the interval, past the
			// send+poll ceiling — which the send path cannot cause, so the message
			// names the work that can: the post-batch Sentry census and Datadog
			// submissions that also run inside the in-flight slot. Datadog is the
			// larger of the two now that the census is two calls rather than one
			// per sampled event, so it is named first.
			log.Printf("[%s] skipping cycle %d (batch=%s): %d batches still draining, at in-flight cap — a batch has outlived %s (2x the %s interval), past the send+poll ceiling: check Datadog submission latency and the two Sentry Discover queries (span-completeness census), which run inside the slot after the batch drains",
				name, cycle, id, limiter.max, 2*cfg.interval, cfg.interval)
			return
		}
		go func() {
			defer limiter.done()
			run(id)
		}()
	}
	fire()
	ticker := time.NewTicker(cfg.interval)
	defer ticker.Stop()
	for range ticker.C {
		fire()
	}
}

type config struct {
	sentryDSN       string
	sentryAuthToken string
	sentryOrg       string
	sentryProject   string
	ddAPIKey        string
	ddSite          string
	interval        time.Duration
	pollTimeout     time.Duration
	pollInterval    time.Duration
	batchSize       int
	sendWindow      time.Duration
	spanSample      int
}

func configFromEnv() (config, error) {
	required := map[string]string{
		"SENTRY_DSN":        os.Getenv("SENTRY_DSN"),
		"SENTRY_AUTH_TOKEN": os.Getenv("SENTRY_AUTH_TOKEN"),
		"SENTRY_ORG":        os.Getenv("SENTRY_ORG"),
		"SENTRY_PROJECT":    os.Getenv("SENTRY_PROJECT"),
		"DD_API_KEY":        os.Getenv("DD_API_KEY"),
	}
	for k, v := range required {
		if v == "" {
			return config{}, fmt.Errorf("required env var %s is not set", k)
		}
	}

	ddSite := os.Getenv("DD_SITE")
	if ddSite == "" {
		ddSite = "datadoghq.com"
	}

	return config{
		sentryDSN:       required["SENTRY_DSN"],
		sentryAuthToken: required["SENTRY_AUTH_TOKEN"],
		sentryOrg:       required["SENTRY_ORG"],
		sentryProject:   required["SENTRY_PROJECT"],
		ddAPIKey:        required["DD_API_KEY"],
		ddSite:          ddSite,
		interval:        envDuration("PROBE_INTERVAL_SECONDS", 120),
		pollTimeout:     envDuration("POLL_TIMEOUT_SECONDS", 120),
		pollInterval:    envDuration("POLL_INTERVAL_SECONDS", 5),
		batchSize:       envInt("PROBE_BATCH_SIZE", 100),
		sendWindow:      envDuration("PROBE_SEND_WINDOW_SECONDS", 100),
		spanSample:      envInt("SPAN_COMPLETENESS_SAMPLE", 20),
	}, nil
}

func envDuration(key string, defaultSeconds int) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return time.Duration(defaultSeconds) * time.Second
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
