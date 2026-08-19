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

	tp, err := initTracer(ctx, cfg.ddAPIKey, cfg.ddSite)
	if err != nil {
		log.Fatalf("init tracer: %v", err)
	}
	defer tp.Shutdown(ctx)

	s := newSentryProbe(cfg.sentryDSN, cfg.sentryAuthToken, cfg.sentryOrg, cfg.sentryProject)
	dd := newDatadogClient(cfg.ddAPIKey, cfg.ddSite)

	baseTags := []string{
		fmt.Sprintf("sentry_org:%s", cfg.sentryOrg),
		fmt.Sprintf("sentry_project:%s", cfg.sentryProject),
	}

	log.Printf("Starting SLO probes (interval=%s, batch=%d, send_window=%s, poll_timeout=%s, max_inflight=%d)",
		cfg.interval, cfg.batchSize, cfg.sendWindow, cfg.pollTimeout, maxInflightBatches)

	var wg sync.WaitGroup
	wg.Add(3)

	// Each probe signal gets its own cap: one signal backing up behind a slow
	// Sentry must not stop the others from firing.

	// Probe 1: trace ingestion latency + reliability.
	go runBatchLoop("trace_ingestion", cfg, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res := probeBatch(ctx, s, cfg, id)
		log.Printf("[trace_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		// "ingestion", not "trace_ingestion": keeps the metric namespace
		// (sentry.ingestion.*) the dashboards already read.
		// postBatchMetrics copies baseTags before appending its probe tag.
		dd.postBatchMetrics("ingestion", res, baseTags)
	})

	// Probe 2: error ingestion latency + reliability.
	go runBatchLoop("error_ingestion", cfg, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res := probeErrorBatch(ctx, s, cfg, id)
		log.Printf("[error_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		dd.postBatchMetrics("error_ingestion", res, baseTags)
	})

	// Probe 3: span completeness (ingestion of the batch, plus sampled
	// completeness over the transactions that arrived).
	go runBatchLoop("span_completeness", cfg, newInflightCap(maxInflightBatches), &wg, func(id string) {
		res, pct, known := probeSpansBatch(ctx, s, cfg, id)
		pctText := "unknown"
		if known {
			pctText = fmt.Sprintf("%.0f%%", pct)
		}
		log.Printf("[span_completeness] batch=%s received=%d/%d received_pct=%s",
			id, res.received, res.sent, pctText)
		dd.postBatchMetrics("span_completeness", res, baseTags)
		if !known {
			// known=false means the completeness sample is missing, not that no
			// spans arrived: either the event-id lookup failed or no sampled
			// event returned a span count. Publishing 0 would manufacture a
			// total-failure reading out of an API hiccup, so the metric is
			// suppressed for this cycle and the gap stays visible as a gap.
			log.Printf("[span_completeness] batch=%s: completeness not measured this cycle, suppressing sentry.span_completeness.received_pct", id)
			return
		}
		// Copy baseTags before appending. All three loops share one baseTags
		// slice; appending in place would be a concurrent write to a shared
		// backing array the moment baseTags has spare capacity, and would
		// scribble one probe's tag into another's metrics.
		dd.postMetricLogged("sentry.span_completeness.received_pct", pct,
			append(append([]string{}, baseTags...), "probe:span_completeness"))
	})

	wg.Wait()
}

// maxInflightBatches bounds concurrent batches per probe signal.
//
// This is load-bearing, not decorative. At defaults one batch runs for up to
// sendWindow (100s) + pollTimeout (120s) ~= 220s against an interval of 120s,
// so consecutive batches overlap in normal healthy operation — two in flight is
// the expected steady state, and the skip log below is a routine sight. The cap
// exists so that a Sentry stall cannot grow the backlog without bound.
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

// batchID builds a unique, deterministic batch identifier (no wall clock so it
// stays testable): probe signal + cycle counter. The trace probes namespace it
// further per Sentry signal (batchKeyFor) before tagging events.
func batchID(signal string, n int) string {
	return fmt.Sprintf("%s-%d", signal, n)
}

// runBatchLoop fires a batch immediately and then every cfg.interval, capping
// how many batches of this signal may be in flight at once. Each batch runs in
// its own goroutine because a batch routinely outlives the interval; the loop
// must keep ticking rather than block on the previous cycle.
func runBatchLoop(name string, cfg config, limiter *inflightCap, wg *sync.WaitGroup, run func(id string)) {
	defer wg.Done()
	log.Printf("[%s] starting (interval=%s, batch=%d)", name, cfg.interval, cfg.batchSize)
	cycle := 0
	fire := func() {
		// Incremented before the cap check so a skipped cycle still consumes its
		// number: batch ids stay unique, and the gaps in the sequence show up in
		// the logs as the skips they were.
		cycle++
		id := batchID(name, cycle)
		if !limiter.try() {
			log.Printf("[%s] skipping cycle %d (batch=%s): %d batches still draining, at in-flight cap — expected at defaults, where a batch can outlast the %s interval",
				name, cycle, id, limiter.max, cfg.interval)
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
