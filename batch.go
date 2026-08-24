package main

import (
	"context"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

// percentile returns the nearest-rank percentile p (0-100) of latencies.
// It returns 0 for empty input and does not mutate the input slice.
func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	rank := int(math.Ceil(p/100*float64(len(sorted)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(sorted) {
		rank = len(sorted) - 1
	}
	return sorted[rank]
}

// sendOffsets returns size offsets from the batch start, evenly spaced across
// window with step window/size (so the last send leaves headroom before the
// window closes). Returns nil for size <= 0.
func sendOffsets(size int, window time.Duration) []time.Duration {
	if size <= 0 {
		return nil
	}
	offsets := make([]time.Duration, size)
	step := window / time.Duration(size)
	for i := range offsets {
		offsets[i] = time.Duration(i) * step
	}
	return offsets
}

// batchResult is the outcome of one batch run.
//
// measured is the measurement-validity bit, and it is what separates "Sentry
// dropped our events" from "we could not look". received is only a numerator
// when we actually managed to ask Sentry what arrived: sends authenticate with
// the DSN while queries authenticate with SENTRY_AUTH_TOKEN, so an expired token
// yields sent=N, received=0 and a 0% success_rate that blames Sentry for our own
// credential failure. That is not hypothetical — a live run produced 21 x
// "401 Invalid token" with all three probes at received=0/N.
//
// It is false by default, which is the safe direction: the early
// `return batchResult{}` paths in runTraceBatch and probeErrorBatch (client
// construction failed, nothing was sent and nothing was queried) leave it false
// for free, so one predicate covers both failure classes. runBatch sets it true
// on the first query() that returns a nil error.
//
// A false measured must suppress the WHOLE signal for the cycle, not just
// received — see postBatchMetrics.
type batchResult struct {
	sent      int
	received  int
	latencies []time.Duration
	measured  bool
}

// latencyTracker records per-id send times and, on each poll, the latency of
// ids seen for the first time. Not safe for concurrent use; callers guard it.
type latencyTracker struct {
	sentAt    map[string]time.Time
	latencies map[string]time.Duration
}

func newLatencyTracker() *latencyTracker {
	return &latencyTracker{
		sentAt:    make(map[string]time.Time),
		latencies: make(map[string]time.Duration),
	}
}

func (t *latencyTracker) markSent(id string, at time.Time) {
	t.sentAt[id] = at
}

func (t *latencyTracker) observe(found map[string]bool, now time.Time) int {
	n := 0
	for id := range found {
		if _, done := t.latencies[id]; done {
			continue
		}
		if sent, ok := t.sentAt[id]; ok {
			t.latencies[id] = now.Sub(sent)
			n++
		}
	}
	return n
}

func (t *latencyTracker) receivedCount() int { return len(t.latencies) }

func (t *latencyTracker) result(sent int) batchResult {
	out := make([]time.Duration, 0, len(t.latencies))
	for _, d := range t.latencies {
		out = append(out, d)
	}
	return batchResult{sent: sent, received: len(t.latencies), latencies: out}
}

// batchDone reports whether polling should stop: either every sent event has
// been received, or we are past the drain deadline (lastSendAt + pollTimeout).
func batchDone(received, size int, now, lastSendAt time.Time, pollTimeout time.Duration) bool {
	if received >= size {
		return true
	}
	return now.After(lastSendAt.Add(pollTimeout))
}

// sentCount returns how many distinct ids were marked sent. markSent is only
// called for sends that succeeded, so this is the denominator for
// success_rate = received / sent: the rate must measure Sentry's ingestion
// reliability, not our own send failures, which would otherwise read as Sentry
// dropping events. Send-failure volume stays visible as the gap between this
// and the configured batch size. Not safe for concurrent use; callers guard it.
func (t *latencyTracker) sentCount() int { return len(t.sentAt) }

// sendFunc sends one event (identified by seq) and returns the id used to find
// it later plus the local send time.
type sendFunc func(ctx context.Context, seq int) (id string, sentAt time.Time, err error)

// queryFunc returns the set of batch ids ingested so far.
type queryFunc func(ctx context.Context) (map[string]bool, error)

type batchConfig struct {
	size         int
	sendWindow   time.Duration
	pollTimeout  time.Duration
	pollInterval time.Duration
	sendWorkers  int
}

// batchConfigFrom derives the batch engine's settings from the process config.
// sendWorkers is fixed: the pacing schedule, not concurrency, sets the send
// rate; the pool only absorbs the tail of slow sends so one lagging flush does
// not push later sends outside the window.
func batchConfigFrom(cfg config) batchConfig {
	return batchConfig{
		size:         cfg.batchSize,
		sendWindow:   cfg.sendWindow,
		pollTimeout:  cfg.pollTimeout,
		pollInterval: cfg.pollInterval,
		sendWorkers:  8,
	}
}

// runBatch paces cfg.size sends across cfg.sendWindow while polling query every
// cfg.pollInterval, recording per-event latency, until all are received or the
// drain deadline passes.
func runBatch(ctx context.Context, cfg batchConfig, send sendFunc, query queryFunc) batchResult {
	tracker := newLatencyTracker()
	var mu sync.Mutex
	// measured is guarded by mu alongside the tracker: it is written in the poll
	// loop and read at both return sites. See batchResult.measured for why the
	// bit exists; one successful query is enough, because it establishes that we
	// were able to ask Sentry at all.
	measured := false
	// finish builds the result. Callers must hold mu.
	finish := func() batchResult {
		res := tracker.result(tracker.sentCount())
		res.measured = measured
		return res
	}
	offsets := sendOffsets(cfg.size, cfg.sendWindow)
	start := time.Now()

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		workers := cfg.sendWorkers
		if workers <= 0 {
			workers = 1
		}
		sem := make(chan struct{}, workers)
		var wg sync.WaitGroup
		for i := 0; i < cfg.size; i++ {
			if wait := offsets[i] - time.Since(start); wait > 0 {
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					wg.Wait()
					return
				}
			}
			wg.Add(1)
			sem <- struct{}{}
			go func(seq int) {
				defer wg.Done()
				defer func() { <-sem }()
				id, at, err := send(ctx, seq)
				if err != nil {
					log.Printf("[batch] send seq=%d: %v", seq, err)
					return
				}
				mu.Lock()
				tracker.markSent(id, at)
				mu.Unlock()
			}(i)
		}
		wg.Wait()
	}()

	// The drain deadline is measured from the last SCHEDULED send, not from the
	// nominal end of the send window. sendOffsets spaces size sends at
	// window/size, so the final one leaves at (size-1)/size * window — the window
	// end is a time at which nothing is sent.
	//
	// Anchoring to the window end padded every batch by one step and broke the
	// small end outright: at PROBE_BATCH_SIZE=1 the only send happens at t=0, yet
	// polling ran to sendWindow+pollTimeout (~220s at defaults) rather than the
	// ~120s the single-event path took. At size 100 the same slack works the
	// other way, letting the sender run past the window and eat into the tail
	// events' drain budget — biasing received DOWN exactly when Sentry is slow.
	//
	// offsets is nil for size <= 0 (sendOffsets), so the deadline collapses to
	// start+pollTimeout and the run terminates rather than indexing a nil slice.
	lastSendAt := start
	if len(offsets) > 0 {
		lastSendAt = start.Add(offsets[len(offsets)-1])
	}
	ticker := time.NewTicker(cfg.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			<-sendDone
			mu.Lock()
			defer mu.Unlock()
			return finish()
		case <-ticker.C:
			found, err := query(ctx)
			now := time.Now()
			mu.Lock()
			if err != nil {
				log.Printf("[batch] query: %v", err)
			} else {
				// One nil error is all it takes: it proves the credentials and
				// the endpoint worked, so received is a real numerator for this
				// cycle. A later transient failure must not retract a
				// measurement we did make.
				measured = true
				tracker.observe(found, now)
			}
			received := tracker.receivedCount()
			mu.Unlock()
			// batchDone asks about the intended size, not the successful sends:
			// a batch whose sends partly failed should still drain to the
			// deadline rather than stop early.
			if batchDone(received, cfg.size, now, lastSendAt, cfg.pollTimeout) {
				<-sendDone
				mu.Lock()
				defer mu.Unlock()
				return finish()
			}
		}
	}
}
