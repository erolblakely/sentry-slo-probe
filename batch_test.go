package main

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPercentile(t *testing.T) {
	lat := []time.Duration{}
	for i := 1; i <= 10; i++ {
		lat = append(lat, time.Duration(i)*time.Second)
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{
		{50, 5 * time.Second},
		{95, 10 * time.Second},
		{99, 10 * time.Second},
	}
	for _, c := range cases {
		if got := percentile(lat, c.p); got != c.want {
			t.Errorf("percentile(%v) = %s, want %s", c.p, got, c.want)
		}
	}
	if got := percentile(nil, 50); got != 0 {
		t.Errorf("percentile(nil) = %s, want 0", got)
	}
	// input must not be reordered
	orig := []time.Duration{3 * time.Second, 1 * time.Second, 2 * time.Second}
	_ = percentile(orig, 50)
	if orig[0] != 3*time.Second {
		t.Errorf("percentile mutated input slice")
	}
}

func TestSendOffsets(t *testing.T) {
	got := sendOffsets(4, 100*time.Second)
	want := []time.Duration{0, 25 * time.Second, 50 * time.Second, 75 * time.Second}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("offset[%d] = %s, want %s", i, got[i], want[i])
		}
	}
	if one := sendOffsets(1, 100*time.Second); len(one) != 1 || one[0] != 0 {
		t.Errorf("sendOffsets(1) = %v, want [0]", one)
	}
	if zero := sendOffsets(0, time.Second); zero != nil {
		t.Errorf("sendOffsets(0) = %v, want nil", zero)
	}
}

func TestLatencyTrackerSuccessivePolls(t *testing.T) {
	t0 := time.Unix(1000, 0)
	tr := newLatencyTracker()
	tr.markSent("a", t0)
	tr.markSent("b", t0)

	if n := tr.observe(map[string]bool{"a": true}, t0.Add(5*time.Second)); n != 1 {
		t.Fatalf("first observe newly = %d, want 1", n)
	}
	// second poll: a already recorded, b newly found
	if n := tr.observe(map[string]bool{"a": true, "b": true}, t0.Add(8*time.Second)); n != 1 {
		t.Fatalf("second observe newly = %d, want 1", n)
	}
	// ignore ids never marked sent
	if n := tr.observe(map[string]bool{"ghost": true}, t0.Add(9*time.Second)); n != 0 {
		t.Fatalf("ghost observe newly = %d, want 0", n)
	}
	if tr.receivedCount() != 2 {
		t.Fatalf("receivedCount = %d, want 2", tr.receivedCount())
	}
	res := tr.result(2)
	if res.sent != 2 || res.received != 2 || len(res.latencies) != 2 {
		t.Fatalf("result = %+v", res)
	}
	// latencies are 5s (a) and 8s (b), order-independent
	got := map[time.Duration]bool{}
	for _, d := range res.latencies {
		got[d] = true
	}
	if !got[5*time.Second] || !got[8*time.Second] {
		t.Errorf("latencies = %v, want {5s,8s}", res.latencies)
	}
}

func TestBatchDone(t *testing.T) {
	last := time.Unix(2000, 0)
	to := 60 * time.Second
	if !batchDone(10, 10, last, last, to) {
		t.Error("all received should be done")
	}
	if batchDone(5, 10, last.Add(30*time.Second), last, to) {
		t.Error("partial before timeout should not be done")
	}
	if !batchDone(5, 10, last.Add(61*time.Second), last, to) {
		t.Error("partial past timeout should be done")
	}
}

// TestBatchConfigFrom pins the field-by-field mapping from process config onto
// the batch engine's config. Every duration is a distinct value, so swapping
// any two fields (sendWindow/pollTimeout in particular, which have the same
// type and plausible magnitudes) fails here instead of surfacing as
// inexplicable pacing during live testing. interval is set too and must not
// appear in the batch config at all: it paces the outer loop, not the batch.
func TestBatchConfigFrom(t *testing.T) {
	cfg := config{
		interval:     11 * time.Second,
		pollTimeout:  22 * time.Second,
		pollInterval: 33 * time.Second,
		batchSize:    44,
		sendWindow:   55 * time.Second,
		spanSample:   66,
	}
	got := batchConfigFrom(cfg)

	if got.size != cfg.batchSize {
		t.Errorf("size = %d, want batchSize %d", got.size, cfg.batchSize)
	}
	if got.sendWindow != cfg.sendWindow {
		t.Errorf("sendWindow = %s, want cfg.sendWindow %s", got.sendWindow, cfg.sendWindow)
	}
	if got.pollTimeout != cfg.pollTimeout {
		t.Errorf("pollTimeout = %s, want cfg.pollTimeout %s", got.pollTimeout, cfg.pollTimeout)
	}
	if got.pollInterval != cfg.pollInterval {
		t.Errorf("pollInterval = %s, want cfg.pollInterval %s", got.pollInterval, cfg.pollInterval)
	}
	// sendWorkers is fixed, not derived: pacing sets the send rate, the pool
	// only absorbs the tail of slow flushes.
	if got.sendWorkers != 8 {
		t.Errorf("sendWorkers = %d, want 8", got.sendWorkers)
	}
}

func TestRunBatchAllArrive(t *testing.T) {
	cfg := batchConfig{
		size: 5, sendWindow: 40 * time.Millisecond,
		pollTimeout: 200 * time.Millisecond, pollInterval: 10 * time.Millisecond,
		sendWorkers: 3,
	}
	var sent sync.Map
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		id := "e" + string(rune('0'+seq))
		sent.Store(id, true)
		return id, time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		found := map[string]bool{}
		sent.Range(func(k, _ any) bool { found[k.(string)] = true; return true })
		return found, nil
	}
	res := runBatch(context.Background(), cfg, send, query)
	if res.sent != 5 || res.received != 5 || len(res.latencies) != 5 {
		t.Fatalf("result = %+v, want sent=received=5", res)
	}
	if !res.measured {
		t.Error("measured = false after successful queries; the cycle's metrics would be suppressed")
	}
}

// TestRunBatchQueryAlwaysFailsIsNotMeasured is the expired-auth-token scenario,
// and the reason batchResult carries a validity bit at all. Sends authenticate
// with the DSN and succeed; every query authenticates with SENTRY_AUTH_TOKEN and
// returns 401. Without the bit that reads out as sent=N, received=0 →
// success_rate 0% — a confident false accusation against Sentry for our own
// credential failure. This was reproduced live: 21 x "401 Invalid token".
//
// sent must still be honest (the events really did leave), but measured must be
// false so the reporting layer suppresses the whole signal for the cycle.
func TestRunBatchQueryAlwaysFailsIsNotMeasured(t *testing.T) {
	cfg := batchConfig{
		size: 3, sendWindow: 10 * time.Millisecond,
		pollTimeout: 30 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 2,
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return "e" + strconv.Itoa(seq), time.Now(), nil
	}
	var calls int64
	query := func(ctx context.Context) (map[string]bool, error) {
		atomic.AddInt64(&calls, 1)
		return nil, errUnauthorized
	}

	res := runBatch(context.Background(), cfg, send, query)

	if atomic.LoadInt64(&calls) == 0 {
		t.Fatal("query never called; the test proves nothing")
	}
	if res.measured {
		t.Error("measured = true although every query failed: an unmeasurable cycle would publish received=0 and read as a Sentry outage")
	}
	if res.sent != 3 {
		t.Errorf("sent = %d, want 3: the sends genuinely succeeded and must stay honest", res.sent)
	}
	if res.received != 0 {
		t.Errorf("received = %d, want 0", res.received)
	}
}

// TestRunBatchMeasuredOnAnySuccessfulQuery: one good poll is enough. A single
// transient 500 in the middle of an otherwise healthy cycle must not throw away
// a measurement we did make — the bit means "we managed to ask", not "every ask
// worked".
func TestRunBatchMeasuredOnAnySuccessfulQuery(t *testing.T) {
	cfg := batchConfig{
		size: 1, sendWindow: 5 * time.Millisecond,
		pollTimeout: 60 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 1,
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return "only", time.Now(), nil
	}
	var calls int64
	query := func(ctx context.Context) (map[string]bool, error) {
		// Fail the first poll, succeed afterwards.
		if atomic.AddInt64(&calls, 1) == 1 {
			return nil, errUnauthorized
		}
		return map[string]bool{"only": true}, nil
	}

	res := runBatch(context.Background(), cfg, send, query)

	if !res.measured {
		t.Error("measured = false although a later query succeeded")
	}
	if res.received != 1 {
		t.Errorf("received = %d, want 1", res.received)
	}
}

func TestRunBatchTimeoutNoneArrive(t *testing.T) {
	cfg := batchConfig{
		size: 3, sendWindow: 10 * time.Millisecond,
		pollTimeout: 30 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 2,
	}
	var calls int64
	// Per-seq ids, not a constant. A constant "x" collapses every send into one
	// sentAt entry, so this exercised the markSent-overwrite path with sent=1
	// rather than a three-send batch, and the sent assertion below could not have
	// caught a regression in the denominator.
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return "x" + strconv.Itoa(seq), time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		atomic.AddInt64(&calls, 1)
		return map[string]bool{}, nil // nothing ever arrives
	}
	start := time.Now()
	res := runBatch(context.Background(), cfg, send, query)
	if res.sent != 3 {
		t.Fatalf("sent = %d, want 3 (one entry per seq)", res.sent)
	}
	if res.received != 0 {
		t.Fatalf("received = %d, want 0", res.received)
	}
	// The queries all succeeded and honestly reported an empty Sentry. That is a
	// real 0% breach and must be published, which is exactly what distinguishes
	// it from TestRunBatchQueryAlwaysFailsIsNotMeasured.
	if !res.measured {
		t.Error("measured = false although every query succeeded; a genuine total loss must still burn error budget")
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("runBatch took %s, expected to stop near drain deadline", elapsed)
	}
	if atomic.LoadInt64(&calls) == 0 {
		t.Fatal("query never called")
	}
}

// TestRunBatchDrainDeadlineAnchoredToLastScheduledSend pins what the drain
// deadline is measured from: the last SCHEDULED send offset, not the nominal end
// of the send window.
//
// At PROBE_BATCH_SIZE=1 the only send happens at t=0, so the batch should stop
// about pollTimeout later. Anchoring to start+sendWindow instead makes it poll
// for sendWindow+pollTimeout — at production defaults that is ~220s of polling
// for an event sent at t=0, against the 120s the single-event path used to take.
//
// The same anchor matters at full size for the opposite reason: the last event
// is scheduled at (size-1)/size * window, and giving it exactly pollTimeout from
// there is what stops the tail's drain budget from being quietly padded.
//
// Timings are deliberately far apart (200ms window vs 40ms timeout) so the two
// behaviours cannot be confused by scheduler jitter.
func TestRunBatchDrainDeadlineAnchoredToLastScheduledSend(t *testing.T) {
	cfg := batchConfig{
		size: 1, sendWindow: 200 * time.Millisecond,
		pollTimeout: 40 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 1,
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return "only", time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return map[string]bool{}, nil // never arrives
	}

	start := time.Now()
	res := runBatch(context.Background(), cfg, send, query)
	elapsed := time.Since(start)

	if res.received != 0 {
		t.Fatalf("received = %d, want 0", res.received)
	}
	// Generous ceiling: the honest bound is ~45ms, the buggy one ~245ms.
	if elapsed > 150*time.Millisecond {
		t.Errorf("runBatch took %s for a single send at t=0 with a %s poll timeout; the drain deadline is anchored to the nominal window end (%s) instead of the last scheduled send",
			elapsed, cfg.pollTimeout, cfg.sendWindow)
	}
	if elapsed < cfg.pollTimeout {
		t.Errorf("runBatch took %s, less than the %s poll timeout: the event was not given its full drain budget", elapsed, cfg.pollTimeout)
	}
}

// TestRunBatchZeroSizeTerminates guards the empty-offsets case the anchoring
// change has to handle: sendOffsets returns nil for size <= 0, so indexing the
// last offset unguarded would panic.
func TestRunBatchZeroSizeTerminates(t *testing.T) {
	cfg := batchConfig{
		size: 0, sendWindow: 10 * time.Millisecond,
		pollTimeout: 20 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 1,
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		t.Error("send called for a zero-size batch")
		return "", time.Time{}, nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return map[string]bool{}, nil
	}

	done := make(chan batchResult, 1)
	go func() { done <- runBatch(context.Background(), cfg, send, query) }()
	select {
	case res := <-done:
		if res.sent != 0 || res.received != 0 {
			t.Errorf("result = %+v, want an empty batch", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runBatch did not terminate for size 0")
	}
}

// errSendFailed stands in for a local send failure (network blip, transport
// error) in tests -- the class of failure that must not be blamed on Sentry.
var errSendFailed = errors.New("send failed")

// errUnauthorized stands in for the Sentry Discover API rejecting our auth
// token. Sends keep working (they use the DSN), so this is the failure that
// makes received meaningless while sent stays real.
var errUnauthorized = errors.New("sentry api 401: Invalid token")

// TestRunBatchFailedSendsExcludedFromSent pins the rule that res.sent counts
// only sends that succeeded, so success_rate = received/sent measures Sentry's
// ingestion rather than our own local send failures. Here 2 of 4 sends fail
// locally and both surviving events arrive, which must read as 2/2 (100%) --
// not 2/4, which would look like Sentry dropping half the batch.
//
// cfg.size stays 4 so received (max 2) never reaches size: the run must stop via
// the drain deadline (sendWindow+pollTimeout ~= 80ms), which also keeps this
// test bounded rather than hanging on regression.
func TestRunBatchFailedSendsExcludedFromSent(t *testing.T) {
	cfg := batchConfig{
		size: 4, sendWindow: 20 * time.Millisecond,
		pollTimeout: 60 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 2,
	}
	var delivered sync.Map
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		if seq >= 2 {
			return "", time.Time{}, errSendFailed
		}
		id := "ok" + string(rune('0'+seq))
		delivered.Store(id, true)
		return id, time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		found := map[string]bool{}
		delivered.Range(func(k, _ any) bool { found[k.(string)] = true; return true })
		return found, nil
	}
	start := time.Now()
	res := runBatch(context.Background(), cfg, send, query)
	if res.sent != 2 {
		t.Errorf("sent = %d, want 2 (failed sends must not count toward sent)", res.sent)
	}
	if res.received != 2 || len(res.latencies) != 2 {
		t.Errorf("received = %d, latencies = %d, want 2 and 2", res.received, len(res.latencies))
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("runBatch took %s, expected to stop near drain deadline", elapsed)
	}
}
