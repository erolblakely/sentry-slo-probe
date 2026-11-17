package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeClock is a settable clock for traceStream, so the deadline arithmetic can
// be pinned to exact instants instead of sleeps.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

var streamT0 = time.Date(2026, 11, 12, 10, 0, 0, 0, time.UTC)

const (
	testStreamTimeout = 120 * time.Second
	testStreamGrace   = 30 * time.Second
)

func newTestStream() (*traceStream, *fakeClock) {
	clk := &fakeClock{t: streamT0}
	return newTraceStream("trace_stream", testStreamTimeout, testStreamGrace, clk.now), clk
}

// findAll is a query that reports every requested trace as arrived.
func findAll(ctx context.Context, ids []string) (map[string]bool, error) {
	found := map[string]bool{}
	for _, id := range ids {
		found[id] = true
	}
	return found, nil
}

// findNone is a query that succeeds and finds nothing.
func findNone(ctx context.Context, ids []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

func failQuery(ctx context.Context, ids []string) (map[string]bool, error) {
	return nil, errUnauthorized
}

// TestTraceStreamArrivalRecordsLatency: latency is the gap between the local
// send stamp and the poll that first saw the trace.
func TestTraceStreamArrivalRecordsLatency(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(57 * time.Second))
	s.poll(context.Background(), findAll)

	w := s.takeWindow()
	if !slices.Equal(w.arrived, []time.Duration{57 * time.Second}) {
		t.Errorf("arrived = %v, want [57s]", w.arrived)
	}
	if w.lost != 0 || w.pending != 0 {
		t.Errorf("lost = %d, pending = %d, want 0 and 0", w.lost, w.pending)
	}
}

// TestTraceStreamLostExactlyAtDeadline pins the deadline boundary. A trace not
// seen one second before the timeout is still in flight; a trace not seen at the
// timeout is lost. Getting this wrong in either direction moves traces between
// "slow" and "lost" and shifts success_rate.
func TestTraceStreamLostExactlyAtDeadline(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(testStreamTimeout - time.Second))
	s.poll(context.Background(), findNone)
	if w := s.takeWindow(); w.lost != 0 || w.pending != 1 {
		t.Fatalf("1s before the deadline: lost = %d, pending = %d, want 0 and 1", w.lost, w.pending)
	}

	clk.set(streamT0.Add(testStreamTimeout))
	s.poll(context.Background(), findNone)
	w := s.takeWindow()
	if w.lost != 1 || w.pending != 0 || len(w.arrived) != 0 {
		t.Errorf("at the deadline: lost = %d, pending = %d, arrived = %v, want 1, 0, []", w.lost, w.pending, w.arrived)
	}
}

// TestTraceStreamFoundPastDeadlineCountsAsArrived: the first successful look
// past the deadline still credits a trace it finds, with its full (pessimistic)
// latency. This matches runBatch, which observes before checking its own drain
// deadline. The opposite order would count a trace Sentry holds as lost.
func TestTraceStreamFoundPastDeadlineCountsAsArrived(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(125 * time.Second))
	s.poll(context.Background(), findAll)

	w := s.takeWindow()
	if w.lost != 0 || !slices.Equal(w.arrived, []time.Duration{125 * time.Second}) {
		t.Errorf("lost = %d, arrived = %v, want 0 and [2m5s]", w.lost, w.arrived)
	}
}

// TestTraceStreamFailedQueryNeverCountsLoss is the expired-token scenario from
// runBatch, applied to the stream. "Lost" means a successful look at Sentry,
// at or past the deadline, did not find the trace. A failed look proves nothing,
// so a trace whose every check failed is never charged to Sentry. It is held a
// grace period in case the API recovers, then dropped from both sides of
// success_rate.
func TestTraceStreamFailedQueryNeverCountsLoss(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(testStreamTimeout + 10*time.Second))
	s.poll(context.Background(), failQuery)
	w := s.takeWindow()
	if w.lost != 0 || w.pending != 1 || w.queryFailed != 1 {
		t.Fatalf("past the deadline with a failed query: lost = %d, pending = %d, queryFailed = %d, want 0, 1, 1", w.lost, w.pending, w.queryFailed)
	}

	// Recovery inside the grace period still resolves it normally.
	s2, clk2 := newTestStream()
	s2.recordSend("b", streamT0, nil)
	clk2.set(streamT0.Add(testStreamTimeout + testStreamGrace - time.Second))
	s2.poll(context.Background(), findNone)
	if w := s2.takeWindow(); w.lost != 1 || w.abandoned != 0 {
		t.Errorf("successful look inside the grace period: lost = %d, abandoned = %d, want 1 and 0", w.lost, w.abandoned)
	}

	// Still failing at timeout+grace: dropped, not lost.
	clk.set(streamT0.Add(testStreamTimeout + testStreamGrace))
	s.poll(context.Background(), failQuery)
	w = s.takeWindow()
	if w.lost != 0 || w.abandoned != 1 || w.pending != 0 {
		t.Errorf("at timeout+grace with every look failed: lost = %d, abandoned = %d, pending = %d, want 0, 1, 0", w.lost, w.abandoned, w.pending)
	}
	if res := w.result(); res.sent != 0 || res.received != 0 {
		t.Errorf("result = sent %d / received %d, want 0/0: an unmeasured trace belongs on neither side", res.sent, res.received)
	}
}

// TestTraceStreamFailedSendIsNotPending: sent counts only sends that left the
// process, as everywhere else in the probe. A failed send must never be polled
// for, or it would time out and read as Sentry losing a trace it never got.
func TestTraceStreamFailedSendIsNotPending(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("", time.Time{}, errors.New("flush: transport did not drain"))
	s.recordSkip()

	clk.set(streamT0.Add(testStreamTimeout))
	called := false
	s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		called = true
		return map[string]bool{}, nil
	})

	if called {
		t.Error("poll queried Sentry although nothing was successfully sent")
	}
	w := s.takeWindow()
	if w.sendFailed != 1 || w.sendSkipped != 1 || w.lost != 0 || w.pending != 0 {
		t.Errorf("sendFailed = %d, sendSkipped = %d, lost = %d, pending = %d, want 1, 1, 0, 0", w.sendFailed, w.sendSkipped, w.lost, w.pending)
	}
}

// TestTraceStreamIgnoresUnrequestedIDs: only traces this stream sent and asked
// about can be credited. A query result that also names another trace must not
// inflate received.
func TestTraceStreamIgnoresUnrequestedIDs(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(10 * time.Second))
	s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		return map[string]bool{"stranger": true}, nil
	})

	w := s.takeWindow()
	if len(w.arrived) != 0 || w.pending != 1 {
		t.Errorf("arrived = %v, pending = %d, want [] and 1", w.arrived, w.pending)
	}
}

// TestTraceStreamPollChunksOldestFirst: one Sentry request can name at most 100
// traces (per_page is the whole retrieval). With more pending, poll splits them,
// oldest first so the traces nearest their deadline are checked first. A failed
// chunk only affects its own traces.
func TestTraceStreamPollChunksOldestFirst(t *testing.T) {
	s, clk := newTestStream()
	for i := 0; i < 150; i++ {
		// Reverse insertion order, so a poll that does not sort cannot pass.
		seq := 149 - i
		s.recordSend(fmt.Sprintf("t%03d", seq), streamT0.Add(time.Duration(seq)*time.Millisecond), nil)
	}

	clk.set(streamT0.Add(testStreamTimeout + time.Second))
	var chunks [][]string
	s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		chunks = append(chunks, slices.Clone(ids))
		if len(chunks) == 2 {
			return nil, errUnauthorized
		}
		return map[string]bool{}, nil
	})

	if len(chunks) != 2 || len(chunks[0]) != 100 || len(chunks[1]) != 50 {
		t.Fatalf("chunk sizes = %v, want [100 50]", chunkSizes(chunks))
	}
	if chunks[0][0] != "t000" || chunks[0][99] != "t099" || chunks[1][0] != "t100" || chunks[1][49] != "t149" {
		t.Errorf("chunks not oldest first: first chunk %s..%s, second %s..%s",
			chunks[0][0], chunks[0][99], chunks[1][0], chunks[1][49])
	}
	w := s.takeWindow()
	if w.lost != 100 || w.pending != 50 || w.queryOK != 1 || w.queryFailed != 1 {
		t.Errorf("lost = %d, pending = %d, queryOK = %d, queryFailed = %d, want 100, 50, 1, 1", w.lost, w.pending, w.queryOK, w.queryFailed)
	}
}

func chunkSizes(chunks [][]string) []int {
	sizes := make([]int, len(chunks))
	for i, c := range chunks {
		sizes[i] = len(c)
	}
	return sizes
}

// TestTraceStreamTakeWindowResets: each window reports only what was decided in
// it. Carrying counts over would double-count into Datadog's summed counts.
func TestTraceStreamTakeWindowResets(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)
	s.recordSend("b", streamT0, nil)
	s.recordSend("", time.Time{}, errors.New("boom"))

	clk.set(streamT0.Add(5 * time.Second))
	s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		return map[string]bool{"a": true}, errorsIfNone(ids)
	})
	if w := s.takeWindow(); len(w.arrived) != 1 || w.sendFailed != 1 {
		t.Fatalf("first window: arrived = %v, sendFailed = %d, want 1 arrival and 1 failure", w.arrived, w.sendFailed)
	}

	w := s.takeWindow()
	if len(w.arrived) != 0 || w.lost != 0 || w.sendFailed != 0 || w.queryFailed != 0 {
		t.Errorf("second window not reset: %+v", w)
	}
	if w.pending != 1 {
		t.Errorf("pending = %d, want 1: b is still in flight and must survive the window boundary", w.pending)
	}
}

func errorsIfNone(ids []string) error {
	if len(ids) == 0 {
		return errors.New("queried with no ids")
	}
	return nil
}

// windowLog collects reported windows from runTraceStream's report goroutine.
type windowLog struct {
	mu sync.Mutex
	ws []streamWindow
}

func (l *windowLog) report(w streamWindow) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ws = append(l.ws, w)
}

func (l *windowLog) snapshot() []streamWindow {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.ws)
}

// startStream runs runTraceStream in the background. It returns the context's
// cancel func and a channel closed when runTraceStream returns.
func startStream(cfg streamConfig, ts *traceStream, io streamIO, l *windowLog) (stop context.CancelFunc, done <-chan struct{}) {
	ctx, stop := context.WithCancel(context.Background())
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		runTraceStream(ctx, cfg, ts, io, l.report)
	}()
	return stop, finished
}

// waitDone fails the test if runTraceStream has not returned within 2s. Call it
// from the test goroutine only.
func waitDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runTraceStream did not return within 2s of cancellation")
	}
}

// TestRunTraceStreamEndToEnd drives the real loops against a fake Sentry that
// has ingested everything: sends start immediately and keep coming, polls
// resolve them, and windows report them.
func TestRunTraceStreamEndToEnd(t *testing.T) {
	cfg := streamConfig{sendInterval: 10 * time.Millisecond, pollInterval: 5 * time.Millisecond, reportInterval: 40 * time.Millisecond, sendWorkers: 2}
	// A 1ms settle, so traces become due for census within the run.
	ts := newTraceStream("trace_stream", time.Second, time.Second, time.Now).withCensus(4, time.Millisecond, time.Minute)
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return fmt.Sprintf("t%d", seq), time.Now(), nil
	}
	allComplete := func(ctx context.Context, ids []string) (map[string]int, error) {
		counts := map[string]int{}
		for _, id := range ids {
			counts[id] = 4
		}
		return counts, nil
	}
	l := &windowLog{}

	stop, done := startStream(cfg, ts, streamIO{send: send, query: findAll, countSpans: allComplete}, l)
	time.Sleep(200 * time.Millisecond)
	stop()
	waitDone(t, done)

	ws := l.snapshot()
	if len(ws) < 2 {
		t.Fatalf("got %d windows in 200ms at a 40ms report interval, want at least 2", len(ws))
	}
	arrived, lost, censused, complete := 0, 0, 0, 0
	for _, w := range ws {
		arrived += len(w.arrived)
		lost += w.lost
		censused += w.censused
		complete += w.complete
	}
	if arrived < 5 || lost != 0 {
		t.Errorf("arrived = %d, lost = %d across windows, want >= 5 and 0", arrived, lost)
	}
	// The census runs on each report tick, so it trails arrivals by up to a
	// window, but over 200ms it must have counted most of them.
	if censused < 3 || complete != censused {
		t.Errorf("censused = %d, complete = %d, want >= 3 and all complete", censused, complete)
	}
}

// TestRunTraceStreamCancelWaitsForSendsAndDoesNotReport: on cancellation the
// stream waits for an in-flight send (no goroutine outlives it) and does not
// publish a final partial window. A partial window would carry a count interval
// it did not cover, on the way out of a routine restart.
func TestRunTraceStreamCancelWaitsForSendsAndDoesNotReport(t *testing.T) {
	cfg := streamConfig{sendInterval: time.Hour, pollInterval: time.Hour, reportInterval: time.Hour, sendWorkers: 1}
	ts := newTraceStream("trace_stream", time.Second, time.Second, time.Now)
	started := make(chan struct{})
	release := make(chan struct{})
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		close(started)
		<-release
		return "t1", time.Now(), nil
	}
	l := &windowLog{}

	stop, done := startStream(cfg, ts, streamIO{send: send, query: findAll}, l)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first send did not start immediately")
	}

	stop()
	select {
	case <-done:
		t.Fatal("runTraceStream returned while a send was still in flight")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	waitDone(t, done)

	if ws := l.snapshot(); len(ws) != 0 {
		t.Errorf("reported %d window(s) on cancellation, want none", len(ws))
	}
}

// TestRunTraceStreamSkipsWhenWorkersBusy: a send that blocks (a slow flush) must
// not delay the schedule or pile up goroutines. Ticks with no free worker are
// skipped and counted.
func TestRunTraceStreamSkipsWhenWorkersBusy(t *testing.T) {
	cfg := streamConfig{sendInterval: 5 * time.Millisecond, pollInterval: time.Hour, reportInterval: 60 * time.Millisecond, sendWorkers: 1}
	ts := newTraceStream("trace_stream", time.Second, time.Second, time.Now)
	release := make(chan struct{})
	var calls int
	var mu sync.Mutex
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		<-release
		return fmt.Sprintf("t%d", seq), time.Now(), nil
	}
	l := &windowLog{}

	stop, done := startStream(cfg, ts, streamIO{send: send, query: findAll}, l)
	time.Sleep(100 * time.Millisecond)
	// Stop before releasing: freeing the worker first would let a tick in the
	// gap start a second send and fail the count for a scheduling reason.
	stop()
	close(release)
	waitDone(t, done)

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Errorf("send called %d times with its only worker blocked, want 1", calls)
	}
	skipped := 0
	for _, w := range l.snapshot() {
		skipped += w.sendSkipped
	}
	if skipped < 5 {
		t.Errorf("sendSkipped = %d over ~20 ticks with the worker blocked, want >= 5", skipped)
	}
}

// TestStreamConfigFrom: one trace per second is the requirement, the poll
// interval comes from config, and the report window is the 60s count interval
// main hands the stream's Datadog client.
func TestStreamConfigFrom(t *testing.T) {
	got := streamConfigFrom(config{streamPollInterval: 3 * time.Second})
	want := streamConfig{sendInterval: time.Second, pollInterval: 3 * time.Second, reportInterval: 60 * time.Second, sendWorkers: 64}
	if got != want {
		t.Errorf("streamConfigFrom = %+v, want %+v", got, want)
	}
}

// TestTraceStreamDeadlineJudgedWhenAsked: whether a trace was missing "by the
// deadline" depends on when the query was asked, not when its response came
// back. A query asked 1s before the deadline that takes 3s to answer says
// nothing about the trace at the deadline. Judging by response time would
// shorten the timeout by up to the 10s HTTP timeout. Latency still runs to the
// response, which keeps it an upper bound.
func TestTraceStreamDeadlineJudgedWhenAsked(t *testing.T) {
	s, clk := newTestStream()
	s.recordSend("a", streamT0, nil)

	clk.set(streamT0.Add(testStreamTimeout - time.Second))
	s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		clk.set(streamT0.Add(testStreamTimeout + 2*time.Second)) // slow response
		return map[string]bool{}, nil
	})
	if w := s.takeWindow(); w.lost != 0 || w.pending != 1 {
		t.Fatalf("asked before the deadline, answered after: lost = %d, pending = %d, want 0 and 1", w.lost, w.pending)
	}

	s2, clk2 := newTestStream()
	s2.recordSend("b", streamT0, nil)
	clk2.set(streamT0.Add(50 * time.Second))
	s2.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
		clk2.set(streamT0.Add(53 * time.Second))
		return map[string]bool{"b": true}, nil
	})
	if w := s2.takeWindow(); !slices.Equal(w.arrived, []time.Duration{53 * time.Second}) {
		t.Errorf("arrived = %v, want [53s]: latency must run to the response, never the request", w.arrived)
	}
}

// TestTraceStreamPollReleasesLockDuringQuery: sends record from their own
// goroutines while a poll waits on Sentry (up to 10s per request). If poll held
// the lock across the query, every send would block behind it.
func TestTraceStreamPollReleasesLockDuringQuery(t *testing.T) {
	s, _ := newTestStream()
	s.recordSend("a", streamT0, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.poll(context.Background(), func(ctx context.Context, ids []string) (map[string]bool, error) {
			s.recordSend("sent-during-query", streamT0, nil)
			return map[string]bool{}, nil
		})
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("poll deadlocked: a send could not record while a query was in flight")
	}
	if w := s.takeWindow(); w.pending != 2 {
		t.Errorf("pending = %d, want 2", w.pending)
	}
}

// TestTraceStreamSuccessfulLookAtGraceEdgeDecides: a successful query at
// exactly timeout+grace still decides the trace. Abandoning first would drop a
// trace we did manage to look at.
func TestTraceStreamSuccessfulLookAtGraceEdgeDecides(t *testing.T) {
	for _, c := range []struct {
		name        string
		query       traceQueryFunc
		wantArrived int
		wantLost    int
	}{
		{"found", findAll, 1, 0},
		{"not found", findNone, 0, 1},
	} {
		t.Run(c.name, func(t *testing.T) {
			s, clk := newTestStream()
			s.recordSend("a", streamT0, nil)
			clk.set(streamT0.Add(testStreamTimeout + testStreamGrace))
			s.poll(context.Background(), c.query)
			w := s.takeWindow()
			if len(w.arrived) != c.wantArrived || w.lost != c.wantLost || w.abandoned != 0 {
				t.Errorf("arrived = %d, lost = %d, abandoned = %d, want %d, %d, 0", len(w.arrived), w.lost, w.abandoned, c.wantArrived, c.wantLost)
			}
		})
	}
}

const (
	testCensusSettle = 60 * time.Second
	testCensusGiveUp = 5 * time.Minute
)

func newCensusStream() (*traceStream, *fakeClock) {
	s, clk := newTestStream()
	return s.withCensus(4, testCensusSettle, testCensusGiveUp), clk
}

// arriveAt sends ids at streamT0 and has a successful poll find them all at
// arrived.
func arriveAt(s *traceStream, clk *fakeClock, arrived time.Time, ids ...string) {
	for _, id := range ids {
		s.recordSend(id, streamT0, nil)
	}
	clk.set(arrived)
	s.poll(context.Background(), findAll)
}

// countsFrom is a span census that answers with fixed per-trace child counts
// and records the ids it was asked about.
type countsFrom struct {
	counts map[string]int
	err    error
	asked  [][]string
}

func (c *countsFrom) count(ctx context.Context, ids []string) (map[string]int, error) {
	c.asked = append(c.asked, slices.Clone(ids))
	if c.err != nil {
		return nil, c.err
	}
	out := map[string]int{}
	for _, id := range ids {
		if n, ok := c.counts[id]; ok {
			out[id] = n
		}
	}
	return out, nil
}

// TestTraceStreamCensusWaitsToSettle: child spans can become queryable a little
// after their root. Counting them the moment the root arrives would score
// healthy traces incomplete, so a trace is censused only once it has been
// arrived for the settle period.
func TestTraceStreamCensusWaitsToSettle(t *testing.T) {
	s, clk := newCensusStream()
	arrived := streamT0.Add(50 * time.Second)
	arriveAt(s, clk, arrived, "a")
	s.takeWindow()
	c := &countsFrom{counts: map[string]int{"a": 4}}

	clk.set(arrived.Add(testCensusSettle - time.Second))
	s.census(context.Background(), c.count)
	if len(c.asked) != 0 {
		t.Fatalf("censused %v 1s before the settle period ended", c.asked)
	}

	clk.set(arrived.Add(testCensusSettle))
	s.census(context.Background(), c.count)
	w := s.takeWindow()
	if w.censused != 1 || w.complete != 1 {
		t.Errorf("censused = %d, complete = %d, want 1 and 1", w.censused, w.complete)
	}

	s.census(context.Background(), c.count)
	if len(c.asked) != 1 {
		t.Errorf("census asked %d times, want 1: a trace is censused once", len(c.asked))
	}
}

// TestTraceStreamCensusScoresByChildCount: complete means every child span is
// stored, all-or-nothing per trace, as in the batch span probe. A trace absent
// from a successful census stored no children at all.
func TestTraceStreamCensusScoresByChildCount(t *testing.T) {
	s, clk := newCensusStream()
	arriveAt(s, clk, streamT0.Add(50*time.Second), "full", "short", "none", "extra")
	clk.set(streamT0.Add(50*time.Second + testCensusSettle))
	c := &countsFrom{counts: map[string]int{"full": 4, "short": 3, "extra": 5}}
	s.census(context.Background(), c.count)

	w := s.takeWindow()
	if w.censused != 4 || w.complete != 2 {
		t.Errorf("censused = %d, complete = %d, want 4 and 2 (full and extra)", w.censused, w.complete)
	}
}

// TestTraceStreamCensusFailureRetriesThenDrops: a failed census proves nothing
// about the spans, so its traces are neither complete nor incomplete. They are
// retried, and dropped unscored once the census has been failing for the
// give-up period.
func TestTraceStreamCensusFailureRetriesThenDrops(t *testing.T) {
	s, clk := newCensusStream()
	arrived := streamT0.Add(50 * time.Second)
	arriveAt(s, clk, arrived, "a")
	failing := &countsFrom{err: errUnauthorized}

	clk.set(arrived.Add(testCensusSettle))
	s.census(context.Background(), failing.count)
	if w := s.takeWindow(); w.censused != 0 || w.censusFailed != 1 {
		t.Fatalf("after a failed census: censused = %d, censusFailed = %d, want 0 and 1", w.censused, w.censusFailed)
	}

	ok := &countsFrom{counts: map[string]int{"a": 4}}
	clk.set(arrived.Add(testCensusSettle + time.Minute))
	s.census(context.Background(), ok.count)
	if w := s.takeWindow(); w.censused != 1 || w.complete != 1 {
		t.Fatalf("retry after recovery: censused = %d, complete = %d, want 1 and 1", w.censused, w.complete)
	}

	s2, clk2 := newCensusStream()
	arriveAt(s2, clk2, arrived, "b")
	clk2.set(arrived.Add(testCensusSettle + testCensusGiveUp))
	s2.census(context.Background(), failing.count)
	w := s2.takeWindow()
	if w.censused != 0 || w.censusDropped != 1 {
		t.Errorf("failing past the give-up period: censused = %d, censusDropped = %d, want 0 and 1", w.censused, w.censusDropped)
	}
}

// TestTraceStreamCensusChunks: the census is one unpaginated request per 100
// traces (per_page is the whole retrieval), oldest arrivals first.
func TestTraceStreamCensusChunks(t *testing.T) {
	s, clk := newCensusStream()
	for i := 0; i < 150; i++ {
		s.recordSend(fmt.Sprintf("t%03d", i), streamT0, nil)
	}
	clk.set(streamT0.Add(50 * time.Second))
	s.poll(context.Background(), findAll)

	clk.set(streamT0.Add(50*time.Second + testCensusSettle))
	c := &countsFrom{counts: map[string]int{}}
	s.census(context.Background(), c.count)

	if sizes := chunkSizes(c.asked); !slices.Equal(sizes, []int{100, 50}) {
		t.Errorf("census chunk sizes = %v, want [100 50]", sizes)
	}
	if w := s.takeWindow(); w.censused != 150 {
		t.Errorf("censused = %d, want 150", w.censused)
	}
}

// TestTraceStreamCensusOnlyArrivedTraces: lost traces have no spans to count,
// and a stream built without census never queries for it.
func TestTraceStreamCensusOnlyArrivedTraces(t *testing.T) {
	s, clk := newCensusStream()
	s.recordSend("lost", streamT0, nil)
	clk.set(streamT0.Add(testStreamTimeout))
	s.poll(context.Background(), findNone)
	clk.set(streamT0.Add(testStreamTimeout + testCensusSettle))
	c := &countsFrom{}
	s.census(context.Background(), c.count)
	if len(c.asked) != 0 {
		t.Errorf("censused %v; a lost trace has nothing to count", c.asked)
	}

	plain, clk2 := newTestStream()
	arriveAt(plain, clk2, streamT0.Add(50*time.Second), "a")
	clk2.set(streamT0.Add(50*time.Second + testCensusSettle))
	plain.census(context.Background(), c.count)
	if len(c.asked) != 0 {
		t.Errorf("a stream without census queried %v", c.asked)
	}
}

// TestTraceStreamCensusOldestFirst: census requests go oldest arrival first,
// so if a census is cut short (cancelled, or failing partway), the traces
// nearest their give-up deadline are the ones that got counted.
func TestTraceStreamCensusOldestFirst(t *testing.T) {
	s, clk := newCensusStream()
	ids := []string{"e", "c", "a", "d", "b"}
	for i, id := range ids {
		s.recordSend(id, streamT0, nil)
		clk.set(streamT0.Add(time.Duration(40+i) * time.Second))
		s.poll(context.Background(), func(ctx context.Context, asked []string) (map[string]bool, error) {
			return map[string]bool{id: true}, nil
		})
	}
	clk.set(streamT0.Add(10 * time.Minute / 4))
	c := &countsFrom{counts: map[string]int{}}
	s.census(context.Background(), c.count)
	if len(c.asked) != 1 || !slices.Equal(c.asked[0], ids) {
		t.Errorf("census asked %v, want [%v] (arrival order)", c.asked, ids)
	}
}

// healthNames are the probe-health metrics every window publishes for signal.
func healthNames(signal string) []string {
	var names []string
	for _, n := range []string{"abandoned", "census_dropped", "census_failed", "http_retried", "pending", "query_failed", "send_failed", "send_skipped", "unanswered"} {
		names = append(names, "sentry."+signal+".probe."+n)
	}
	return names
}

func sortedNames(groups ...[]string) []string {
	var all []string
	for _, g := range groups {
		all = append(all, g...)
	}
	sort.Strings(all)
	return all
}

var reliabilityNames = []string{
	"sentry.trace_stream.latency_ms.p50",
	"sentry.trace_stream.latency_ms.p95",
	"sentry.trace_stream.latency_ms.p99",
	"sentry.trace_stream.received",
	"sentry.trace_stream.sent",
	"sentry.trace_stream.success_rate",
}

var censusNames = []string{
	"sentry.trace_stream.census.checked",
	"sentry.trace_stream.census.complete",
	"sentry.trace_stream.complete_pct",
}

// TestReportTraceStream pins what one window publishes: probe health always;
// latency and reliability only when something was decided; completeness only
// when something was counted. All of it in one Datadog request, so a slow
// Datadog costs one timeout per window, not one per metric, and cannot push the
// next window back.
func TestReportTraceStream(t *testing.T) {
	baseTags := []string{"sentry_org:o", "sentry_project:p"}

	t.Run("a full window", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)
		// 3 arrived, 1 lost; 4 counted, 3 complete; plus probe-health noise
		// that must not leak into the Sentry-health numbers.
		w := streamWindow{
			arrived: []time.Duration{time.Second, 2 * time.Second, 3 * time.Second},
			lost:    1, pending: 57, sendFailed: 2, sendSkipped: 5, abandoned: 4, queryOK: 29, queryFailed: 1,
			censused: 4, complete: 3, censusFailed: 6, censusDropped: 7, unanswered: 8, retried: 9,
		}
		reportTraceStream(d, "trace_stream", w, baseTags)

		series := rec.captured(t)
		if want := sortedNames(reliabilityNames, censusNames, healthNames("trace_stream")); !slices.Equal(metricNames(series), want) {
			t.Fatalf("metric names\n got: %v\nwant: %v", metricNames(series), want)
		}
		if rec.requests != 1 {
			t.Errorf("window posted in %d requests, want 1", rec.requests)
		}
		assertTags(t, series, []string{"probe:trace_stream", "sentry_org:o", "sentry_project:p"})
		for _, c := range []struct {
			metric string
			value  float64
		}{
			{"sentry.trace_stream.latency_ms.p50", 2000},
			{"sentry.trace_stream.latency_ms.p95", 3000},
			{"sentry.trace_stream.sent", 4}, // decided: arrived + lost
			{"sentry.trace_stream.received", 3},
			{"sentry.trace_stream.success_rate", 75},
			{"sentry.trace_stream.census.checked", 4},
			{"sentry.trace_stream.census.complete", 3},
			{"sentry.trace_stream.complete_pct", 75},
			{"sentry.trace_stream.probe.pending", 57},
			{"sentry.trace_stream.probe.send_failed", 2},
			{"sentry.trace_stream.probe.send_skipped", 5},
			{"sentry.trace_stream.probe.query_failed", 1},
			{"sentry.trace_stream.probe.abandoned", 4},
			{"sentry.trace_stream.probe.census_failed", 6},
			{"sentry.trace_stream.probe.census_dropped", 7},
			{"sentry.trace_stream.probe.unanswered", 8},
			{"sentry.trace_stream.probe.http_retried", 9},
		} {
			if got := metricValue(t, series, c.metric); got != c.value {
				t.Errorf("%s = %v, want %v", c.metric, got, c.value)
			}
		}
		// Tallies are counts (they sum across windows, and back metric-based
		// SLOs: received/sent, complete/checked); levels are gauges.
		types := map[string]int{
			"sentry.trace_stream.latency_ms.p50": ddTypeGauge, "sentry.trace_stream.latency_ms.p95": ddTypeGauge,
			"sentry.trace_stream.latency_ms.p99": ddTypeGauge, "sentry.trace_stream.success_rate": ddTypeGauge,
			"sentry.trace_stream.complete_pct": ddTypeGauge, "sentry.trace_stream.probe.pending": ddTypeGauge,
			"sentry.trace_stream.sent": ddTypeCount, "sentry.trace_stream.received": ddTypeCount,
			"sentry.trace_stream.census.checked": ddTypeCount, "sentry.trace_stream.census.complete": ddTypeCount,
		}
		for _, n := range healthNames("trace_stream") {
			if n != "sentry.trace_stream.probe.pending" {
				types[n] = ddTypeCount
			}
		}
		assertMetricTypes(t, series, types)
	})

	t.Run("total loss publishes reliability but no latency", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)
		reportTraceStream(d, "trace_stream", streamWindow{lost: 60, queryOK: 30}, baseTags)
		series := rec.captured(t)
		want := sortedNames([]string{"sentry.trace_stream.received", "sentry.trace_stream.sent", "sentry.trace_stream.success_rate"}, healthNames("trace_stream"))
		if !slices.Equal(metricNames(series), want) {
			t.Fatalf("metric names\n got: %v\nwant: %v", metricNames(series), want)
		}
		if got := metricValue(t, series, "sentry.trace_stream.success_rate"); got != 0 {
			t.Errorf("success_rate = %v, want 0", got)
		}
	})

	// The first minute after startup decides nothing (traces take ~60s to
	// arrive), and neither does a window in which every query failed.
	// Publishing reliability would post sent=0 and success_rate=0, a false
	// total outage. Probe health still goes out: it is about the probe, and a
	// window of failing queries is exactly when it matters.
	t.Run("a window that decided nothing publishes only probe health", func(t *testing.T) {
		for _, w := range []streamWindow{
			{pending: 60, queryOK: 30},                   // startup
			{pending: 60, queryFailed: 30, abandoned: 3}, // every query failed
		} {
			d, rec := newTestDatadogClient(t)
			reportTraceStream(d, "trace_stream", w, baseTags)
			series := rec.captured(t)
			if !slices.Equal(metricNames(series), healthNames("trace_stream")) {
				t.Errorf("window %+v published %v, want only probe health", w, metricNames(series))
			}
			if got := metricValue(t, series, "sentry.trace_stream.probe.query_failed"); got != float64(w.queryFailed) {
				t.Errorf("query_failed = %v, want %d", got, w.queryFailed)
			}
		}
	})

	// The census looks at traces that arrived a minute or more ago, so it can
	// have work in a window that decided nothing new. A failed census counted
	// nothing, so it publishes no completeness: 0% would read as total span
	// loss.
	t.Run("completeness independent of the decided-gate, and absent when nothing was counted", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)
		reportTraceStream(d, "trace_stream", streamWindow{queryOK: 30, censused: 10, complete: 10}, baseTags)
		if want := sortedNames(censusNames, healthNames("trace_stream")); !slices.Equal(metricNames(rec.captured(t)), want) {
			t.Errorf("metric names = %v, want %v", metricNames(rec.captured(t)), want)
		}

		d2, rec2 := newTestDatadogClient(t)
		reportTraceStream(d2, "trace_stream", streamWindow{arrived: []time.Duration{time.Second}, queryOK: 30, censusFailed: 1}, baseTags)
		for _, name := range metricNames(rec2.captured(t)) {
			if strings.Contains(name, "census.") || strings.HasSuffix(name, "complete_pct") {
				t.Errorf("%s published although nothing was counted", name)
			}
		}
	})

	t.Run("named for the stream's signal", func(t *testing.T) {
		d, rec := newTestDatadogClient(t)
		w := streamWindow{arrived: []time.Duration{time.Second}, queryOK: 30, censused: 2, complete: 2}
		reportTraceStream(d, "trace_stream_otlp", w, baseTags)
		series := rec.captured(t)
		for _, s := range series {
			if !strings.HasPrefix(s.Metric, "sentry.trace_stream_otlp.") {
				t.Errorf("metric %s is not under sentry.trace_stream_otlp.", s.Metric)
			}
		}
		assertTags(t, series, []string{"probe:trace_stream_otlp", "sentry_org:o", "sentry_project:p"})
	})
}

// TestStreamSDKSendDoesNotQueueBehindASlowSentry: the Sentry SDK's transport
// sends one request at a time, so with one shared client a slow Sentry would
// queue each stream trace behind the previous ones, and our own queue would be
// measured as Sentry's latency. Each stream send gets its own client; the
// barrier answers only once every send's request is in flight together.
func TestStreamSDKSendDoesNotQueueBehindASlowSentry(t *testing.T) {
	const sends = 4
	srv, stuck := newBarrierServer(t, sends)
	send := streamSDKSend("http://public@"+strings.TrimPrefix(srv.URL, "http://")+"/1", "B1", &deliveryStats{})

	var wg sync.WaitGroup
	errs := make(chan error, sends)
	for i := 0; i < sends; i++ {
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			_, _, err := send(context.Background(), seq)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("send: %v", err)
		}
	}
	if n := stuck.Load(); n > 0 {
		t.Errorf("%d requests waited at the barrier for the others: sends were queued behind each other", n)
	}
}

// captureLog redirects the standard logger for the duration of the test.
func captureLog(t *testing.T) *strings.Builder {
	t.Helper()
	var buf strings.Builder
	var mu sync.Mutex
	log.SetOutput(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return buf.Write(p)
	}))
	t.Cleanup(func() { log.SetOutput(os.Stderr) })
	return &buf
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// TestTraceStreamLogsUnderItsOwnName: two streams run side by side, and their
// failed sends, lookups and census counts are logged per stream. A shared
// hard-coded label filed the OTLP stream's failures under the SDK stream's
// name, so the logs disagreed with the per-stream metrics.
func TestTraceStreamLogsUnderItsOwnName(t *testing.T) {
	logs := captureLog(t)
	clk := &fakeClock{t: streamT0}
	s := newTraceStream("trace_stream_otlp", testStreamTimeout, testStreamGrace, clk.now).
		withCensus(4, testCensusSettle, testCensusGiveUp)

	arriveAt(s, clk, streamT0.Add(50*time.Second), "a")
	s.recordSend("b", streamT0, nil)
	s.poll(context.Background(), failQuery)
	clk.set(streamT0.Add(50*time.Second + testCensusSettle))
	s.census(context.Background(), (&countsFrom{err: errUnauthorized}).count)

	cfg := streamConfig{sendInterval: time.Hour, pollInterval: time.Hour, reportInterval: time.Hour, sendWorkers: 1}
	sent := make(chan struct{})
	failing := func(ctx context.Context, seq int) (string, time.Time, error) {
		defer close(sent)
		return "", time.Time{}, errors.New("refused")
	}
	stop, done := startStream(cfg, s, streamIO{send: failing, query: findAll}, &windowLog{})
	<-sent
	stop()
	waitDone(t, done)

	out := logs.String()
	for _, want := range []string{"[trace_stream_otlp] query", "[trace_stream_otlp] span census", "[trace_stream_otlp] send seq="} {
		if !strings.Contains(out, want) {
			t.Errorf("log has no %q line:\n%s", want, out)
		}
	}
	if strings.Contains(out, "[trace_stream] ") {
		t.Errorf("the OTLP stream logged under the SDK stream's name:\n%s", out)
	}
}
