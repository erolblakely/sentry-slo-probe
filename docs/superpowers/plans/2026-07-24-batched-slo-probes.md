# Batched SLO Probes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Change each SLO probe from sending one synthetic event per cycle to sending a paced batch of 100 events per type every ~2 minutes, measuring latency percentiles and ingestion reliability.

**Architecture:** A new generic `batch.go` orchestrates a paced sender + a single batch-query poller over string ids, returning `{sent, received, latencies}`. Each probe supplies a "send one event (returns id)" closure and a "query the batch" closure; the three probe loops in `main.go` run a batch per cycle and emit a metric set to Datadog. Events in a cycle share a `probe_batch:<cycleId>` tag so one paginated Discover query finds them all.

**Tech Stack:** Go 1.25.6, `github.com/getsentry/sentry-go` v0.43.0, OpenTelemetry (existing), Datadog v2 series API (existing). No new dependencies.

## Global Constraints

- Go version floor: **1.25.6** (module `slo-login-tracer`).
- **No new third-party dependencies.** Percentiles are computed in-process with the stdlib (`sort`, `math`).
- **Backward compatible:** `PROBE_BATCH_SIZE=1` must reproduce single-event behavior.
- Datadog metric names exactly: `sentry.<signal>.latency_ms.p50|p95|p99`, `sentry.<signal>.sent`, `sentry.<signal>.received`, `sentry.<signal>.success_rate`, and (span only) `sentry.span_completeness.received_pct`, where `<signal>` ∈ `{ingestion, error_ingestion, span_completeness}`.
- Metric tags exactly: `sentry_org:<org>`, `sentry_project:<project>`, `probe:<signal>`.
- All work happens on a feature branch off `main` (do **not** commit to `main`). Commit messages end with the trailer:
  `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`
- TDD: write the failing test first for every unit that has testable logic. Glue/orchestration tasks that cannot be unit-tested deterministically are explicitly marked and verified in the final end-to-end task.

## File Structure

- **Create `batch.go`** — pure helpers (`percentile`, `sendOffsets`, `batchDone`), the `latencyTracker` type, and the `runBatch` orchestrator. Types: `batchConfig`, `batchResult`, `sendFunc`, `queryFunc`.
- **Create `batch_test.go`** — tests for `percentile`, `sendOffsets`, `latencyTracker`, `batchDone`, and `runBatch` (fake send/query, millisecond durations).
- **Create `config_test.go`** — tests for `configFromEnv`.
- **Create `sentry_api_test.go`** — tests for `findBatch` / `findBatchEventIDs` against an `httptest` server.
- **Create `datadog_test.go`** — tests for `postBatchMetrics` / `postMetric` against an `httptest` server.
- **Modify `main.go`** — new config fields + `envInt`, in-flight-batch cap, probe loops run batches.
- **Modify `probe.go`** — `sendTraceOnce`/query closures; keep `sendTraceWithSpans` for reuse; add `baseURL` plumbing.
- **Modify `probe_error.go`** — batch send/query for errors.
- **Modify `probe_spans.go`** — batch send/query + sampled completeness.
- **Modify `sentry_api.go`** — add `baseURL` to `sentryProbe`; add `findBatch` + `findBatchEventIDs`; route existing endpoints through `baseURL`.
- **Modify `datadog.go`** — add `baseURL` field, `postBatchMetrics`, `postMetricLogged`.

---

### Task 1: Config — new env vars and parsing

**Files:**
- Modify: `main.go` (the `config` struct, `configFromEnv`, add `envInt`)
- Test: `config_test.go` (create)

**Interfaces:**
- Produces: `config` struct gains fields `batchSize int`, `sendWindow time.Duration`, `spanSample int`. Existing fields unchanged. `envInt(key string, def int) int`.

- [ ] **Step 1: Write the failing test**

Create `config_test.go`:

```go
package main

import (
	"testing"
	"time"
)

func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

func TestConfigFromEnvDefaults(t *testing.T) {
	setEnv(t, map[string]string{
		"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
		"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
	})
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.batchSize != 100 {
		t.Errorf("batchSize = %d, want 100", cfg.batchSize)
	}
	if cfg.interval != 120*time.Second {
		t.Errorf("interval = %s, want 2m", cfg.interval)
	}
	if cfg.sendWindow != 100*time.Second {
		t.Errorf("sendWindow = %s, want 100s", cfg.sendWindow)
	}
	if cfg.spanSample != 20 {
		t.Errorf("spanSample = %d, want 20", cfg.spanSample)
	}
}

func TestConfigFromEnvOverrides(t *testing.T) {
	setEnv(t, map[string]string{
		"SENTRY_DSN": "d", "SENTRY_AUTH_TOKEN": "t",
		"SENTRY_ORG": "o", "SENTRY_PROJECT": "p", "DD_API_KEY": "k",
		"PROBE_BATCH_SIZE": "1", "PROBE_INTERVAL_SECONDS": "30",
		"PROBE_SEND_WINDOW_SECONDS": "10", "SPAN_COMPLETENESS_SAMPLE": "5",
	})
	cfg, err := configFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.batchSize != 1 || cfg.interval != 30*time.Second ||
		cfg.sendWindow != 10*time.Second || cfg.spanSample != 5 {
		t.Errorf("overrides not applied: %+v", cfg)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestConfigFromEnv -v`
Expected: FAIL — `cfg.batchSize` / `cfg.sendWindow` / `cfg.spanSample` undefined (compile error).

- [ ] **Step 3: Add the fields, `envInt`, and parsing**

In `main.go`, add to the `config` struct (after `pollInterval`):

```go
	batchSize  int
	sendWindow time.Duration
	spanSample int
```

Change the two existing defaults in `configFromEnv`'s returned struct and add the new fields:

```go
		interval:     envDuration("PROBE_INTERVAL_SECONDS", 120),
		pollTimeout:  envDuration("POLL_TIMEOUT_SECONDS", 120),
		pollInterval: envDuration("POLL_INTERVAL_SECONDS", 5),
		batchSize:    envInt("PROBE_BATCH_SIZE", 100),
		sendWindow:   envDuration("PROBE_SEND_WINDOW_SECONDS", 100),
		spanSample:   envInt("SPAN_COMPLETENESS_SAMPLE", 20),
```

Add the helper next to `envDuration`:

```go
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestConfigFromEnv -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add main.go config_test.go
git commit -m "feat: add batch-probe config (batch size, send window, span sample)"
```

---

### Task 2: `percentile` helper

**Files:**
- Create: `batch.go`
- Test: `batch_test.go` (create)

**Interfaces:**
- Produces: `percentile(latencies []time.Duration, p float64) time.Duration` — nearest-rank percentile; returns 0 for empty input; does not mutate the caller's slice.

- [ ] **Step 1: Write the failing test**

Create `batch_test.go`:

```go
package main

import (
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestPercentile -v`
Expected: FAIL — `percentile` undefined.

- [ ] **Step 3: Write minimal implementation**

Create `batch.go`:

```go
package main

import (
	"math"
	"sort"
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestPercentile -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add batch.go batch_test.go
git commit -m "feat: add nearest-rank percentile helper"
```

---

### Task 3: `sendOffsets` pacing schedule

**Files:**
- Modify: `batch.go`
- Test: `batch_test.go`

**Interfaces:**
- Produces: `sendOffsets(size int, window time.Duration) []time.Duration` — offsets from batch start, evenly spaced with step `window/size`; `size<=0` returns nil; index 0 is always 0.

- [ ] **Step 1: Write the failing test**

Append to `batch_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestSendOffsets -v`
Expected: FAIL — `sendOffsets` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `batch.go`:

```go
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestSendOffsets -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add batch.go batch_test.go
git commit -m "feat: add batch send-pacing schedule"
```

---

### Task 4: `latencyTracker`

**Files:**
- Modify: `batch.go`
- Test: `batch_test.go`

**Interfaces:**
- Produces:
  - `newLatencyTracker() *latencyTracker`
  - `(*latencyTracker).markSent(id string, at time.Time)`
  - `(*latencyTracker).observe(found map[string]bool, now time.Time) int` — records latency (`now - sentAt`) for ids seen for the first time that were marked sent; returns count newly recorded.
  - `(*latencyTracker).receivedCount() int`
  - `(*latencyTracker).result(sent int) batchResult`
  - types `batchResult{sent, received int; latencies []time.Duration}`

- [ ] **Step 1: Write the failing test**

Append to `batch_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestLatencyTracker -v`
Expected: FAIL — `newLatencyTracker` / `batchResult` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `batch.go`:

```go
// batchResult is the outcome of one batch run.
type batchResult struct {
	sent      int
	received  int
	latencies []time.Duration
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestLatencyTracker -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add batch.go batch_test.go
git commit -m "feat: add latency tracker for batch arrivals"
```

---

### Task 5: `batchDone` stop condition

**Files:**
- Modify: `batch.go`
- Test: `batch_test.go`

**Interfaces:**
- Produces: `batchDone(received, size int, now, lastSendAt time.Time, pollTimeout time.Duration) bool` — true when all received, or when `now` is past `lastSendAt + pollTimeout`.

- [ ] **Step 1: Write the failing test**

Append to `batch_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestBatchDone -v`
Expected: FAIL — `batchDone` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `batch.go`:

```go
// batchDone reports whether polling should stop: either every sent event has
// been received, or we are past the drain deadline (lastSendAt + pollTimeout).
func batchDone(received, size int, now, lastSendAt time.Time, pollTimeout time.Duration) bool {
	if received >= size {
		return true
	}
	return now.After(lastSendAt.Add(pollTimeout))
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestBatchDone -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add batch.go batch_test.go
git commit -m "feat: add batch stop condition"
```

---

### Task 6: `runBatch` orchestrator

**Files:**
- Modify: `batch.go`
- Test: `batch_test.go`

**Interfaces:**
- Consumes: `sendOffsets`, `latencyTracker`, `batchDone` (Tasks 3-5).
- Produces:
  - types `sendFunc func(ctx context.Context, seq int) (id string, sentAt time.Time, err error)`, `queryFunc func(ctx context.Context) (map[string]bool, error)`, `batchConfig{size int; sendWindow, pollTimeout, pollInterval time.Duration; sendWorkers int}`
  - `runBatch(ctx context.Context, cfg batchConfig, send sendFunc, query queryFunc) batchResult`

- [ ] **Step 1: Write the failing test**

Append to `batch_test.go` (add imports `context`, `sync`, `sync/atomic` at the top of the file):

```go
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
}

func TestRunBatchTimeoutNoneArrive(t *testing.T) {
	cfg := batchConfig{
		size: 3, sendWindow: 10 * time.Millisecond,
		pollTimeout: 30 * time.Millisecond, pollInterval: 5 * time.Millisecond,
		sendWorkers: 2,
	}
	var calls int64
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return "x", time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		atomic.AddInt64(&calls, 1)
		return map[string]bool{}, nil // nothing ever arrives
	}
	start := time.Now()
	res := runBatch(context.Background(), cfg, send, query)
	if res.received != 0 {
		t.Fatalf("received = %d, want 0", res.received)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("runBatch took %s, expected to stop near drain deadline", elapsed)
	}
	if atomic.LoadInt64(&calls) == 0 {
		t.Fatal("query never called")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestRunBatch -v`
Expected: FAIL — `runBatch` / `batchConfig` undefined.

- [ ] **Step 3: Write minimal implementation**

Append to `batch.go` (add imports `context`, `log`, `sync` to the file's import block):

```go
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

// runBatch paces cfg.size sends across cfg.sendWindow while polling query every
// cfg.pollInterval, recording per-event latency, until all are received or the
// drain deadline passes.
func runBatch(ctx context.Context, cfg batchConfig, send sendFunc, query queryFunc) batchResult {
	tracker := newLatencyTracker()
	var mu sync.Mutex
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

	lastSendAt := start.Add(cfg.sendWindow)
	ticker := time.NewTicker(cfg.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			<-sendDone
			mu.Lock()
			defer mu.Unlock()
			return tracker.result(cfg.size)
		case <-ticker.C:
			found, err := query(ctx)
			now := time.Now()
			mu.Lock()
			if err != nil {
				log.Printf("[batch] query: %v", err)
			} else {
				tracker.observe(found, now)
			}
			received := tracker.receivedCount()
			mu.Unlock()
			if batchDone(received, cfg.size, now, lastSendAt, cfg.pollTimeout) {
				<-sendDone
				mu.Lock()
				defer mu.Unlock()
				return tracker.result(cfg.size)
			}
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestRunBatch -v -race`
Expected: PASS, no race warnings.

- [ ] **Step 5: Commit**

```bash
git add batch.go batch_test.go
git commit -m "feat: add runBatch paced sender + poller orchestrator"
```

---

### Task 7: Sentry base URL + batch queries

**Files:**
- Modify: `sentry_api.go`, `probe.go` (constructor `newSentryProbe`)
- Test: `sentry_api_test.go` (create)

**Interfaces:**
- Consumes: `sentryProbe`, `apiError`, `(*sentryProbe).get` (existing).
- Produces:
  - `sentryProbe` gains field `baseURL string`; `newSentryProbe(dsn, authToken, org, project string)` sets `baseURL: "https://sentry.io"`.
  - `(*sentryProbe).findBatch(dataset, batchID, idField string, limit int) (map[string]bool, error)` — Discover query filtered by `probe_batch:<batchID>`, returns set of `idField` values.
  - `(*sentryProbe).findBatchEventIDs(batchID string, limit int) (map[string]string, error)` — for transactions, returns traceID→eventID for arrived events in the batch.

- [ ] **Step 1: Write the failing test**

Create `sentry_api_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFindBatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("query"); got != "probe_batch:B1" {
			t.Errorf("query = %q, want probe_batch:B1", got)
		}
		if got := r.URL.Query().Get("dataset"); got != "errors" {
			t.Errorf("dataset = %q, want errors", got)
		}
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"probe_seq":"0"},{"probe_seq":"3"},{"probe_seq":"3"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	found, err := s.findBatch("errors", "B1", "probe_seq", 100)
	if err != nil {
		t.Fatalf("findBatch: %v", err)
	}
	if len(found) != 2 || !found["0"] || !found["3"] {
		t.Errorf("found = %v, want {0,3}", found)
	}
}

func TestFindBatchEventIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"data":[{"trace":"T1","id":"E1"},{"trace":"T2","id":"E2"}]}`))
	}))
	defer srv.Close()

	s := newSentryProbe("dsn", "tok", "org", "proj")
	s.baseURL = srv.URL
	m, err := s.findBatchEventIDs("B1", 100)
	if err != nil {
		t.Fatalf("findBatchEventIDs: %v", err)
	}
	if m["T1"] != "E1" || m["T2"] != "E2" {
		t.Errorf("map = %v, want T1->E1,T2->E2", m)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestFindBatch -v`
Expected: FAIL — `baseURL` / `findBatch` / `findBatchEventIDs` undefined.

- [ ] **Step 3: Implement base URL + batch queries**

In `probe.go`, change the `sentryProbe` struct and constructor:

```go
type sentryProbe struct {
	dsn       string
	authToken string
	org       string
	project   string
	baseURL   string
}

func newSentryProbe(dsn, authToken, org, project string) *sentryProbe {
	return &sentryProbe{dsn: dsn, authToken: authToken, org: org, project: project, baseURL: "https://sentry.io"}
}
```

In `sentry_api.go`, replace every hardcoded `"https://sentry.io/api/0/..."` prefix with `s.baseURL + "/api/0/..."`. For example `findTrace`'s endpoint becomes:

```go
	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())
```

Apply the same `s.baseURL` substitution to `errorExists` and `fetchEventSpanCount`.

Add the batch queries to `sentry_api.go` (needs `strconv` in the import block):

```go
// findBatch queries Discover for events tagged probe_batch:batchID and returns
// the set of distinct idField values seen.
func (s *sentryProbe) findBatch(dataset, batchID, idField string, limit int) (map[string]bool, error) {
	q := url.Values{}
	q.Set("dataset", dataset)
	q.Set("statsPeriod", "1h")
	q.Set("query", fmt.Sprintf("probe_batch:%s", batchID))
	q.Set("field", idField)
	q.Set("per_page", strconv.Itoa(limit))

	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []map[string]any `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		return nil, err
	}
	found := make(map[string]bool, len(result.Data))
	for _, row := range result.Data {
		if v, ok := row[idField]; ok && v != nil {
			found[fmt.Sprint(v)] = true
		}
	}
	return found, nil
}

// findBatchEventIDs returns traceID->eventID for transactions in the batch.
func (s *sentryProbe) findBatchEventIDs(batchID string, limit int) (map[string]string, error) {
	q := url.Values{}
	q.Set("dataset", "transactions")
	q.Set("statsPeriod", "1h")
	q.Set("query", fmt.Sprintf("probe_batch:%s", batchID))
	q.Set("field", "trace")
	q.Add("field", "id")
	q.Set("per_page", strconv.Itoa(limit))

	endpoint := fmt.Sprintf("%s/api/0/organizations/%s/events/?%s",
		s.baseURL, url.PathEscape(s.org), q.Encode())

	var result struct {
		Data []struct {
			Trace string `json:"trace"`
			ID    string `json:"id"`
		} `json:"data"`
	}
	if err := s.get(endpoint, &result); err != nil {
		return nil, err
	}
	out := make(map[string]string, len(result.Data))
	for _, row := range result.Data {
		if row.Trace != "" && row.ID != "" {
			out[row.Trace] = row.ID
		}
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestFindBatch -v`
Expected: PASS (both). Also run `go build ./...` to confirm the `baseURL` substitutions compile.

- [ ] **Step 5: Commit**

```bash
git add sentry_api.go probe.go sentry_api_test.go
git commit -m "feat: add Sentry batch queries + injectable base URL"
```

---

### Task 8: Datadog batch metrics + logged failures

**Files:**
- Modify: `datadog.go`
- Test: `datadog_test.go` (create)

**Interfaces:**
- Consumes: `percentile` (Task 2), `batchResult` (Task 4).
- Produces:
  - `datadogClient` gains field `baseURL string`; `newDatadogClient(apiKey, site string)` sets `baseURL: "https://api." + site`.
  - `(*datadogClient).postBatchMetrics(signal string, res batchResult, baseTags []string)` — posts p50/p95/p99 + sent + received + success_rate.
  - `(*datadogClient).postMetricLogged(name string, value float64, tags []string)` — posts and logs on error.

- [ ] **Step 1: Write the failing test**

Create `datadog_test.go`:

```go
package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestPostBatchMetrics(t *testing.T) {
	var mu sync.Mutex
	names := map[string]bool{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAllTest(r)
		mu.Lock()
		for _, n := range extractMetricNames(body) {
			names[n] = true
		}
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"errors":[]}`))
	}))
	defer srv.Close()

	d := newDatadogClient("key", "datadoghq.com")
	d.baseURL = srv.URL
	res := batchResult{sent: 10, received: 8, latencies: []time.Duration{time.Second, 2 * time.Second}}
	d.postBatchMetrics("ingestion", res, []string{"sentry_org:o"})

	for _, want := range []string{
		"sentry.ingestion.latency_ms.p50", "sentry.ingestion.latency_ms.p95",
		"sentry.ingestion.latency_ms.p99", "sentry.ingestion.sent",
		"sentry.ingestion.received", "sentry.ingestion.success_rate",
	} {
		if !names[want] {
			t.Errorf("missing metric %q (got %v)", want, names)
		}
	}
}
```

Add small test helpers at the bottom of `datadog_test.go`:

```go
func readAllTest(r *http.Request) (string, error) {
	b, err := io.ReadAll(r.Body)
	return string(b), err
}

func extractMetricNames(body string) []string {
	var out []string
	const key = "\"metric\":\""
	for i := 0; i+len(key) < len(body); i++ {
		if body[i:i+len(key)] == key {
			j := i + len(key)
			start := j
			for j < len(body) && body[j] != '"' {
				j++
			}
			out = append(out, body[start:j])
		}
	}
	return out
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestPostBatchMetrics -v`
Expected: FAIL — `baseURL` / `postBatchMetrics` undefined.

- [ ] **Step 3: Implement base URL + batch metrics**

In `datadog.go`, add `baseURL` to the struct and constructor:

```go
type datadogClient struct {
	apiKey  string
	site    string
	baseURL string
	http    *http.Client
}

func newDatadogClient(apiKey, site string) *datadogClient {
	return &datadogClient{
		apiKey:  apiKey,
		site:    site,
		baseURL: "https://api." + site,
		http:    &http.Client{Timeout: 10 * time.Second},
	}
}
```

In `postMetric`, replace the URL line:

```go
	url := d.baseURL + "/api/v2/series"
```

Add the batch helpers to `datadog.go` (needs `log` in the import block):

```go
func (d *datadogClient) postMetricLogged(name string, value float64, tags []string) {
	if err := d.postMetric(name, value, tags); err != nil {
		log.Printf("[datadog] post %s: %v", name, err)
	}
}

// postBatchMetrics emits the latency percentiles + reliability metrics for one
// probe signal.
func (d *datadogClient) postBatchMetrics(signal string, res batchResult, baseTags []string) {
	tags := append(append([]string{}, baseTags...), "probe:"+signal)
	d.postMetricLogged("sentry."+signal+".latency_ms.p50", float64(percentile(res.latencies, 50).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".latency_ms.p95", float64(percentile(res.latencies, 95).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".latency_ms.p99", float64(percentile(res.latencies, 99).Milliseconds()), tags)
	d.postMetricLogged("sentry."+signal+".sent", float64(res.sent), tags)
	d.postMetricLogged("sentry."+signal+".received", float64(res.received), tags)
	rate := 0.0
	if res.sent > 0 {
		rate = float64(res.received) / float64(res.sent) * 100
	}
	d.postMetricLogged("sentry."+signal+".success_rate", rate, tags)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./... -run TestPostBatchMetrics -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add datadog.go datadog_test.go
git commit -m "feat: add Datadog batch metric set + logged failures"
```

---

### Task 9: Rework probes to run batches

**Files:**
- Modify: `probe.go`, `probe_error.go`, `probe_spans.go`

**Interfaces:**
- Consumes: `runBatch`, `batchConfig`, `batchResult` (Task 6); `findBatch`, `findBatchEventIDs` (Task 7); `sendTraceWithSpans`, `fetchEventSpanCount` (existing); `sentry-go`.
- Produces:
  - `probeBatch(ctx, s, cfg, batchID) batchResult` (trace ingestion) in `probe.go`
  - `probeErrorBatch(ctx, s, cfg, batchID) batchResult` in `probe_error.go`
  - `probeSpansBatch(ctx, s, cfg, batchID) (batchResult, float64)` in `probe_spans.go` (second return = sampled `received_pct`)
  - `batchConfigFrom(cfg config) batchConfig` in `batch.go`
  - Each send closure sets Sentry tags `probe_batch:<batchID>` and, for errors, `probe_seq:<seq>`.

- [ ] **Step 1: Add `batchConfigFrom` and a tagged trace sender**

Append to `batch.go`:

```go
func batchConfigFrom(cfg config) batchConfig {
	return batchConfig{
		size:         cfg.batchSize,
		sendWindow:   cfg.sendWindow,
		pollTimeout:  cfg.pollTimeout,
		pollInterval: cfg.pollInterval,
		sendWorkers:  8,
	}
}
```

In `probe.go`, add a batch-aware trace sender that reuses one client and tags the transaction. Add `sync` and `strconv` to the file's imports if missing:

```go
// sendTraceTagged sends one probe transaction with n child spans, tagged for
// batch identification, using the supplied client. Returns the trace id.
func sendTraceTagged(client *sentry.Client, batchID string, seq, n int) (string, time.Time, error) {
	hub := sentry.NewHub(client, sentry.NewScope())
	hub.Scope().SetTag("probe_batch", batchID)
	hub.Scope().SetTag("probe_seq", strconv.Itoa(seq))
	ctx := sentry.SetHubOnContext(context.Background(), hub)

	span := sentry.StartTransaction(ctx, "probe.login",
		sentry.WithOpName("slo.probe"),
		sentry.WithDescription("Synthetic login probe for SLO measurement"),
	)
	span.SetTag("probe_batch", batchID)
	for i := 0; i < n; i++ {
		child := span.StartChild(fmt.Sprintf("probe.span.%d", i))
		sleep(5, 20)
		child.Status = sentry.SpanStatusOK
		child.Finish()
	}
	span.Finish()
	traceID := span.TraceID.String()
	client.Flush(10 * time.Second)
	return traceID, time.Now(), nil
}

func newTracingClient(dsn string) (*sentry.Client, error) {
	return sentry.NewClient(sentry.ClientOptions{
		Dsn:              dsn,
		EnableTracing:    true,
		TracesSampleRate: 1.0,
		Environment:      "probe",
		Release:          "sentry-slo-probe@1.0.0",
	})
}
```

- [ ] **Step 2: Implement `probeBatch` (trace ingestion)**

Append to `probe.go`:

```go
// probeBatch sends a paced batch of probe transactions and measures ingestion
// latency + reliability by trace id.
func probeBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) batchResult {
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("[trace_ingestion] client: %v", err)
		return batchResult{sent: cfg.batchSize}
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return sendTraceTagged(client, batchID, seq, 4)
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("transactions", batchID, "trace", cfg.batchSize)
	}
	return runBatch(ctx, batchConfigFrom(cfg), send, query)
}
```

(Add `"log"` to `probe.go` imports if not already present.)

- [ ] **Step 3: Implement `probeErrorBatch`**

Replace the body work in `probe_error.go` with a batch version (keep the file's `package`/imports; add `context`, `log`, `strconv`, `time`, and `sentry-go`):

```go
func probeErrorBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) batchResult {
	client, err := sentry.NewClient(sentry.ClientOptions{
		Dsn:         s.dsn,
		Environment: "probe",
		Release:     "sentry-slo-probe@1.0.0",
	})
	if err != nil {
		log.Printf("[error_ingestion] client: %v", err)
		return batchResult{sent: cfg.batchSize}
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		seqID := strconv.Itoa(seq)
		hub := sentry.NewHub(client, sentry.NewScope())
		hub.Scope().SetTag("probe_batch", batchID)
		hub.Scope().SetTag("probe_seq", seqID)
		hub.CaptureMessage(fmt.Sprintf("SLO error probe batch %s seq %s", batchID, seqID))
		client.Flush(10 * time.Second)
		return seqID, time.Now(), nil
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("errors", batchID, "probe_seq", cfg.batchSize)
	}
	return runBatch(ctx, batchConfigFrom(cfg), send, query)
}
```

- [ ] **Step 4: Implement `probeSpansBatch` with sampled completeness**

Replace `probe_spans.go`'s probe with a batch version (keep `expectedSpans = 5`; imports: `context`, `log`, `time`, `sentry-go`):

```go
// probeSpansBatch runs a batch of transactions and, on a sample of arrived
// transactions, measures span completeness.
func probeSpansBatch(ctx context.Context, s *sentryProbe, cfg config, batchID string) (batchResult, float64) {
	client, err := newTracingClient(s.dsn)
	if err != nil {
		log.Printf("[span_completeness] client: %v", err)
		return batchResult{sent: cfg.batchSize}, 0
	}
	send := func(ctx context.Context, seq int) (string, time.Time, error) {
		return sendTraceTagged(client, batchID, seq, expectedSpans)
	}
	query := func(ctx context.Context) (map[string]bool, error) {
		return s.findBatch("transactions", batchID, "trace", cfg.batchSize)
	}
	res := runBatch(ctx, batchConfigFrom(cfg), send, query)

	// Sampled completeness: fetch span detail for up to spanSample arrived txns.
	ids, err := s.findBatchEventIDs(batchID, cfg.batchSize)
	if err != nil {
		log.Printf("[span_completeness] event ids: %v", err)
		return res, 0
	}
	sample, complete := 0, 0
	for _, eventID := range ids {
		if sample >= cfg.spanSample {
			break
		}
		sample++
		n, err := s.fetchEventSpanCount(eventID)
		if err != nil {
			log.Printf("[span_completeness] span count: %v", err)
			continue
		}
		if n >= expectedSpans {
			complete++
		}
	}
	pct := 0.0
	if sample > 0 {
		pct = float64(complete) / float64(sample) * 100
	}
	return res, pct
}
```

- [ ] **Step 5: Verify build and existing unit tests**

Run: `go build ./... && go test ./... -v`
Expected: build succeeds; all Task 1-8 tests still PASS. (The reworked probes have no new unit tests — they are network glue verified in Task 11.)

- [ ] **Step 6: Commit**

```bash
git add probe.go probe_error.go probe_spans.go batch.go
git commit -m "feat: run probes as paced batches with reliability + sampled completeness"
```

---

### Task 10: Wire batches into `main.go` with in-flight cap

**Files:**
- Modify: `main.go`
- Test: `main_test.go` (create) — the in-flight cap only.

**Interfaces:**
- Consumes: `probeBatch`, `probeErrorBatch`, `probeSpansBatch` (Task 9); `postBatchMetrics` (Task 8); `config` (Task 1).
- Produces: `inflightCap` type with `try() bool` / `done()`; `runBatchLoop(...)` replacing `runProbeLoop`; `batchID(signal string, n int) string` (deterministic — no time/random).

- [ ] **Step 1: Write the failing test (cap only)**

Create `main_test.go`:

```go
package main

import "testing"

func TestInflightCap(t *testing.T) {
	c := newInflightCap(2)
	if !c.try() || !c.try() {
		t.Fatal("first two acquisitions should succeed")
	}
	if c.try() {
		t.Fatal("third acquisition should fail at cap 2")
	}
	c.done()
	if !c.try() {
		t.Fatal("acquisition should succeed after done()")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./... -run TestInflightCap -v`
Expected: FAIL — `newInflightCap` undefined.

- [ ] **Step 3: Implement cap, batch id, and loop**

In `main.go` add (imports need `sync`, `fmt`, already present, plus keep existing):

```go
// inflightCap bounds concurrent in-flight batches for one probe type.
type inflightCap struct {
	mu   sync.Mutex
	n    int
	max  int
}

func newInflightCap(max int) *inflightCap { return &inflightCap{max: max} }

func (c *inflightCap) try() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n >= c.max {
		return false
	}
	c.n++
	return true
}

func (c *inflightCap) done() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.n > 0 {
		c.n--
	}
}

// batchID builds a unique, deterministic batch identifier (no wall clock so it
// stays testable): probe signal + cycle counter.
func batchID(signal string, n int) string {
	return fmt.Sprintf("%s-%d", signal, n)
}

// runBatchLoop fires a batch every cfg.interval, capping concurrent batches.
func runBatchLoop(name string, cfg config, cap *inflightCap, wg *sync.WaitGroup, run func(batchID string)) {
	defer wg.Done()
	log.Printf("[%s] starting (interval=%s, batch=%d)", name, cfg.interval, cfg.batchSize)
	cycle := 0
	fire := func() {
		cycle++
		id := batchID(name, cycle)
		if !cap.try() {
			log.Printf("[%s] skipping cycle %d: previous batch still draining", name, cycle)
			return
		}
		go func() {
			defer cap.done()
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
```

Replace the three `go runProbeLoop(...)` blocks and `runProbeLoop` in `main()` with batch loops:

```go
	caps := map[string]*inflightCap{
		"trace_ingestion":   newInflightCap(2),
		"error_ingestion":   newInflightCap(2),
		"span_completeness": newInflightCap(2),
	}

	go runBatchLoop("trace_ingestion", cfg, caps["trace_ingestion"], &wg, func(id string) {
		res := probeBatch(ctx, s, cfg, id)
		log.Printf("[trace_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		dd.postBatchMetrics("ingestion", res, baseTags)
	})

	go runBatchLoop("error_ingestion", cfg, caps["error_ingestion"], &wg, func(id string) {
		res := probeErrorBatch(ctx, s, cfg, id)
		log.Printf("[error_ingestion] batch=%s received=%d/%d p50=%s p95=%s",
			id, res.received, res.sent, percentile(res.latencies, 50), percentile(res.latencies, 95))
		dd.postBatchMetrics("error_ingestion", res, baseTags)
	})

	go runBatchLoop("span_completeness", cfg, caps["span_completeness"], &wg, func(id string) {
		res, pct := probeSpansBatch(ctx, s, cfg, id)
		log.Printf("[span_completeness] batch=%s received=%d/%d received_pct=%.0f%%",
			id, res.received, res.sent, pct)
		dd.postBatchMetrics("span_completeness", res, baseTags)
		dd.postMetricLogged("sentry.span_completeness.received_pct", pct, append(append([]string{}, baseTags...), "probe:span_completeness"))
	})
```

Delete the now-unused `runProbeLoop` function. Keep `wg.Add(3)` and `wg.Wait()`.

- [ ] **Step 4: Run tests + build**

Run: `go build ./... && go test ./... -v`
Expected: build succeeds; `TestInflightCap` and all prior tests PASS.

- [ ] **Step 5: Commit**

```bash
git add main.go main_test.go
git commit -m "feat: wire batch probe loops with in-flight cap"
```

---

### Task 11: End-to-end verification against live Sentry + Datadog

**Files:** none (verification only)

- [ ] **Step 1: Full unit + race pass**

Run: `go test ./... -race && go vet ./...`
Expected: all PASS, no vet issues.

- [ ] **Step 2: Short live smoke run (small batch, fast cycle)**

Run a bounded batch so it completes quickly:

```bash
set -a && . ./.env && set +a
PROBE_BATCH_SIZE=10 PROBE_SEND_WINDOW_SECONDS=10 PROBE_INTERVAL_SECONDS=60 \
  POLL_TIMEOUT_SECONDS=90 POLL_INTERVAL_SECONDS=3 \
  go run . > /tmp/batch_run.log 2>&1 &
```

- [ ] **Step 3: Confirm healthy batch results**

Watch `/tmp/batch_run.log` for one cycle of each probe. Expected lines like:
```
[trace_ingestion]   batch=trace_ingestion-1 received=10/10 p50=... p95=...
[error_ingestion]   batch=error_ingestion-1 received=10/10 p50=... p95=...
[span_completeness] batch=span_completeness-1 received=10/10 received_pct=100%
```
Expected: `received` close to `sent` (allowing for ingestion lag within the timeout), no `permanent error`, no OTLP `failed to send`. Stop the run (`pkill -f "go run"` / kill the built binary).

- [ ] **Step 4: Confirm metrics accepted by Datadog**

Grep the log for `[datadog] post` errors. Expected: none (submissions return 202). Optionally confirm in Datadog that `sentry.ingestion.latency_ms.p95` and `sentry.*.success_rate` are present.

- [ ] **Step 5: Update `.env.example` and commit**

Add the new tunables to `.env.example` under the probe-tuning block:

```
PROBE_BATCH_SIZE=100          # synthetic events per type per cycle
PROBE_SEND_WINDOW_SECONDS=100 # pace the batch across this window
SPAN_COMPLETENESS_SAMPLE=20   # arrived transactions sampled for span-count check
```

Change the existing `PROBE_INTERVAL_SECONDS` default comment to `120`.

```bash
git add .env.example
git commit -m "docs: document batch-probe tunables in .env.example"
```

---

## Self-Review

**1. Spec coverage:**
- Goal (100/type/2min, paced, latency + reliability) → Tasks 1, 6, 9, 10. ✓
- Batch-query polling (vs per-event) → Tasks 6, 7. ✓
- Config table (all 6 vars) → Task 1 + Task 11 Step 5. ✓
- Send model (paced, unique seq, shared batch tag, bounded pool) → Tasks 3, 6, 9. ✓
- Measurement model (Discover query, local-clock latency, stop conditions) → Tasks 4, 5, 6, 7. ✓
- Decision A (poll-granular latency) → inherent in Task 4/6. ✓
- Span completeness sampling (Decision B, sample 20) → Task 9 Step 4. ✓
- Datadog metric set (exact names) + log-on-failure → Task 8, Task 10. ✓
- Decision C (overlap allowed, cap 2) → Task 10. ✓
- Code structure (batch.go, probe reworks, sentry_api, datadog, main) → Tasks 2-10. ✓
- OTel: per-batch span, no per-event child explosion → **partially**: the reworked send path no longer opens the old per-run spans; each batch cycle is not re-wrapped in a probe span in this plan. Acceptable because per-run tracing was internal observability, not an SLO signal; noted here so it is a conscious omission rather than a gap. If per-batch tracing is wanted, add a `tracer.Start` around each `run(id)` in Task 10's closures.
- Testing (percentile, pacing, poller vs fake query, config) → Tasks 1-8. ✓

**2. Placeholder scan:** No TBD/TODO; every code step shows complete code. ✓

**3. Type consistency:** `batchResult`, `batchConfig`, `sendFunc`, `queryFunc`, `latencyTracker`, `percentile`, `findBatch`, `findBatchEventIDs`, `postBatchMetrics`, `inflightCap` are defined once and consumed with matching signatures across tasks. `signal` values (`ingestion`/`error_ingestion`/`span_completeness`) match existing metric names. ✓
