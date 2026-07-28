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
