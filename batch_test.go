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
