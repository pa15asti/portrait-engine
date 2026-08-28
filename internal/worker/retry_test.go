package worker

import (
	"testing"
	"time"
)

func TestBackoff_JitterBounds(t *testing.T) {
	// For each attempt the delay must fall within [exp/2, exp] where exp is the
	// capped exponential. Sample repeatedly to exercise the jitter.
	cases := []struct {
		attempt int
		expMin  time.Duration
		expMax  time.Duration
	}{
		{1, 1 * time.Second, 2 * time.Second},    // exp = 2s
		{2, 2 * time.Second, 4 * time.Second},    // exp = 4s
		{3, 4 * time.Second, 8 * time.Second},    // exp = 8s
		{10, 30 * time.Second, 60 * time.Second}, // capped at 60s
	}
	for _, tc := range cases {
		for i := 0; i < 200; i++ {
			d := backoff(tc.attempt)
			if d < tc.expMin || d > tc.expMax {
				t.Fatalf("attempt %d: backoff %v out of [%v, %v]", tc.attempt, d, tc.expMin, tc.expMax)
			}
		}
	}
}

func TestBackoff_MonotonicMedian(t *testing.T) {
	// The lower bound must grow with the attempt number until the cap.
	if backoff(1) > 2*time.Second {
		t.Errorf("attempt 1 exceeded its ceiling")
	}
	if backoff(3) < 4*time.Second {
		t.Errorf("attempt 3 below its floor")
	}
}

func TestBackoff_NonPositiveAttempt(t *testing.T) {
	// Defensive: attempt <= 0 is treated as attempt 1.
	d := backoff(0)
	if d < 1*time.Second || d > 2*time.Second {
		t.Errorf("backoff(0) = %v, want within attempt-1 range", d)
	}
}
