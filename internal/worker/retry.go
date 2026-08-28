package worker

import (
	"math/rand/v2"
	"time"
)

const (
	backoffBase = 2 * time.Second
	backoffCap  = 60 * time.Second
)

// backoff is exponential with equal jitter: uniform in [exp/2, exp] where
// exp = base * 2^(attempt-1), capped. Jitter keeps competing workers from
// retrying in lockstep. attempt is 1-based.
func backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 5 {
		shift = 5
	}
	exp := backoffBase << shift
	if exp > backoffCap {
		exp = backoffCap
	}
	half := exp / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
