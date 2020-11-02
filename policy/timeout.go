package policy

import (
	"math"
	"time"
)

// Timeout functions accept an attempt (from 0 to the maximum integer) and
// return an expected duration for which the attempt should run.
type Timeout func(int) time.Duration

// ConstantTimeout returns a Timeout function that always returns a constant
// duration.
func ConstantTimeout(duration time.Duration) Timeout {
	return func(int) time.Duration {
		return duration
	}
}

// MaxTimeout returns a Timeout function that restricts another Timeout function
// to return a maximum duration.
func MaxTimeout(duration time.Duration, timeout Timeout) Timeout {
	return func(attempt int) time.Duration {
		customTimeout := timeout(attempt)
		if customTimeout > duration {
			return duration
		}
		return customTimeout
	}
}

// LinearBackoff returns a Timeout function that scales the duration returned by
// another Timeout function linearly with respect to the attempt.
func LinearBackoff(rate float64, timeout Timeout) Timeout {
	return func(attempt int) time.Duration {
		return time.Duration(rate*float64(attempt)) * timeout(attempt)
	}
}

// ExponentialBackoff returns a Timeout function that scales the duration
// returned by another Timeout function exponentially with respect to the
// attempt.
func ExponentialBackoff(rate float64, timeout Timeout) Timeout {
	return func(attempt int) time.Duration {
		return time.Duration(math.Pow(rate, float64(attempt))) * timeout(attempt)
	}
}
