package conn

import (
	"math"
	"time"
)

func DefaultTimeout(attempt int) time.Duration {
	return 10 * time.Second
}

func MaxTimeout(duration time.Duration, timeout func(int) time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		customTimeout := timeout(attempt)
		if customTimeout > duration {
			return duration
		}
		return customTimeout
	}
}

func LinearBackoff(backOffRate time.Duration, timeout func(int) time.Duration) func(int) time.Duration {
	backOffRateInSeconds := backOffRate / time.Second
	return func(attempt int) time.Duration {
		return backOffRateInSeconds * timeout(attempt)
	}
}

func ExponentialBackoff(backOffRate float64, timeout func(int) time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		return timeout(attempt) * time.Duration(math.Pow(backOffRate, float64(attempt)))
	}
}
