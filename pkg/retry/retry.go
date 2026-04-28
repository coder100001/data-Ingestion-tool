package retry

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"
)

// Config holds retry configuration parameters
type Config struct {
	MaxRetries        int
	InitialIntervalMs int
	MaxIntervalMs     int
}

// DefaultConfig returns a default retry configuration
func DefaultConfig() Config {
	return Config{
		MaxRetries:        3,
		InitialIntervalMs: 100,
		MaxIntervalMs:     10000,
	}
}

// Operation is a function that can be retried
type Operation func() error

// WithRetry executes the given operation with exponential backoff retry strategy.
// It returns nil if the operation succeeds, or the last error if all retries are exhausted.
func WithRetry(cfg Config, op Operation) error {
	return WithRetryContext(context.Background(), cfg, op)
}

// WithRetryContext executes the given operation with exponential backoff retry strategy
// and supports cancellation via context.
func WithRetryContext(ctx context.Context, cfg Config, op Operation) error {
	if cfg.MaxRetries <= 0 {
		return op()
	}

	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if err := op(); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt < cfg.MaxRetries {
			delay := calculateDelay(cfg.InitialIntervalMs, cfg.MaxIntervalMs, attempt)

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return fmt.Errorf("retry cancelled: %w", ctx.Err())
			case <-timer.C:
			}
		}
	}

	return fmt.Errorf("retry exhausted after %d attempts: %w", cfg.MaxRetries, lastErr)
}

// calculateDelay computes the backoff delay for a given attempt using exponential backoff with jitter.
// The delay doubles with each attempt, capped at maxIntervalMs, with random jitter to prevent thundering herd.
func calculateDelay(initialIntervalMs, maxIntervalMs, attempt int) time.Duration {
	if attempt <= 0 {
		return time.Duration(initialIntervalMs) * time.Millisecond
	}

	delayMs := float64(initialIntervalMs) * math.Pow(2, float64(attempt))
	if delayMs > float64(maxIntervalMs) {
		delayMs = float64(maxIntervalMs)
	}

	// Add random jitter (±25%) to prevent thundering herd problem
	jitter := delayMs * 0.25 * (rand.Float64()*2 - 1)
	delayMs += jitter

	return time.Duration(delayMs) * time.Millisecond
}
