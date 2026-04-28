package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWithRetry_SuccessOnFirstAttempt(t *testing.T) {
	callCount := 0
	op := func() error {
		callCount++
		return nil
	}

	cfg := DefaultConfig()
	err := WithRetry(cfg, op)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected 1 call, got %d", callCount)
	}
}

func TestWithRetry_FailThenSuccess(t *testing.T) {
	callCount := 0
	op := func() error {
		callCount++
		if callCount < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	cfg := Config{
		MaxRetries:        5,
		InitialIntervalMs: 10,
		MaxIntervalMs:     100,
	}
	err := WithRetry(cfg, op)

	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if callCount != 3 {
		t.Fatalf("expected 3 calls, got %d", callCount)
	}
}

func TestWithRetry_AlwaysFails(t *testing.T) {
	callCount := 0
	op := func() error {
		callCount++
		return errors.New("persistent error")
	}

	cfg := Config{
		MaxRetries:        3,
		InitialIntervalMs: 10,
		MaxIntervalMs:     100,
	}
	err := WithRetry(cfg, op)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if callCount != cfg.MaxRetries+1 {
		t.Fatalf("expected %d calls, got %d", cfg.MaxRetries+1, callCount)
	}
}

func TestCalculateDelay_ExponentialBackoff(t *testing.T) {
	tests := []struct {
		initial      int
		max          int
		attempt      int
		expectedBase time.Duration
		tolerance    time.Duration
	}{
		{100, 10000, 0, 100 * time.Millisecond, 1 * time.Millisecond},
		{100, 10000, 1, 200 * time.Millisecond, 50 * time.Millisecond},
		{100, 10000, 2, 400 * time.Millisecond, 100 * time.Millisecond},
		{100, 10000, 3, 800 * time.Millisecond, 200 * time.Millisecond},
		{100, 500, 10, 500 * time.Millisecond, 125 * time.Millisecond},
	}

	for _, tt := range tests {
		got := calculateDelay(tt.initial, tt.max, tt.attempt)
		diff := got - tt.expectedBase
		if diff < 0 {
			diff = -diff
		}
		if diff > tt.tolerance {
			t.Errorf("calculateDelay(%d, %d, %d) = %v, expected ~%v (±%v)",
				tt.initial, tt.max, tt.attempt, got, tt.expectedBase, tt.tolerance)
		}
	}
}

func TestWithRetryContext_CancelledContext(t *testing.T) {
	callCount := 0
	op := func() error {
		callCount++
		return errors.New("temporary error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cfg := Config{
		MaxRetries:        10,
		InitialIntervalMs: 100,
		MaxIntervalMs:     1000,
	}

	// Cancel context after a short delay
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := WithRetryContext(ctx, cfg, op)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("expected early exit due to cancellation, but took %v", elapsed)
	}
}
