package backoff

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

func TestDelayGrowsAndIsCapped(t *testing.T) {
	p := Policy{Initial: time.Second, Max: 30 * time.Second, Rand: func() float64 { return 0 }} // no jitter
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, time.Second}, // attempt < 1 behaves like the first
		{1, time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
	}
	for _, tt := range tests {
		if got := p.Delay(tt.attempt); got != tt.want {
			t.Errorf("Delay(%d) = %s, want %s", tt.attempt, got, tt.want)
		}
	}
	for _, attempt := range []int{6, 10, 100, 1000} {
		if got := p.Delay(attempt); got != 30*time.Second {
			t.Errorf("Delay(%d) = %s, want the 30s cap", attempt, got)
		}
	}
}

func TestDelayJitterIsWithinBounds(t *testing.T) {
	p := Policy{Initial: time.Second, Max: 10 * time.Second}
	for attempt := 1; attempt <= 20; attempt++ {
		for range 100 {
			d := p.Delay(attempt)
			if d <= 0 || d > 10*time.Second {
				t.Fatalf("Delay(%d) = %s, outside (0,10s]", attempt, d)
			}
		}
	}
	// With jitter, delays differ.
	seen := map[time.Duration]bool{}
	for range 50 {
		seen[p.Delay(5)] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct delays in 50 draws — jitter missing", len(seen))
	}
}

func TestRetrySucceedsAfterFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		var delays []time.Duration
		p := Policy{Initial: time.Second, Max: 30 * time.Second, Rand: func() float64 { return 0 },
			OnRetry: func(_ int, _ error, d time.Duration) { delays = append(delays, d) }}
		start := time.Now()
		err := Retry(context.Background(), p, func(context.Context) error {
			calls++
			if calls < 4 {
				return errors.New("boom")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Retry: %v", err)
		}
		if calls != 4 {
			t.Errorf("fn called %d times, want 4", calls)
		}
		if want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}; len(delays) != 3 ||
			delays[0] != want[0] || delays[1] != want[1] || delays[2] != want[2] {
			t.Errorf("delays %v, want %v", delays, want)
		}
		if elapsed := time.Since(start); elapsed != 7*time.Second {
			t.Errorf("elapsed %s, want 7s", elapsed)
		}
	})
}

func TestRetryStopsAtMaxElapsed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		want := errors.New("still broken")
		calls := 0
		p := Policy{Initial: time.Second, Max: time.Minute, MaxElapsed: 10 * time.Second,
			Rand: func() float64 { return 0 }}
		start := time.Now()
		err := Retry(context.Background(), p, func(context.Context) error { calls++; return want })
		if !errors.Is(err, want) {
			t.Errorf("error %v, want %v", err, want)
		}
		if time.Since(start) > 10*time.Second {
			t.Errorf("slept past MaxElapsed: %s", time.Since(start))
		}
		if calls < 3 {
			t.Errorf("only %d attempts within the budget", calls)
		}
	})
}

func TestRetryStopsOnContextCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		want := errors.New("boom")
		calls := 0
		p := Policy{Initial: time.Second, Max: 30 * time.Second, Rand: func() float64 { return 0 }}
		err := Retry(ctx, p, func(context.Context) error { calls++; return want })
		if !errors.Is(err, want) {
			t.Errorf("error %v, want the last failure %v", err, want)
		}
		if calls > 3 {
			t.Errorf("%d attempts, want at most 3 within 3s", calls)
		}
	})
}

func TestRetryUnlimitedRunsUntilCancelled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		calls := 0
		p := Policy{Initial: time.Second, Max: 30 * time.Second} // MaxElapsed 0 = unlimited
		_ = Retry(ctx, p, func(context.Context) error { calls++; return errors.New("boom") })
		// One hour of 30s-capped, jittered delays: far more attempts than any bounded policy.
		if calls < 100 {
			t.Errorf("%d attempts in an hour, want many more", calls)
		}
	})
}

func TestSleep(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if !Sleep(context.Background(), time.Second) {
			t.Error("Sleep(1s) = false, want true")
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if Sleep(ctx, time.Hour) {
			t.Error("Sleep on a cancelled context = true, want false")
		}
		if !Sleep(context.Background(), 0) {
			t.Error("Sleep(0) = false, want true")
		}
	})
}
