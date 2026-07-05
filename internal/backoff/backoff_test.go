package backoff

import (
	"testing"
	"time"
)

func TestDelayGrowsExponentially(t *testing.T) {
	// With full jitter, delay for attempt n lies in [base*2^(n-1)/2, base*2^(n-1)].
	for attempt, want := range map[int]time.Duration{1: 5 * time.Second, 2: 10 * time.Second, 3: 20 * time.Second} {
		for i := 0; i < 100; i++ {
			d := Delay(attempt)
			if d < want/2 || d > want {
				t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, want/2, want)
			}
		}
	}
}

func TestDelayIsCapped(t *testing.T) {
	for i := 0; i < 100; i++ {
		if d := Delay(100); d > 15*time.Minute {
			t.Fatalf("delay %v exceeds cap", d)
		}
	}
}

func TestDelayHandlesBadInput(t *testing.T) {
	if d := Delay(0); d <= 0 {
		t.Fatalf("attempt 0: non-positive delay %v", d)
	}
	if d := Delay(-5); d <= 0 {
		t.Fatalf("negative attempt: non-positive delay %v", d)
	}
}
