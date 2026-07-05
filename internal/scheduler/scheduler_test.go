package scheduler

import (
	"testing"
	"time"
)

func TestValidateCron(t *testing.T) {
	for _, ok := range []string{"* * * * *", "*/5 * * * *", "0 9 * * 1-5", "@hourly", "@every 30s"} {
		if err := ValidateCron(ok); err != nil {
			t.Errorf("ValidateCron(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "banana", "61 * * * *", "* * * *"} {
		if err := ValidateCron(bad); err == nil {
			t.Errorf("ValidateCron(%q) = nil, want error", bad)
		}
	}
}

func TestNextAfter(t *testing.T) {
	base := time.Date(2026, 7, 6, 10, 30, 0, 0, time.UTC)

	next, err := NextAfter("*/15 * * * *", "UTC", base)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 7, 6, 10, 45, 0, 0, time.UTC); !next.Equal(want) {
		t.Errorf("next = %v, want %v", next, want)
	}
}

func TestNextAfterRespectsTimezone(t *testing.T) {
	// 9am daily in Kolkata (UTC+5:30) is 3:30 UTC.
	base := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC)
	next, err := NextAfter("0 9 * * *", "Asia/Kolkata", base)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Date(2026, 7, 6, 3, 30, 0, 0, time.UTC); !next.UTC().Equal(want) {
		t.Errorf("next = %v, want %v", next.UTC(), want)
	}
}

func TestNextAfterBadTimezone(t *testing.T) {
	if _, err := NextAfter("* * * * *", "Mars/OlympusMons", time.Now()); err == nil {
		t.Error("want error for bad timezone")
	}
}
