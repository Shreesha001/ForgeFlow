package store

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// Integration tests for the claim/lease engine. They need a real Postgres:
//
//	FORGEFLOW_TEST_DATABASE_URL=postgres://... go test ./internal/store
//
// Skipped otherwise so `go test ./...` stays green without infrastructure.
func testStore(t *testing.T) *Store {
	t.Helper()
	url := os.Getenv("FORGEFLOW_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FORGEFLOW_TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	st, err := New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.pool.Exec(ctx, `TRUNCATE jobs, runs RESTART IDENTITY CASCADE`); err != nil {
		t.Fatal(err)
	}
	return st
}

func mkJob(t *testing.T, st *Store, maxAttempts int) Job {
	t.Helper()
	now := time.Now()
	j, err := st.CreateJob(context.Background(), Job{
		Name: "test", Executor: "shell", Payload: json.RawMessage(`{"command":"true"}`),
		Timezone: "UTC", Priority: 5, MaxAttempts: maxAttempts, TimeoutSeconds: 60,
		CatchUp: "skip", NextRunAt: &now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestClaimIsExclusive(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	j := mkJob(t, st, 3)

	const runs = 20
	for range runs {
		if _, err := st.EnqueueRun(ctx, j.ID, time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	// 10 workers race to claim; every run must be claimed exactly once.
	var mu sync.Mutex
	seen := map[int64]string{}
	var wg sync.WaitGroup
	for w := range 10 {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			for {
				claims, err := st.Claim(ctx, worker, 3, 30*time.Second)
				if err != nil {
					t.Error(err)
					return
				}
				if len(claims) == 0 {
					return
				}
				mu.Lock()
				for _, c := range claims {
					if prev, dup := seen[c.ID]; dup {
						t.Errorf("run %d claimed twice: %s and %s", c.ID, prev, worker)
					}
					seen[c.ID] = worker
				}
				mu.Unlock()
			}
		}("w" + string(rune('0'+w)))
	}
	wg.Wait()
	if len(seen) != runs {
		t.Fatalf("claimed %d runs, want %d", len(seen), runs)
	}
}

func TestLeaseRecoveryAfterWorkerDeath(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	j := mkJob(t, st, 3)
	if _, err := st.EnqueueRun(ctx, j.ID, time.Now()); err != nil {
		t.Fatal(err)
	}

	// "Worker" claims with a tiny lease and then dies (never heartbeats).
	claims, err := st.Claim(ctx, "doomed", 1, 50*time.Millisecond)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claims=%v err=%v", claims, err)
	}
	time.Sleep(100 * time.Millisecond)

	n, err := st.RecoverExpiredLeases(ctx)
	if err != nil || n != 1 {
		t.Fatalf("recovered=%d err=%v, want 1", n, err)
	}

	// The run is pending again and claimable by a healthy worker, attempt 2.
	claims, err = st.Claim(ctx, "healthy", 1, 30*time.Second)
	if err != nil || len(claims) != 1 {
		t.Fatalf("reclaim: claims=%v err=%v", claims, err)
	}
	if claims[0].Attempt != 2 {
		t.Errorf("attempt = %d, want 2", claims[0].Attempt)
	}
	// Error field records the dead worker for debuggability.
	if claims[0].Error == nil || !strings.Contains(*claims[0].Error, "doomed") {
		t.Errorf("error = %v, want mention of dead worker", claims[0].Error)
	}
}

func TestSlowWorkerCannotOverwriteNewOwner(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	j := mkJob(t, st, 5)
	if _, err := st.EnqueueRun(ctx, j.ID, time.Now()); err != nil {
		t.Fatal(err)
	}

	claims, _ := st.Claim(ctx, "slow", 1, 50*time.Millisecond)
	runID := claims[0].ID
	time.Sleep(100 * time.Millisecond)
	if _, err := st.RecoverExpiredLeases(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Claim(ctx, "new-owner", 1, 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// The zombie finishes late; its write must be rejected.
	ok, err := st.CompleteRun(ctx, runID, "slow", "zombie output")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("zombie worker overwrote the new owner's run")
	}
	run, _ := st.GetRun(ctx, runID)
	if run.Status != "running" || run.WorkerID == nil || *run.WorkerID != "new-owner" {
		t.Fatalf("run corrupted by zombie: %+v", run)
	}
}

func TestFailRunRetriesThenDeadLetters(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	j := mkJob(t, st, 2)
	if _, err := st.EnqueueRun(ctx, j.ID, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Attempt 1 fails -> retryable (pending, scheduled in the future).
	claims, _ := st.Claim(ctx, "w1", 1, 30*time.Second)
	if _, err := st.FailRun(ctx, claims[0].ID, "w1", "boom", time.Now()); err != nil {
		t.Fatal(err)
	}
	run, _ := st.GetRun(ctx, claims[0].ID)
	if run.Status != "pending" {
		t.Fatalf("after failure 1: status=%s, want pending", run.Status)
	}

	// Attempt 2 fails -> dead (max_attempts=2 exhausted).
	claims, _ = st.Claim(ctx, "w1", 1, 30*time.Second)
	if len(claims) != 1 {
		t.Fatalf("retry not claimable: %v", claims)
	}
	if _, err := st.FailRun(ctx, claims[0].ID, "w1", "boom again", time.Now()); err != nil {
		t.Fatal(err)
	}
	run, _ = st.GetRun(ctx, claims[0].ID)
	if run.Status != "dead" {
		t.Fatalf("after failure 2: status=%s, want dead", run.Status)
	}
}

func TestIdempotentJobCreation(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	key := "billing-2026-07"
	now := time.Now()
	base := Job{Name: "bill", IdempotencyKey: &key, Executor: "shell",
		Payload: json.RawMessage(`{}`), Timezone: "UTC", Priority: 5,
		MaxAttempts: 3, TimeoutSeconds: 60, CatchUp: "skip", NextRunAt: &now}

	a, err := st.CreateJob(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	b, err := st.CreateJob(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatalf("duplicate submission created two jobs: %d and %d", a.ID, b.ID)
	}
}
