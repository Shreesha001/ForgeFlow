// Package worker implements ForgeFlow's execution nodes: a poll-claim-execute
// loop with a bounded goroutine pool, lease heartbeats, crash recovery of other
// workers' runs, and graceful drain on shutdown.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Shreesha001/ForgeFlow/internal/backoff"
	"github.com/Shreesha001/ForgeFlow/internal/config"
	"github.com/Shreesha001/ForgeFlow/internal/executor"
	"github.com/Shreesha001/ForgeFlow/internal/metrics"
	"github.com/Shreesha001/ForgeFlow/internal/store"
)

type Worker struct {
	st  *store.Store
	cfg config.Config
	log *slog.Logger

	slots chan struct{} // semaphore bounding concurrent executions
	wg    sync.WaitGroup
}

func New(st *store.Store, cfg config.Config, log *slog.Logger) *Worker {
	return &Worker{
		st:    st,
		cfg:   cfg,
		log:   log.With("component", "worker", "worker_id", cfg.WorkerID),
		slots: make(chan struct{}, cfg.Concurrency),
	}
}

// Run polls until ctx is cancelled, then drains: in-flight runs finish (their
// own timeouts still apply) but no new runs are claimed.
func (w *Worker) Run(ctx context.Context) {
	w.log.Info("worker started", "concurrency", w.cfg.Concurrency)

	go w.heartbeatLoop(ctx)
	go w.recoveryLoop(ctx)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			w.log.Info("draining: waiting for in-flight runs")
			w.wg.Wait()
			w.log.Info("worker stopped")
			return
		case <-ticker.C:
			w.poll(ctx)
		}
	}
}

// poll claims as many runs as there are free slots and dispatches them.
func (w *Worker) poll(ctx context.Context) {
	free := cap(w.slots) - len(w.slots)
	if free == 0 {
		return
	}
	claims, err := w.st.Claim(ctx, w.cfg.WorkerID, free, w.cfg.LeaseDuration)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Error("claim failed", "err", err)
		}
		return
	}
	for _, c := range claims {
		metrics.RunsClaimed.Inc()
		w.slots <- struct{}{}
		w.wg.Add(1)
		go func(c store.ClaimedRun) {
			defer w.wg.Done()
			defer func() { <-w.slots }()
			w.execute(c)
		}(c)
	}
}

// execute runs one claimed run to completion. It deliberately uses
// context.Background(): a shutdown must not kill in-flight work, only the
// job's own timeout may.
func (w *Worker) execute(c store.ClaimedRun) {
	log := w.log.With("run_id", c.ID, "job", c.JobName, "attempt", c.Attempt)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(c.TimeoutSeconds)*time.Second)
	defer cancel()

	start := time.Now()
	output, err := executor.Execute(ctx, c.Executor, c.Payload)
	metrics.RunDuration.Observe(time.Since(start).Seconds())

	dbctx, dbcancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer dbcancel()

	if err == nil {
		ok, uerr := w.st.CompleteRun(dbctx, c.ID, w.cfg.WorkerID, output)
		if uerr != nil {
			log.Error("complete update failed", "err", uerr)
			return
		}
		if !ok {
			log.Warn("lost lease before completion; result discarded")
			return
		}
		metrics.RunsCompleted.WithLabelValues("succeeded").Inc()
		log.Info("run succeeded", "duration", time.Since(start).Round(time.Millisecond))
		return
	}

	retryAt := time.Now().Add(backoff.Delay(c.Attempt))
	ok, uerr := w.st.FailRun(dbctx, c.ID, w.cfg.WorkerID, err.Error(), retryAt)
	if uerr != nil {
		log.Error("fail update failed", "err", uerr)
		return
	}
	if !ok {
		log.Warn("lost lease before failure report")
		return
	}
	if c.Attempt >= c.MaxAttempts {
		metrics.RunsCompleted.WithLabelValues("dead").Inc()
		log.Error("run dead-lettered", "err", err)
	} else {
		metrics.RunsCompleted.WithLabelValues("failed").Inc()
		log.Warn("run failed, will retry", "err", err, "retry_at", retryAt.Round(time.Second))
	}
}

// heartbeatLoop extends leases on everything this worker is running.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := w.st.Heartbeat(ctx, w.cfg.WorkerID, w.cfg.LeaseDuration); err != nil && ctx.Err() == nil {
				w.log.Error("heartbeat failed", "err", err)
			}
		}
	}
}

// recoveryLoop re-queues runs whose workers stopped heartbeating.
func (w *Worker) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.LeaseDuration / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := w.st.RecoverExpiredLeases(ctx)
			if err != nil && ctx.Err() == nil {
				w.log.Error("lease recovery failed", "err", err)
				continue
			}
			if n > 0 {
				metrics.LeasesRecovered.Add(float64(n))
				w.log.Warn("recovered runs from dead workers", "count", n)
			}
		}
	}
}
