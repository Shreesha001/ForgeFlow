// Package scheduler turns job schedules into due runs. Every node runs a
// Scheduler, but only the one holding the Postgres advisory lock acts, so the
// scheduler is highly available with no extra coordination service.
package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/Shreesha001/ForgeFlow/internal/config"
	"github.com/Shreesha001/ForgeFlow/internal/metrics"
	"github.com/Shreesha001/ForgeFlow/internal/store"
)

var cronParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ValidateCron checks a cron expression at job-creation time.
func ValidateCron(expr string) error {
	_, err := cronParser.Parse(expr)
	return err
}

// NextAfter computes the first firing of a cron expression after t, in the
// job's timezone (this is where DST correctness comes from).
func NextAfter(expr, tz string, t time.Time) (time.Time, error) {
	sched, err := cronParser.Parse(expr)
	if err != nil {
		return time.Time{}, err
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return time.Time{}, fmt.Errorf("timezone %q: %w", tz, err)
	}
	return sched.Next(t.In(loc)), nil
}

type Scheduler struct {
	st  *store.Store
	cfg config.Config
	log *slog.Logger
}

func New(st *store.Store, cfg config.Config, log *slog.Logger) *Scheduler {
	return &Scheduler{st: st, cfg: cfg, log: log.With("component", "scheduler")}
}

// Run campaigns for leadership forever. While leader it ticks; if the database
// connection holding the lock dies, it detects that and steps down.
func (s *Scheduler) Run(ctx context.Context) {
	for ctx.Err() == nil {
		lead, err := s.st.TryLead(ctx)
		if err != nil {
			if ctx.Err() == nil {
				s.log.Error("leader campaign failed", "err", err)
			}
			sleep(ctx, 5*time.Second)
			continue
		}
		if lead == nil {
			sleep(ctx, 5*time.Second) // someone else leads; try again later
			continue
		}

		s.log.Info("became scheduler leader")
		metrics.IsLeader.Set(1)
		s.lead(ctx, lead)
		metrics.IsLeader.Set(0)
		lead.Resign(context.WithoutCancel(ctx))
		s.log.Info("resigned scheduler leadership")
	}
}

func (s *Scheduler) lead(ctx context.Context, lead *store.Leadership) {
	ticker := time.NewTicker(s.cfg.SchedulerTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !lead.StillLeader(ctx) {
				s.log.Warn("lost leadership connection, stepping down")
				return
			}
			metrics.SchedulerTicks.Inc()
			s.tick(ctx)
		}
	}
}

// tick materialises runs for every due job and advances its clock.
func (s *Scheduler) tick(ctx context.Context) {
	jobs, err := s.st.DueJobs(ctx, 100)
	if err != nil {
		if ctx.Err() == nil {
			s.log.Error("due-jobs query failed", "err", err)
		}
		return
	}
	now := time.Now()
	for _, j := range jobs {
		due := *j.NextRunAt
		var next *time.Time

		if j.CronExpr != nil {
			// Catch-up policy: after downtime several ticks may be overdue.
			// 'skip' fast-forwards past them all firing once; 'once' also
			// fires once — the difference is the timestamp the run carries.
			n, err := NextAfter(*j.CronExpr, j.Timezone, now)
			if err != nil {
				s.log.Error("cron parse failed, skipping job", "job_id", j.ID, "err", err)
				continue
			}
			next = &n
			if j.CatchUp == "skip" {
				due = now
			}
		}

		if err := s.st.AdvanceJob(ctx, j.ID, *j.NextRunAt, due, next); err != nil {
			s.log.Error("advance failed", "job_id", j.ID, "err", err)
			continue
		}
		s.log.Info("enqueued run", "job_id", j.ID, "job", j.Name, "due", due.Round(time.Second))
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
