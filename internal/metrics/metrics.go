// Package metrics exposes ForgeFlow's Prometheus instrumentation.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RunsClaimed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forgeflow_runs_claimed_total",
		Help: "Runs claimed by this worker.",
	})
	RunsCompleted = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "forgeflow_runs_completed_total",
		Help: "Runs finished by this worker, by outcome.",
	}, []string{"outcome"}) // succeeded | failed | dead

	RunDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "forgeflow_run_duration_seconds",
		Help:    "Wall-clock execution time of runs.",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 14),
	})
	LeasesRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forgeflow_leases_recovered_total",
		Help: "Runs recovered from dead workers by this node.",
	})
	SchedulerTicks = promauto.NewCounter(prometheus.CounterOpts{
		Name: "forgeflow_scheduler_ticks_total",
		Help: "Scheduler ticks executed while leader.",
	})
	IsLeader = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "forgeflow_scheduler_is_leader",
		Help: "1 if this node currently holds scheduler leadership.",
	})
)
