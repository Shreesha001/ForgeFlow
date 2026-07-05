// Package config loads ForgeFlow configuration from environment variables.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DatabaseURL string
	ListenAddr  string
	WorkerID    string

	// Worker tuning.
	Concurrency       int           // parallel runs per worker
	PollInterval      time.Duration // how often a worker polls for claimable runs
	LeaseDuration     time.Duration // lease length granted on claim/heartbeat
	HeartbeatInterval time.Duration // how often a worker extends its leases

	// Scheduler tuning.
	SchedulerTick time.Duration // how often the leader materialises due runs
}

func Load() Config {
	host, _ := os.Hostname()
	return Config{
		DatabaseURL:       getenv("FORGEFLOW_DATABASE_URL", "postgres://forgeflow:forgeflow@localhost:5432/forgeflow?sslmode=disable"),
		ListenAddr:        getenv("FORGEFLOW_LISTEN_ADDR", ":8080"),
		WorkerID:          getenv("FORGEFLOW_WORKER_ID", host+"-"+strconv.Itoa(os.Getpid())),
		Concurrency:       getint("FORGEFLOW_CONCURRENCY", 8),
		PollInterval:      getdur("FORGEFLOW_POLL_INTERVAL", time.Second),
		LeaseDuration:     getdur("FORGEFLOW_LEASE_DURATION", 30*time.Second),
		HeartbeatInterval: getdur("FORGEFLOW_HEARTBEAT_INTERVAL", 10*time.Second),
		SchedulerTick:     getdur("FORGEFLOW_SCHEDULER_TICK", time.Second),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getint(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func getdur(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}
