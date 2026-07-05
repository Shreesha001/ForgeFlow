// Package api is ForgeFlow's HTTP surface: job management, run history,
// health, Prometheus metrics and the dashboard.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Shreesha001/ForgeFlow/internal/scheduler"
	"github.com/Shreesha001/ForgeFlow/internal/store"
)

//go:embed dashboard.html
var dashboardHTML []byte

type Server struct {
	st  *store.Store
	log *slog.Logger
}

func New(st *store.Store, log *slog.Logger) *Server {
	return &Server{st: st, log: log.With("component", "api")}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/jobs", s.createJob)
	mux.HandleFunc("GET /api/jobs", s.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}", s.getJob)
	mux.HandleFunc("POST /api/jobs/{id}/pause", s.setStatus("paused"))
	mux.HandleFunc("POST /api/jobs/{id}/resume", s.setStatus("active"))
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.setStatus("cancelled"))
	mux.HandleFunc("POST /api/jobs/{id}/trigger", s.trigger)
	mux.HandleFunc("GET /api/runs", s.listRuns)
	mux.HandleFunc("GET /api/runs/{id}", s.getRun)
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(dashboardHTML)
	})
	return s.logging(mux)
}

// createJobRequest is the public job-submission contract.
type createJobRequest struct {
	Name           string          `json:"name"`
	IdempotencyKey *string         `json:"idempotency_key"`
	Executor       string          `json:"executor"`
	Payload        json.RawMessage `json:"payload"`
	CronExpr       *string         `json:"cron_expr"`
	RunAt          *time.Time      `json:"run_at"`
	Timezone       string          `json:"timezone"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	CatchUp        string          `json:"catch_up"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	var req createJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON: %v", err)
		return
	}

	// Defaults.
	if req.Timezone == "" {
		req.Timezone = "UTC"
	}
	if req.Priority == 0 {
		req.Priority = 5
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = 3
	}
	if req.TimeoutSeconds == 0 {
		req.TimeoutSeconds = 300
	}
	if req.CatchUp == "" {
		req.CatchUp = "skip"
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	// Validation.
	switch {
	case req.Name == "":
		httpError(w, http.StatusBadRequest, "name is required")
		return
	case req.Executor != "shell" && req.Executor != "http":
		httpError(w, http.StatusBadRequest, "executor must be 'shell' or 'http'")
		return
	case req.CronExpr != nil && req.RunAt != nil:
		httpError(w, http.StatusBadRequest, "cron_expr and run_at are mutually exclusive")
		return
	case req.Priority < 1 || req.Priority > 9:
		httpError(w, http.StatusBadRequest, "priority must be 1-9 (1 = highest)")
		return
	}
	if req.CronExpr != nil {
		if err := scheduler.ValidateCron(*req.CronExpr); err != nil {
			httpError(w, http.StatusBadRequest, "invalid cron_expr: %v", err)
			return
		}
	}

	job := store.Job{
		Name:           req.Name,
		IdempotencyKey: req.IdempotencyKey,
		Executor:       req.Executor,
		Payload:        req.Payload,
		CronExpr:       req.CronExpr,
		RunAt:          req.RunAt,
		Timezone:       req.Timezone,
		Priority:       req.Priority,
		MaxAttempts:    req.MaxAttempts,
		TimeoutSeconds: req.TimeoutSeconds,
		CatchUp:        req.CatchUp,
	}

	// Seed the scheduler clock: recurring jobs start at their first cron
	// firing; one-off jobs at run_at; immediate jobs right now.
	now := time.Now()
	switch {
	case req.CronExpr != nil:
		next, err := scheduler.NextAfter(*req.CronExpr, req.Timezone, now)
		if err != nil {
			httpError(w, http.StatusBadRequest, "%v", err)
			return
		}
		job.NextRunAt = &next
	case req.RunAt != nil:
		job.NextRunAt = req.RunAt
	default:
		job.NextRunAt = &now
	}

	created, err := s.st.CreateJob(r.Context(), job)
	if err != nil {
		s.fail(w, "create job", err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) trigger(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid id")
		return
	}
	run, err := s.st.EnqueueRun(r.Context(), id, time.Now())
	if errors.Is(err, store.ErrNotFound) {
		httpError(w, http.StatusNotFound, "no active job %d", id)
		return
	}
	if err != nil {
		s.fail(w, "trigger", err)
		return
	}
	writeJSON(w, http.StatusCreated, run)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid id")
		return
	}
	job, err := s.st.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		httpError(w, http.StatusNotFound, "job %d not found", id)
		return
	}
	if err != nil {
		s.fail(w, "get job", err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	jobs, err := s.st.ListJobs(r.Context(), queryLimit(r))
	if err != nil {
		s.fail(w, "list jobs", err)
		return
	}
	writeJSON(w, http.StatusOK, orEmpty(jobs))
}

func (s *Server) setStatus(status string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := pathID(r)
		if err != nil {
			httpError(w, http.StatusBadRequest, "invalid id")
			return
		}
		job, err := s.st.SetJobStatus(r.Context(), id, status)
		if errors.Is(err, store.ErrNotFound) {
			httpError(w, http.StatusNotFound, "job %d not found", id)
			return
		}
		if err != nil {
			s.fail(w, "set status", err)
			return
		}
		writeJSON(w, http.StatusOK, job)
	}
}

func (s *Server) listRuns(w http.ResponseWriter, r *http.Request) {
	var jobID int64
	if v := r.URL.Query().Get("job_id"); v != "" {
		jobID, _ = strconv.ParseInt(v, 10, 64)
	}
	runs, err := s.st.ListRuns(r.Context(), jobID, queryLimit(r))
	if err != nil {
		s.fail(w, "list runs", err)
		return
	}
	writeJSON(w, http.StatusOK, orEmpty(runs))
}

func (s *Server) getRun(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid id")
		return
	}
	run, err := s.st.GetRun(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		httpError(w, http.StatusNotFound, "run %d not found", id)
		return
	}
	if err != nil {
		s.fail(w, "get run", err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	stats, err := s.st.Stats(r.Context())
	if err != nil {
		s.fail(w, "stats", err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// ---- helpers ----

func (s *Server) fail(w http.ResponseWriter, op string, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	s.log.Error(op+" failed", "err", err)
	httpError(w, http.StatusInternalServerError, "internal error")
}

func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/metrics" && r.URL.Path != "/healthz" {
			s.log.Info("http", "method", r.Method, "path", r.URL.Path, "duration", time.Since(start).Round(time.Microsecond))
		}
	})
}

func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

func queryLimit(r *http.Request) int {
	if n, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && n > 0 && n <= 1000 {
		return n
	}
	return 100
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, format string, args ...any) {
	writeJSON(w, code, map[string]string{"error": fmt.Sprintf(format, args...)})
}

func orEmpty[T any](s []T) []T {
	if s == nil {
		return []T{}
	}
	return s
}
