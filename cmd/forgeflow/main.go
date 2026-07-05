// ForgeFlow is a distributed job scheduler. One binary runs any combination of
// roles: api (HTTP server), scheduler (leader-elected cron engine) and worker
// (executes runs). Default is all three, so `forgeflow` alone is a full node
// and scaling out is just running more copies.
//
//	forgeflow            # api + scheduler + worker
//	forgeflow worker     # execution node only
//	forgeflow api        # api + scheduler (no execution)
//	forgeflow migrate    # apply migrations and exit
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/Shreesha001/ForgeFlow/internal/api"
	"github.com/Shreesha001/ForgeFlow/internal/config"
	"github.com/Shreesha001/ForgeFlow/internal/scheduler"
	"github.com/Shreesha001/ForgeFlow/internal/store"
	"github.com/Shreesha001/ForgeFlow/internal/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "forgeflow:", err)
		os.Exit(1)
	}
}

func run() error {
	roles := os.Args[1:]
	if len(roles) == 0 {
		roles = []string{"api", "scheduler", "worker"}
	}
	for _, r := range roles {
		if !slices.Contains([]string{"api", "scheduler", "worker", "migrate"}, r) {
			return fmt.Errorf("unknown role %q (want: api, scheduler, worker, migrate)", r)
		}
	}

	cfg := config.Load()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil)).With("node", cfg.WorkerID)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := connectWithRetry(ctx, cfg.DatabaseURL, log)
	if err != nil {
		return err
	}
	defer st.Close()

	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if slices.Contains(roles, "migrate") {
		log.Info("migrations applied")
		return nil
	}
	log.Info("forgeflow starting", "roles", roles)

	var wg sync.WaitGroup

	if slices.Contains(roles, "scheduler") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			scheduler.New(st, cfg, log).Run(ctx)
		}()
	}

	if slices.Contains(roles, "worker") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker.New(st, cfg, log).Run(ctx)
		}()
	}

	if slices.Contains(roles, "api") {
		srv := &http.Server{Addr: cfg.ListenAddr, Handler: api.New(st, log).Handler()}
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			shctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			srv.Shutdown(shctx)
		}()
		log.Info("api listening", "addr", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			stop()
			wg.Wait()
			return err
		}
	}

	wg.Wait()
	log.Info("forgeflow stopped")
	return nil
}

// connectWithRetry tolerates Postgres coming up after us (docker compose).
func connectWithRetry(ctx context.Context, url string, log *slog.Logger) (*store.Store, error) {
	for {
		st, err := store.New(ctx, url)
		if err == nil {
			return st, nil
		}
		log.Warn("database not ready, retrying in 2s", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}
