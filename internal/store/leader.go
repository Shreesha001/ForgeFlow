package store

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// schedulerLockKey is the advisory lock key that elects the scheduler leader.
const schedulerLockKey = 0xF0F6E // "FORGE"

// Leadership is a held advisory lock. It is tied to one pooled connection;
// releasing the connection releases leadership.
type Leadership struct {
	conn *pgxpool.Conn
}

// TryLead attempts to become the scheduler leader via a Postgres session-level
// advisory lock. It is non-blocking: returns (nil, nil) if another node leads.
// If the leader crashes, Postgres drops its connection and the lock frees
// itself — no separate failure detector needed.
func (s *Store) TryLead(ctx context.Context) (*Leadership, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var got bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, schedulerLockKey).Scan(&got); err != nil {
		conn.Release()
		return nil, err
	}
	if !got {
		conn.Release()
		return nil, nil
	}
	return &Leadership{conn: conn}, nil
}

// StillLeader verifies the lock-holding connection is alive. If the network
// partitioned us from Postgres, this fails and the caller must stop scheduling.
func (l *Leadership) StillLeader(ctx context.Context) bool {
	return l.conn.Ping(ctx) == nil
}

// Resign releases leadership.
func (l *Leadership) Resign(ctx context.Context) {
	_, _ = l.conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, schedulerLockKey)
	l.conn.Release()
}
