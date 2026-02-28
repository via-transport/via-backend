// Package database provides PostgreSQL connection pooling and schema migration
// for the Via backend.
package database

import (
	"context"
	"embed"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Config holds PostgreSQL connection parameters.
type Config struct {
	Host     string
	Port     string
	User     string
	Password string
	DBName   string
	SSLMode  string
	MaxConns int32
}

// ConfigFromEnv builds a Config from environment variables with sane defaults.
func ConfigFromEnv() Config {
	return Config{
		Host:     envOr("PG_HOST", "localhost"),
		Port:     envOr("PG_PORT", "5432"),
		User:     envOr("PG_USER", "via"),
		Password: envOr("PG_PASSWORD", "via-dev-password"),
		DBName:   envOr("PG_DATABASE", "via"),
		SSLMode:  envOr("PG_SSLMODE", "disable"),
		MaxConns: 20,
	}
}

func (c Config) DSN() string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%s/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.DBName, c.SSLMode,
	)
}

// Connect creates a pgxpool connection pool with retry.
func Connect(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse pg config: %w", err)
	}
	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	var pool *pgxpool.Pool
	for attempt := 1; attempt <= 10; attempt++ {
		pool, err = pgxpool.NewWithConfig(ctx, poolCfg)
		if err == nil {
			if pingErr := pool.Ping(ctx); pingErr == nil {
				break
			} else {
				pool.Close()
				err = pingErr
			}
		}
		log.Printf("[postgres] connect attempt %d/10 failed: %v", attempt, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to postgres after retries: %w", err)
	}

	log.Printf("[postgres] connected to %s:%s/%s (max_conns=%d)", cfg.Host, cfg.Port, cfg.DBName, cfg.MaxConns)
	return pool, nil
}

// Migrate runs all SQL files from the embedded migrations directory in order.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	// Ensure migration tracking table exists.
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS _migrations (
			filename TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create _migrations table: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	// Sort alphabetically (001_initial.sql, 002_..., etc.)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})

	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}

		// Check if already applied.
		var applied bool
		err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM _migrations WHERE filename=$1)`, e.Name()).Scan(&applied)
		if err != nil {
			return fmt.Errorf("check migration %s: %w", e.Name(), err)
		}
		if applied {
			continue
		}

		data, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx for %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, string(data)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("exec migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO _migrations (filename) VALUES ($1)`, e.Name()); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", e.Name(), err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", e.Name(), err)
		}
		log.Printf("[postgres] applied migration: %s", e.Name())
	}

	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
