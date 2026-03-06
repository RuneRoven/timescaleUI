package db

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ConnConfig holds database connection parameters.
type ConnConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}

// DSN returns the connection string.
func (c ConnConfig) DSN() string {
	sslmode := c.SSLMode
	if sslmode == "" {
		sslmode = "disable"
	}
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&connect_timeout=10",
		url.QueryEscape(c.User), url.QueryEscape(c.Password),
		c.Host, c.Port, c.DBName, sslmode)
}

// NewPool creates a pgxpool and verifies connectivity.
func NewPool(ctx context.Context, cfg ConnConfig) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("parse pool config: %w", err)
	}
	poolCfg.MaxConns = 10

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pool (%s:%d/%s): %w", cfg.Host, cfg.Port, cfg.DBName, err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s:%d/%s: %w", cfg.Host, cfg.Port, cfg.DBName, err)
	}

	return pool, nil
}

// HealthCheck verifies the pool is connected and TimescaleDB is available.
func HealthCheck(ctx context.Context, pool *pgxpool.Pool) error {
	var version string
	err := pool.QueryRow(ctx, "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").Scan(&version)
	if err != nil {
		return fmt.Errorf("timescaledb extension not found: %w", err)
	}
	return nil
}
