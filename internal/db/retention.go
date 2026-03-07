package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RetentionPolicy represents a retention policy on a hypertable.
type RetentionPolicy struct {
	JobID       int
	Schema      string
	Table       string
	DropAfter   string
	Schedule    string
	Scheduled   bool
}

// ListRetentionPolicies returns all retention policies.
func ListRetentionPolicies(ctx context.Context, pool *pgxpool.Pool) ([]RetentionPolicy, error) {
	rows, err := pool.Query(ctx, `
		SELECT j.job_id, j.hypertable_schema, j.hypertable_name,
			j.config->>'drop_after',
			j.schedule_interval::text,
			j.scheduled
		FROM timescaledb_information.jobs j
		WHERE j.proc_name = 'policy_retention'
		ORDER BY j.hypertable_schema, j.hypertable_name`)
	if err != nil {
		return nil, fmt.Errorf("list retention policies: %w", err)
	}
	defer rows.Close()

	var result []RetentionPolicy
	for rows.Next() {
		var p RetentionPolicy
		if err := rows.Scan(&p.JobID, &p.Schema, &p.Table, &p.DropAfter, &p.Schedule, &p.Scheduled); err != nil {
			return nil, err
		}
		result = append(result, p)
	}
	return result, rows.Err()
}

// AddRetentionPolicy adds a retention policy.
func AddRetentionPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table, dropAfter string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	if err := ValidateInterval(dropAfter); err != nil {
		return err
	}

	sql := fmt.Sprintf("SELECT add_retention_policy('%s.%s', INTERVAL '%s', if_not_exists => true)", schema, table, dropAfter)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add retention policy: %w", err)
	}
	return nil
}

// RemoveRetentionPolicy removes a retention policy.
func RemoveRetentionPolicy(ctx context.Context, pool *pgxpool.Pool, schema, table string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(table); err != nil {
		return err
	}

	sql := fmt.Sprintf("SELECT remove_retention_policy('%s.%s', if_exists => true)", schema, table)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("remove retention policy: %w", err)
	}
	return nil
}
