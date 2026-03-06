package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ContinuousAggregate represents a continuous aggregate.
type ContinuousAggregate struct {
	Schema             string
	Name               string
	ViewOwner          string
	MaterializedOnly   bool
	SourceHypertable   string
	ViewDefinition     string
	CompressionEnabled bool
}

// ListContinuousAggregates returns all continuous aggregates.
func ListContinuousAggregates(ctx context.Context, pool *pgxpool.Pool) ([]ContinuousAggregate, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			ca.materialization_hypertable_schema,
			ca.view_name,
			ca.view_owner,
			ca.materialized_only,
			COALESCE(ca.hypertable_schema || '.' || ca.hypertable_name, ''),
			ca.view_definition,
			ca.compression_enabled
		FROM timescaledb_information.continuous_aggregates ca
		ORDER BY ca.view_schema, ca.view_name`)
	if err != nil {
		return nil, fmt.Errorf("list continuous aggregates: %w", err)
	}
	defer rows.Close()

	var result []ContinuousAggregate
	for rows.Next() {
		var ca ContinuousAggregate
		if err := rows.Scan(&ca.Schema, &ca.Name, &ca.ViewOwner, &ca.MaterializedOnly,
			&ca.SourceHypertable, &ca.ViewDefinition, &ca.CompressionEnabled); err != nil {
			return nil, fmt.Errorf("scan CA: %w", err)
		}
		result = append(result, ca)
	}
	return result, rows.Err()
}

// CreateContinuousAggregate creates a new continuous aggregate.
func CreateContinuousAggregate(ctx context.Context, pool *pgxpool.Pool, name, query string, materializedOnly bool) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}

	withClause := "WITH (timescaledb.continuous"
	if materializedOnly {
		withClause += ", timescaledb.materialized_only=true"
	}
	withClause += ")"

	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s %s AS %s WITH NO DATA",
		name, withClause, query)

	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create continuous aggregate: %w", err)
	}
	return nil
}

// RefreshContinuousAggregate refreshes a continuous aggregate over a time range.
func RefreshContinuousAggregate(ctx context.Context, pool *pgxpool.Pool, name, start, end string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	sql := fmt.Sprintf("CALL refresh_continuous_aggregate('%s', %s, %s)", name, start, end)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("refresh continuous aggregate: %w", err)
	}
	return nil
}

// CADetail holds detailed info for a single continuous aggregate.
type CADetail struct {
	ContinuousAggregate
	RefreshJobID     *int
	ScheduleInterval string
	StartOffset      string
	EndOffset        string
}

// GetContinuousAggregateDetail returns detailed info about a specific CA.
func GetContinuousAggregateDetail(ctx context.Context, pool *pgxpool.Pool, schema, name string) (*CADetail, error) {
	if err := ValidateIdentifier(schema); err != nil {
		return nil, err
	}
	if err := ValidateIdentifier(name); err != nil {
		return nil, err
	}

	d := &CADetail{}
	err := pool.QueryRow(ctx, `
		SELECT
			ca.materialization_hypertable_schema,
			ca.view_name,
			ca.view_owner,
			ca.materialized_only,
			COALESCE(ca.hypertable_schema || '.' || ca.hypertable_name, ''),
			ca.view_definition,
			ca.compression_enabled
		FROM timescaledb_information.continuous_aggregates ca
		WHERE ca.materialization_hypertable_schema = $1 AND ca.view_name = $2`,
		schema, name).Scan(&d.Schema, &d.Name, &d.ViewOwner, &d.MaterializedOnly,
		&d.SourceHypertable, &d.ViewDefinition, &d.CompressionEnabled)
	if err != nil {
		return nil, fmt.Errorf("get CA detail: %w", err)
	}

	// Refresh policy
	var jobID *int
	var interval, startOff, endOff *string
	err = pool.QueryRow(ctx, `
		SELECT j.job_id, j.schedule_interval::text,
			COALESCE(j.config->>'start_offset', ''),
			COALESCE(j.config->>'end_offset', '')
		FROM timescaledb_information.jobs j
		WHERE j.proc_name = 'policy_refresh_continuous_aggregate'
		  AND j.hypertable_schema = $1
		  AND j.hypertable_name = $2
		LIMIT 1`, schema, name).Scan(&jobID, &interval, &startOff, &endOff)
	if err == nil {
		d.RefreshJobID = jobID
		if interval != nil {
			d.ScheduleInterval = *interval
		}
		if startOff != nil {
			d.StartOffset = *startOff
		}
		if endOff != nil {
			d.EndOffset = *endOff
		}
	}

	return d, nil
}

// SetMaterializedOnly toggles the materialized_only setting on a CA.
func SetMaterializedOnly(ctx context.Context, pool *pgxpool.Pool, schema, name string, materializedOnly bool) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(name); err != nil {
		return err
	}

	sql := fmt.Sprintf("ALTER MATERIALIZED VIEW %s.%s SET (timescaledb.materialized_only = %t)",
		schema, name, materializedOnly)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("set materialized_only: %w", err)
	}
	return nil
}

// AddRefreshPolicy adds a continuous aggregate refresh policy.
func AddRefreshPolicy(ctx context.Context, pool *pgxpool.Pool, schema, name, startOffset, endOffset, scheduleInterval string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(name); err != nil {
		return err
	}

	sql := fmt.Sprintf(`SELECT add_continuous_aggregate_policy('%s.%s',
		start_offset => INTERVAL '%s',
		end_offset => INTERVAL '%s',
		schedule_interval => INTERVAL '%s',
		if_not_exists => true)`,
		schema, name, startOffset, endOffset, scheduleInterval)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("add refresh policy: %w", err)
	}
	return nil
}

// RemoveRefreshPolicy removes a continuous aggregate refresh policy.
func RemoveRefreshPolicy(ctx context.Context, pool *pgxpool.Pool, schema, name string) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(name); err != nil {
		return err
	}

	sql := fmt.Sprintf("SELECT remove_continuous_aggregate_policy('%s.%s', if_not_exists => true)", schema, name)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("remove refresh policy: %w", err)
	}
	return nil
}

// RecreateContinuousAggregate drops and recreates a CA with a new query.
func RecreateContinuousAggregate(ctx context.Context, pool *pgxpool.Pool, schema, name, query string, materializedOnly bool) error {
	if err := ValidateIdentifier(schema); err != nil {
		return err
	}
	if err := ValidateIdentifier(name); err != nil {
		return err
	}

	qualifiedName := fmt.Sprintf("%s.%s", schema, name)

	// Drop existing
	dropSQL := fmt.Sprintf("DROP MATERIALIZED VIEW IF EXISTS %s CASCADE", qualifiedName)
	if _, err := pool.Exec(ctx, dropSQL); err != nil {
		return fmt.Errorf("drop CA for recreate: %w", err)
	}

	// Recreate
	withClause := "WITH (timescaledb.continuous"
	if materializedOnly {
		withClause += ", timescaledb.materialized_only=true"
	}
	withClause += ")"

	createSQL := fmt.Sprintf("CREATE MATERIALIZED VIEW %s %s AS %s WITH NO DATA",
		qualifiedName, withClause, query)
	if _, err := pool.Exec(ctx, createSQL); err != nil {
		return fmt.Errorf("recreate CA: %w", err)
	}
	return nil
}

// DropContinuousAggregate drops a continuous aggregate.
func DropContinuousAggregate(ctx context.Context, pool *pgxpool.Pool, name string) error {
	if err := ValidateIdentifier(name); err != nil {
		return err
	}
	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s CASCADE", name)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("drop continuous aggregate: %w", err)
	}
	return nil
}
