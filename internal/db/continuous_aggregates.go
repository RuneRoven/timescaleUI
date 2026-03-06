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
