package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// validateQualifiedName validates a potentially schema-qualified identifier (e.g. "public.my_view").
func validateQualifiedName(name string) error {
	parts := strings.SplitN(name, ".", 2)
	for _, p := range parts {
		if err := ValidateIdentifier(p); err != nil {
			return err
		}
	}
	return nil
}

// DBFunction represents a database function.
type DBFunction struct {
	Schema     string
	Name       string
	ResultType string
	ArgTypes   string
	Type       string // function, procedure, aggregate
	Language   string
	Source     string
}

// DBView represents a database view.
type DBView struct {
	Schema     string
	Name       string
	Definition string
	IsMatView  bool
}

// ListFunctions returns all user-defined functions.
func ListFunctions(ctx context.Context, pool *pgxpool.Pool) ([]DBFunction, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			n.nspname,
			p.proname,
			pg_get_function_result(p.oid),
			pg_get_function_arguments(p.oid),
			CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' WHEN 'w' THEN 'window' END,
			l.lanname,
			CASE WHEN p.prokind != 'a' THEN COALESCE(pg_get_functiondef(p.oid), '') ELSE '' END
		FROM pg_proc p
		JOIN pg_namespace n ON p.pronamespace = n.oid
		JOIN pg_language l ON p.prolang = l.oid
		WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', '_timescaledb_internal', '_timescaledb_functions')
		  AND n.nspname NOT LIKE 'pg_toast%'
		ORDER BY n.nspname, p.proname`)
	if err != nil {
		return nil, fmt.Errorf("list functions: %w", err)
	}
	defer rows.Close()

	var result []DBFunction
	for rows.Next() {
		var f DBFunction
		if err := rows.Scan(&f.Schema, &f.Name, &f.ResultType, &f.ArgTypes,
			&f.Type, &f.Language, &f.Source); err != nil {
			return nil, err
		}
		result = append(result, f)
	}
	return result, rows.Err()
}

// CreateMaterializedView creates a new materialized view.
func CreateMaterializedView(ctx context.Context, pool *pgxpool.Pool, name, query string, withData bool) error {
	if err := validateQualifiedName(name); err != nil {
		return err
	}
	dataClause := "WITH NO DATA"
	if withData {
		dataClause = "WITH DATA"
	}
	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s %s", name, query, dataClause)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("create materialized view: %w", err)
	}
	return nil
}

// RefreshMaterializedView refreshes a materialized view.
func RefreshMaterializedView(ctx context.Context, pool *pgxpool.Pool, name string) error {
	if err := validateQualifiedName(name); err != nil {
		return err
	}
	sql := fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", name)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("refresh materialized view: %w", err)
	}
	return nil
}

// DropView drops a view or materialized view.
func DropView(ctx context.Context, pool *pgxpool.Pool, name string, materialized bool) error {
	if err := validateQualifiedName(name); err != nil {
		return err
	}
	kind := "VIEW"
	if materialized {
		kind = "MATERIALIZED VIEW"
	}
	sql := fmt.Sprintf("DROP %s %s CASCADE", kind, name)
	if _, err := pool.Exec(ctx, sql); err != nil {
		return fmt.Errorf("drop view: %w", err)
	}
	return nil
}

// ListViews returns all user-defined views.
func ListViews(ctx context.Context, pool *pgxpool.Pool) ([]DBView, error) {
	rows, err := pool.Query(ctx, `
		SELECT schemaname, viewname, definition, false
		FROM pg_views
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema', '_timescaledb_internal', '_timescaledb_functions', 'timescaledb_information')
		  AND schemaname NOT LIKE 'pg_toast%'
		UNION ALL
		SELECT schemaname, matviewname, definition, true
		FROM pg_matviews
		WHERE schemaname NOT IN ('pg_catalog', 'information_schema', '_timescaledb_internal', '_timescaledb_functions', 'timescaledb_information')
		  AND schemaname NOT LIKE 'pg_toast%'
		ORDER BY 1, 2`)
	if err != nil {
		return nil, fmt.Errorf("list views: %w", err)
	}
	defer rows.Close()

	var result []DBView
	for rows.Next() {
		var v DBView
		if err := rows.Scan(&v.Schema, &v.Name, &v.Definition, &v.IsMatView); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}
