package db

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ExplainResult holds the output of an EXPLAIN ANALYZE query.
type ExplainResult struct {
	Plan         string
	PlanningTime string
	ExecTime     string
	Duration     time.Duration
	Error        string
}

// ExplainQuery runs EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) on a SQL query
// inside a rolled-back transaction so it never mutates data.
func ExplainQuery(ctx context.Context, pool *pgxpool.Pool, sql string) *ExplainResult {
	start := time.Now()
	result := &ExplainResult{}

	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	tx, err := pool.Begin(queryCtx)
	if err != nil {
		result.Error = fmt.Sprintf("begin transaction: %v", err)
		result.Duration = time.Since(start)
		return result
	}
	defer tx.Rollback(queryCtx)

	rows, err := tx.Query(queryCtx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+sql)
	if err != nil {
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}
	defer rows.Close()

	var lines []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			result.Error = fmt.Sprintf("scan explain: %v", err)
			result.Duration = time.Since(start)
			return result
		}
		lines = append(lines, line)

		// Extract timing from plan text
		if strings.HasPrefix(line, "Planning Time:") {
			result.PlanningTime = strings.TrimSpace(strings.TrimPrefix(line, "Planning Time:"))
		} else if strings.HasPrefix(line, "Execution Time:") {
			result.ExecTime = strings.TrimSpace(strings.TrimPrefix(line, "Execution Time:"))
		}
	}

	if err := rows.Err(); err != nil {
		result.Error = err.Error()
	}

	result.Plan = strings.Join(lines, "\n")
	result.Duration = time.Since(start)
	return result
}

// QueryResult holds the results of a SQL query.
type QueryResult struct {
	Columns  []string
	Rows     [][]string
	RowCount int
	Duration time.Duration
	Error    string
}

// ExecuteQuery runs a user-provided SQL query with safety constraints.
func ExecuteQuery(ctx context.Context, pool *pgxpool.Pool, sql string, readOnly bool, rowLimit int) *QueryResult {
	start := time.Now()
	result := &QueryResult{}

	// Set a query timeout
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	tx, err := pool.Begin(queryCtx)
	if err != nil {
		result.Error = fmt.Sprintf("begin transaction: %v", err)
		result.Duration = time.Since(start)
		return result
	}
	defer tx.Rollback(queryCtx)

	if readOnly {
		if _, err := tx.Exec(queryCtx, "SET TRANSACTION READ ONLY"); err != nil {
			result.Error = fmt.Sprintf("set read only: %v", err)
			result.Duration = time.Since(start)
			return result
		}
	}

	// Enforce row limit by wrapping in a subquery if it looks like a SELECT
	rows, err := tx.Query(queryCtx, sql)
	if err != nil {
		result.Error = err.Error()
		result.Duration = time.Since(start)
		return result
	}
	defer rows.Close()

	// Get column names
	fields := rows.FieldDescriptions()
	for _, f := range fields {
		result.Columns = append(result.Columns, string(f.Name))
	}

	// Scan rows
	for rows.Next() {
		if result.RowCount >= rowLimit {
			break
		}
		values, err := rows.Values()
		if err != nil {
			result.Error = fmt.Sprintf("scan row: %v", err)
			break
		}
		row := make([]string, len(values))
		for i, v := range values {
			if v == nil {
				row[i] = "NULL"
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result.Rows = append(result.Rows, row)
		result.RowCount++
	}

	if err := rows.Err(); err != nil {
		result.Error = err.Error()
	}

	result.Duration = time.Since(start)

	// Commit for read-only transactions
	if readOnly {
		tx.Commit(queryCtx)
	}

	return result
}
