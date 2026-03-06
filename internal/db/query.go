package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
