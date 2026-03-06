package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DashboardStats holds aggregate statistics for the dashboard.
type DashboardStats struct {
	HypertableCount   int
	ChunkCount        int
	DBSize            string
	CompressionRatio  string
	ActiveJobs        int
	FailedJobs        int
	CACount           int
	RetentionPolicies int
	RecentJobs        []JobSummary
}

// JobSummary is a brief job description for the dashboard.
type JobSummary struct {
	JobID            int
	ProcName         string
	Hypertable       string
	LastRunStartedAt time.Time
	LastRunStatus    string
	NextStart        time.Time
}

// GetDashboardStats fetches all dashboard metrics.
func GetDashboardStats(ctx context.Context, pool *pgxpool.Pool) (*DashboardStats, error) {
	stats := &DashboardStats{}

	// Hypertable count
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.hypertables`).Scan(&stats.HypertableCount)

	// Chunk count
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.chunks`).Scan(&stats.ChunkCount)

	// Database size
	_ = pool.QueryRow(ctx,
		`SELECT pg_size_pretty(pg_database_size(current_database()))`).Scan(&stats.DBSize)

	// Compression ratio
	var compressedSize, uncompressedSize *int64
	err := pool.QueryRow(ctx, `
		SELECT sum(before_compression_total_bytes), sum(after_compression_total_bytes)
		FROM hypertable_compression_stats(NULL)
		WHERE before_compression_total_bytes > 0`).Scan(&uncompressedSize, &compressedSize)
	if err == nil && uncompressedSize != nil && compressedSize != nil && *compressedSize > 0 {
		ratio := float64(*uncompressedSize) / float64(*compressedSize)
		stats.CompressionRatio = formatRatio(ratio)
	} else {
		stats.CompressionRatio = "N/A"
	}

	// Active jobs
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.jobs WHERE scheduled = true`).Scan(&stats.ActiveJobs)

	// Failed jobs (24h)
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.job_stats
		WHERE last_run_status = 'Failed'
		AND last_run_started_at > now() - interval '24 hours'`).Scan(&stats.FailedJobs)

	// Continuous aggregates count
	_ = pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.continuous_aggregates`).Scan(&stats.CACount)

	// Retention policies count
	_ = pool.QueryRow(ctx, `
		SELECT count(*) FROM timescaledb_information.jobs
		WHERE proc_name = 'policy_retention'`).Scan(&stats.RetentionPolicies)

	// Recent jobs
	rows, err := pool.Query(ctx, `
		SELECT j.job_id, j.proc_name,
			COALESCE(j.hypertable_schema || '.' || j.hypertable_name, ''),
			COALESCE(js.last_run_started_at, '1970-01-01'::timestamptz),
			COALESCE(js.last_run_status, ''),
			COALESCE(js.next_start, '1970-01-01'::timestamptz)
		FROM timescaledb_information.jobs j
		LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
		ORDER BY js.last_run_started_at DESC NULLS LAST
		LIMIT 10`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var j JobSummary
			if err := rows.Scan(&j.JobID, &j.ProcName, &j.Hypertable,
				&j.LastRunStartedAt, &j.LastRunStatus, &j.NextStart); err == nil {
				stats.RecentJobs = append(stats.RecentJobs, j)
			}
		}
	}

	return stats, nil
}

func formatRatio(r float64) string {
	if r < 10 {
		return fmt.Sprintf("%.1fx", r)
	}
	return fmt.Sprintf("%.0fx", r)
}
