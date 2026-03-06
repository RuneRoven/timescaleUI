package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Job represents a TimescaleDB background job.
type Job struct {
	JobID            int
	AppName          string
	ProcSchema       string
	ProcName         string
	Owner            string
	Scheduled        bool
	ScheduleInterval string
	HypertableSchema string
	HypertableName   string
	Config           string
	NextStart        time.Time
	LastRunStartedAt time.Time
	LastRunStatus    string
	LastRunDuration  string
	TotalRuns        int
	TotalSuccesses   int
	TotalFailures    int
}

// ListJobs returns all background jobs with their stats.
func ListJobs(ctx context.Context, pool *pgxpool.Pool) ([]Job, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			j.job_id,
			COALESCE(j.application_name, ''),
			j.proc_schema,
			j.proc_name,
			j.owner,
			j.scheduled,
			j.schedule_interval::text,
			COALESCE(j.hypertable_schema, ''),
			COALESCE(j.hypertable_name, ''),
			COALESCE(j.config::text, '{}'),
			COALESCE(js.next_start, '1970-01-01'::timestamptz),
			COALESCE(js.last_run_started_at, '1970-01-01'::timestamptz),
			COALESCE(js.last_run_status, ''),
			COALESCE(js.last_run_duration::text, ''),
			COALESCE(js.total_runs, 0),
			COALESCE(js.total_successes, 0),
			COALESCE(js.total_failures, 0)
		FROM timescaledb_information.jobs j
		LEFT JOIN timescaledb_information.job_stats js ON j.job_id = js.job_id
		ORDER BY j.job_id`)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()

	var result []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(&j.JobID, &j.AppName, &j.ProcSchema, &j.ProcName,
			&j.Owner, &j.Scheduled, &j.ScheduleInterval,
			&j.HypertableSchema, &j.HypertableName, &j.Config,
			&j.NextStart, &j.LastRunStartedAt, &j.LastRunStatus,
			&j.LastRunDuration, &j.TotalRuns, &j.TotalSuccesses, &j.TotalFailures); err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		result = append(result, j)
	}
	return result, rows.Err()
}

// PauseJob pauses a background job.
func PauseJob(ctx context.Context, pool *pgxpool.Pool, jobID int) error {
	_, err := pool.Exec(ctx, `SELECT alter_job($1, scheduled => false)`, jobID)
	if err != nil {
		return fmt.Errorf("pause job %d: %w", jobID, err)
	}
	return nil
}

// ResumeJob resumes a paused background job.
func ResumeJob(ctx context.Context, pool *pgxpool.Pool, jobID int) error {
	_, err := pool.Exec(ctx, `SELECT alter_job($1, scheduled => true)`, jobID)
	if err != nil {
		return fmt.Errorf("resume job %d: %w", jobID, err)
	}
	return nil
}

// RunJobNow triggers immediate execution of a background job.
func RunJobNow(ctx context.Context, pool *pgxpool.Pool, jobID int) error {
	_, err := pool.Exec(ctx, `SELECT alter_job($1, next_start => now())`, jobID)
	if err != nil {
		return fmt.Errorf("run job %d now: %w", jobID, err)
	}
	return nil
}
