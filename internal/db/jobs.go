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

// JobError represents an error from a background job execution.
type JobError struct {
	JobID      int
	ProcSchema string
	ProcName   string
	PID        int
	StartTime  time.Time
	FinishTime time.Time
	SQLErrCode string
	ErrMessage string
}

// GetJob returns a single job by ID.
func GetJob(ctx context.Context, pool *pgxpool.Pool, jobID int) (*Job, error) {
	var j Job
	err := pool.QueryRow(ctx, `
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
		WHERE j.job_id = $1`, jobID).Scan(
		&j.JobID, &j.AppName, &j.ProcSchema, &j.ProcName,
		&j.Owner, &j.Scheduled, &j.ScheduleInterval,
		&j.HypertableSchema, &j.HypertableName, &j.Config,
		&j.NextStart, &j.LastRunStartedAt, &j.LastRunStatus,
		&j.LastRunDuration, &j.TotalRuns, &j.TotalSuccesses, &j.TotalFailures)
	if err != nil {
		return nil, fmt.Errorf("get job %d: %w", jobID, err)
	}
	return &j, nil
}

// GetJobErrors returns recent errors for a job.
func GetJobErrors(ctx context.Context, pool *pgxpool.Pool, jobID int) ([]JobError, error) {
	rows, err := pool.Query(ctx, `
		SELECT
			job_id,
			COALESCE(proc_schema, ''),
			COALESCE(proc_name, ''),
			COALESCE(pid, 0),
			COALESCE(start_time, '1970-01-01'::timestamptz),
			COALESCE(finish_time, '1970-01-01'::timestamptz),
			COALESCE(sqlerrcode, ''),
			COALESCE(err_message, '')
		FROM timescaledb_information.job_errors
		WHERE job_id = $1
		ORDER BY start_time DESC
		LIMIT 50`, jobID)
	if err != nil {
		return nil, fmt.Errorf("get job errors %d: %w", jobID, err)
	}
	defer rows.Close()

	var result []JobError
	for rows.Next() {
		var e JobError
		if err := rows.Scan(&e.JobID, &e.ProcSchema, &e.ProcName, &e.PID,
			&e.StartTime, &e.FinishTime, &e.SQLErrCode, &e.ErrMessage); err != nil {
			return nil, fmt.Errorf("scan job error: %w", err)
		}
		result = append(result, e)
	}
	return result, rows.Err()
}

// UpdateJobSchedule changes the schedule interval of a job.
func UpdateJobSchedule(ctx context.Context, pool *pgxpool.Pool, jobID int, interval string) error {
	_, err := pool.Exec(ctx, `SELECT alter_job($1, schedule_interval => $2::interval)`, jobID, interval)
	if err != nil {
		return fmt.Errorf("update job %d schedule: %w", jobID, err)
	}
	return nil
}

// UpdateJobConfig changes the config of a job.
func UpdateJobConfig(ctx context.Context, pool *pgxpool.Pool, jobID int, config string) error {
	_, err := pool.Exec(ctx, `SELECT alter_job($1, config => $2::jsonb)`, jobID, config)
	if err != nil {
		return fmt.Errorf("update job %d config: %w", jobID, err)
	}
	return nil
}
