package handlers

import (
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewJobHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *JobHandler {
	return &JobHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *JobHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	jobs, err := db.ListJobs(r.Context(), pool)
	if err != nil {
		h.logger.Error("list jobs", "error", err)
	}

	data := map[string]any{"Jobs": jobs}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/jobs.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/jobs.html", templates.PageData{
		Title:     "Jobs",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Jobs",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *JobHandler) Detail(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	jobID, err := strconv.Atoi(r.PathValue("id"))
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	job, err := db.GetJob(r.Context(), pool, jobID)
	if err != nil {
		h.logger.Error("get job", "error", err, "job_id", jobID)
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	errors, err := db.GetJobErrors(r.Context(), pool, jobID)
	if err != nil {
		h.logger.Error("get job errors", "error", err, "job_id", jobID)
	}

	data := map[string]any{"Job": job, "Errors": errors}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/job_detail.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/job_detail.html", templates.PageData{
		Title:     fmt.Sprintf("Job %d", jobID),
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Jobs",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *JobHandler) UpdateSchedule(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	jobID, err := strconv.Atoi(r.FormValue("job_id"))
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	interval := r.FormValue("schedule_interval")
	if interval == "" {
		http.Error(w, "Schedule interval is required", http.StatusBadRequest)
		return
	}

	if err := db.UpdateJobSchedule(r.Context(), pool, jobID, interval); err != nil {
		h.logger.Error("update job schedule", "error", err, "job_id", jobID)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/jobs/%d", jobID), http.StatusSeeOther)
}

func (h *JobHandler) UpdateConfig(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	jobID, err := strconv.Atoi(r.FormValue("job_id"))
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	config := r.FormValue("config")
	if config == "" {
		http.Error(w, "Config is required", http.StatusBadRequest)
		return
	}

	if err := db.UpdateJobConfig(r.Context(), pool, jobID, config); err != nil {
		h.logger.Error("update job config", "error", err, "job_id", jobID)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/jobs/%d", jobID), http.StatusSeeOther)
}

func (h *JobHandler) Action(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	jobID, err := strconv.Atoi(r.FormValue("job_id"))
	if err != nil {
		http.Error(w, "Invalid job ID", http.StatusBadRequest)
		return
	}

	action := r.FormValue("action")
	switch action {
	case "pause":
		err = db.PauseJob(r.Context(), pool, jobID)
	case "resume":
		err = db.ResumeJob(r.Context(), pool, jobID)
	case "run_now":
		err = db.RunJobNow(r.Context(), pool, jobID)
	default:
		http.Error(w, "Unknown action", http.StatusBadRequest)
		return
	}

	if err != nil {
		h.logger.Error("job action", "action", action, "job_id", jobID, "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/jobs", http.StatusSeeOther)
}
