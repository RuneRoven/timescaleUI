package handlers

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CAHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewCAHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *CAHandler {
	return &CAHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *CAHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	cas, err := db.ListContinuousAggregates(r.Context(), pool)
	if err != nil {
		h.logger.Error("list CAs", "error", err)
	}

	data := map[string]any{"ContinuousAggregates": cas}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/continuous_aggregates.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/continuous_aggregates.html", templates.PageData{
		Title:     "Continuous Aggregates",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Continuous Aggregates",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *CAHandler) Detail(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	schema := r.PathValue("schema")
	name := r.PathValue("name")

	detail, err := db.GetContinuousAggregateDetail(r.Context(), pool, schema, name)
	if err != nil {
		h.logger.Error("get CA detail", "error", err, "schema", schema, "name", name)
		http.Error(w, "Continuous aggregate not found", http.StatusNotFound)
		return
	}

	data := map[string]any{"CA": detail}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/continuous_aggregate_detail.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/continuous_aggregate_detail.html", templates.PageData{
		Title:     fmt.Sprintf("%s.%s", schema, name),
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Continuous Aggregates",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *CAHandler) ToggleMaterializedOnly(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	name := r.FormValue("name")
	materializedOnly := r.FormValue("materialized_only") == "true"

	if err := db.SetMaterializedOnly(r.Context(), pool, schema, name, materializedOnly); err != nil {
		h.logger.Error("toggle materialized_only", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/continuous-aggregates/%s/%s", schema, name), http.StatusSeeOther)
}

func (h *CAHandler) AddPolicy(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	name := r.FormValue("name")
	startOffset := r.FormValue("start_offset")
	endOffset := r.FormValue("end_offset")
	scheduleInterval := r.FormValue("schedule_interval")

	if err := db.AddRefreshPolicy(r.Context(), pool, schema, name, startOffset, endOffset, scheduleInterval); err != nil {
		h.logger.Error("add refresh policy", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/continuous-aggregates/%s/%s", schema, name), http.StatusSeeOther)
}

func (h *CAHandler) RemovePolicy(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	name := r.FormValue("name")

	if err := db.RemoveRefreshPolicy(r.Context(), pool, schema, name); err != nil {
		h.logger.Error("remove refresh policy", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/continuous-aggregates/%s/%s", schema, name), http.StatusSeeOther)
}

func (h *CAHandler) UpdateDefinition(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	name := r.FormValue("name")
	query := r.FormValue("query")
	materializedOnly := r.FormValue("materialized_only") == "on"

	if err := db.RecreateContinuousAggregate(r.Context(), pool, schema, name, query, materializedOnly); err != nil {
		h.logger.Error("recreate CA", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/continuous-aggregates/%s/%s", schema, name), http.StatusSeeOther)
}

func (h *CAHandler) Create(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	name := r.FormValue("name")
	query := r.FormValue("query")
	materializedOnly := r.FormValue("materialized_only") == "on"

	if err := db.CreateContinuousAggregate(r.Context(), pool, name, query, materializedOnly); err != nil {
		h.logger.Error("create CA", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/continuous-aggregates", http.StatusSeeOther)
}

func (h *CAHandler) Refresh(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	name := r.FormValue("name")
	start := r.FormValue("start")
	end := r.FormValue("end")

	if start == "" {
		start = "NULL"
	} else {
		start = "'" + start + "'::timestamptz"
	}
	if end == "" {
		end = "NULL"
	} else {
		end = "'" + end + "'::timestamptz"
	}

	if err := db.RefreshContinuousAggregate(r.Context(), pool, name, start, end); err != nil {
		h.logger.Error("refresh CA", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/continuous-aggregates", http.StatusSeeOther)
}

func (h *CAHandler) Delete(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	name := r.FormValue("name")
	if err := db.DropContinuousAggregate(r.Context(), pool, name); err != nil {
		h.logger.Error("drop CA", "error", err)
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/continuous-aggregates", http.StatusSeeOther)
}
