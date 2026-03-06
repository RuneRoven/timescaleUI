package handlers

import (
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
