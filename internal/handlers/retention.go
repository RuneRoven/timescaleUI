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

type RetentionHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewRetentionHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *RetentionHandler {
	return &RetentionHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *RetentionHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	policies, err := db.ListRetentionPolicies(r.Context(), pool)
	if err != nil {
		h.logger.Error("list retention policies", "error", err)
	}

	hypertables, err := db.ListHypertables(r.Context(), pool)
	if err != nil {
		h.logger.Error("list hypertables for retention", "error", err)
	}

	data := map[string]any{"Policies": policies, "Hypertables": hypertables}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/retention.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/retention.html", templates.PageData{
		Title:     "Retention",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Retention",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *RetentionHandler) Create(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	dropAfter := r.FormValue("drop_after")

	if err := db.AddRetentionPolicy(r.Context(), pool, schema, table, dropAfter); err != nil {
		h.logger.Error("add retention policy", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/retention", http.StatusSeeOther)
}

func (h *RetentionHandler) Delete(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")

	if err := db.RemoveRetentionPolicy(r.Context(), pool, schema, table); err != nil {
		h.logger.Error("remove retention policy", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/retention", http.StatusSeeOther)
}
