package handlers

import (
	"log/slog"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type FunctionHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewFunctionHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *FunctionHandler {
	return &FunctionHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *FunctionHandler) CreateMatView(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	name := r.FormValue("name")
	query := r.FormValue("query")
	withData := r.FormValue("with_data") == "on"

	if name == "" || query == "" {
		http.Error(w, "name and query are required", http.StatusBadRequest)
		return
	}

	if err := db.CreateMaterializedView(r.Context(), pool, name, query, withData); err != nil {
		h.logger.Error("create materialized view", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if templates.IsHTMX(r) {
		w.Header().Set("HX-Redirect", "/functions")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/functions", http.StatusSeeOther)
}

func (h *FunctionHandler) RefreshMatView(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	name := r.FormValue("name")

	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if err := db.RefreshMaterializedView(r.Context(), pool, name); err != nil {
		h.logger.Error("refresh materialized view", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if templates.IsHTMX(r) {
		w.Header().Set("HX-Redirect", "/functions")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/functions", http.StatusSeeOther)
}

func (h *FunctionHandler) DropView(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	name := r.FormValue("name")
	materialized := r.FormValue("materialized") == "true"

	if name == "" {
		http.Error(w, "name is required", http.StatusBadRequest)
		return
	}

	if err := db.DropView(r.Context(), pool, name, materialized); err != nil {
		h.logger.Error("drop view", "error", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if templates.IsHTMX(r) {
		w.Header().Set("HX-Redirect", "/functions")
		w.WriteHeader(http.StatusOK)
		return
	}
	http.Redirect(w, r, "/functions", http.StatusSeeOther)
}

func (h *FunctionHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	functions, err := db.ListFunctions(r.Context(), pool)
	if err != nil {
		h.logger.Error("list functions", "error", err)
	}

	views, err := db.ListViews(r.Context(), pool)
	if err != nil {
		h.logger.Error("list views", "error", err)
	}

	data := map[string]any{"Functions": functions, "Views": views}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/functions.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/functions.html", templates.PageData{
		Title:     "Functions & Views",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Functions",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}
