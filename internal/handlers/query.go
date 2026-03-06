package handlers

import (
	"log/slog"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type QueryHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
	readOnly bool
	rowLimit int
}

func NewQueryHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger, readOnly bool, rowLimit int) *QueryHandler {
	return &QueryHandler{pool: pool, renderer: renderer, logger: logger, readOnly: readOnly, rowLimit: rowLimit}
}

func (h *QueryHandler) Page(w http.ResponseWriter, r *http.Request) {
	data := map[string]any{"ReadOnly": h.readOnly, "RowLimit": h.rowLimit}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/query.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/query.html", templates.PageData{
		Title:     "SQL Runner",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "SQL Runner",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *QueryHandler) Execute(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sql := r.FormValue("sql")
	if sql == "" {
		h.renderer.Partial(w, http.StatusOK, "partials/query_result.html", &db.QueryResult{Error: "No SQL provided"})
		return
	}

	h.logger.Info("execute query", "user", middleware.UserFromContext(r.Context()), "read_only", h.readOnly)
	result := db.ExecuteQuery(r.Context(), pool, sql, h.readOnly, h.rowLimit)

	h.renderer.Partial(w, http.StatusOK, "partials/query_result.html", result)
}

func (h *QueryHandler) Analyze(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	sql := r.FormValue("sql")
	if sql == "" {
		h.renderer.Partial(w, http.StatusOK, "partials/explain_result.html", &db.ExplainResult{Error: "No SQL provided"})
		return
	}

	h.logger.Info("explain analyze", "user", middleware.UserFromContext(r.Context()))
	result := db.ExplainQuery(r.Context(), pool, sql)

	h.renderer.Partial(w, http.StatusOK, "partials/explain_result.html", result)
}
