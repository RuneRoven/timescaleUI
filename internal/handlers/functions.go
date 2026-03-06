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
