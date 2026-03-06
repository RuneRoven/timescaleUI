package handlers

import (
	"log/slog"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TieringHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewTieringHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *TieringHandler {
	return &TieringHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *TieringHandler) Page(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	info, err := db.GetTieringInfo(r.Context(), pool)
	if err != nil {
		h.logger.Error("get tiering info", "error", err)
		info = &db.TieringInfo{Available: false}
	}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/tiering.html", info)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/tiering.html", templates.PageData{
		Title:     "Tiering",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Tiering",
		CSRFToken: csrfFromContext(r),
		Content:   info,
	})
}
