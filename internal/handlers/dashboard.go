package handlers

import (
	"log/slog"
	"net/http"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DashboardHandler serves the dashboard page.
type DashboardHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

// NewDashboardHandler creates a dashboard handler.
func NewDashboardHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *DashboardHandler {
	return &DashboardHandler{pool: pool, renderer: renderer, logger: logger}
}

// ServeHTTP renders the dashboard page.
func (h *DashboardHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if pool == nil {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}

	stats, err := db.GetDashboardStats(r.Context(), pool)
	if err != nil {
		h.logger.Error("fetch dashboard stats", "error", err)
		stats = &db.DashboardStats{CompressionRatio: "N/A", DBSize: "N/A"}
	}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/dashboard.html", stats)
		return
	}

	h.renderer.Page(w, http.StatusOK, "pages/dashboard.html", templates.PageData{
		Title:     "Dashboard",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Dashboard",
		CSRFToken: csrfFromContext(r),
		Content:   stats,
	})
}

func csrfFromContext(r *http.Request) string {
	if tok, ok := r.Context().Value("csrf_token").(string); ok {
		return tok
	}
	return ""
}
