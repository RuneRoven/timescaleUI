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

type CompressionHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewCompressionHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *CompressionHandler {
	return &CompressionHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *CompressionHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	stats, err := db.ListCompressionStats(r.Context(), pool)
	if err != nil {
		h.logger.Error("list compression stats", "error", err)
	}

	data := map[string]any{"Stats": stats}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/compression.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/compression.html", templates.PageData{
		Title:     "Compression",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Compression",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *CompressionHandler) Enable(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	segmentBy := r.FormValue("segment_by")
	orderBy := r.FormValue("order_by")

	if err := db.EnableCompression(r.Context(), pool, schema, table, segmentBy, orderBy); err != nil {
		h.logger.Error("enable compression", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	// Optionally add compression policy
	if compressAfter := r.FormValue("compress_after"); compressAfter != "" {
		if err := db.AddCompressionPolicy(r.Context(), pool, schema, table, compressAfter); err != nil {
			h.logger.Error("add compression policy", "error", err)
		}
	}

	http.Redirect(w, r, "/compression", http.StatusSeeOther)
}

func (h *CompressionHandler) Disable(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")

	if err := db.DisableCompression(r.Context(), pool, schema, table); err != nil {
		h.logger.Error("disable compression", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/compression", http.StatusSeeOther)
}

func (h *CompressionHandler) CompressNow(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")

	if err := db.CompressChunks(r.Context(), pool, schema, table); err != nil {
		h.logger.Error("compress chunks", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, "/compression", http.StatusSeeOther)
}
