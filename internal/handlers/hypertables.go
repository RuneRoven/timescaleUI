package handlers

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
)

type HypertableHandler struct {
	pool     func() *pgxpool.Pool
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewHypertableHandler(pool func() *pgxpool.Pool, renderer *templates.Renderer, logger *slog.Logger) *HypertableHandler {
	return &HypertableHandler{pool: pool, renderer: renderer, logger: logger}
}

func (h *HypertableHandler) List(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if pool == nil {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}

	hypertables, err := db.ListHypertables(r.Context(), pool)
	if err != nil {
		h.logger.Error("list hypertables", "error", err)
	}

	data := map[string]any{"Hypertables": hypertables}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/hypertables.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/hypertables.html", templates.PageData{
		Title:     "Hypertables",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Hypertables",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *HypertableHandler) Detail(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	schema := r.PathValue("schema")
	table := r.PathValue("table")

	detail, err := db.GetHypertable(r.Context(), pool, schema, table)
	if err != nil {
		h.logger.Error("get hypertable detail", "error", err, "schema", schema, "table", table)
		http.Error(w, "Hypertable not found", http.StatusNotFound)
		return
	}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/hypertable_detail.html", detail)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/hypertable_detail.html", templates.PageData{
		Title:     fmt.Sprintf("%s.%s", schema, table),
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Hypertables",
		CSRFToken: csrfFromContext(r),
		Content:   detail,
	})
}

func (h *HypertableHandler) UpdateChunkInterval(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	interval := r.FormValue("chunk_interval")

	if err := db.SetChunkTimeInterval(r.Context(), pool, schema, table, interval); err != nil {
		h.logger.Error("set chunk time interval", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) AddReorderPolicy(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	indexName := r.FormValue("index_name")

	if err := db.AddReorderPolicy(r.Context(), pool, schema, table, indexName); err != nil {
		h.logger.Error("add reorder policy", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) RemoveReorderPolicy(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")

	if err := db.RemoveReorderPolicy(r.Context(), pool, schema, table); err != nil {
		h.logger.Error("remove reorder policy", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) CreateIndex(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	indexName := r.FormValue("index_name")
	columns := r.FormValue("columns")
	unique := r.FormValue("unique") == "on"

	if err := db.CreateIndex(r.Context(), pool, schema, table, indexName, columns, unique); err != nil {
		h.logger.Error("create index", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) DropIndex(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	indexName := r.FormValue("index_name")

	if err := db.DropIndex(r.Context(), pool, schema, indexName); err != nil {
		h.logger.Error("drop index", "error", err)
		http.Error(w, fmt.Sprintf("Failed: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) CreateForm(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	tables, err := db.ListRegularTables(r.Context(), pool)
	if err != nil {
		h.logger.Error("list regular tables", "error", err)
	}

	data := map[string]any{"Tables": tables}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/hypertable_create.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/hypertable_create.html", templates.PageData{
		Title:     "Create Hypertable",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Hypertables",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *HypertableHandler) Create(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	schema := r.FormValue("schema")
	table := r.FormValue("table")
	timeColumn := r.FormValue("time_column")
	chunkInterval := r.FormValue("chunk_interval")

	if err := db.CreateHypertable(r.Context(), pool, schema, table, timeColumn, chunkInterval); err != nil {
		h.logger.Error("create hypertable", "error", err)
		http.Error(w, fmt.Sprintf("Failed to create hypertable: %v", err), http.StatusUnprocessableEntity)
		return
	}

	http.Redirect(w, r, fmt.Sprintf("/hypertables/%s/%s", schema, table), http.StatusSeeOther)
}

func (h *HypertableHandler) GetColumns(w http.ResponseWriter, r *http.Request) {
	pool := h.pool()
	schema := r.URL.Query().Get("schema")
	table := r.URL.Query().Get("table")

	// Support "schema.table" format from the create form's table_full param
	if schema == "" && table == "" {
		if full := r.URL.Query().Get("table_full"); full != "" {
			if parts := strings.SplitN(full, ".", 2); len(parts) == 2 {
				schema = parts[0]
				table = parts[1]
			}
		}
	}

	cols, err := db.ListTableColumns(r.Context(), pool, schema, table)
	if err != nil {
		h.logger.Error("list columns", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	h.renderer.Partial(w, http.StatusOK, "partials/column_options.html", cols)
}
