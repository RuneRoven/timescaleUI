package handlers

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/RuneRoven/timescaleUI/internal/config"
	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type SettingsHandler struct {
	creds    *config.CredentialStore
	pool     func() *pgxpool.Pool
	setPool  func(*pgxpool.Pool)
	renderer *templates.Renderer
	logger   *slog.Logger
}

func NewSettingsHandler(creds *config.CredentialStore, pool func() *pgxpool.Pool, setPool func(*pgxpool.Pool), renderer *templates.Renderer, logger *slog.Logger) *SettingsHandler {
	return &SettingsHandler{creds: creds, pool: pool, setPool: setPool, renderer: renderer, logger: logger}
}

func (h *SettingsHandler) Page(w http.ResponseWriter, r *http.Request) {
	creds, err := h.creds.Load()
	if err != nil {
		h.logger.Error("load creds for settings", "error", err)
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	data := map[string]any{
		"AdminUser": creds.AdminUser,
		"DBHost":    creds.DBHost,
		"DBPort":    creds.DBPort,
		"DBUser":    creds.DBUser,
		"DBName":    creds.DBName,
		"DBSSLMode": creds.DBSSLMode,
	}

	if templates.IsHTMX(r) {
		h.renderer.Partial(w, http.StatusOK, "pages/settings.html", data)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/settings.html", templates.PageData{
		Title:     "Settings",
		User:      middleware.UserFromContext(r.Context()),
		Active:    "Settings",
		CSRFToken: csrfFromContext(r),
		Content:   data,
	})
}

func (h *SettingsHandler) UpdatePassword(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	currentPassword := r.FormValue("current_password")
	newPassword := r.FormValue("new_password")
	confirmPassword := r.FormValue("confirm_password")

	creds, err := h.creds.Load()
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(creds.AdminHash), []byte(currentPassword)); err != nil {
		h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "error", Message: "Current password is incorrect"})
		return
	}

	if len(newPassword) < 8 {
		h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "error", Message: "Password must be at least 8 characters"})
		return
	}

	if newPassword != confirmPassword {
		h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "error", Message: "Passwords do not match"})
		return
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(newPassword), 12)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	creds.AdminHash = string(hash)
	if err := h.creds.Save(creds); err != nil {
		http.Error(w, "Failed to save", http.StatusInternalServerError)
		return
	}

	h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "success", Message: "Password updated successfully"})
}

func (h *SettingsHandler) UpdateDB(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	creds, err := h.creds.Load()
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	// Verify current admin password
	currentPassword := r.FormValue("current_password")
	if err := bcrypt.CompareHashAndPassword([]byte(creds.AdminHash), []byte(currentPassword)); err != nil {
		h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "error", Message: "Current password is incorrect"})
		return
	}

	dbHost := strings.TrimSpace(r.FormValue("db_host"))
	dbPort := 5432
	fmt.Sscanf(r.FormValue("db_port"), "%d", &dbPort)
	dbUser := strings.TrimSpace(r.FormValue("db_user"))
	dbPassword := r.FormValue("db_password")
	dbName := strings.TrimSpace(r.FormValue("db_name"))
	dbSSLMode := r.FormValue("db_sslmode")

	// If password not provided, keep existing
	if dbPassword == "" {
		dbPassword = creds.DBPassword
	}

	// Test new connection
	pool, err := db.NewPool(r.Context(), db.ConnConfig{
		Host: dbHost, Port: dbPort, User: dbUser, Password: dbPassword, DBName: dbName, SSLMode: dbSSLMode,
	})
	if err != nil {
		h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "error", Message: fmt.Sprintf("Connection failed: %v", err)})
		return
	}

	creds.DBHost = dbHost
	creds.DBPort = dbPort
	creds.DBUser = dbUser
	creds.DBPassword = dbPassword
	creds.DBName = dbName
	creds.DBSSLMode = dbSSLMode

	if err := h.creds.Save(creds); err != nil {
		pool.Close()
		http.Error(w, "Failed to save", http.StatusInternalServerError)
		return
	}

	h.setPool(pool)
	h.renderer.Partial(w, http.StatusOK, "partials/alert.html", templates.Flash{Type: "success", Message: "Database connection updated successfully"})
}
