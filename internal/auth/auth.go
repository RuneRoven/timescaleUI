package auth

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/RuneRoven/timescaleUI/internal/config"
	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"golang.org/x/crypto/bcrypt"
)

// Handler manages authentication routes.
type Handler struct {
	creds    *config.CredentialStore
	sessions *SessionStore
	renderer *templates.Renderer
	logger   *slog.Logger
	getPool  func() interface{} // returns *pgxpool.Pool or nil
	setPool  func(interface{})  // sets pool on server
}

// NewHandler creates auth route handlers.
func NewHandler(creds *config.CredentialStore, sessions *SessionStore, renderer *templates.Renderer, logger *slog.Logger) *Handler {
	return &Handler{
		creds:    creds,
		sessions: sessions,
		renderer: renderer,
		logger:   logger,
	}
}

// SetPoolAccessors sets callbacks for getting/setting the DB pool.
func (h *Handler) SetPoolAccessors(get func() interface{}, set func(interface{})) {
	h.getPool = get
	h.setPool = set
}

// HandleSetupPage shows the setup wizard.
func (h *Handler) HandleSetupPage(w http.ResponseWriter, r *http.Request) {
	if h.creds.Exists() {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/setup.html", templates.PageData{
		Title:     "Setup",
		CSRFToken: csrfToken(r),
	})
}

// HandleSetup processes the setup wizard form.
func (h *Handler) HandleSetup(w http.ResponseWriter, r *http.Request) {
	if h.creds.Exists() {
		http.Redirect(w, r, "/login", http.StatusSeeOther)
		return
	}

	if err := r.ParseForm(); err != nil {
		h.renderer.Page(w, http.StatusBadRequest, "pages/setup.html", templates.PageData{
			Title: "Setup", CSRFToken: csrfToken(r),
		})
		return
	}

	adminUser := strings.TrimSpace(r.FormValue("admin_user"))
	adminPass := r.FormValue("admin_password")
	adminPassConfirm := r.FormValue("admin_password_confirm")
	dbHost := strings.TrimSpace(r.FormValue("db_host"))
	dbPort := r.FormValue("db_port")
	dbUser := strings.TrimSpace(r.FormValue("db_user"))
	dbPassword := r.FormValue("db_password")
	dbName := strings.TrimSpace(r.FormValue("db_name"))
	dbSSLMode := r.FormValue("db_sslmode")

	if adminUser == "" || adminPass == "" || dbHost == "" || dbUser == "" || dbName == "" {
		h.renderSetupError(w, r, "All fields are required")
		return
	}
	if len(adminPass) < 8 {
		h.renderSetupError(w, r, "Password must be at least 8 characters")
		return
	}
	if adminPass != adminPassConfirm {
		h.renderSetupError(w, r, "Passwords do not match")
		return
	}

	port := 5432
	if dbPort != "" {
		fmt.Sscanf(dbPort, "%d", &port)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(adminPass), 12)
	if err != nil {
		h.logger.Error("bcrypt hash failed", "error", err)
		h.renderSetupError(w, r, "Internal error, please try again")
		return
	}

	creds := &config.Credentials{
		AdminUser:  adminUser,
		AdminHash:  string(hash),
		DBHost:     dbHost,
		DBPort:     port,
		DBUser:     dbUser,
		DBPassword: dbPassword,
		DBName:     dbName,
		DBSSLMode:  dbSSLMode,
	}

	// Test DB connection
	pool, err := db.NewPool(r.Context(), db.ConnConfig{
		Host:     dbHost,
		Port:     port,
		User:     dbUser,
		Password: dbPassword,
		DBName:   dbName,
		SSLMode:  dbSSLMode,
	})
	if err != nil {
		h.renderSetupError(w, r, fmt.Sprintf("Database connection failed: %v", err))
		return
	}

	if err := h.creds.Save(creds); err != nil {
		pool.Close()
		h.logger.Error("save credentials failed", "error", err)
		h.renderSetupError(w, r, "Failed to save credentials")
		return
	}

	// Set pool on server
	if h.setPool != nil {
		h.setPool(pool)
	}

	h.logger.Info("setup completed", "admin_user", adminUser, "db_host", dbHost)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

// HandleLoginPage shows the login form.
func (h *Handler) HandleLoginPage(w http.ResponseWriter, r *http.Request) {
	if !h.creds.Exists() {
		http.Redirect(w, r, "/setup", http.StatusSeeOther)
		return
	}
	h.renderer.Page(w, http.StatusOK, "pages/login.html", templates.PageData{
		Title:     "Login",
		CSRFToken: csrfToken(r),
	})
}

// HandleLogin processes login form submission.
func (h *Handler) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")

	creds, err := h.creds.Load()
	if err != nil {
		h.logger.Error("load credentials for login", "error", err)
		h.renderLoginError(w, r, "Internal error")
		return
	}

	if username != creds.AdminUser {
		h.renderLoginError(w, r, "Invalid username or password")
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(creds.AdminHash), []byte(password)); err != nil {
		h.renderLoginError(w, r, "Invalid username or password")
		return
	}

	if err := h.sessions.Create(w, username); err != nil {
		h.logger.Error("create session", "error", err)
		h.renderLoginError(w, r, "Internal error")
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// HandleLogout destroys the session.
func (h *Handler) HandleLogout(w http.ResponseWriter, r *http.Request) {
	h.sessions.Destroy(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (h *Handler) renderSetupError(w http.ResponseWriter, r *http.Request, msg string) {
	h.renderer.Page(w, http.StatusUnprocessableEntity, "pages/setup.html", templates.PageData{
		Title:     "Setup",
		CSRFToken: csrfToken(r),
		Flashes:   []templates.Flash{{Type: "error", Message: msg}},
	})
}

func (h *Handler) renderLoginError(w http.ResponseWriter, r *http.Request, msg string) {
	h.renderer.Page(w, http.StatusUnprocessableEntity, "pages/login.html", templates.PageData{
		Title:     "Login",
		CSRFToken: csrfToken(r),
		Content:   msg,
	})
}

func csrfToken(r *http.Request) string {
	if tok, ok := r.Context().Value("csrf_token").(string); ok {
		return tok
	}
	return ""
}
