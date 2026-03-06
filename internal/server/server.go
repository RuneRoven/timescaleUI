package server

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io/fs"
	"log/slog"
	"math/big"
	"net/http"
	"time"

	"github.com/RuneRoven/timescaleUI/internal/auth"
	"github.com/RuneRoven/timescaleUI/internal/config"
	"github.com/RuneRoven/timescaleUI/internal/handlers"
	"github.com/RuneRoven/timescaleUI/internal/middleware"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/gorilla/securecookie"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Server holds all dependencies for the HTTP server.
type Server struct {
	cfg      *config.Config
	creds    *config.CredentialStore
	pool     *pgxpool.Pool
	renderer *templates.Renderer
	sessions *auth.SessionStore
	mux      *http.ServeMux
	handler  http.Handler
	logger   *slog.Logger
	httpSrv  *http.Server
}

// New creates a new Server with all routes and middleware wired up.
func New(cfg *config.Config, creds *config.CredentialStore, pool *pgxpool.Pool, renderer *templates.Renderer, staticFS fs.FS, logger *slog.Logger) *Server {
	secure := cfg.TLSCert != "" || cfg.TLSAuto

	// Session store: generate random keys
	hashKey := securecookie.GenerateRandomKey(32)
	blockKey := securecookie.GenerateRandomKey(32)
	sessions := auth.NewSessionStore(hashKey, blockKey, cfg.SessionTTL, secure)

	s := &Server{
		cfg:      cfg,
		creds:    creds,
		pool:     pool,
		renderer: renderer,
		sessions: sessions,
		mux:      http.NewServeMux(),
		logger:   logger,
	}

	s.routes(staticFS)
	return s
}

// SetPool updates the database pool (used after setup wizard configures DB).
func (s *Server) SetPool(pool *pgxpool.Pool) {
	s.pool = pool
}

// Pool returns the current database pool.
func (s *Server) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *Server) routes(staticFS fs.FS) {
	// Auth handler
	authH := auth.NewHandler(s.creds, s.sessions, s.renderer, s.logger)
	authH.SetPoolAccessors(
		func() interface{} { return s.pool },
		func(p interface{}) {
			if pool, ok := p.(*pgxpool.Pool); ok {
				s.pool = pool
			}
		},
	)

	// Dashboard handler
	dashH := handlers.NewDashboardHandler(func() *pgxpool.Pool { return s.pool }, s.renderer, s.logger)

	// Rate limiters
	loginRL := middleware.NewRateLimiter(5, 5)   // 5 req/min on login
	apiRL := middleware.NewRateLimiter(60, 20)    // 60 req/min on API

	// CSRF key
	csrfKey := securecookie.GenerateRandomKey(32)
	secure := s.cfg.TLSCert != "" || s.cfg.TLSAuto
	csrfMW := middleware.CSRF(csrfKey, secure)

	// Mux for public routes (no auth required)
	s.mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	s.mux.HandleFunc("GET /health", s.handleHealth)

	// Setup routes (no auth, has CSRF)
	s.mux.HandleFunc("GET /setup", authH.HandleSetupPage)
	s.mux.HandleFunc("POST /setup", authH.HandleSetup)

	// Login routes (no auth, has rate limit)
	s.mux.Handle("GET /login", loginRL.Limit(http.HandlerFunc(authH.HandleLoginPage)))
	s.mux.Handle("POST /login", loginRL.Limit(http.HandlerFunc(authH.HandleLogin)))

	// Logout
	s.mux.HandleFunc("POST /logout", authH.HandleLogout)

	// Pool accessor for handlers
	getPool := func() *pgxpool.Pool { return s.pool }
	setPool := func(p *pgxpool.Pool) { s.pool = p }

	// Feature handlers
	htH := handlers.NewHypertableHandler(getPool, s.renderer, s.logger)
	caH := handlers.NewCAHandler(getPool, s.renderer, s.logger)
	compH := handlers.NewCompressionHandler(getPool, s.renderer, s.logger)
	retH := handlers.NewRetentionHandler(getPool, s.renderer, s.logger)
	queryH := handlers.NewQueryHandler(getPool, s.renderer, s.logger, s.cfg.ReadOnly, s.cfg.QueryRowLimit)
	jobH := handlers.NewJobHandler(getPool, s.renderer, s.logger)
	funcH := handlers.NewFunctionHandler(getPool, s.renderer, s.logger)
	tierH := handlers.NewTieringHandler(getPool, s.renderer, s.logger)
	settH := handlers.NewSettingsHandler(s.creds, getPool, setPool, s.renderer, s.logger)

	// Authenticated routes
	requireAuth := middleware.RequireAuth(s.sessions)

	// Dashboard
	s.mux.Handle("GET /{$}", requireAuth(dashH))

	// Hypertables
	s.mux.Handle("GET /hypertables", requireAuth(http.HandlerFunc(htH.List)))
	s.mux.Handle("GET /hypertables/create", requireAuth(http.HandlerFunc(htH.CreateForm)))
	s.mux.Handle("POST /hypertables/create", requireAuth(http.HandlerFunc(htH.Create)))
	s.mux.Handle("GET /hypertables/columns", requireAuth(http.HandlerFunc(htH.GetColumns)))
	s.mux.Handle("GET /hypertables/{schema}/{table}", requireAuth(http.HandlerFunc(htH.Detail)))
	s.mux.Handle("POST /hypertables/update-chunk-interval", requireAuth(http.HandlerFunc(htH.UpdateChunkInterval)))
	s.mux.Handle("POST /hypertables/add-reorder-policy", requireAuth(http.HandlerFunc(htH.AddReorderPolicy)))
	s.mux.Handle("POST /hypertables/remove-reorder-policy", requireAuth(http.HandlerFunc(htH.RemoveReorderPolicy)))
	s.mux.Handle("POST /hypertables/create-index", requireAuth(http.HandlerFunc(htH.CreateIndex)))
	s.mux.Handle("POST /hypertables/drop-index", requireAuth(http.HandlerFunc(htH.DropIndex)))

	// Continuous Aggregates
	s.mux.Handle("GET /continuous-aggregates", requireAuth(http.HandlerFunc(caH.List)))
	s.mux.Handle("GET /continuous-aggregates/{schema}/{name}", requireAuth(http.HandlerFunc(caH.Detail)))
	s.mux.Handle("POST /continuous-aggregates/create", requireAuth(http.HandlerFunc(caH.Create)))
	s.mux.Handle("POST /continuous-aggregates/refresh", requireAuth(http.HandlerFunc(caH.Refresh)))
	s.mux.Handle("POST /continuous-aggregates/delete", requireAuth(http.HandlerFunc(caH.Delete)))
	s.mux.Handle("POST /continuous-aggregates/toggle-materialized", requireAuth(http.HandlerFunc(caH.ToggleMaterializedOnly)))
	s.mux.Handle("POST /continuous-aggregates/update-definition", requireAuth(http.HandlerFunc(caH.UpdateDefinition)))
	s.mux.Handle("POST /continuous-aggregates/add-policy", requireAuth(http.HandlerFunc(caH.AddPolicy)))
	s.mux.Handle("POST /continuous-aggregates/remove-policy", requireAuth(http.HandlerFunc(caH.RemovePolicy)))

	// Compression
	s.mux.Handle("GET /compression", requireAuth(http.HandlerFunc(compH.List)))
	s.mux.Handle("GET /compression/{schema}/{table}", requireAuth(http.HandlerFunc(compH.Detail)))
	s.mux.Handle("POST /compression/enable", requireAuth(http.HandlerFunc(compH.Enable)))
	s.mux.Handle("POST /compression/disable", requireAuth(http.HandlerFunc(compH.Disable)))
	s.mux.Handle("POST /compression/compress", requireAuth(http.HandlerFunc(compH.CompressNow)))
	s.mux.Handle("POST /compression/update-settings", requireAuth(http.HandlerFunc(compH.UpdateSettings)))
	s.mux.Handle("POST /compression/update-policy", requireAuth(http.HandlerFunc(compH.UpdatePolicy)))

	// Retention
	s.mux.Handle("GET /retention", requireAuth(http.HandlerFunc(retH.List)))
	s.mux.Handle("POST /retention/create", requireAuth(http.HandlerFunc(retH.Create)))
	s.mux.Handle("POST /retention/delete", requireAuth(http.HandlerFunc(retH.Delete)))

	// SQL Runner
	s.mux.Handle("GET /query", requireAuth(http.HandlerFunc(queryH.Page)))
	s.mux.Handle("POST /query/execute", requireAuth(http.HandlerFunc(queryH.Execute)))

	// Jobs
	s.mux.Handle("GET /jobs", requireAuth(http.HandlerFunc(jobH.List)))
	s.mux.Handle("GET /jobs/{id}", requireAuth(http.HandlerFunc(jobH.Detail)))
	s.mux.Handle("POST /jobs/action", requireAuth(http.HandlerFunc(jobH.Action)))
	s.mux.Handle("POST /jobs/update-schedule", requireAuth(http.HandlerFunc(jobH.UpdateSchedule)))
	s.mux.Handle("POST /jobs/update-config", requireAuth(http.HandlerFunc(jobH.UpdateConfig)))

	// Functions & Views
	s.mux.Handle("GET /functions", requireAuth(http.HandlerFunc(funcH.List)))
	s.mux.Handle("GET /functions/view/{schema}/{name}", requireAuth(http.HandlerFunc(funcH.ViewDetail)))
	s.mux.Handle("POST /functions/update-view-definition", requireAuth(http.HandlerFunc(funcH.UpdateViewDefinition)))
	s.mux.Handle("POST /functions/create-matview", requireAuth(http.HandlerFunc(funcH.CreateMatView)))
	s.mux.Handle("POST /functions/refresh-matview", requireAuth(http.HandlerFunc(funcH.RefreshMatView)))
	s.mux.Handle("POST /functions/drop-view", requireAuth(http.HandlerFunc(funcH.DropView)))

	// Tiering
	s.mux.Handle("GET /tiering", requireAuth(http.HandlerFunc(tierH.Page)))

	// Settings
	s.mux.Handle("GET /settings", requireAuth(http.HandlerFunc(settH.Page)))
	s.mux.Handle("POST /settings/password", requireAuth(http.HandlerFunc(settH.UpdatePassword)))
	s.mux.Handle("POST /settings/database", requireAuth(http.HandlerFunc(settH.UpdateDB)))

	// Build handler chain: logging -> security headers -> CSRF -> rate limit -> mux
	var h http.Handler = s.mux
	h = csrfMW(h)
	h = apiRL.Limit(h)
	h = s.securityHeaders(h)
	h = middleware.Logging(s.logger)(h)
	s.handler = h
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if s.pool != nil {
		if err := s.pool.Ping(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, `{"status":"unhealthy","error":%q}`, err.Error())
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, `{"status":"ok"}`)
}

// Handler returns the root handler with the full middleware chain.
func (s *Server) Handler() http.Handler {
	return s.handler
}

// Mux returns the underlying ServeMux for registering additional routes.
func (s *Server) Mux() *http.ServeMux {
	return s.mux
}

// Sessions returns the session store.
func (s *Server) Sessions() *auth.SessionStore {
	return s.sessions
}

func (s *Server) securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		next.ServeHTTP(w, r)
	})
}

// ListenAndServe starts the HTTP(S) server.
func (s *Server) ListenAndServe() error {
	s.httpSrv = &http.Server{
		Addr:         s.cfg.ListenAddr,
		Handler:      s.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	if s.cfg.TLSCert != "" && s.cfg.TLSKey != "" {
		s.logger.Info("starting HTTPS server", "addr", s.cfg.ListenAddr)
		return s.httpSrv.ListenAndServeTLS(s.cfg.TLSCert, s.cfg.TLSKey)
	}

	if s.cfg.TLSAuto {
		tlsCfg, err := selfSignedTLS()
		if err != nil {
			return fmt.Errorf("generate self-signed TLS: %w", err)
		}
		s.httpSrv.TLSConfig = tlsCfg
		s.logger.Info("starting HTTPS server (auto-TLS)", "addr", s.cfg.ListenAddr)
		return s.httpSrv.ListenAndServeTLS("", "")
	}

	s.logger.Info("starting HTTP server", "addr", s.cfg.ListenAddr)
	return s.httpSrv.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpSrv == nil {
		return nil
	}
	return s.httpSrv.Shutdown(ctx)
}

func selfSignedTLS() (*tls.Config, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"TimescaleUI"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}, nil
}
