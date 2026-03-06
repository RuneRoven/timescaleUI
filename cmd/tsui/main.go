package main

import (
	"context"
	"io/fs"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/RuneRoven/timescaleUI/internal/config"
	"github.com/RuneRoven/timescaleUI/internal/db"
	"github.com/RuneRoven/timescaleUI/internal/server"
	"github.com/RuneRoven/timescaleUI/internal/templates"
	"github.com/RuneRoven/timescaleUI/web"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	credStore, err := config.NewCredentialStore(cfg.DataDir, cfg.EncryptionKey)
	if err != nil {
		return err
	}

	renderer, err := templates.New(web.FS, logger)
	if err != nil {
		return err
	}

	staticFS, err := fs.Sub(web.FS, "static")
	if err != nil {
		return err
	}

	srv := server.New(cfg, credStore, nil, renderer, staticFS, logger)

	// If credentials exist, connect to DB on startup
	if credStore.Exists() {
		creds, err := credStore.Load()
		if err != nil {
			logger.Warn("failed to load credentials, starting in setup mode", "error", err)
		} else {
			pool, err := db.NewPool(ctx, db.ConnConfig{
				Host:     creds.DBHost,
				Port:     creds.DBPort,
				User:     creds.DBUser,
				Password: creds.DBPassword,
				DBName:   creds.DBName,
				SSLMode:  creds.DBSSLMode,
			})
			if err != nil {
				logger.Warn("failed to connect to database, starting in setup mode", "error", err)
			} else {
				srv.SetPool(pool)
				defer pool.Close()
			}
		}
	} else {
		logger.Info("no credentials found, starting in setup mode")
	}

	// Start server in goroutine
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()

	// Wait for shutdown signal or server error
	select {
	case <-ctx.Done():
		logger.Info("shutting down gracefully")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return srv.Shutdown(shutCtx)
	case err := <-errCh:
		return err
	}
}
