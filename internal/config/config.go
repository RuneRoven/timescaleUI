package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration parsed from environment variables.
type Config struct {
	ListenAddr    string
	DataDir       string
	EncryptionKey string // hex-encoded 32-byte key, empty = auto-generate
	TLSCert       string
	TLSKey        string
	TLSAuto       bool
	SessionTTL    time.Duration
	ReadOnly      bool
	QueryRowLimit int
}

func Load() (*Config, error) {
	cfg := &Config{
		ListenAddr:    envOr("TSUI_LISTEN_ADDR", ":8080"),
		DataDir:       envOr("TSUI_DATA_DIR", "/data"),
		EncryptionKey: os.Getenv("TSUI_ENCRYPTION_KEY"),
		TLSCert:       os.Getenv("TSUI_TLS_CERT"),
		TLSKey:        os.Getenv("TSUI_TLS_KEY"),
		ReadOnly:      envBool("TSUI_READ_ONLY"),
	}

	ttl, err := time.ParseDuration(envOr("TSUI_SESSION_TTL", "8h"))
	if err != nil {
		return nil, fmt.Errorf("invalid TSUI_SESSION_TTL: %w", err)
	}
	cfg.SessionTTL = ttl

	cfg.TLSAuto = envBool("TSUI_TLS_AUTO")

	rowLimit, err := strconv.Atoi(envOr("TSUI_QUERY_ROW_LIMIT", "1000"))
	if err != nil {
		return nil, fmt.Errorf("invalid TSUI_QUERY_ROW_LIMIT: %w", err)
	}
	cfg.QueryRowLimit = rowLimit

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("create data dir %s: %w", cfg.DataDir, err)
	}

	return cfg, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envBool(key string) bool {
	v := os.Getenv(key)
	return v == "true" || v == "1" || v == "yes"
}
