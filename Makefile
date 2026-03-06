.PHONY: build run dev docker docker-up docker-down clean tailwind

APP_NAME := tsui
GO_FILES := $(shell find . -name '*.go' -not -path './vendor/*')

# Build the binary with embedded assets
build: tailwind
	go build -ldflags="-s -w" -o bin/$(APP_NAME) ./cmd/tsui

# Run locally (requires Tailwind CSS to be built)
run: build
	TSUI_DATA_DIR=./data TSUI_LISTEN_ADDR=:8080 ./bin/$(APP_NAME)

# Dev mode: build and run with auto-reload friendly settings
dev: tailwind
	TSUI_DATA_DIR=./data TSUI_LISTEN_ADDR=:8080 go run ./cmd/tsui

# Build Tailwind CSS
tailwind:
	@which npx > /dev/null 2>&1 && npx tailwindcss -i web/static/css/tailwind-input.css -o web/static/css/app.css --minify || echo "npx not found, skipping tailwind build"

# Build Docker image
docker:
	docker build -t timescaleui .

# Start with docker compose
docker-up:
	docker compose up -d

# Stop docker compose
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf bin/
	rm -f web/static/css/app.css

# Tidy Go modules
tidy:
	go mod tidy

# Vet and check
check:
	go vet ./...
	go build ./...
