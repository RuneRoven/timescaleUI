#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="timescaleui"
IMAGE_TAG="${1:-latest}"

echo "Building ${IMAGE_NAME}:${IMAGE_TAG}..."
docker build -t "${IMAGE_NAME}:${IMAGE_TAG}" .
echo "Done. Image: ${IMAGE_NAME}:${IMAGE_TAG}"
echo ""
echo "Example docker-compose.yml:"
echo ""
cat <<'YAML'
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    environment:
      POSTGRES_PASSWORD: password
    volumes:
      - ./tsdb-data:/var/lib/postgresql/data
    networks:
      - tsui-internal
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 5s
      retries: 5

  tsui:
YAML
echo "    image: ${IMAGE_NAME}:${IMAGE_TAG}"
cat <<'YAML'
    ports:
      - "8080:8080"
    environment:
      TSUI_LISTEN_ADDR: ":8080"
      TSUI_DATA_DIR: "/data"
    volumes:
      - ./tsui-data:/data
    networks:
      - tsui-internal
    depends_on:
      timescaledb:
        condition: service_healthy

networks:
  tsui-internal:
    driver: bridge
YAML
